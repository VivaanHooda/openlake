use anyhow::{Context, Result};
use clap::Args as ClapArgs;
use compio::net::TcpStream;
use futures::stream::StreamExt as _;
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use openlake_server::config::Config;
use openlake_storage::NodeAddr;

#[derive(ClapArgs)]
pub struct TopologyArgs {
    /// openlake.toml. The same file openlaked reads.
    #[arg(long)]
    pub config: PathBuf,

    /// Probe each node's RPC listener and annotate the layout with live state.
    #[arg(long)]
    pub probe: bool,

    /// Per node probe timeout in seconds. Requires --probe. [default: 2]
    #[arg(long, requires = "probe")]
    pub probe_timeout_secs: Option<u64>,
}

/// Default per node probe timeout, used when --probe-timeout-secs is omitted.
const DEFAULT_PROBE_TIMEOUT_SECS: u64 = 2;

/// Cap on probes in flight at once, so a large cluster cannot open an
/// unbounded number of sockets simultaneously.
const MAX_CONCURRENT_PROBES: usize = 64;

pub async fn run(args: TopologyArgs) -> Result<()> {
    let text = std::fs::read_to_string(&args.config)
        .with_context(|| format!("read {}", args.config.display()))?;
    let cfg: Config =
        toml::from_str(&text).with_context(|| format!("parse {}", args.config.display()))?;

    println!("openlake cluster topology: {}", args.config.display());
    println!();

    // Probing is opt-in: by default `topology` reports the declared layout
    // without touching the network. With --probe it also reports liveness.
    let liveness = if args.probe {
        let secs = args
            .probe_timeout_secs
            .unwrap_or(DEFAULT_PROBE_TIMEOUT_SECS);
        Some(probe(&cfg.nodes, Duration::from_secs(secs)).await)
    } else {
        None
    };

    let (report, warnings) = render(&cfg.nodes, liveness.as_ref());
    print!("{report}");

    for w in warnings {
        eprintln!("{w}");
    }
    Ok(())
}

/// Connect to every node's RPC listener, mapping `rpc_addr -> reachable`.
///
/// A node counts as up when the TCP connect succeeds within `timeout`; any
/// timeout or connection error counts as down. Probes run concurrently with a
/// bounded fan-out of `MAX_CONCURRENT_PROBES`, so a large cluster stays close
/// to one `timeout` of wall time without opening every socket at once.
async fn probe(nodes: &[NodeAddr], timeout: Duration) -> BTreeMap<SocketAddr, bool> {
    futures::stream::iter(nodes.iter().map(|n| {
        let addr = n.rpc_addr;
        async move {
            let ok = matches!(
                compio::time::timeout(timeout, TcpStream::connect(addr)).await,
                Ok(Ok(_))
            );
            (addr, ok)
        }
    }))
    .buffer_unordered(MAX_CONCURRENT_PROBES)
    .collect()
    .await
}

/// Render the declared cluster layout, sorted by node id.
///
/// When `liveness` is `Some`, a `state` column and an alive count are added
/// from the probe results keyed by `rpc_addr`; when `None`, the layout is
/// reported exactly as declared, without any network state.
fn render(
    nodes: &[NodeAddr],
    liveness: Option<&BTreeMap<SocketAddr, bool>>,
) -> (String, Vec<String>) {
    if nodes.is_empty() {
        return (
            "config declares zero nodes, nothing to lay out.\n".to_string(),
            Vec::new(),
        );
    }

    let mut sorted: Vec<&NodeAddr> = nodes.iter().collect();
    sorted.sort_unstable_by_key(|n| n.id);

    let mut out = String::new();
    if liveness.is_some() {
        out.push_str("  node    disks    state    rpc address\n");
        out.push_str("  ----    -----    -----    -----------\n");
    } else {
        out.push_str("  node    disks    rpc address\n");
        out.push_str("  ----    -----    -----------\n");
    }
    for n in &sorted {
        match liveness {
            Some(map) => {
                let state = match map.get(&n.rpc_addr) {
                    Some(true) => "up",
                    Some(false) => "DOWN",
                    None => "?",
                };
                let _ = writeln!(
                    out,
                    "  {:>4}    {:>5}    {:<5}    {}",
                    n.id, n.disk_count, state, n.rpc_addr
                );
            }
            None => {
                let _ = writeln!(
                    out,
                    "  {:>4}    {:>5}    {}",
                    n.id, n.disk_count, n.rpc_addr
                );
            }
        }
    }
    out.push('\n');

    let count = sorted.len();
    let total_disks: u32 = sorted.iter().map(|n| n.disk_count as u32).sum();
    let _ = writeln!(
        out,
        "{} node{} configured, {} disk{} total.",
        count,
        if count == 1 { "" } else { "s" },
        total_disks,
        if total_disks == 1 { "" } else { "s" },
    );

    if let Some(map) = liveness {
        let alive = sorted
            .iter()
            .filter(|n| map.get(&n.rpc_addr) == Some(&true))
            .count();
        let _ = writeln!(
            out,
            "{} / {} node{} alive.",
            alive,
            count,
            if count == 1 { "" } else { "s" }
        );
    }

    let mut dup_ids: Vec<u16> = sorted
        .windows(2)
        .filter(|w| w[0].id == w[1].id)
        .map(|w| w[0].id)
        .collect();
    dup_ids.dedup();
    let warnings = dup_ids
        .into_iter()
        .map(|id| format!("warning: node id {id} declared more than once."))
        .collect();

    (out, warnings)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: u16, addr: &str, disk_count: u16) -> NodeAddr {
        NodeAddr {
            id,
            rpc_addr: addr.parse::<SocketAddr>().unwrap(),
            disk_count,
        }
    }

    #[test]
    fn empty_config_reports_no_nodes() {
        let (report, warnings) = render(&[], None);
        assert!(report.contains("zero nodes"));
        assert!(warnings.is_empty());
    }

    #[test]
    fn nodes_are_sorted_by_id() {
        let nodes = vec![
            node(2, "127.0.0.1:9002", 1),
            node(0, "127.0.0.1:9000", 1),
            node(1, "127.0.0.1:9001", 1),
        ];
        let (report, warnings) = render(&nodes, None);
        let p0 = report.find("127.0.0.1:9000").unwrap();
        let p1 = report.find("127.0.0.1:9001").unwrap();
        let p2 = report.find("127.0.0.1:9002").unwrap();
        assert!(p0 < p1 && p1 < p2, "nodes should be ordered by id");
        assert!(report.contains("3 nodes configured"));
        assert!(warnings.is_empty());
    }

    #[test]
    fn single_node_uses_singular() {
        let (report, _) = render(&[node(0, "127.0.0.1:9000", 1)], None);
        assert!(report.contains("1 node configured"));
        assert!(report.contains("1 disk total"));
    }

    #[test]
    fn duplicate_ids_are_flagged() {
        let nodes = vec![node(0, "127.0.0.1:9000", 1), node(0, "10.0.0.1:9000", 2)];
        let (_, warnings) = render(&nodes, None);
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("node id 0 declared more than once."));
    }

    #[test]
    fn disk_count_appears_in_render() {
        let (report, _) = render(
            &[node(0, "127.0.0.1:9000", 4), node(1, "127.0.0.1:9001", 4)],
            None,
        );
        assert!(report.contains("8 disks total"));
    }

    #[test]
    fn default_layout_has_no_state_column() {
        let (report, _) = render(&[node(0, "127.0.0.1:9000", 1)], None);
        assert!(!report.contains("state"));
        assert!(!report.contains("alive"));
    }

    #[test]
    fn probed_layout_shows_state_and_alive_count() {
        let nodes = vec![node(0, "127.0.0.1:9000", 1), node(1, "127.0.0.1:9001", 1)];
        let liveness = BTreeMap::from([
            ("127.0.0.1:9000".parse::<SocketAddr>().unwrap(), true),
            ("127.0.0.1:9001".parse::<SocketAddr>().unwrap(), false),
        ]);
        let (report, _) = render(&nodes, Some(&liveness));
        assert!(report.contains("state"));
        assert!(report.contains("up"));
        assert!(report.contains("DOWN"));
        assert!(report.contains("1 / 2 nodes alive."));
    }
}
