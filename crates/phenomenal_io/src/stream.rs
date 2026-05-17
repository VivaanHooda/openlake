//! Owned-buffer byte streams for the data plane.
//!
//! The data path crosses S3 frontend → engine → backend → wire and must
//! never materialise an object end to end. `ByteStream` and `ByteSink`
//! are the dyn-compatible reader/writer pair every layer hands off to
//! the next; bytes flow through one stripe at a time and never live in
//! a per-object buffer.
//!
//! Why a custom trait rather than `compio::io::AsyncRead`/`AsyncWrite`:
//! compio's traits are generic over the buffer (`B: IoBuf` / `IoBufMut`)
//! so the kernel can pin it for io_uring/kqueue, which makes them not
//! object-safe. We take ownership of a `Vec<u8>` *inside* the impl (so
//! compio's submit-and-recycle model is preserved) but expose a
//! `&mut [u8]` / `&[u8]` surface to the caller so the engine can hold
//! `Box<dyn ByteStream>` / `Box<dyn ByteSink>` heterogeneously.

use async_trait::async_trait;
use bytes::Bytes;
use compio::buf::{BufResult, IntoInner, IoBuf};
use compio::io::AsyncRead;

use crate::alloc::PooledBuffer;
use crate::error::{IoError, IoResult};
use crate::tuning::STREAM_CHUNK_BYTES;

/// Owned-buffer reader. Yields refcounted `Bytes` chunks — the
/// underlying allocation lives in either an axum/hyper `BytesMut`
/// (network sources, refcount-only slice) or a pool-backed
/// `PooledBuffer` frozen via `PooledBuffer::freeze()` (file/socket
/// sources, kernel-written then refcount-handoff).
///
/// Returning `Bytes` instead of forcing a fixed buffer type lets
/// every layer downstream (etag hash, xl.meta tail, writev iovec,
/// HTTP response frame) consume the bytes by reference — no
/// userspace memcpy at the trait boundary. Adapters from
/// `axum::body::Body`, `compio::AsyncRead`, etc. all hand back the
/// allocation they already own.
///
/// Cancellation safety: pool-backed impls hold the `PooledBuffer`
/// across the `await` and only `freeze()` after the io_uring SQE has
/// completed (or been canceled by `OpFuture::drop`). The kernel is
/// provably done with the memory by the time `Bytes` ownership is
/// taken.
#[async_trait(?Send)]
pub trait ByteStream {
    /// Returns the next chunk of bytes. `Ok(bytes)` with `len > 0` is
    /// data; `Ok(empty)` is EOF; `Err(_)` is an I/O failure. The
    /// returned `Bytes` may be 1 byte or up to several MiB — the
    /// caller does not control the size, only consumes the chunk.
    async fn read(&mut self) -> IoResult<Bytes>;
}

/// Owned-buffer writer. Caller hands ownership of a `Bytes` (which
/// may wrap a `PooledBuffer` via `freeze()`, an axum frame, or any
/// owned byte allocation); impl writes the whole buffer to its
/// destination. The `Bytes` Drop (refcount → 0) recycles the
/// underlying allocation back to whichever pool/owner created it.
#[async_trait(?Send)]
pub trait ByteSink {
    /// Write `buf` in full. The impl owns the bytes for the duration
    /// of the call; on completion the `Bytes` refcount is dropped
    /// (returning the allocation to its pool, if pool-backed).
    async fn write_all(&mut self, buf: Bytes) -> IoResult<()>;

    /// Flush any buffered bytes and finalise. Implementations that
    /// front a remote RPC use this to read the peer's status frame;
    /// implementations that front a local file use this to `fsync`
    /// and close. After `finish` returns no more writes are accepted.
    async fn finish(&mut self) -> IoResult<()>;
}

/// Read up to `dst.len()` bytes into `dst` by pulling chunks from `s`
/// until either `dst` is full or `s` hits EOF. Returns the count
/// filled. Used by the CLI/test paths that own a `Vec<u8>` and want
/// it populated; the per-chunk `copy_from_slice` here is a deliberate
/// trade — `dst` is `&mut [u8]`, so we have to write *somewhere* the
/// caller chose. Hot paths should consume `Bytes` from the trait
/// directly and skip this helper.
pub async fn read_full(s: &mut dyn ByteStream, dst: &mut [u8]) -> IoResult<usize> {
    let want = dst.len();
    if want == 0 {
        return Ok(0);
    }
    let mut filled = 0;
    while filled < want {
        let chunk = s.read().await?;
        if chunk.is_empty() {
            return Ok(filled);
        }
        let take = (want - filled).min(chunk.len());
        dst[filled..filled + take].copy_from_slice(&chunk[..take]);
        filled += take;
    }
    Ok(filled)
}

/// Pump exactly `size` bytes from a `ByteStream` source into a
/// `ByteSink` destination. Returns `UnexpectedEof` on a short source.
///
/// Each `src.read()` yields a `Bytes` (refcount-only) that we hand
/// straight to `dst.write_all` — **zero memcpy** at the trait
/// boundary. The chunk size is decided by the source (one HTTP
/// frame, one TCP recv, one disk read, etc.).
pub async fn pump_n(
    src: &mut dyn ByteStream,
    dst: &mut dyn ByteSink,
    size: u64,
) -> IoResult<()> {
    let mut moved = 0u64;
    while moved < size {
        let chunk = src.read().await?;
        if chunk.is_empty() {
            return Err(IoError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!("pump_n: source ended at {moved}/{size}"),
            )));
        }
        let n = chunk.len() as u64;
        // Trim if the source over-delivered past `size` (rare, but
        // possible for sources whose chunks aren't bounded by size).
        let chunk = if moved + n > size {
            // bytes::Bytes::slice (zero copy refcount)
            bytes::Bytes::slice(&chunk, ..(size - moved) as usize)
        } else {
            chunk
        };
        let chunk_len = chunk.len() as u64;
        dst.write_all(chunk).await?;
        moved += chunk_len;
    }
    Ok(())
}

/// Pump exactly `size` bytes from a compio `AsyncRead` source into a
/// `ByteSink` destination, using a 1 MiB pool-backed transfer buffer.
/// Returns `UnexpectedEof` on a short source.
///
/// Compared to [`pump_n`], this function avoids the source-side
/// trait-adapter memcpy by reading directly from compio's
/// owned-buffer API into the same `PooledBuffer` we hand to
/// `dst.write_all`. There is no `LimitedCompioReader`-style
/// intermediate scratch.
///
/// The kernel writes directly into `buf.slice(0..want)` via
/// `compio::AsyncRead::read`'s slice machinery, bounded to exactly
/// `want` bytes per iteration — so we don't need a separate
/// "remaining bytes" counter on a wrapper type. The loop's
/// `moved < size` test is the only cap.
pub async fn pump_compio_to_sink<R: AsyncRead + Unpin>(
    src:  &mut R,
    dst:  &mut dyn ByteSink,
    size: u64,
) -> IoResult<()> {
    let mut moved = 0u64;
    while moved < size {
        let want = (size - moved).min(STREAM_CHUNK_BYTES as u64) as usize; // 4 MiB cap
        // Fresh PooledBuffer per iteration: we hand ownership to
        // the sink as a `Bytes` (via `freeze()`), so we can't reuse
        // the same allocation for the next read. The pool recycles
        // it once the sink's `Bytes` is dropped.
        let buf = PooledBuffer::with_capacity(want);
        let slice = buf.slice(0..want);
        let BufResult(res, slice_back) = src.read(slice).await;
        let mut buf = slice_back.into_inner();
        let n = res.map_err(IoError::Io)?;
        if n == 0 {
            return Err(IoError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!("pump_compio_to_sink: source ended at {moved}/{size}"),
            )));
        }
        buf.truncate(n);
        // freeze() hands the kernel-filled allocation to a `Bytes`
        // owner without copy. The pool recycles when the Bytes
        // refcount drops to 0 inside the sink.
        dst.write_all(buf.freeze()).await?;
        moved += n as u64;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// In-memory implementations: useful for tests and for the inline (≤128 KiB)
// payload path where the engine still hands a single buffer to xl.meta. The
// streaming surface is uniform so callers don't branch between inline and
// EC paths.
// ---------------------------------------------------------------------------

/// Adapter that exposes a fixed `Vec<u8>` as a `ByteStream`. The
/// `Vec` is wrapped as `Bytes` once at construction (zero copy) and
/// each `read()` returns a refcounted slice — no userspace memcpy.
pub struct VecByteStream {
    buf: Bytes,
}

impl VecByteStream {
    pub fn new(buf: Vec<u8>) -> Self {
        Self { buf: Bytes::from(buf) }
    }
}

#[async_trait(?Send)]
impl ByteStream for VecByteStream {
    async fn read(&mut self) -> IoResult<Bytes> {
        // Yield the whole remaining buffer in one shot (zero copy).
        // Subsequent calls return empty (EOF).
        Ok(std::mem::take(&mut self.buf))
    }
}

/// Adapter that exposes a `bytes::Bytes` as a `ByteStream`. Used by
/// the engine's inline GET path when the payload is a single span
/// (read off xl.meta as one zero-copy slice).
pub struct BytesByteStream {
    buf: bytes::Bytes,
}

impl BytesByteStream {
    pub fn new(buf: bytes::Bytes) -> Self {
        Self { buf }
    }
}

#[async_trait(?Send)]
impl ByteStream for BytesByteStream {
    async fn read(&mut self) -> IoResult<Bytes> {
        Ok(std::mem::take(&mut self.buf))
    }
}

/// Adapter that exposes a `Vec<Bytes>` rope as a `ByteStream`,
/// yielding each frame in order with zero copy. Used by the inline
/// GET path: `fi.data` is already a refcounted rope (one frame per
/// HTTP frame of the original PUT, or one frame from a disk read);
/// we just hand them out.
pub struct RopeByteStream {
    frames: std::collections::VecDeque<bytes::Bytes>,
}

impl RopeByteStream {
    pub fn new(frames: Vec<bytes::Bytes>) -> Self {
        Self { frames: frames.into() }
    }
}

#[async_trait(?Send)]
impl ByteStream for RopeByteStream {
    async fn read(&mut self) -> IoResult<Bytes> {
        Ok(self.frames.pop_front().unwrap_or_default())
    }
}

/// Wraps a `ByteStream` to expose a contiguous byte window: drops the
/// first `skip` bytes from the inner stream and yields at most `take`
/// bytes after that, then signals EOF. Used by the engine's range GET
/// path so it can compose on top of the existing full-object walker
/// (inline body or EC stripe walker) without introducing a parallel
/// reader. All slicing is zero copy via `bytes::Bytes::slice`.
///
/// Boundary safe across arbitrary inner chunk sizes: a chunk that
/// straddles the skip/take transition is sliced once; chunks fully
/// inside the skip window are dropped without copying; chunks past
/// the take window are never requested.
pub struct SkipTakeStream {
    inner:     Box<dyn ByteStream>,
    to_skip:   u64,
    remaining: u64,
}

impl SkipTakeStream {
    pub fn new(inner: Box<dyn ByteStream>, skip: u64, take: u64) -> Self {
        Self { inner, to_skip: skip, remaining: take }
    }
}

#[async_trait(?Send)]
impl ByteStream for SkipTakeStream {
    async fn read(&mut self) -> IoResult<Bytes> {
        loop {
            if self.remaining == 0 {
                return Ok(Bytes::new());
            }
            let chunk = self.inner.read().await?;
            if chunk.is_empty() {
                // Upstream EOF before we filled the window. The caller
                // validated bounds against `info.size` before opening
                // us, so this only fires on a truncated source (which
                // is itself an integrity problem one layer down).
                return Ok(Bytes::new());
            }
            if self.to_skip >= chunk.len() as u64 {
                self.to_skip -= chunk.len() as u64;
                continue;
            }
            let drop = self.to_skip as usize;
            self.to_skip = 0;
            // UFCS on Bytes::slice — the bare `chunk.slice(..)` form
            // would dispatch to `compio::buf::IoBuf::slice` (imported
            // at the top of the file for the pump loops below) and
            // return `compio::buf::Slice<Bytes>` instead of `Bytes`.
            let kept = if drop == 0 { chunk } else { bytes::Bytes::slice(&chunk, drop..) };
            let take = (self.remaining as usize).min(kept.len());
            self.remaining -= take as u64;
            return Ok(if take == kept.len() { kept } else { bytes::Bytes::slice(&kept, ..take) });
        }
    }
}

/// Sink that accumulates writes into a `Vec<u8>`. Used as the inline
/// staging buffer for ≤128 KiB EC reconstructions and in tests.
#[derive(Default)]
pub struct VecByteSink {
    pub buf: Vec<u8>,
}

impl VecByteSink {
    pub fn new() -> Self {
        Self { buf: Vec::new() }
    }
    pub fn with_capacity(cap: usize) -> Self {
        Self { buf: Vec::with_capacity(cap) }
    }
    pub fn into_inner(self) -> Vec<u8> {
        self.buf
    }
}

#[async_trait(?Send)]
impl ByteSink for VecByteSink {
    async fn write_all(&mut self, buf: Bytes) -> IoResult<()> {
        // VecByteSink is the test/inline-staging Vec<u8> sink; the
        // memcpy here is unavoidable because the destination is a
        // `Vec<u8>` chosen by the test harness, not a writev iovec
        // or pool slot.
        self.buf.extend_from_slice(&buf[..]);
        Ok(())
    }
    async fn finish(&mut self) -> IoResult<()> {
        Ok(())
    }
}

// Compio source/sink adapters used to live here (`LimitedCompioReader`
// and `CompioWriter`) — both have been removed. They were trait
// adapters that bridged compio's owned-buffer `AsyncRead`/`AsyncWrite`
// to our object-safe `ByteStream`/`ByteSink` surface, but each adapter
// pass added one extra memcpy at the trait boundary. The single
// production caller (`rpc_server::handle_create_file_stream`) now uses
// `pump_compio_to_sink` to read directly from the compio TCP/TLS
// stream into the same pool-backed buffer it hands to the sink, with
// no intermediate scratch. `CompioWriter` was unused. If we ever need
// a "bound an `AsyncRead` to N bytes and expose as `ByteStream`"
// helper again, see git history for the implementation.

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;

    /// Test source that yields the supplied chunks in order, then EOF.
    /// Each chunk is owned, so the SkipTakeStream slicing exercises
    /// real refcount semantics on `bytes::Bytes`.
    struct ChunkSource(VecDeque<Bytes>);

    #[async_trait(?Send)]
    impl ByteStream for ChunkSource {
        async fn read(&mut self) -> IoResult<Bytes> {
            Ok(self.0.pop_front().unwrap_or_default())
        }
    }

    fn chunks(parts: &[&[u8]]) -> Box<dyn ByteStream> {
        Box::new(ChunkSource(parts.iter().map(|b| Bytes::copy_from_slice(b)).collect()))
    }

    async fn drain(s: &mut dyn ByteStream) -> Vec<u8> {
        let mut out = Vec::new();
        loop {
            let c = s.read().await.unwrap();
            if c.is_empty() {
                return out;
            }
            out.extend_from_slice(&c);
        }
    }

    #[compio::test]
    async fn skip_take_within_single_chunk() {
        let mut s = SkipTakeStream::new(chunks(&[b"0123456789"]), 2, 5);
        assert_eq!(drain(&mut s).await, b"23456");
    }

    #[compio::test]
    async fn skip_take_across_chunks() {
        let mut s = SkipTakeStream::new(chunks(&[b"01234", b"56789", b"abcde"]), 3, 8);
        assert_eq!(drain(&mut s).await, b"3456789a");
    }

    #[compio::test]
    async fn skip_consumes_full_chunks_then_partial() {
        // skip 8 bytes drops the first two chunks entirely, then yields
        // the remaining take from the third chunk.
        let mut s = SkipTakeStream::new(chunks(&[b"AAAA", b"BBBB", b"CCCC"]), 8, 4);
        assert_eq!(drain(&mut s).await, b"CCCC");
    }

    #[compio::test]
    async fn take_zero_yields_eof_immediately() {
        let mut s = SkipTakeStream::new(chunks(&[b"data"]), 0, 0);
        assert_eq!(drain(&mut s).await, Vec::<u8>::new());
    }

    #[compio::test]
    async fn take_exceeding_upstream_truncates_at_eof() {
        // The window claims 100 bytes but the source only has 3; we
        // serve 3 and stop. Production code never reaches this because
        // the handler validates bounds, but the adapter must be safe.
        let mut s = SkipTakeStream::new(chunks(&[b"abc"]), 0, 100);
        assert_eq!(drain(&mut s).await, b"abc");
    }

    #[compio::test]
    async fn skip_past_eof_yields_empty() {
        let mut s = SkipTakeStream::new(chunks(&[b"abc"]), 99, 1);
        assert_eq!(drain(&mut s).await, Vec::<u8>::new());
    }

    #[compio::test]
    async fn zero_skip_returns_prefix_take() {
        let mut s = SkipTakeStream::new(chunks(&[b"hello world"]), 0, 5);
        assert_eq!(drain(&mut s).await, b"hello");
    }
}
