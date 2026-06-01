# Contributing to OpenLake

  Thanks for contributing. PRs, issues, and discussion are all welcome.

  ## Getting help

  - Chat: [Discord](https://discord.gg/TNXqVSnP6x)
  - Bugs and feature requests: [GitHub Issues](https://github.com/openlake-project/openlake/issues)
  - Website: [theopenlake.com](https://theopenlake.com)

  ## Filing an issue

  Include:

  - OpenLake version or commit SHA
  - OS and kernel
  - Minimal reproduction
  - Logs (`RUST_LOG=debug`)

  ## Sending a pull request

  1. Fork and branch off `main`.
  2. One logical change per PR.
  3. Add or update tests.
  4. Run `cargo fmt`, `cargo clippy --workspace --all-targets`, `cargo test --workspace`.
  5. Open against `main`, link related issues.

  ## Style

  - `rustfmt` is authoritative.
  - `clippy` warnings are CI errors.
  - Public APIs need doc comments.

  ## License

  Contributions are licensed under Apache 2.0 (see `LICENSE`).
