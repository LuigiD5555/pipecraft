# PipeCraft

PipeCraft is the standalone workflow/scheduling runtime consumed by Extracta as a versioned binary.

## Release contract

The Rust source, Cargo.lock and build verification belong to this repository. Extracta must not vendor this source tree.

Tagged releases publish:

- `pipecraft-linux-x86_64`
- `pipecraft-linux-x86_64.sha256`

The release workflow requires the complete Rust workspace (`Cargo.toml`, `crates/`, and release verification scripts), then runs source checks, rustfmt, workspace tests, Clippy with warnings denied, a locked release build, and a binary `--version` smoke before publishing.

Extracta pins the exact version and SHA-256 before installation.
