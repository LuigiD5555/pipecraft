# Developer guide and release verification

## Source lineage

This preview evolves the recovered `1.6.0-alpha.1` code, which was built by
applying the archived script from the Styler repository to the original
`1.5.0-alpha.1` workspace. The historical source recipe is preserved in
`RECOVERY_PROVENANCE.json` and `CAMBIOS_15_A_16.diff`. The SHA-256 of the
historical 1.6 ELF is `3fd8e31b5ea60f45e3cb82d808bbe55738118f71da52f2c2576d1c5e70e54ac2`.
**That hash does not certify this 1.7 source preview.**

## Getting ready to contribute

- Install a stable Rust toolchain with rustfmt and clippy.
- Have Python 3.9+, pytest optional for the thin client.
- Do not add Django, site-specific scrapers or host credentials to Rust crates.
- Maintain `pipecraft/v1` and `pipecraft.ipc/v1` compatibility unless a major
  and explicitly versioned migration is required.
- Do not create a second scheduler loop in an external adapter.

## Verification commands

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo build --release -p pipecraft-cli
PYTHONPATH=python/src python -m pytest -q python/tests
python scripts/check_release_sources.py
```

The source package **has not passed Cargo tests** in its creation environment
because Cargo and rustc are not installed and network resolution is disabled.
A reviewer must run these commands and resolve any compile or behavior issues
before tagging a binary release. The Rust source was statically inspected and
Python IPC client tests were run separately.

## Test architecture

1. Pure admission and capacity tests use fixed timestamps, no network and no
   database. They verify determinism, resource exclusion, validation, fairness
   and pressure scaling.
2. Original runtime tests verify DAG dependencies, cancellation and resource
   coordination. No tests are removed as part of 1.7.
3. Service tests verify job persistence and fail-closed startup on corruption.
4. Python client tests use a fake Unix IPC server and test structured request
   serialization without invoking Rust.
5. Before an actual adapter replaces an application scheduler, run the app's
   real database atomicity/fencing suite, crash and ACK-loss matrix, browser
   capacity tests and side-by-side scheduling benchmarks.

## Release checklist

- [ ] Cargo.lock generated, checked in and dependency policy approved.
- [ ] `cargo fmt`, `clippy`, `test` pass on pinned compiler.
- [ ] Tests of request size/timeout, startup recovery and parallel resume pass.
- [ ] Python tests pass with installed package.
- [ ] Linux IPC run and inline spec smoke test pass.
- [ ] Windows/macOS support explicitly tested or marked unsupported.
- [ ] Release binary checksum and reproducible build metadata recorded.
- [ ] Socket permissions and trust boundaries reviewed.
- [ ] Previous IPC clients verified against new daemon.
- [ ] Behavior of legacy DAG scheduling compared with 1.6 fixtures.
- [ ] Advisory admission documented as non-executing.
- [ ] Release notes and migration instructions reviewed.

## Important future work

A production-grade external WorkProvider integration needs a confirmed-offer
protocol, crash-consistent persisted offer state, monotonic claim epochs, lease
renewal, signed/authorized provider ownership, revocation, fencing and
idempotent outcome receipts. Until these are delivered with E2E tests, the
new admission API is for planning and comparison only.
