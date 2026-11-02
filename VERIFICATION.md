# Verification status — PipeCraft 1.7.0-alpha.1

The following checks were performed on the generated source tree:

- Python SDK tests: **3 passed, 0 failed**.
- Python source compilation: **PASS**.
- Offline structure, documentation and sample JSON check: **PASS**.
- Rust lexical/delimiter scan of 30 `.rs` files: **0 structural mismatches**.

**Not executed:** Cargo build, rustfmt, Clippy, Rust unit/integration tests,
resident service functional smoke tests, cross-platform tests and binary
reproducibility. The preparation environment has no Rust compiler or Cargo,
and cannot reach Rust distribution hosts.

There is **no validated 1.7 binary** in this source archive. Before release:

```bash
./scripts/verify-release.sh
```

Generate and commit `Cargo.lock`, and record the Rust toolchain, build commit
and artifact hash. Do not label the static scan as Rust compilation.

See `docs/development.md` for the full verification checklist and explicit
limits of the read-only admission planner.
