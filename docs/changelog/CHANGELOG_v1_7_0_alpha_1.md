# PipeCraft 1.7.0-alpha.1 — source preview

**Status:** developer preview, not an independently compiled/released binary.

## Added

- Domain-neutral pure admission model and deterministic decision API.
- Read-only IPC `plan_admission` and offline `admission-plan SNAPSHOT.json` CLI.
- Optional capacity policy based on persisted evidence, CPU and memory signals
  provided by a caller, with typed decisions and explanation reasons.
- Python client methods `validate_spec`, `plan_spec`, `submit_spec`,
  `plan_admission`, plus tests.
- Admission JSON example, architecture/IPC/operations/development documentation.

## Changed

- JobStore refuses to silently drop unreadable or invalid JSON records at
  startup. This is a deliberate fail-closed change.
- Service reserves one active attempt per run ID, blocking concurrent resume
  submission within one service process.
- IPC service limits active connections (64) and bounds read/write I/O
  operations (10 seconds). Request limit is 8 MiB.
- DAG selection now shares the deterministic ranking helper in legacy mode;
  existing topological order remains the default behavior.
- Version changed to `1.7.0-alpha.1`; `pipecraft/v1` and
  `pipecraft.ipc/v1` stay unchanged.

## Not yet implemented or certified

- No confirmed external offers, claims, database transactions, epochs, leases,
  provider authentication, fencing or transaction receipts.
- No verified Rust build or Cargo test run in the source creation environment.
- No guarantee of exact parity with Extracta's aging/backfill algorithm.
- No distributed resource coordinator or automatic exactly-once side effects.
- Missing reproducible Cargo.lock until built and checked in.

This version provides source material to **begin validation** of these changes,
not permission to cut over a production scheduler.
