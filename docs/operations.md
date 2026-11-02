# Operations and troubleshooting

## Install and start

1. Build with `cargo build --release -p pipecraft-cli` in a trusted toolchain.
2. Create or select a workspace with `.pipelines/workspace.yaml`.
3. Validate pipelines offline before enabling side effects.
4. Start the service under an unprivileged operating-system account:

```bash
pipecraft --root /srv/workflows serve --recovery manual --max-pipelines 8 --max-tasks 32
```

The runtime creates a Unix socket under `/srv/workflows/.pipelines/`. Protect
workspace ownership: the account allowed to submit executed pipelines can
potentially run local commands with the same privileges as the runtime.

## Operational lifecycle

- `list`, `validate`, `plan`, and `admission-plan` do not execute work.
- `run` and `submit` default to dry-run; `--execute` opts into effects.
- `cancel` is cooperative: it stops further scheduling and asks active
  executors to cancel; it cannot undo an external side effect already made.
- `resume-job` can repeat interrupted steps; make handlers idempotent.
- The service normally changes unfinished records to `interrupted` on restart.
  Explicit `resume-job` is safest. `--recovery auto` risks replaying effects.
- Runtime resource names are locked only within one running coordinator. Two
  independent PipeCraft processes do not share resource locks.

## Durable files

```text
.pipelines/runtime/jobs/*.json            # job intent and lifecycle
.pipelines/runs/<run-id>/state.json       # DAG step state
.pipelines/runs/<run-id>/events.jsonl     # timeline
.pipelines/runs/<run-id>/report.json      # latest outcome
.pipelines/runs/<run-id>/attempts/        # archived attempt outcomes
```

Back up these records together with your pipeline definitions. Never modify
`state.json` or job records concurrently with a running service. Preserve
`pipeline.snapshot.yaml` because it defines what a resumed job runs.

## Failure cases

| Symptom | Recommended action |
| --- | --- |
| `service already appears to be listening` | Check the socket and existing process; do not delete a live service socket |
| `invalid durable job record` on startup | Stop; copy the corrupt file for forensic analysis; restore from backup or repair under reviewed procedure |
| `job ... already has an active attempt` | Do not submit concurrent `resume`; query `status` and wait for the current attempt |
| `REQUEST_TOO_LARGE` | Reduce request size (max 8 MiB) or use a versioned artifact reference pattern |
| `IPC request timed out` | Verify client framing (trailing newline), socket permissions and service health |
| `JOB_NOT_ACTIVE` | Job was already finished, cancelled or has no active cancellation token |
| `REPORT_NOT_READY` | Wait for terminal state; inspect events and durable job record |
| `RESUME_STATE_MISMATCH` | Compare snapshot, selected steps, labels and dry-run flag; do not force replay |
| `plan_admission` unsupported | Upgrade daemon to verified 1.7 build or omit the read-only preview operation |
| Jobs stop advancing | Inspect resource contention, max workers, effects, cancellation and logs |

## Backups and upgrades

Take a consistent backup of `.pipelines/` with the service stopped (or an
application-consistent snapshot). Record binary SHA-256, Cargo.lock,
configuration, workspace schema and Rust compiler version. Test upgrades in a
copy of the workspace, including resume and corruption scenarios.

**Do not** simultaneously start the old and new service against the same
workspace. The workspace lock prevents ordinary duplicate service ownership,
but it is not a distributed HA lease. There is no automatic two-primary
recovery or production cutover protocol in this release.

## Health

`pipecraft ... jobs --json` and the IPC `ping` endpoint expose basic health.
There is no built-in Prometheus/OpenTelemetry exporter or `doctor` CLI in this
source preview. An external supervisor should restart the daemon only after
investigating corrupt state and verifying any in-flight effects.
