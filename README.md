# PipeCraft

**Local-first, Rust-native pipeline orchestration with deterministic DAG execution,
resource coordination, durable job records, and advisory work admission.**

> **Release status: 1.7.0-alpha.1 source preview.** This source package is derived
> from the recovered PipeCraft 1.6.0-alpha.1 source and historical Styler build
> recipe. The new Rust changes have **not** been compiled or run with Cargo in
> the preparation environment (Rust toolchain unavailable). Python SDK tests
> were run. Do not treat this package as a production-certified binary.

PipeCraft turns a process into a versionable sequence of steps. It can run a
single YAML pipeline, run independent DAG branches concurrently, or keep a
local service alive so clients can submit and inspect durable jobs. In 1.7,
there is also a **read-only admission planner** for external work providers.

PipeCraft does **not** require a cloud account, Redis, a hosted UI, or a Python
scheduler. It knows nothing about your application's domain. External data,
policies, authentication, and effect commits belong to the application.

## Contents

- [Quick start](#quick-start)
- [A first pipeline](#a-first-pipeline)
- [Concurrent execution and resources](#concurrent-execution-and-resources)
- [Resident service](#resident-service)
- [Python SDK](#python-sdk)
- [Structured IPC and inline specifications](#structured-ipc-and-inline-specifications)
- [Read-only admission planning (new)](#read-only-admission-planning-new)
- [Persistence and recovery](#persistence-and-recovery)
- [Supported commands](#supported-commands)
- [Safety and operational boundaries](#safety-and-operational-boundaries)
- [Development and verification](#development-and-verification)
- [Documentation and repository layout](#documentation-and-repository-layout)

## Quick start

### Prerequisites

- A stable Rust compiler and Cargo with Rust 2021 edition support. The original
  workspace declares `rust-version = "1.75"`, but dependency minimum versions
  have **not** been revalidated for this source preview. Prefer a current stable
  Rust toolchain for the first verified build.
- Linux/macOS for the default Unix-domain-socket service. Windows uses a
  local TCP endpoint in the existing implementation; see the security notes.
- Python 3.9+ only if you want the optional thin Python SDK.

```bash
# In the extracted source directory:
cargo build --release -p pipecraft-cli
cargo test --workspace
./target/release/pipecraft --version
```

The binary will be at `target/release/pipecraft`. You may add it to `PATH`.

### Try an included example

```bash
./target/release/pipecraft --root examples/01-hello-world list
./target/release/pipecraft --root examples/01-hello-world validate hello-world --json
./target/release/pipecraft --root examples/01-hello-world plan hello-world --json
./target/release/pipecraft --root examples/01-hello-world run hello-world --json
```

The last command is a **dry run**: it plans side effects without executing them.
To run the example's `date` command, explicitly opt in:

```bash
./target/release/pipecraft --root examples/01-hello-world run hello-world --execute
```

Use only pipelines you trust. The `command` step can start local processes when
execution is explicitly enabled.

## A first pipeline

A workspace is a directory with `.pipelines/workspace.yaml` and a pipeline
folder. A minimal setup is:

```text
my-workspace/
  .pipelines/
    workspace.yaml
    pipelines/
      hello.yaml
```

`my-workspace/.pipelines/workspace.yaml`:

```yaml
schema_version: pipecraft/v1
workspace:
  name: my-workspace
paths:
  pipeline_dir: .pipelines/pipelines
  runs_dir: .pipelines/runs
  outputs_dir: .pipelines/out
```

`my-workspace/.pipelines/pipelines/hello.yaml`:

```yaml
schema_version: pipecraft/v1
name: hello
steps:
  - id: introduction
    type: note
    description: Start the workflow.
  - id: check
    type: checklist
    needs: [introduction]
    with:
      items:
        - Confirm the input is ready.
        - Inspect the report.
```

Use `pipecraft --root my-workspace validate hello`, then `plan`, then `run`.
For a fuller sample, use `examples/01-hello-world` (known original schema).

### Pipeline vocabulary

| Field | Purpose |
| --- | --- |
| `schema_version: pipecraft/v1` | Versioned pipeline format |
| `name` | Human-readable pipeline identifier |
| `steps` | Ordered declarations forming a DAG |
| `id` | Stable, unique step identity |
| `type` | Executor: `note`, `checklist`, `command`, `plugin`, etc. |
| `needs` | Predecessors that must settle before this step becomes ready |
| `run_if` | Dependency outcome rule: `all_success`, `all_complete`, `any_failed`, `always` |
| `with` | Type-specific input mapping |
| `requires` / `provides` | Opaque runtime capabilities |
| `exclusive_resources` / `shared_resources` | Cross-pipeline concurrency constraints |
| `barrier` | Run this step alone inside its pipeline |
| `requires_approval` | Require explicit approval for a gated step |
| `on_error` | Failure behavior defined by the pipeline policy |

The original executor registry includes `note`, `checklist`, `command`,
`plugin`, `file_check`, `boundary_check`, `git_diff`, `copy_or_sync`,
`manual_approval`, and `target_plan`. Consult `docs/concepts.md` and
`docs/recipes.md` for their options. Unknown step types are not silently
executed as shell commands.

## Concurrent execution and resources

PipeCraft uses Tokio to run independent ready DAG nodes concurrently.
Dependency edges are always respected. Resource names are arbitrary strings
chosen by the pipeline author:

```yaml
steps:
  - id: fetch_a
    type: note
    exclusive_resources: [network:shared-host]
  - id: fetch_b
    type: note
    exclusive_resources: [network:shared-host]
  - id: merge
    type: checklist
    needs: [fetch_a, fetch_b]
    with:
      items: [Review the two results]
```

`fetch_a` and `fetch_b` cannot simultaneously hold that exclusive resource
under the same live runtime coordinator. A shared holder may coexist with
other shared holders, but conflicts with an exclusive holder.

```bash
./target/release/pipecraft --root examples/08-concurrent-dag run concurrent-demo --execute --max-workers 2
./target/release/pipecraft --root examples/09-multi-pipeline-global-resources run-many alpha beta --execute --max-pipelines 2 --max-tasks 4
```

**Important:** these built-in locks are process-local. They are not durable,
not distributed, and are not a substitute for database locks or fencing.
The DAG runner retains the established topological selection order; the same
stable ranking helper is now shared with the external admission planner.

## Resident service

For clients that should survive process restarts or submit jobs asynchronously:

```bash
./target/release/pipecraft --root examples/01-hello-world serve --max-pipelines 8 --max-tasks 32 --recovery manual
```

In another terminal:

```bash
./target/release/pipecraft --root examples/01-hello-world submit hello-world --json
./target/release/pipecraft --root examples/01-hello-world jobs --json
./target/release/pipecraft --root examples/01-hello-world status RUN_ID --json
./target/release/pipecraft --root examples/01-hello-world job-events RUN_ID --json
./target/release/pipecraft --root examples/01-hello-world job-report RUN_ID --json
./target/release/pipecraft --root examples/01-hello-world cancel RUN_ID --json
./target/release/pipecraft --root examples/01-hello-world resume-job RUN_ID --json
```

Replace `RUN_ID` with the value returned by `submit`. Add `--execute` to
`submit` only when you intentionally authorize side effects. The safe default
is dry-run; `--approve` separately authorizes steps requiring approval.

On Unix, the default socket is `<root>/.pipelines/pipecraft.sock`, permission
`0600`. The protocol is line-delimited JSON, `pipecraft.ipc/v1`. The daemon
now caps concurrent IPC connections at 64 and enforces a 10-second per-operation
read/write timeout; requests have an 8 MiB limit.

## Python SDK

The optional Python package is **a client, not an execution engine**:

```bash
python -m pip install -e ./python
```

```python
from pipecraft import PipeCraft

client = PipeCraft(root="examples/01-hello-world")
print(client.ping())  # The resident service must be running.
run_id = client.submit("hello-world", execute=False)
print(client.wait(run_id))
print(client.report(run_id))
```

The Python client also supports `validate_spec`, `plan_spec`, `submit_spec`,
and `plan_admission` against the resident service.

## Structured IPC and inline specifications

PipeCraft 1.6 introduced `validate_spec`, `plan_spec`, and `submit_spec`. These
accept a JSON object containing the `pipecraft/v1` pipeline spec, without
writing a temporary YAML file. The service still stores a durable YAML snapshot
internally for replay.

```python
from pipecraft import PipeCraft

pc = PipeCraft(root="my-workspace")
spec = {
    "schema_version": "pipecraft/v1",
    "name": "inline-example",
    "steps": [{"id": "hello", "type": "note", "description": "Hello from IPC"}],
}
assert pc.validate_spec(spec)["valid"]
print(pc.plan_spec(spec))
run_id = pc.submit_spec(spec, execute=False)
print(pc.wait(run_id))
```

Raw IPC works over the same socket. Example request and response envelopes,
capabilities, failure codes and versioning are in [docs/ipc.md](docs/ipc.md).

## Read-only admission planning (new)

PipeCraft 1.7 adds a deterministic planner for *dynamic external work*. It can
rank independently queued jobs by priority, waiting time and group LRU, check
exclusive resources and group/scope occupancy, and calculate an optional
adaptive capacity from supplied evidence. Unlike the DAG runner, this external
planner is intentionally **not connected to an executor**.

From the project root:

```bash
./target/release/pipecraft admission-plan examples/10-external-admission/snapshot.json --json
```

From Python (service required):

```python
from pipecraft import PipeCraft
import json
from pathlib import Path

snapshot = json.loads(Path("examples/10-external-admission/snapshot.json").read_text())
result = PipeCraft(root="my-workspace").plan_admission(snapshot)
print(result["offered"], result["blocked"], result["requires_confirmation"])
```

- All timestamps ending in `_ms` are Unix epoch **milliseconds**, UTC.
- `revision` identifies the provider snapshot; identical input produces an
  identical decision.
- `group` represents an independent fairness cohort. `last_dispatch_ms`
  describes prior successful dispatch; it is *input*, not state mutated by the
  planner.
- Lower `priority` ranks first. Waiting time lowers effective priority by one
  every `aging_ms` (default 120 seconds).
- `scopes` represent mutually exclusive domains such as an origin or a route.
- `weight` and `capacity` are resource points; a candidate cannot exceed
  available capacity unless `allow_idle_overweight` is explicitly enabled.
- `not_before_ms` delays eligibility, while `hard_block` denies the offer.
- `adaptive_capacity` is optional and uses *provided* clean/bad observations,
  available memory and CPU ratio. There is no hidden host sampling.

**Critical contract:** `offered` is an advisory recommendation, **not a claim**.
This release does **not** provide a durable offer ledger, `confirm_offer`, a
transactional PostgreSQL adapter, cross-service leases, epochs, or fencing for
external work. An integrating application must enforce those guarantees before
executing any recommended work. Do not use this API as a production task
queue. See [docs/admission.md](docs/admission.md).

## Persistence and recovery

The resident service uses separate persisted records:

```text
.pipelines/runtime/jobs/<run-id>.json
.pipelines/runs/<run-id>/pipeline.snapshot.yaml
.pipelines/runs/<run-id>/state.json
.pipelines/runs/<run-id>/events.jsonl
.pipelines/runs/<run-id>/report.json
.pipelines/runs/<run-id>/attempts/attempt-001.json
```

Job records describe service lifecycle; `state.json` tracks DAG completion;
events and reports are evidence. Successful steps with a compatible run
fingerprint may be reused during resume. **Interrupted or failed steps may
execute again**, including their external side effects. This is at-least-once
behavior, **not exactly-once**. Use idempotent handlers and durable application
receipts for operations that must not be duplicated.

Changes in 1.7:

1. Corrupt `.json` job records now cause startup to fail visibly; jobs are
   not silently omitted. Restore or investigate the offending record, retaining
   its original bytes for audit. No automatic deletion/quarantine occurs.
2. Concurrent service `resume` requests for the same run cannot both place a
   new active attempt in the same service process.
3. The default recovery policy remains `manual`. `auto` may replay external
   effects and is appropriate only for idempotent pipelines.

Existing `pipecraft.ipc/v1` operations and `pipecraft/v1` YAML are retained.
Persisted state from earlier alpha releases has **not** been independently
qualified under the reconstructed build; back up the workspace before upgrade.

## Supported commands

| Command | Effect |
| --- | --- |
| `list` | List pipelines in a workspace |
| `validate NAME` | Validate a pipeline, no execution |
| `plan NAME` | Calculate DAG order, no execution |
| `route LABELS` | Resolve labels to a pipeline |
| `run NAME` | Dry-run by default; `--execute` enables work |
| `run-many NAME...` | Run several pipelines with shared runtime limits |
| `serve` | Start resident service |
| `submit NAME` | Submit to service, dry-run unless `--execute` |
| `status RUN_ID`, `jobs` | Inspect current jobs |
| `cancel RUN_ID`, `resume-job RUN_ID` | Cooperatively cancel or resume |
| `job-events RUN_ID`, `job-report RUN_ID` | Inspect persisted evidence |
| `run-labels LABELS` | Route and run from labels |
| **`admission-plan SNAPSHOT.json`** | Read-only external scheduling preview |

Run `pipecraft --help` and `pipecraft COMMAND --help` for exact options.

## Safety and operational boundaries

- Never pass secrets, cookies, raw HTML, or credentials in admission snapshots,
  error messages, labels, or logs.
- Never expose the local IPC endpoint to untrusted users. The Unix socket has
  user-only permissions, but account-level compromise gives command execution
  to an attacker through trusted pipelines.
- On platforms using TCP, the transport does **not** provide built-in TLS or
  authentication. Restrict to trusted loopback and review access controls.
- A `command` or `plugin` pipeline can perform arbitrary local side effects
  when explicitly executed. The approval flag is **not** a security sandbox.
- External API side effects require domain-owned authorization, idempotency,
  transaction checks and fencing.
- Preserve the recovered source history, lockfile once generated, checksums,
  compiler version and CI logs before distributing binaries.

## Development and verification

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo build --release -p pipecraft-cli
PYTHONPATH=python/src python -m pytest -q python/tests
python scripts/check_release_sources.py
```

The source archive does not contain a Rust `Cargo.lock` because it was absent
from the recovered distribution. Generate one with Cargo in a connected,
controlled environment and commit it before a reproducible binary release.
See [docs/development.md](docs/development.md) for test targets and restrictions.

## Documentation and repository layout

| Path | Contents |
| --- | --- |
| `crates/pipecraft-core` | Parser, schema and domain-neutral pipeline definitions |
| `crates/pipecraft-graph` | DAG ordering and dependencies |
| `crates/pipecraft-runtime` | Tokio executor, resource manager, admission and capacity policies |
| `crates/pipecraft-service` | Resident daemon, IPC and job store |
| `crates/pipecraft-cli` | CLI entry point |
| `python/` | Optional stdlib-based IPC client |
| `examples/` | Starter pipelines, concurrency and external admission examples |
| `docs/admission.md` | External admission reference, rules, limitations |
| `docs/architecture.md` | Components, ownership and flow diagrams |
| `docs/ipc.md` | IPC protocol, envelopes, limits, examples |
| `docs/operations.md` | Deployment, recovery and troubleshooting |
| `docs/development.md` | Source provenance, tests and release requirements |
| `docs/changelog/CHANGELOG_v1_7_0_alpha_1.md` | Version changes and limitations |

**Source provenance:** see `RECOVERY_PROVENANCE.json` and
`docs/development.md`. This is an independent source preview, not a claim that
its compiled ELF has the same hash as the historical PipeCraft 1.6 binary.

License: Apache-2.0. See [LICENSE](LICENSE).
