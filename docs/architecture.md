# Architecture and ownership

## Components

```mermaid
flowchart TB
  CLI[CLI] --> CORE[Core parser and pipeline model]
  CLI --> RT[Tokio runtime]
  Python[Python SDK] --> IPC[Versioned local IPC]
  IPC --> Service[Resident service]
  Service --> RT
  RT --> DAG[DAG readiness and stable ranking]
  RT --> Resource[Process-local resource coordinator]
  RT --> Store[Run state and event log]
  Service --> Jobs[Durable job store]
  CLI --> Admission[Pure admission planner]
  IPC --> Admission
  Admission --> Capacity[Pure capacity policy]
  Admission --> Result[Advisory plan only]
```

`pipecraft-core` parses YAML and JSON-backed inline specs into the same
pipeline model. `pipecraft-graph` enforces dependencies. The runtime uses
Tokio to execute ready nodes, with process-local resource coordination and
persisted DAG status. The service owns job records and IPC request handling.

The new `admission` module has no I/O. Its inputs are an immutable snapshot,
provided capacity signals, existing active claims and fairness history. Its
output is a deterministic proposal. Both DAG ordering and external work
ranking use the same `ranked_ids` implementation, but the external planner is
**not yet connected to the DAG runner's dispatch loop**.

## Safety and domain boundary

```mermaid
sequenceDiagram
    participant App as External application
    participant PC as PipeCraft admission planner
    participant DB as Application durable store
    participant Ex as Application executor
    App->>PC: plan_admission(snapshot)
    PC-->>App: advisory offered IDs + blocked reasons
    Note over App,PC: No reservation occurred
    App->>DB: Confirm eligibility and unique fenced claim
    DB-->>App: Committed claim receipt or reject
    alt Claim confirmed
        App->>Ex: Execute with claim epoch and idempotency key
        Ex->>DB: Validate claim and persist business outcome
    else Rejected or stale
        App->>PC: Re-plan using updated snapshot
    end
```

The application owns the work record and all side effects. PipeCraft may only
make a selection recommendation until the future confirmed-offer protocol is
implemented and tested. The service's `state.json` is not an application
transaction log. A successful DAG status does not imply that an application's
business operation committed.

## Version compatibility

- YAML: `pipecraft/v1` unchanged.
- IPC: `pipecraft.ipc/v1` unchanged, with additive `plan_admission` request.
- Inline specifications: `validate_spec`, `plan_spec`, `submit_spec` retained.
- Pure module API: new in 1.7, experimental. The return value is advisory.
- No migration of existing persisted jobs is performed automatically.
