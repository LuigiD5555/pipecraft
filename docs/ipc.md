# Local IPC protocol

PipeCraft's resident service speaks **newline-delimited JSON**, one request
and one response per connection. The protocol envelope remains
`pipecraft.ipc/v1`. The 1.7 preview adds one read-only operation without
changing earlier request shapes.

## Endpoint

- Unix: `<workspace>/.pipelines/pipecraft.sock`, owned by the runtime account,
  socket permissions `0600`.
- Non-Unix: the existing loopback TCP fallback is not authenticated or
  encrypted; use only on a trusted machine and restrict access.
- Requests are limited to 8 MiB and have a 10-second read timeout. Responses
  also have 10-second write timeout. At most 64 incoming connections are active.
- The client closes each connection after one request/response.
- Requests must be terminated with `\n`. The `protocol` field is set on the
  response, not required on every request.

## Envelope

```json
{"op": "ping"}
```

Response (abbreviated):

```json
{"protocol":"pipecraft.ipc/v1","ok":true,"data":{"service":"pipecraft","version":"1.7.0-alpha.1","capabilities":["submit_spec","validate_spec","plan_spec","plan_admission","command","plugin","resume"]}}
```

On error:

```json
{"protocol":"pipecraft.ipc/v1","ok":false,"error":{"code":"INVALID_REQUEST","message":"..."}}
```

Do not rely on ordering of JSON object keys or human error messages. Clients
should handle `ok`, the stable `error.code` and the structured `data` object.

## Operations

| `op` | Inputs | Behavior |
| --- | --- | --- |
| `ping` | none | Version and advertised capabilities |
| `submit` | `pipeline`, `execute`, `approve`, `labels`, `max_workers`, `only`, `from_step` | Submit named pipeline |
| `validate_spec` | `spec` | Validate inline object, no side effects |
| `plan_spec` | `spec`, `only`, `from_step` | Plan inline DAG, no side effects |
| `submit_spec` | `spec`, execution fields | Submit inline pipeline, snapshots definition |
| `status` | `run_id` | Durable job record |
| `jobs` | none | All known jobs |
| `cancel` | `run_id` | Request cooperative cancellation |
| `resume` | `run_id` | Resume job with replay caveats |
| `events` | `run_id`, `after`, `limit` | Paginated event stream |
| `report` | `run_id` | Final report, if available |
| **`plan_admission`** | `snapshot` | **Advisory** work admission, no claim/execute |

### Inline pipeline request

```json
{"op":"submit_spec","spec":{"schema_version":"pipecraft/v1","name":"inline-demo","steps":[{"id":"hello","type":"note","description":"Hello"}]},"execute":false,"approve":false,"labels":[],"max_workers":2}
```

### External admission request

```json
{"op":"plan_admission","snapshot":{"revision":1,"now_ms":1700000000000,"capacity":2,"candidates":[{"id":"job-1","group":"example","priority":100,"ready_at_ms":1699999990000,"weight":1}],"active":[]}}
```

The response contains `offered`, `blocked`, `capacity_used`, `capacity_total`,
`next_wakeup_at_ms`, `revision`, `requires_confirmation:true`. This operation
is **read-only**; its decisions do not reserve any capacity or authorize
execution. See [admission.md](admission.md).

## Compatibility

`pipecraft/v1` YAML remains the serialized pipeline schema; it is separate
from `pipecraft.ipc/v1`. Clients should call `ping`, check the protocol and
capabilities, and reject unsupported operations. Do not assume that
`plan_admission` exists on a 1.6 daemon. A 1.7 client can still submit legacy
pipelines via `submit`/`submit_spec` if capabilities match.

## Failure and security notes

- Timeouts should be treated as **unknown outcome**, not proof a submission
  failed. Query the job list or durable receipt before retrying a side effect.
- `status:succeeded` is a pipeline outcome, not proof that your application's
  domain transaction committed.
- Do not expose the socket to untrusted users or place secrets in `spec`,
  `labels`, `snapshot`, or `error` messages.
- The read-only planner currently has no authentication, host-level lease or
  provider capability negotiation beyond the existing local IPC endpoint.


## External-work v1 contract simulation (G1)

PipeCraft advertises the optional capability `external_work_contract_v1`. The `external_work_contract` operation validates and round-trips a typed `pipecraft.external-work/v1` message over the existing local IPC channel. It is deliberately side-effect free: it does not reserve capacity, create a claim, start a worker, execute a pipeline, or authorize an external effect.

The transport direction frozen for G1 is provider-driven pull over the existing trusted local IPC service. No daemon callback to arbitrary provider ports is introduced. Malformed messages return `INVALID_EXTERNAL_WORK`; legacy `pipecraft.ipc/v1` operations and read-only `plan_admission` remain unchanged.


## S02/S03 provider feed and fake-provider acceptance

`plan_external_feed` accepts a full `provider_snapshot` or a
`provider_delta` (both `pipecraft.external-work/v1`). The provider's
`snapshot_revision` is monotonic, and a delta must match the previously
persisted `base_revision`. The resident service persists normalized feed
state and rejects invalid/corrupt evidence at restart. Its
`provisional_offers` are **advisory** and carry
`requires_confirmation:true`, **not** a runnable claim.

For an optional worker admission guard, set
`enforce_worker_registration:true` and use `register_worker`,
`available_workers`, and `cancel_worker`. Worker TTL is enforced
against the feed's `observed_at_ms`. Registration is deliberately not
authoritative: no registration alone permits an external operation.
Provider capacity and registration state are bounded; overload returns
a typed IPC error rather than allocating unlimited work.

S03's `s03_fake_provider_e2e` verifies the resident service with
an **in-test fake provider**. The fake provider owns the confirmation,
epoch and idempotent fake-effect receipt; PipeCraft only validates
the typed messages and offers scheduling decisions. Simulated execution
occurs only after fake authoritative confirmation. Lost-ACK replay
reuses a receipt without running the fake effect twice. Invalid
confirmations fail closed. This demonstrates **test-only**
execution orchestration, **not** production or real Extracta execution;
Extracta's F02/F03 PostgreSQL gates are separate.

The old `ping`, `plan_admission`, `submit`, `submit_spec`,
`status` and `pipecraft.ipc/v1` shapes are unchanged.
Python's thin client offers optional `plan_external_feed` and worker
registration methods without replacing existing methods.
No cutover, provider claim or genuine domain effect is authorized
by these S03 messages.
