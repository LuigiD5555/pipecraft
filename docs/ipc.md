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
