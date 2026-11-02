# Read-only external admission API (1.7.0-alpha.1)

The planner is a deterministic function of one snapshot. It does **not** poll
queues, own database connections, start workers, reserve resources or issue
valid execution tokens. It is suitable for simulations, policy evaluation and
comparisons with an application's current scheduler, not yet for production
replacement of an application's claim protocol.

## Input

Pass an `AdmissionSnapshot` as JSON to:

```bash
pipecraft admission-plan examples/10-external-admission/snapshot.json --json
```

or `PipeCraft.plan_admission(snapshot)` through IPC `plan_admission`.

| Field | Type | Meaning |
| --- | --- | --- |
| `revision` | unsigned integer | Immutable identity of the provider's snapshot |
| `now_ms` | unsigned integer | Virtual UTC epoch time in milliseconds |
| `capacity` | optional unsigned integer | Effective resource budget; overrides adaptive calculation |
| `settings.policy` | `legacy` or `fair` | Legacy preserves candidate order, fair ranks deterministically |
| `settings.aging_ms` | integer | Each interval decreases effective priority by one |
| `settings.heavy_wait_guard_ms` | integer | How long a heavy candidate waits before backfill protection |
| `settings.initial_capacity` | integer | Default budget if no capacity input |
| `settings.ceiling_capacity` / `floor_capacity` | integer | Hard bounds used by admission |
| `settings.allow_idle_overweight` | boolean | Allow overweight proposal only when idle (never bypass physical locks) |
| `last_dispatch_ms` | mapping group → timestamp | Fairness history supplied by the domain |
| `active` | list | Active work including group, weight, scope, exclusive/shared resources |
| `candidates` | list | Potential work with stable ID, group, priority, weights and restrictions |
| `adaptive_capacity` | optional object | `profile` and persisted `evidence` for pure capacity calculation |

Each candidate contains `id`, `group`, `priority` (lower first),
`ready_at_ms`, optional `not_before_ms`, `weight`, `scopes`,
`exclusive_resources`, `shared_resources`, optional `hard_block`,
`dependencies_met`, `group_limit`. Missing fields use safe defaults; unknown
fields follow the current serde behavior and should not be used to communicate
security policy. The application must mark all hard policy refusals explicitly.

Use Unix epoch milliseconds, **not** ISO timestamp strings. Use stable,
non-secret identifiers. A pending dependency is not offered even if the priority
is high. A hard block remains blocked until a new snapshot removes it.

## Decision rules

1. Reject missing/duplicate IDs, zero weights/group limits, and contradictory
   exclusive/shared declarations.
2. Derive an effective capacity from an explicit budget or a pure calculation.
3. Rank in legacy input order or fair order: `priority - floor(wait_ms / aging_ms)`,
   then oldest group dispatch, earliest `ready_at_ms`, stable ID.
4. Filter dependency, hard policy and time restrictions.
5. Reject a candidate if its group, scope or named resources conflict with
   active reservations or earlier proposals in the same planning call.
6. Prevent exceeding the effective capacity (unless idle-overweight option is
   explicitly enabled). Return `blocked` reasons and earliest `next_wakeup_at_ms`
   for not-yet-ready candidates.

`HEAVY_BACKFILL_GUARD` is a preliminary conservative safeguard, **not** a
verified implementation of the Extracta scheduler's exact heavy reservation
semantics. The complete parity suite remains an integration task.

## Capacity profile

`adaptive_capacity.profile` takes configurable bounds, clean-result growth,
widespread failure threshold, memory reserve, MiB per point, and CPU load
ratio. `adaptive_capacity.evidence` takes `clean`, `bad`, `bad_groups`,
`memory_available_mib`, `cpu_load_ratio`. These values are supplied by the
host; the runtime does not read its CPU or memory itself.

When both `capacity` and `adaptive_capacity` are set, the explicit `capacity`
is used but the adaptive profile is still validated. When CPU/memory observations
are missing, the associated pressure adjustment is skipped.

## Output

```json
{
  "revision": 42,
  "offered": ["job-a"],
  "blocked": [{"id": "job-b", "reason": "RESOURCE_BUSY"}],
  "capacity_used": 3,
  "capacity_total": 5,
  "next_wakeup_at_ms": null,
  "requires_confirmation": true
}
```

The above is a schema illustration, not an asserted exact result of the
supplied example snapshot. The actual decision should be read from the runtime.

**No client may execute merely because its ID appears in `offered`.**
A domain database must atomically recheck policy, owner/group/scope occupancy,
resource budgets, lease freshness and idempotency; issue a durable receipt;
and fence all writes. The 1.7 preview does not implement that adapter, nor a
durable offer ledger or external worker registration.

## Limitations to resolve before scheduler replacement

- Confirmed offers, epochs, leases, heartbeat, revocation, fencing, and
  crash/ACK reconciliation are not exposed by this preview.
- The DAG runner's process-local resource coordinator and the read-only
  external planner are not yet one global cross-process resource authority.
- Source-specific cooldown inheritance and reason equivalence depend on a
  domain-supplied snapshot; they are not computed automatically.
- Fairness and heavy-task backfill have deterministic tests, but equivalence
  with another scheduler must be measured on identical trace fixtures.
- No high-availability/multi-host guarantee or exactly-once external effects.

These are explicit future development gates, not undocumented capabilities.
