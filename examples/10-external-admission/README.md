# External admission planning (developer preview)

This example demonstrates the **read-only** scheduling planner for dynamically
supplied work. It does not run the jobs, reserve resources or contact any external
system. The work identities and resource names are examples; PipeCraft treats
them as opaque strings.

From the project root after building:

```bash
./target/release/pipecraft admission-plan examples/10-external-admission/snapshot.json --json
```

Or start a service in a prepared PipeCraft workspace and run:

```bash
PYTHONPATH=python/src python examples/10-external-admission/plan_from_python.py
```

The example represents two jobs sharing an origin and a third job on hold.
`last_dispatch_ms` permits fair scheduling by group; `scopes` prevent two
proposals for the same origin. `adaptive_capacity` computes a resource budget
from explicit evidence; nothing is sampled implicitly.

**Safety boundary:** `offered` does not authorize an executor. The host
application must validate current policy and commit a unique, fenced claim in
its own durable store. Recheck after stale snapshots or process restarts.
