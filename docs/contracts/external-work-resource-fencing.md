# External-work resource, fencing and replay contract

C03 freezes resource and durability semantics without implementing PostgreSQL claims inside PipeCraft.

- Resource demands are typed and bounded.
- PostgreSQL owns leader and claim epochs.
- The owner token is opaque and secret.
- Lease authorization uses provider/DB time.
- `operation_key` remains stable across attempts.
- Immutable receipts outrank runtime journal observations.
- Ambiguous external effects become `UNCERTAIN` and require reconciliation.
- Legacy DAG resource locks and resident-service JobStatus remain unchanged during G1.
