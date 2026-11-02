# ADR-RESOURCE-03 — Resource, leader, claim and replay semantics

Status: accepted

Resource demand is typed by namespaced resource key, positive points, positive quantity, SHARED or EXCLUSIVE mode, and positive limit. Worker capability remains a separate requirement. A domain source/scope gate is not automatically a mutex.

PostgreSQL is authoritative for `leadership_epoch` and `claim_epoch`. PipeCraft observes these values but cannot mint an authoritative domain claim. A claim fence consists of work ID, attempt ID, claim epoch, leadership epoch, opaque owner token and DB-time lease deadline. `owner_token` remains sensitive/write-only.

Replay dispositions are `SAFE_TO_REPLAY`, `RECEIPT_ALREADY_COMMITTED`, `RECONCILIATION_REQUIRED`, `FORBIDDEN_UNCERTAIN`, and `STALE_FENCE`. An UNCERTAIN effect is never automatically retried. A committed receipt outranks PipeCraft journal observations and exact duplicates must reuse the authoritative result.
