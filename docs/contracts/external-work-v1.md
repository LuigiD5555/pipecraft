# External-work v1 semantic contract

This document freezes the C01 vocabulary for the future `pipecraft.external-work/v1` protocol. It does **not** add mutating IPC operations yet.

## Canonical identities

The contract keeps independent identities for the installation, provider, domain work unit, PipeCraft pipeline run, worker attempt, volatile offer, PostgreSQL claim epoch, PostgreSQL leadership epoch, business operation and durable receipt. `snapshot_revision` is the monotonic provider-feed revision.

`owner_token` is intentionally marked sensitive/write-only in the schema. It must travel only on the trusted execution path and must not appear in generic decision traces, logs or golden fixtures.

A PipeCraft `run_id` is not domain idempotency. `work_id`, `operation_key`, claim epochs and receipts remain independent.

## Lifecycle state

The accepted external-work states are:

`QUEUED`, `BLOCKED`, `OFFERED`, `CLAIMED`, `RUNNING`, `SUCCEEDED_VERIFIED`, `DEFERRED`, `NEEDS_HUMAN`, `FAILED_RETRYABLE`, `FAILED_FINAL`, `CANCELLED`, `UNCERTAIN`, `LEASE_EXPIRED`.

This state machine is separate from the existing resident-service `JobStatus` enum. Legacy DAG/job status remains readable and unchanged.

## Confirmation and outcome

Offer confirmation returns one of:

`CONFIRMED`, `STALE_OFFER`, `STALE_SNAPSHOT`, `POLICY_BLOCKED`, `SOURCE_BUSY`, `SCOPE_BUSY`, `NOT_FOUND`, `NO_PRIMARY`, `RECONCILIATION_REQUIRED`.

Verified worker/domain outcomes are:

`SUCCEEDED_VERIFIED`, `DEFERRED`, `NEEDS_HUMAN`, `FAILED_RETRYABLE`, `FAILED_FINAL`, `CANCELLED`, `UNCERTAIN`.

Unknown or unverifiable external side effects must resolve to `UNCERTAIN`; PipeCraft must not infer domain success from a process exit code.

## Semantic error/reason allowlist

The C01 allowlist freezes provider/admission semantic codes only. Transport framing and IPC errors remain the responsibility of C02.

The allowlist is stored in `schemas/external-work-v1.schema.json` and includes the confirmation rejection codes plus `BAD_RESOURCE_REQUEST`, `RESOURCE_BUSY`, `WORKER_CAPABILITY_UNAVAILABLE`, `HEAVY_BACKFILL_RESERVED` and `CAPACITY_BUSY`.

All later protocol messages must reuse these definitions instead of defining competing names.
