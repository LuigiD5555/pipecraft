# ADR-12 — API compatibility

Status: **accepted**

Legacy `pipecraft.ipc/v1` remains supported and unchanged for existing job/DAG operations. External work is an optional, capability-negotiated `pipecraft.external-work/v1` contract.

Unknown external-work message variants/capabilities fail closed. Existing `plan_admission` remains read-only. A non-tag main build validates compatibility but never overwrites an existing release.
