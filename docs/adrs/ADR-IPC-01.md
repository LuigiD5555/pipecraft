# ADR-IPC-01 — External-work transport direction

Status: accepted

External-work integration uses the existing local IPC service and a provider-driven pull/long-poll model. PipeCraft does not call arbitrary provider ports and does not create a second daemon.

The G1 operation `external_work_contract` is contract-only, simulated and side-effect free. It proves typed Rust/Python transport compatibility. Actual offers, claims and worker dispatch remain gated for later units.

Legacy `pipecraft.ipc/v1` is unchanged. The optional capability is `external_work_contract_v1`; unsupported or malformed messages fail closed.
