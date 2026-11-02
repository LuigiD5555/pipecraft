# ADR-01 — External provider transport direction

Status: **accepted**

Use the existing local Unix IPC service with provider-driven pull/long-poll. PipeCraft must not call arbitrary Django/provider ports and must not introduce a second daemon.

The G1 `external_work_contract` operation is side-effect free. Mutating offer/claim/dispatch operations remain gated for later units.

This is the canonical ADR-01 decision; `ADR-IPC-01.md` contains the detailed rationale.
