# ADR-04 — Durable claim owner

Status: **accepted**

PostgreSQL remains authoritative for durable external-work claims. PipeCraft may issue an offer and observe a confirmed claim, but cannot mint the authoritative domain claim.

A confirmed claim is identified/fenced by work ID, attempt ID, claim epoch, leadership epoch, opaque owner token and DB-time lease. The short domain transaction is the final authorization step.

No second mutable claim ledger is introduced in PipeCraft.
