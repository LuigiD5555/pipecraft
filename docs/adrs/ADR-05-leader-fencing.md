# ADR-05 — Leader fencing

Status: **accepted**

Leadership is fenced by a monotonically increasing PostgreSQL `leadership_epoch` per installation. The local PipeCraft workspace lock remains an additional process-safety mechanism, not the domain authority.

Offers or outcomes carrying a stale leadership epoch must fail closed. Lease authorization uses database time; local monotonic time is only for waiting/deadline mechanics.
