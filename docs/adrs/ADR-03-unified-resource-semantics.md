# ADR-03 — Unified resource semantics

Status: **accepted**

The target resource vocabulary is typed: weighted points, quantity, SHARED/EXCLUSIVE mode, bounded limits and explicit worker capability. DAG and external work must eventually consume one PipeCraft runtime ledger.

Domain source/scope eligibility and browser physical occupancy remain external gates until their dedicated integration units. This ADR freezes semantics, not the later allocator implementation.

See `ADR-RESOURCE-03.md` for detailed G1 constraints.
