#!/usr/bin/env python3
"""Offline source-archive structure and documentation smoke checks."""
from __future__ import annotations
from pathlib import Path
import json

ROOT = Path(__file__).resolve().parents[1]
REQUIRED = [
    "Cargo.toml", "README.md", "LICENSE", "docs/admission.md",
    "docs/ipc.md", "docs/architecture.md", "docs/operations.md",
    "docs/development.md", "crates/pipecraft-runtime/src/admission.rs",
    "crates/pipecraft-runtime/src/capacity.rs",
    "crates/pipecraft-service/src/server.rs", "examples/10-external-admission/snapshot.json",
]
missing = [file for file in REQUIRED if not (ROOT / file).is_file()]
if missing:
    raise SystemExit("Missing source files: " + ", ".join(missing))
manifest = (ROOT / "Cargo.toml").read_text()
assert 'version = "1.7.0-alpha.2"' in manifest
assert "plan_admission" in (ROOT / "crates/pipecraft-service/src/server.rs").read_text()
snapshot = json.loads((ROOT / "examples/10-external-admission/snapshot.json").read_text())
assert snapshot["revision"] >= 1 and snapshot["candidates"]
print("Source structure, version, documentation and example JSON: PASS")
print("This is not a Rust build/test verification.")
