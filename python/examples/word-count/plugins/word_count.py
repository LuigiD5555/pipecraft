#!/usr/bin/env python3
"""Word-count plugin using only the language-neutral JSON protocol."""

import json
import sys
from pathlib import Path

ctx = json.load(sys.stdin)
with_ = ctx.get("with", {})
path = with_.get("path", "README.md")
cwd = Path(ctx.get("cwd") or ".")
target = cwd / path

if not target.is_file():
    json.dump({"success": False, "status": "failed", "message": f"no such file: {target}"}, sys.stdout)
    raise SystemExit(1)

words = len(target.read_text(encoding="utf-8", errors="replace").split())
json.dump(
    {
        "success": True,
        "status": "ok",
        "message": f"{words} words in {path}",
        "data": {"path": str(path), "words": words},
    },
    sys.stdout,
)
