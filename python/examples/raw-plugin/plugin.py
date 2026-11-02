#!/usr/bin/env python3
"""A PipeCraft plugin needs no Python SDK; stdin/stdout JSON is the protocol."""

import json
import sys

context = json.load(sys.stdin)
result = {
    "success": True,
    "status": "ok",
    "message": f"handled {context.get('step_id', 'step')}",
    "data": {"runtime": "external-python-process"},
}
json.dump(result, sys.stdout)
