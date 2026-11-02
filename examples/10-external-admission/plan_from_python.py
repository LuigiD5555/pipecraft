"""Read-only PipeCraft scheduling demonstration (Python SDK, resident service)."""
import json
from pathlib import Path
from pipecraft import PipeCraft

snapshot = json.loads(Path(__file__).with_name("snapshot.json").read_text())
client = PipeCraft(root=".")
print(json.dumps(client.plan_admission(snapshot), indent=2))
print("Advisory only: an external domain store must confirm a durable claim.")
