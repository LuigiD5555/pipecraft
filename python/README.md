# PipeCraft Python SDK 1.7.0a1

This is a **thin, standard-library-only IPC client** for the PipeCraft Rust
runtime. It does not execute DAGs, start process runners, arbitrate resources
or implement business logic. Rust remains the runtime authority.

## Installation

```bash
python -m pip install -e ./python
```

Start the Rust service in a separate terminal:

```bash
pipecraft --root my-workspace serve --recovery manual
```

## Submit a named pipeline

```python
from pipecraft import PipeCraft

pc = PipeCraft("my-workspace")
run_id = pc.submit("build", execute=False, max_workers=4)
job = pc.wait(run_id)
report = pc.report(run_id)
print(job, report)
```

`execute=False` is the default. With `execute=True`, trusted `command` and
`plugin` steps may have local effects. Approval of gated steps is separate.

## Use an inline pipeline specification

```python
from pipecraft import PipeCraft

pc = PipeCraft("my-workspace")
spec = {
    "schema_version": "pipecraft/v1",
    "name": "inline",
    "steps": [{"id": "hello", "type": "note", "description": "Hello"}],
}
print(pc.validate_spec(spec))
print(pc.plan_spec(spec))
run_id = pc.submit_spec(spec)
print(pc.wait(run_id))
```

## Read-only external admission planning

```python
import json
from pathlib import Path
from pipecraft import PipeCraft

snapshot = json.loads(Path("examples/10-external-admission/snapshot.json").read_text())
result = PipeCraft("my-workspace").plan_admission(snapshot)
print(result["offered"], result["blocked"])
```

**Important:** the `offered` list is a non-binding suggestion. PipeCraft 1.7
has not yet implemented external transactional claims, epochs, leases or
fencing. Integrators must confirm eligibility and ownership in their own
store before doing any side effects.

## Connection options

The default Unix endpoint is `<workspace>/.pipelines/pipecraft.sock`.
Override it with the constructor `endpoint=` or `PIPECRAFT_ENDPOINT`.
`PIPECRAFT_BIN` optionally selects the Rust CLI binary for one-shot commands.
Python exceptions inherit from `PipeCraftError` / `PipeCraftCommandError`.

See the top-level [README](../README.md) and [IPC reference](../docs/ipc.md).
