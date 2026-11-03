"""Thin stdlib-only client for the PipeCraft Rust runtime service.

Python owns no scheduler, process runner, retry logic, graph state or persistence.
It speaks the versioned local IPC protocol and maps JSON to small Python views.
Legacy discovery/validation commands still delegate to the Rust CLI.
"""

from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import time
from pathlib import Path
from typing import Any, Iterator

from .exceptions import PipeCraftCommandError
from .models import Job, PipelineReport


class PipeCraft:
    def __init__(
        self,
        root: str | Path = ".",
        *,
        endpoint: str | None = None,
        binary_path: str | Path | None = None,
    ) -> None:
        self.root = Path(root)
        self.endpoint = endpoint or os.environ.get("PIPECRAFT_ENDPOINT") or self._default_endpoint()
        self._binary = Path(binary_path) if binary_path else None

    def _default_endpoint(self) -> str:
        if os.name == "nt":
            return "tcp://127.0.0.1:47831"
        return str(self.root / ".pipelines" / "pipecraft.sock")

    @property
    def binary(self) -> Path:
        if self._binary is not None:
            return self._binary
        found = os.environ.get("PIPECRAFT_BIN") or shutil.which("pipecraft")
        if not found:
            raise PipeCraftCommandError("PipeCraft Rust binary not found; set PIPECRAFT_BIN or add pipecraft to PATH")
        self._binary = Path(found)
        return self._binary

    def _ipc(self, payload: dict[str, Any]) -> dict[str, Any]:
        raw = (json.dumps(payload, separators=(",", ":")) + "\n").encode()
        try:
            if self.endpoint.startswith("tcp://"):
                host_port = self.endpoint.removeprefix("tcp://")
                host, port_text = host_port.rsplit(":", 1)
                sock = socket.create_connection((host, int(port_text)), timeout=5)
            else:
                sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                sock.settimeout(5)
                sock.connect(self.endpoint)
            with sock:
                sock.sendall(raw)
                chunks = bytearray()
                while not chunks.endswith(b"\n"):
                    part = sock.recv(65536)
                    if not part:
                        break
                    chunks.extend(part)
                    if len(chunks) > 8 * 1024 * 1024:
                        raise PipeCraftCommandError("PipeCraft IPC response exceeded 8 MiB")
        except OSError as error:
            raise PipeCraftCommandError(f"could not contact PipeCraft runtime service at {self.endpoint}: {error}") from error
        try:
            response = json.loads(chunks.decode())
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise PipeCraftCommandError("PipeCraft runtime service returned invalid JSON") from error
        if not isinstance(response, dict):
            raise PipeCraftCommandError("PipeCraft IPC response was not an object")
        if not response.get("ok", False):
            detail = response.get("error") or {}
            if isinstance(detail, dict):
                message = f"{detail.get('code', 'SERVICE_ERROR')}: {detail.get('message', 'request failed')}"
            else:
                message = str(detail)
            raise PipeCraftCommandError(message)
        data = response.get("data", {})
        return data if isinstance(data, dict) else {"value": data}

    def ping(self) -> dict[str, Any]:
        return self._ipc({"op": "ping"})

    def validate_spec(self, spec: dict[str, Any]) -> dict[str, Any]:
        """Validate an inline pipeline without creating a YAML file."""
        return self._ipc({"op": "validate_spec", "spec": spec})

    def plan_spec(
        self, spec: dict[str, Any], *, only: list[str] | None = None,
        from_step: str | None = None,
    ) -> dict[str, Any]:
        """Compute the DAG order for an inline pipeline (no execution)."""
        return self._ipc({"op": "plan_spec", "spec": spec,
                          "only": only or [], "from_step": from_step})

    def submit_spec(
        self, spec: dict[str, Any], *, execute: bool = False, approve: bool = False,
        labels: list[str] | None = None, max_workers: int = 4,
        only: list[str] | None = None, from_step: str | None = None,
    ) -> str:
        """Submit an inline pipeline to the resident Rust service."""
        data = self._ipc({"op": "submit_spec", "spec": spec, "execute": execute,
                          "approve": approve, "labels": labels or [],
                          "max_workers": max(1, max_workers), "only": only or [],
                          "from_step": from_step})
        return str(data["run_id"])

    def external_work_contract(self, message: dict[str, Any]) -> dict[str, Any]:
        """Validate/round-trip external-work/v1 without claims or execution."""
        if not isinstance(message, dict):
            raise TypeError("message must be a mapping")
        if message.get("protocol") != "pipecraft.external-work/v1":
            raise ValueError("message protocol must be pipecraft.external-work/v1")
        if not isinstance(message.get("request_id"), str) or not message["request_id"]:
            raise ValueError("message request_id is required")
        return self._ipc({"op": "external_work_contract", "message": message})

    def plan_external_feed(
        self, message: dict[str, Any], *,
        capacity: int | None = None,
        enforce_worker_registration: bool = False,
    ) -> dict[str, Any]:
        """S03 provider feed is advisory: proposed offers need domain confirmation."""
        if not isinstance(message, dict):
            raise TypeError("message must be a mapping")
        if message.get("protocol") != "pipecraft.external-work/v1":
            raise ValueError("unsupported provider feed protocol")
        if message.get("op") not in ("provider_snapshot", "provider_delta"):
            raise ValueError("expected provider_snapshot or provider_delta")
        payload: dict[str, Any] = {
            "op": "plan_external_feed", "message": message,
            "enforce_worker_registration": enforce_worker_registration,
        }
        if capacity is not None:
            if capacity <= 0:
                raise ValueError("capacity must be positive")
            payload["capacity"] = capacity
        return self._ipc(payload)

    def register_worker(
        self, worker_id: str, capabilities: list[str], *, now_ms: int, ttl_ms: int,
    ) -> dict[str, Any]:
        """Register advisory capability/TTL, not an Extracta execution claim."""
        return self._ipc({
            "op": "register_worker", "worker_id": worker_id,
            "capabilities": capabilities, "now_ms": now_ms, "ttl_ms": ttl_ms,
        })

    def cancel_worker(self, worker_id: str, *, now_ms: int) -> dict[str, Any]:
        return self._ipc({"op": "cancel_worker", "worker_id": worker_id, "now_ms": now_ms})

    def available_workers(self, capability: str, *, now_ms: int) -> dict[str, Any]:
        return self._ipc({"op": "available_workers", "capability": capability, "now_ms": now_ms})

    def plan_admission(self, snapshot: dict[str, Any]) -> dict[str, Any]:
        """Obtain advisory scheduling decisions. Never a claim or execute grant.

        Consumers must transactionally confirm ownership with their own domain
        store before performing external effects. Requires PipeCraft >=1.7.
        """
        if not isinstance(snapshot, dict):
            raise TypeError("snapshot must be a mapping")
        return self._ipc({"op": "plan_admission", "snapshot": snapshot})

    def submit(
        self,
        pipeline: str,
        *,
        execute: bool = False,
        approve: bool = False,
        labels: list[str] | None = None,
        max_workers: int = 4,
        from_step: str | None = None,
        only: list[str] | None = None,
    ) -> str:
        data = self._ipc({
            "op": "submit",
            "pipeline": pipeline,
            "execute": execute,
            "approve": approve,
            "labels": labels or [],
            "max_workers": max(1, max_workers),
            "from_step": from_step,
            "only": only or [],
        })
        return str(data["run_id"])

    def status(self, run_id: str) -> Job:
        return Job.from_dict(self._ipc({"op": "status", "run_id": run_id}))

    def jobs(self) -> list[Job]:
        data = self._ipc({"op": "jobs"})
        raw = data.get("value", data)
        if isinstance(raw, list):
            return [Job.from_dict(item) for item in raw if isinstance(item, dict)]
        # Rust serializes the Jobs response as the data value itself; older
        # service builds may wrap it. Keep both forms cheap to consume.
        return []

    def cancel(self, run_id: str) -> dict[str, Any]:
        return self._ipc({"op": "cancel", "run_id": run_id})

    def resume(self, run_id: str) -> dict[str, Any]:
        return self._ipc({"op": "resume", "run_id": run_id})

    def events_page(self, run_id: str, *, after: int = 0, limit: int = 200) -> dict[str, Any]:
        return self._ipc({"op": "events", "run_id": run_id, "after": max(0, after), "limit": max(1, min(limit, 1000))})

    def events(self, run_id: str) -> Iterator[dict[str, Any]]:
        after = 0
        while True:
            page = self.events_page(run_id, after=after)
            events = page.get("events", [])
            for event in events:
                if isinstance(event, dict):
                    yield event
            next_after = int(page.get("next", after))
            if not events or next_after <= after:
                break
            after = next_after

    def report(self, run_id: str) -> PipelineReport:
        return PipelineReport.from_dict(self._ipc({"op": "report", "run_id": run_id}))

    def wait(self, run_id: str, *, poll_interval: float = 0.2, timeout: float | None = None) -> Job:
        started = time.monotonic()
        while True:
            job = self.status(run_id)
            if job.terminal:
                return job
            if timeout is not None and time.monotonic() - started >= timeout:
                raise TimeoutError(f"PipeCraft job {run_id} did not finish within {timeout}s")
            time.sleep(max(0.02, poll_interval))

    def run(self, pipeline: str, **kwargs: Any) -> PipelineReport:
        run_id = self.submit(pipeline, **kwargs)
        job = self.wait(run_id)
        if job.status == "cancelled":
            raise PipeCraftCommandError(f"PipeCraft job {run_id} was cancelled")
        return self.report(run_id)

    def _cli_json(self, args: list[str], *, allow_failure: bool = False) -> dict[str, Any]:
        command = [str(self.binary), "--root", str(self.root), *args, "--json"]
        process = subprocess.run(command, capture_output=True, text=True)
        try:
            value = json.loads(process.stdout)
        except json.JSONDecodeError as error:
            raise PipeCraftCommandError("PipeCraft CLI returned non-JSON output", returncode=process.returncode, stderr=process.stderr) from error
        if process.returncode != 0 and not allow_failure:
            raise PipeCraftCommandError(str(value.get("errors", "PipeCraft command failed")), returncode=process.returncode, stderr=process.stderr)
        return value

    def list(self) -> list[str]:
        return [str(name) for name in self._cli_json(["list"]).get("pipelines", [])]

    def validate(self, pipeline: str) -> dict[str, Any]:
        return self._cli_json(["validate", pipeline], allow_failure=True)

    def plan(self, pipeline: str) -> list[str]:
        return [str(step) for step in self._cli_json(["plan", pipeline]).get("order", [])]
