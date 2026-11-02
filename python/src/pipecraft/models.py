"""Small typed views over JSON owned by the Rust PipeCraft runtime."""
from __future__ import annotations
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class Job:
    run_id: str
    status: str
    pipeline: str
    execute: bool = False
    attempts: int = 0
    message: str = ""
    report_path: str | None = None
    attempt_reports: list[str] = field(default_factory=list)
    pipeline_snapshot_path: str = ""
    runtime_version: str = ""
    events_path: str = ""
    state_path: str = ""
    warning: str | None = None

    @property
    def terminal(self) -> bool:
        return self.status in {"succeeded", "failed", "cancelled", "interrupted"}

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "Job":
        request = value.get("request") if isinstance(value.get("request"), dict) else {}
        return cls(
            run_id=str(value.get("run_id", "")),
            status=str(value.get("status", "")),
            pipeline=str(request.get("pipeline", value.get("pipeline", ""))),
            execute=bool(request.get("execute", False)),
            attempts=int(value.get("attempts", 0) or 0),
            message=str(value.get("message", "")),
            report_path=value.get("report_path"),
            attempt_reports=[str(item) for item in value.get("attempt_reports", [])],
            pipeline_snapshot_path=str(value.get("pipeline_snapshot_path", "")),
            runtime_version=str(value.get("runtime_version", "")),
            events_path=str(value.get("events_path", "")),
            state_path=str(value.get("state_path", "")),
            warning=value.get("warning"),
        )


@dataclass
class StepResult:
    step_id: str
    step_type: str
    success: bool
    status: str
    message: str
    output: str = ""
    data: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "StepResult":
        data = value.get("data", {})
        return cls(str(value.get("step_id", "")), str(value.get("type", value.get("step_type", ""))), bool(value.get("success", False)), str(value.get("status", "")), str(value.get("message", "")), str(value.get("output", "")), data if isinstance(data, dict) else {"value": data})


@dataclass
class PipelineReport:
    run_id: str
    pipeline: str
    success: bool
    dry_run: bool
    results: list[StepResult] = field(default_factory=list)
    order: list[str] = field(default_factory=list)
    run_dir: str = ""
    artifacts_dir: str = ""
    logs_dir: str = ""
    events_path: str = ""
    state_path: str = ""
    plan_fingerprint: str = ""
    max_workers: int = 1
    runtime: str = ""
    global_max_tasks: int = 1
    report_path: str | None = None

    @classmethod
    def from_dict(cls, value: dict[str, Any], *, report_path: str | None = None) -> "PipelineReport":
        return cls(
            run_id=str(value.get("run_id", "")), pipeline=str(value.get("pipeline", "")),
            success=bool(value.get("success", False)), dry_run=bool(value.get("dry_run", False)),
            results=[StepResult.from_dict(item) for item in value.get("results", []) if isinstance(item, dict)],
            order=[str(item) for item in value.get("order", [])], run_dir=str(value.get("run_dir", "")),
            artifacts_dir=str(value.get("artifacts_dir", "")), logs_dir=str(value.get("logs_dir", "")),
            events_path=str(value.get("events_path", "")), state_path=str(value.get("state_path", "")),
            plan_fingerprint=str(value.get("plan_fingerprint", "")), max_workers=int(value.get("max_workers", 1) or 1),
            runtime=str(value.get("runtime", "")), global_max_tasks=int(value.get("global_max_tasks", 1) or 1),
            report_path=report_path,
        )

    @classmethod
    def from_file(cls, path: str | Path) -> "PipelineReport":
        path = Path(path)
        return cls.from_dict(json.loads(path.read_text(encoding="utf-8")), report_path=str(path))

    def step(self, step_id: str) -> StepResult | None:
        return next((result for result in self.results if result.step_id == step_id), None)
