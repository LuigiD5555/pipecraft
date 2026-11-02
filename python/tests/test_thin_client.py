import json
import socket
import threading
import time
from pathlib import Path

from pipecraft import Job, PipeCraft


def _unix_server(path: Path, responses: list[dict]):
    def serve():
        if path.exists():
            path.unlink()
        server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        server.bind(str(path))
        server.listen(len(responses))
        try:
            for response in responses:
                conn, _ = server.accept()
                with conn:
                    conn.recv(65536)
                    conn.sendall((json.dumps(response) + "\n").encode())
        finally:
            server.close()
    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    for _ in range(100):
        if path.exists():
            break
        time.sleep(0.005)
    return thread


def test_ipc_ping_and_status(tmp_path):
    endpoint = tmp_path / "pc.sock"
    responses = [
        {"protocol": "pipecraft.ipc/v1", "ok": True, "data": {"service": "pipecraft"}},
        {"protocol": "pipecraft.ipc/v1", "ok": True, "data": {"run_id": "r1", "status": "running", "request": {"pipeline": "demo", "execute": True}, "attempts": 1}},
    ]
    thread = _unix_server(endpoint, responses)
    pc = PipeCraft(tmp_path, endpoint=str(endpoint))
    assert pc.ping()["service"] == "pipecraft"
    job = pc.status("r1")
    assert isinstance(job, Job)
    assert job.pipeline == "demo"
    assert not job.terminal
    thread.join(timeout=2)


def test_job_terminal_states():
    assert Job("a", "succeeded", "p").terminal
    assert Job("a", "interrupted", "p").terminal
    assert not Job("a", "running", "p").terminal


def test_inline_spec_and_admission_use_structured_ipc(monkeypatch, tmp_path):
    pc = PipeCraft(tmp_path)
    requests = []
    def capture(payload):
        requests.append(payload)
        if payload["op"] == "submit_spec":
            return {"run_id": "inline-1"}
        if payload["op"] == "plan_admission":
            return {"offered": ["work-1"], "requires_confirmation": True}
        return {"valid": True, "order": ["a"]}
    monkeypatch.setattr(pc, "_ipc", capture)
    spec = {"schema_version": "pipecraft/v1", "name": "demo", "steps": []}
    assert pc.validate_spec(spec)["valid"]
    assert pc.plan_spec(spec)["order"] == ["a"]
    assert pc.submit_spec(spec) == "inline-1"
    snapshot = {"revision": 1, "now_ms": 100, "candidates": []}
    result = pc.plan_admission(snapshot)
    assert result["requires_confirmation"] is True
    assert [item["op"] for item in requests] == ["validate_spec", "plan_spec", "submit_spec", "plan_admission"]



def test_external_work_contract_uses_structured_ipc_without_side_effects(monkeypatch, tmp_path):
    pc = PipeCraft(tmp_path)
    requests = []

    def capture(payload):
        requests.append(payload)
        return {
            "protocol": "pipecraft.external-work/v1",
            "simulated": True,
            "side_effects": False,
            "message": payload["message"],
        }

    monkeypatch.setattr(pc, "_ipc", capture)
    message = {
        "op": "provider_snapshot",
        "protocol": "pipecraft.external-work/v1",
        "request_id": "req-python-1",
        "installation_id": "install-demo",
        "provider_id": "extracta/acquisition/v1",
        "snapshot_revision": 1,
        "observed_at_ms": 100,
        "work": [],
        "active_claims": [],
        "resource_evidence_revision": 1,
    }
    result = pc.external_work_contract(message)
    assert result["simulated"] is True
    assert result["side_effects"] is False
    assert requests == [{"op": "external_work_contract", "message": message}]
