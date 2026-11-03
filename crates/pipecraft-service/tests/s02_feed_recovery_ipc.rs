//! S02: exercise the real resident IPC and durable provider feed together.
use pipecraft_service::{IpcRequest, RuntimeService, ServiceConfig};
use serde_json::{json, Value};

fn candidate(work_id: &str) -> Value {
    json!({
        "work_id": work_id,
        "owner_group": "source:1",
        "queue_kind": "HTTP_JOB",
        "priority": 10,
        "eligible_since_ms": 900_000,
        "ready_at_ms": 900_000,
        "weight": 1,
        "source_limit": 2,
        "scopes": [],
        "resources": [],
        "required_worker_capability": "extracta.http"
    })
}

fn snapshot(revision: u64, observed_at_ms: u64, work: Vec<Value>, enforce: bool) -> IpcRequest {
    serde_json::from_value(json!({
        "op": "plan_external_feed",
        "capacity": 2,
        "enforce_worker_registration": enforce,
        "message": {
            "op": "provider_snapshot",
            "protocol": "pipecraft.external-work/v1",
            "request_id": format!("snapshot-{revision}"),
            "installation_id": "install-s02",
            "provider_id": "extracta/acquisition/v1",
            "snapshot_revision": revision,
            "observed_at_ms": observed_at_ms,
            "resource_evidence_revision": 1,
            "work": work,
            "active_claims": []
        }
    }))
    .unwrap()
}

fn delta(base: u64, revision: u64) -> IpcRequest {
    serde_json::from_value(json!({
        "op": "plan_external_feed",
        "capacity": 2,
        "message": {
            "op": "provider_delta",
            "protocol": "pipecraft.external-work/v1",
            "request_id": format!("delta-{revision}"),
            "installation_id": "install-s02",
            "provider_id": "extracta/acquisition/v1",
            "base_revision": base,
            "snapshot_revision": revision,
            "observed_at_ms": 1_001_000,
            "upsert": [candidate("HTTP_JOB:2")],
            "removed_work_ids": ["HTTP_JOB:1"]
        }
    }))
    .unwrap()
}

#[tokio::test]
async fn delta_is_durable_and_restart_rejects_stale_provider_revision() {
    let root = tempfile::tempdir().unwrap();
    let service = RuntimeService::open(root.path().to_path_buf(), ServiceConfig::default())
        .await
        .unwrap();
    let first = service
        .handle(snapshot(4, 1_000_000, vec![candidate("HTTP_JOB:1")], false))
        .await;
    assert!(first.ok, "{:?}", first.error);
    assert_eq!(first.data.unwrap()["decision"]["offered"], json!(["HTTP_JOB:1"]));

    let applied = service.handle(delta(4, 5)).await;
    assert!(applied.ok, "{:?}", applied.error);
    let data = applied.data.unwrap();
    assert_eq!(data["snapshot_revision"], 5);
    assert_eq!(data["decision"]["offered"], json!(["HTTP_JOB:2"]));
    assert_eq!(data["side_effects"], false);
    assert!(data.get("claim_epoch").is_none());
    assert!(data.get("receipt_id").is_none());

    let wrong_base = service.handle(delta(4, 6)).await;
    assert!(!wrong_base.ok);
    assert_eq!(wrong_base.error.unwrap().code, "INVALID_EXTERNAL_FEED");
    drop(service);

    let reopened = RuntimeService::open(root.path().to_path_buf(), ServiceConfig::default())
        .await
        .unwrap();
    let stale = reopened
        .handle(snapshot(4, 1_000_000, vec![candidate("HTTP_JOB:1")], false))
        .await;
    assert!(!stale.ok);
    assert_eq!(stale.error.unwrap().code, "INVALID_EXTERNAL_FEED");
    let rejected = reopened.handle(delta(4, 6)).await;
    assert!(!rejected.ok);
    let recovered = reopened
        .handle(snapshot(5, 1_001_000, vec![candidate("HTTP_JOB:2")], false))
        .await;
    // A same-revision snapshot may not replace a different durable request identity.
    assert!(!recovered.ok);
    drop(reopened);

    let durable_path = root.path().join(".pipelines/runtime/external-feeds.json");
    std::fs::write(durable_path, b"corrupt provider feed").unwrap();
    assert!(RuntimeService::open(root.path().to_path_buf(), ServiceConfig::default())
        .await
        .is_err());
}

#[tokio::test]
async fn enforced_workers_expire_and_cancel_without_authorizing_execution() {
    let root = tempfile::tempdir().unwrap();
    let service = RuntimeService::open(root.path().to_path_buf(), ServiceConfig::default())
        .await
        .unwrap();
    let missing = service
        .handle(snapshot(1, 1_000_000, vec![candidate("HTTP_JOB:1")], true))
        .await;
    assert!(missing.ok, "{:?}", missing.error);
    assert!(missing.data.unwrap()["decision"]["offered"]
        .as_array()
        .unwrap()
        .is_empty());

    let register: IpcRequest = serde_json::from_value(json!({
        "op": "register_worker",
        "worker_id": "worker:1",
        "capabilities": ["extracta.http"],
        "now_ms": 1_000_000,
        "ttl_ms": 5_000
    }))
    .unwrap();
    assert!(service.handle(register).await.ok);
    let offered = service
        .handle(snapshot(2, 1_001_000, vec![candidate("HTTP_JOB:1")], true))
        .await;
    assert!(offered.ok, "{:?}", offered.error);
    assert_eq!(offered.data.unwrap()["decision"]["offered"], json!(["HTTP_JOB:1"]));

    let expired = service
        .handle(snapshot(3, 1_006_000, vec![candidate("HTTP_JOB:1")], true))
        .await;
    assert!(expired.ok, "{:?}", expired.error);
    assert!(expired.data.unwrap()["decision"]["offered"]
        .as_array()
        .unwrap()
        .is_empty());
    drop(service);

    // Worker registrations are ephemeral: restart must not invent an active worker.
    let reopened = RuntimeService::open(root.path().to_path_buf(), ServiceConfig::default())
        .await
        .unwrap();
    let after_restart = reopened
        .handle(snapshot(4, 1_007_000, vec![candidate("HTTP_JOB:1")], true))
        .await;
    assert!(after_restart.ok, "{:?}", after_restart.error);
    assert!(after_restart.data.unwrap()["decision"]["offered"]
        .as_array()
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn oversized_provider_feed_is_refused_without_persisting_revision() {
    let root = tempfile::tempdir().unwrap();
    let service = RuntimeService::open(root.path().to_path_buf(), ServiceConfig::default())
        .await
        .unwrap();
    let oversized: Vec<Value> = (1..=10_001)
        .map(|n| candidate(&format!("HTTP_JOB:{n}")))
        .collect();
    let denied = service.handle(snapshot(100, 1_000_000, oversized, false)).await;
    assert!(!denied.ok);
    assert_eq!(denied.error.unwrap().code, "INVALID_EXTERNAL_FEED");

    let valid = service
        .handle(snapshot(1, 1_000_000, vec![candidate("HTTP_JOB:1")], false))
        .await;
    assert!(valid.ok, "{:?}", valid.error);
}
