//! S01 public IPC smoke over the resident service, not merely the pure driver.
use pipecraft_service::{IpcRequest, RuntimeService, ServiceConfig};
use serde_json::json;

fn snapshot(revision: u64) -> serde_json::Value {
    json!({
        "op": "plan_external_feed",
        "capacity": 1,
        "message": {
            "op": "provider_snapshot",
            "protocol": "pipecraft.external-work/v1",
            "request_id": "req-s01",
            "installation_id": "install-s01",
            "provider_id": "extracta/acquisition/v1",
            "snapshot_revision": revision,
            "observed_at_ms": 1_000_000,
            "resource_evidence_revision": 1,
            "work": [{
                "work_id": "HTTP_JOB:1",
                "owner_group": "source:5",
                "queue_kind": "HTTP_JOB",
                "priority": 100,
                "eligible_since_ms": 900_000,
                "ready_at_ms": 900_000,
                "weight": 1,
                "source_limit": 1,
                "scopes": [],
                "resources": [],
                "required_worker_capability": "extracta.http"
            }],
            "active_claims": []
        }
    })
}

#[tokio::test]
async fn resident_ipc_admits_snapshot_and_returns_only_provisional_offers() {
    let root = tempfile::tempdir().unwrap();
    let service = RuntimeService::open(
        root.path().to_path_buf(),
        ServiceConfig::default(),
    )
    .await
    .unwrap();
    let ping = service.handle(IpcRequest::Ping).await;
    assert!(ping.ok);
    assert!(ping.data.unwrap()["capabilities"]
        .as_array()
        .unwrap()
        .iter()
        .any(|value| value == "plan_external_feed"));

    let request: IpcRequest = serde_json::from_value(snapshot(21)).unwrap();
    let result = service.handle(request).await;
    assert!(result.ok, "{:?}", result.error);
    let data = result.data.unwrap();
    assert_eq!(data["snapshot_revision"], 21);
    assert_eq!(data["decision"]["offered"], json!(["HTTP_JOB:1"]));
    assert_eq!(data["provisional_offers"][0]["required_worker_capability"], "extracta.http");
    assert_eq!(data["provisional_offers"][0]["requires_confirmation"], true);
    assert_eq!(data["side_effects"], false);
    assert!(data.get("claim_epoch").is_none());
    assert!(data.get("leadership_epoch").is_none());
    assert!(data.get("receipt_id").is_none());

    let stale: IpcRequest = serde_json::from_value(snapshot(20)).unwrap();
    let rejected = service.handle(stale).await;
    assert!(!rejected.ok);
    assert_eq!(rejected.error.unwrap().code, "INVALID_EXTERNAL_FEED");
}
