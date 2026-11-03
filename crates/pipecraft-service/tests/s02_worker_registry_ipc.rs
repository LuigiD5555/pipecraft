//! S02 resident IPC registration is advisory and never grants a provider claim.
use pipecraft_service::{IpcRequest, RuntimeService, ServiceConfig};
use serde_json::json;

#[tokio::test]
async fn worker_registration_ttl_cancellation_and_capabilities_over_real_ipc() {
    let root = tempfile::tempdir().unwrap();
    let service = RuntimeService::open(root.path().to_path_buf(), ServiceConfig::default())
        .await
        .unwrap();
    let ping = service.handle(IpcRequest::Ping).await;
    assert!(ping.ok);
    assert!(ping.data.unwrap()["capabilities"]
        .as_array()
        .unwrap()
        .iter()
        .any(|value| value == "register_worker"));

    let register: IpcRequest = serde_json::from_value(json!({
        "op": "register_worker",
        "worker_id": "worker:1",
        "capabilities": ["extracta.http"],
        "now_ms": 1000000,
        "ttl_ms": 5000
    }))
    .unwrap();
    let accepted = service.handle(register).await;
    assert!(accepted.ok, "{:?}", accepted.error);
    let data = accepted.data.unwrap();
    assert_eq!(data["worker"]["expires_at_ms"], 1005000);
    assert_eq!(data["side_effects"], false);
    assert!(data.get("claim_epoch").is_none());
    assert!(data.get("leadership_epoch").is_none());

    let available: IpcRequest = serde_json::from_value(json!({
        "op": "available_workers", "capability": "extracta.http", "now_ms": 1004000
    }))
    .unwrap();
    let result = service.handle(available).await;
    assert!(result.ok);
    assert_eq!(result.data.unwrap()["workers"].as_array().unwrap().len(), 1);

    let cancel: IpcRequest = serde_json::from_value(json!({
        "op": "cancel_worker", "worker_id": "worker:1", "now_ms": 1004000
    }))
    .unwrap();
    let result = service.handle(cancel).await;
    assert!(result.ok);
    assert_eq!(result.data.unwrap()["worker"]["cancelled"], true);

    let available: IpcRequest = serde_json::from_value(json!({
        "op": "available_workers", "capability": "extracta.http", "now_ms": 1004001
    }))
    .unwrap();
    let result = service.handle(available).await;
    assert!(result.ok);
    assert!(result.data.unwrap()["workers"]
        .as_array()
        .unwrap()
        .is_empty());
}
