//! S03: actual Unix NDJSON socket integration, not a direct service.handle mock.
#![cfg(unix)]

use std::path::Path;

use pipecraft_service::{RuntimeService, ServiceConfig};
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

async fn exchange(socket: &Path, request: Value) -> Value {
    let mut stream = UnixStream::connect(socket).await.unwrap();
    stream
        .write_all(format!("{request}\n").as_bytes())
        .await
        .unwrap();
    let mut line = String::new();
    let mut reader = BufReader::new(stream);
    reader.read_line(&mut line).await.unwrap();
    serde_json::from_str(&line).unwrap()
}

async fn service_socket(root: &Path) -> (std::path::PathBuf, tokio::task::JoinHandle<()>) {
    let service = RuntimeService::open(root.to_path_buf(), ServiceConfig::default())
        .await
        .unwrap();
    let socket = root.join("resident-s03.sock");
    let path = socket.clone();
    let task = tokio::spawn(async move {
        service.serve_unix(path).await.unwrap();
    });
    for _ in 0..200 {
        if tokio::fs::try_exists(&socket).await.unwrap_or(false) {
            return (socket, task);
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    task.abort();
    panic!("S03 resident service failed to bind socket");
}

#[tokio::test]
async fn fake_provider_and_legacy_client_roundtrip_over_real_ndjson_socket() {
    let root = tempfile::tempdir().unwrap();
    let (socket, task) = service_socket(root.path()).await;

    let ping = exchange(&socket, json!({"op": "ping"})).await;
    assert_eq!(ping["protocol"], "pipecraft.ipc/v1");
    assert_eq!(ping["ok"], true);
    assert_eq!(ping["data"]["service"], "pipecraft");

    let legacy = exchange(
        &socket,
        json!({
            "op": "plan_admission",
            "snapshot": {"revision": 1, "now_ms": 1_000_000, "capacity": 2,
                "candidates": [], "active": []}
        }),
    )
    .await;
    assert_eq!(legacy["ok"], true);
    assert_eq!(legacy["data"]["requires_confirmation"], true);

    let registered = exchange(
        &socket,
        json!({
            "op": "register_worker", "worker_id": "socket-worker",
            "capabilities": ["extracta.http"], "now_ms": 1_000_000, "ttl_ms": 10_000
        }),
    )
    .await;
    assert_eq!(registered["ok"], true);

    let feed = exchange(
        &socket,
        json!({
            "op": "plan_external_feed", "capacity": 1,
            "enforce_worker_registration": true,
            "message": {
                "op": "provider_snapshot",
                "protocol": "pipecraft.external-work/v1",
                "request_id": "s03-socket-1",
                "installation_id": "s03-socket",
                "provider_id": "extracta/fake/v1",
                "snapshot_revision": 1,
                "observed_at_ms": 1_000_000,
                "resource_evidence_revision": 1,
                "work": [{
                    "work_id": "HTTP_JOB:9", "owner_group": "source:9",
                    "queue_kind": "HTTP_JOB", "priority": 5,
                    "eligible_since_ms": 900_000, "ready_at_ms": 900_000,
                    "weight": 1, "source_limit": 1,
                    "scopes": [], "resources": [],
                    "required_worker_capability": "extracta.http"
                }],
                "active_claims": []
            }
        }),
    )
    .await;
    assert_eq!(feed["ok"], true);
    assert_eq!(feed["data"]["decision"]["offered"], json!(["HTTP_JOB:9"]));
    assert_eq!(
        feed["data"]["provisional_offers"][0]["requires_confirmation"],
        true
    );
    assert_eq!(feed["data"]["side_effects"], false);

    // The provider's fake claim is transmitted and validated, not minted by the service.
    let confirmation = exchange(
        &socket,
        json!({
            "op": "external_work_contract",
            "message": {
                "op": "offer_confirmation",
                "protocol": "pipecraft.external-work/v1",
                "request_id": "s03-socket-confirm",
                "installation_id": "s03-socket",
                "provider_id": "extracta/fake/v1",
                "offer_id": "fake-offer-9",
                "work_id": "HTTP_JOB:9",
                "snapshot_revision": 1,
                "result": "CONFIRMED",
                "claim_epoch": 7,
                "claim_id": "fake-claim-9",
                "lease_until_ms": 1_010_000,
                "assigned_worker_capability": "extracta.http",
                "operation_keys": ["fake:HTTP_JOB:9:attempt1"]
            }
        }),
    )
    .await;
    assert_eq!(confirmation["ok"], true);
    assert_eq!(confirmation["data"]["side_effects"], false);
    assert_eq!(confirmation["data"]["message"]["claim_epoch"], 7);

    let invalid = exchange(
        &socket,
        json!({
            "op": "external_work_contract",
            "message": {
                "op": "offer_confirmation",
                "protocol": "pipecraft.external-work/v1",
                "request_id": "s03-socket-bad",
                "installation_id": "s03-socket",
                "provider_id": "extracta/fake/v1",
                "offer_id": "fake-offer-bad",
                "work_id": "HTTP_JOB:9",
                "snapshot_revision": 1,
                "result": "CONFIRMED"
            }
        }),
    )
    .await;
    assert_eq!(invalid["ok"], false);
    assert_eq!(invalid["error"]["code"], "INVALID_EXTERNAL_WORK");

    task.abort();
    let _ = task.await;
}
