//! S02 true resident IPC tests for durable provider delta, restart and fail-closed evidence.
use pipecraft_service::{IpcRequest, RuntimeService, ServiceConfig};
use serde_json::{json, Value};

fn work(id: usize) -> Value {
    json!({
        "work_id": format!("HTTP_JOB:{id}"),
        "owner_group": "source:1",
        "queue_kind": "HTTP_JOB",
        "priority": 1,
        "eligible_since_ms": 900_000,
        "ready_at_ms": 900_000,
        "weight": 1,
        "source_limit": 1,
        "scopes": [],
        "resources": []
    })
}

fn snapshot(revision: u64, work: Vec<Value>) -> IpcRequest {
    serde_json::from_value(json!({
        "op": "plan_external_feed",
        "capacity": 3,
        "message": {
            "op": "provider_snapshot",
            "protocol": "pipecraft.external-work/v1",
            "request_id": "s02-snap",
            "installation_id": "s02-install",
            "provider_id": "extracta/acquisition/v1",
            "snapshot_revision": revision,
            "observed_at_ms": 1_000_000,
            "resource_evidence_revision": 1,
            "work": work,
            "active_claims": []
        }
    }))
    .unwrap()
}

fn delta(base: u64, revision: u64, upsert: Vec<Value>, removed: Vec<&str>) -> IpcRequest {
    serde_json::from_value(json!({
        "op": "plan_external_feed",
        "capacity": 3,
        "message": {
            "op": "provider_delta",
            "protocol": "pipecraft.external-work/v1",
            "request_id": "s02-delta",
            "installation_id": "s02-install",
            "provider_id": "extracta/acquisition/v1",
            "base_revision": base,
            "snapshot_revision": revision,
            "observed_at_ms": 1_000_100,
            "upsert": upsert,
            "removed_work_ids": removed
        }
    }))
    .unwrap()
}

#[tokio::test]
async fn accepted_delta_is_durable_and_rejected_stale_delta_cannot_rollback_revision() {
    let dir = tempfile::tempdir().unwrap();
    let service = RuntimeService::open(dir.path().to_path_buf(), ServiceConfig::default())
        .await
        .unwrap();
    let first = service.handle(snapshot(4, vec![work(1)])).await;
    assert!(first.ok, "{:?}", first.error);
    let changed = service
        .handle(delta(4, 5, vec![work(2)], vec!["HTTP_JOB:1"]))
        .await;
    assert!(changed.ok, "{:?}", changed.error);
    let data = changed.data.unwrap();
    assert_eq!(data["snapshot_revision"], 5);
    assert_eq!(data["decision"]["offered"], json!(["HTTP_JOB:2"]));
    assert_eq!(data["side_effects"], false);

    let missing_worker = serde_json::from_value(json!({
        "op": "plan_external_feed",
        "enforce_worker_registration": true,
        "capacity": 3,
        "message": {
            "op": "provider_snapshot",
            "protocol": "pipecraft.external-work/v1",
            "request_id": "s02-worker-guard",
            "installation_id": "guard-install",
            "provider_id": "extracta/guard/v1",
            "snapshot_revision": 1,
            "observed_at_ms": 1000000,
            "resource_evidence_revision": 1,
            "work": [{
                "work_id": "HTTP_JOB:99",
                "owner_group": "source:1",
                "queue_kind": "HTTP_JOB",
                "priority": 1,
                "eligible_since_ms": 900000,
                "ready_at_ms": 900000,
                "weight": 1,
                "source_limit": 1,
                "scopes": [],
                "resources": [],
                "required_worker_capability": "extracta.http"
            }],
            "active_claims": []
        }
    })).unwrap();
    let guard = service.handle(missing_worker).await;
    assert!(guard.ok, "{:?}", guard.error);
    assert!(guard.data.unwrap()["provisional_offers"].as_array().unwrap().is_empty());
    let stale = service.handle(delta(4, 6, vec![work(3)], vec![])).await;
    assert!(!stale.ok);
    let duplicate_removal = service
        .handle(delta(5, 6, vec![], vec!["HTTP_JOB:2", "HTTP_JOB:2"]))
        .await;
    assert!(!duplicate_removal.ok);
    drop(service);

    let restarted = RuntimeService::open(dir.path().to_path_buf(), ServiceConfig::default())
        .await
        .unwrap();
    let old = restarted.handle(delta(4, 6, vec![], vec![])).await;
    assert!(!old.ok, "restart must retain watermark");
    let next = restarted
        .handle(delta(5, 6, vec![work(3)], vec!["HTTP_JOB:2"]))
        .await;
    assert!(next.ok, "{:?}", next.error);
    assert_eq!(
        next.data.unwrap()["decision"]["offered"],
        json!(["HTTP_JOB:3"])
    );
}

#[tokio::test]
async fn invalid_or_oversized_feed_cannot_replace_valid_durable_state() {
    let dir = tempfile::tempdir().unwrap();
    let service = RuntimeService::open(dir.path().to_path_buf(), ServiceConfig::default())
        .await
        .unwrap();
    let first = service.handle(snapshot(9, vec![work(1)])).await;
    assert!(first.ok, "{:?}", first.error);
    let oversized = (1..=10_001).map(work).collect();
    let denied = service.handle(snapshot(10, oversized)).await;
    assert!(!denied.ok);
    assert_eq!(denied.error.unwrap().code, "INVALID_EXTERNAL_FEED");
    drop(service);

    let restarted = RuntimeService::open(dir.path().to_path_buf(), ServiceConfig::default())
        .await
        .unwrap();
    let next = restarted.handle(delta(9, 10, vec![work(2)], vec![])).await;
    assert!(next.ok, "{:?}", next.error);
    drop(restarted);

    tokio::fs::write(
        dir.path().join(".pipelines/runtime/external-feeds.json"),
        b"{corrupted",
    )
    .await
    .unwrap();
    let error = RuntimeService::open(dir.path().to_path_buf(), ServiceConfig::default())
        .await
        .err()
        .expect("corrupted feed must prevent startup");
    assert!(error.contains("invalid durable provider feed"), "{error}");
}
