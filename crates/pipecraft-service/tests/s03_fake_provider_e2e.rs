//! S03 fake-provider end-to-end. No real network, Extracta DB, or external effects.
//! The fake provider, NOT PipeCraft, owns epochs, claims and durable fake receipts.
use std::collections::BTreeMap;

use pipecraft_service::{IpcRequest, RuntimeService, ServiceConfig};
use serde_json::{json, Value};

fn ipc(value: Value) -> IpcRequest {
    serde_json::from_value(value).unwrap()
}

fn provider_message(op: &str, extra: Value) -> Value {
    let mut value = json!({
        "op": op,
        "protocol": "pipecraft.external-work/v1",
        "request_id": "s03-request",
        "installation_id": "s03-install",
        "provider_id": "extracta/fake/v1"
    });
    value
        .as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    value
}

fn snapshot() -> Value {
    provider_message(
        "provider_snapshot",
        json!({
            "snapshot_revision": 1,
            "observed_at_ms": 1_000_000,
            "resource_evidence_revision": 1,
            "active_claims": [],
            "work": [{
                "work_id": "HTTP_JOB:7",
                "owner_group": "source:7",
                "queue_kind": "HTTP_JOB",
                "priority": 5,
                "eligible_since_ms": 900_000,
                "ready_at_ms": 900_000,
                "weight": 1,
                "source_limit": 1,
                "scopes": [],
                "resources": [],
                "required_worker_capability": "extracta.http"
            }]
        }),
    )
}

#[derive(Default)]
struct FakeProvider {
    // This test-local stand-in represents the authoritative provider's durable
    // idempotency ledger. It is deliberately NOT PipeCraft scheduler state.
    receipts: BTreeMap<String, String>,
    executed: usize,
}

impl FakeProvider {
    fn confirm(&self, offer_id: &str, eligible: bool) -> Value {
        provider_message(
            "offer_confirmation",
            if eligible {
                json!({
                    "offer_id": offer_id,
                    "work_id": "HTTP_JOB:7",
                    "snapshot_revision": 1,
                    "result": "CONFIRMED",
                    "claim_epoch": 1,
                    "claim_id": "fenced-claim-1",
                    "lease_until_ms": 1_020_000,
                    "assigned_worker_capability": "extracta.http",
                    "operation_keys": ["fake:HTTP_JOB:7:attempt1"]
                })
            } else {
                json!({
                    "offer_id": offer_id,
                    "work_id": "HTTP_JOB:7",
                    "snapshot_revision": 1,
                    "result": "POLICY_BLOCKED"
                })
            },
        )
    }

    fn execute_once(&mut self, confirmation: &Value) -> Result<String, &'static str> {
        if confirmation["result"] != "CONFIRMED"
            || confirmation["claim_epoch"] != 1
            || confirmation["claim_id"] != "fenced-claim-1"
            || confirmation["work_id"] != "HTTP_JOB:7"
        {
            return Err("not a confirmed current fake claim");
        }
        let operation = confirmation["operation_keys"][0]
            .as_str()
            .ok_or("missing fake operation key")?;
        if let Some(receipt) = self.receipts.get(operation) {
            return Ok(receipt.clone());
        }
        self.executed += 1;
        let receipt = format!("fake-receipt-{}", self.executed);
        self.receipts.insert(operation.to_string(), receipt.clone());
        Ok(receipt)
    }

    fn outcome(&self, receipt: String) -> Value {
        provider_message(
            "verified_outcome",
            json!({
                "work_id": "HTTP_JOB:7",
                "attempt_id": "attempt1",
                "claim_epoch": 1,
                "status": "SUCCEEDED_VERIFIED",
                "receipt_id": receipt,
                "operation_key": "fake:HTTP_JOB:7:attempt1",
                "result_digest": format!("sha256:{}", "a".repeat(64))
            }),
        )
    }
}

#[tokio::test]
async fn fake_provider_executes_only_after_confirm_and_lost_ack_does_not_duplicate() {
    let root = tempfile::tempdir().unwrap();
    let service = RuntimeService::open(root.path().to_path_buf(), ServiceConfig::default())
        .await
        .unwrap();
    let register = service
        .handle(ipc(json!({
            "op": "register_worker", "worker_id": "s03-worker",
            "capabilities": ["extracta.http"], "now_ms": 1_000_000, "ttl_ms": 20_000
        })))
        .await;
    assert!(register.ok, "{:?}", register.error);

    let feed = service
        .handle(ipc(json!({
            "op": "plan_external_feed", "message": snapshot(),
            "capacity": 1, "enforce_worker_registration": true
        })))
        .await;
    assert!(feed.ok, "{:?}", feed.error);
    let planned = feed.data.unwrap();
    assert_eq!(planned["decision"]["offered"], json!(["HTTP_JOB:7"]));
    assert_eq!(planned["side_effects"], false);
    assert_eq!(
        planned["provisional_offers"][0]["requires_confirmation"],
        true
    );
    assert!(planned.get("claim_epoch").is_none());
    assert!(planned.get("receipt_id").is_none());

    let mut provider = FakeProvider::default();
    let refused = provider.confirm("offer-s03-7", false);
    let refused_ipc = service
        .handle(ipc(json!({
            "op": "external_work_contract", "message": refused
        })))
        .await;
    assert!(refused_ipc.ok);
    assert!(provider.execute_once(&refused).is_err());
    assert_eq!(provider.executed, 0);

    let confirmation = provider.confirm("offer-s03-7", true);
    let confirmed = service
        .handle(ipc(json!({
            "op": "external_work_contract", "message": confirmation
        })))
        .await;
    assert!(confirmed.ok, "{:?}", confirmed.error);
    assert_eq!(confirmed.data.unwrap()["side_effects"], false);
    let receipt = provider.execute_once(&confirmation).unwrap();
    assert_eq!(provider.executed, 1);

    // ACK of the fake effect is lost. The provider replays the same key.
    let replayed = provider.execute_once(&confirmation).unwrap();
    assert_eq!(receipt, replayed);
    assert_eq!(provider.executed, 1);
    let verified = service
        .handle(ipc(json!({
            "op": "external_work_contract", "message": provider.outcome(replayed)
        })))
        .await;
    assert!(verified.ok, "{:?}", verified.error);
    let data = verified.data.unwrap();
    assert_eq!(data["message"]["status"], "SUCCEEDED_VERIFIED");
    assert_eq!(data["message"]["receipt_id"], "fake-receipt-1");
    assert_eq!(data["side_effects"], false);
}

#[tokio::test]
async fn malformed_claim_and_old_ipc_request_remain_fail_closed_compatible() {
    let root = tempfile::tempdir().unwrap();
    let service = RuntimeService::open(root.path().to_path_buf(), ServiceConfig::default())
        .await
        .unwrap();

    let malformed = provider_message(
        "offer_confirmation",
        json!({
            "offer_id": "offer-bad",
            "work_id": "HTTP_JOB:7",
            "snapshot_revision": 1,
            "result": "CONFIRMED",
            "claim_epoch": 1,
            "claim_id": "fenced-claim-1",
            "lease_until_ms": 1_020_000,
            "assigned_worker_capability": "extracta.http",
            "operation_keys": []
        }),
    );
    let denied = service
        .handle(ipc(json!({
            "op": "external_work_contract", "message": malformed
        })))
        .await;
    assert!(!denied.ok);
    assert_eq!(denied.error.unwrap().code, "INVALID_EXTERNAL_WORK");

    let ping = service.handle(ipc(json!({"op": "ping"}))).await;
    assert!(ping.ok);
    let pong = ping.data.unwrap();
    assert_eq!(pong["protocol"], "pipecraft.ipc/v1");
    assert!(pong["capabilities"]
        .as_array()
        .unwrap()
        .iter()
        .any(|v| v == "submit_spec"));
    assert!(pong["capabilities"]
        .as_array()
        .unwrap()
        .iter()
        .any(|v| v == "plan_admission"));

    // Older client sends only the old admission shape, no S02 fields.
    let legacy = service
        .handle(ipc(json!({
            "op": "plan_admission",
            "snapshot": {
                "revision": 1, "now_ms": 1_000_000, "capacity": 2,
                "candidates": [], "active": []
            }
        })))
        .await;
    assert!(legacy.ok, "{:?}", legacy.error);
    assert_eq!(legacy.data.unwrap()["requires_confirmation"], true);

    // The older service request shape remains parseable without new fields.
    assert!(matches!(
        ipc(json!({"op": "submit", "pipeline": "legacy-pipeline"})),
        IpcRequest::Submit { .. }
    ));
}

#[tokio::test]
async fn fake_provider_expired_worker_does_not_propose_live_work() {
    let root = tempfile::tempdir().unwrap();
    let service = RuntimeService::open(root.path().to_path_buf(), ServiceConfig::default())
        .await
        .unwrap();
    assert!(
        service
            .handle(ipc(json!({
                "op": "register_worker", "worker_id": "s03-worker",
                "capabilities": ["extracta.http"], "now_ms": 990_000, "ttl_ms": 1_000
            })))
            .await
            .ok
    );
    let denied = service
        .handle(ipc(json!({
            "op": "plan_external_feed", "message": snapshot(),
            "capacity": 1, "enforce_worker_registration": true
        })))
        .await;
    assert!(denied.ok, "{:?}", denied.error);
    assert!(denied.data.unwrap()["provisional_offers"]
        .as_array()
        .unwrap()
        .is_empty());
}
