use pipecraft_service::protocol::{
    validate_external_work_message, ExternalWorkMessage, OfferConfirmationResult,
    EXTERNAL_WORK_PROTOCOL_VERSION,
};
use serde_json::json;

fn provider_snapshot() -> serde_json::Value {
    json!({
        "op": "provider_snapshot",
        "protocol": EXTERNAL_WORK_PROTOCOL_VERSION,
        "request_id": "req-roundtrip-1",
        "installation_id": "install-demo",
        "provider_id": "extracta/acquisition/v1",
        "snapshot_revision": 1009,
        "observed_at_ms": 1793635200000u64,
        "work": [{
            "work_id": "EXTRACTOR_RUN:821",
            "owner_group": "source:42",
            "queue_kind": "EXTRACTOR_RUN",
            "priority": 100,
            "eligible_since_ms": 1793635100000u64,
            "ready_at_ms": 1793635100000u64,
            "weight": 1,
            "scopes": ["source:42"],
            "resources": [{
                "resource_key": "browser-profile:19",
                "points": 3,
                "quantity": 1,
                "share_mode": "EXCLUSIVE",
                "limit": 1
            }],
            "required_worker_capability": "browser",
            "source_limit": 1
        }],
        "active_claims": [],
        "resource_evidence_revision": 501
    })
}

#[test]
fn provider_snapshot_roundtrips_through_typed_rust_contract() {
    let value = provider_snapshot();
    let message: ExternalWorkMessage = serde_json::from_value(value.clone()).unwrap();
    validate_external_work_message(&message).unwrap();
    let encoded = serde_json::to_value(&message).unwrap();
    assert_eq!(encoded, value);
}

#[test]
fn duplicate_work_ids_fail_closed() {
    let mut value = provider_snapshot();
    let first = value["work"][0].clone();
    value["work"].as_array_mut().unwrap().push(first);
    let message: ExternalWorkMessage = serde_json::from_value(value).unwrap();
    assert_eq!(
        validate_external_work_message(&message).unwrap_err(),
        "duplicate work_id in provider snapshot"
    );
}

#[test]
fn confirmed_offer_requires_authoritative_claim_fields() {
    let message = ExternalWorkMessage::OfferConfirmation {
        protocol: EXTERNAL_WORK_PROTOCOL_VERSION.to_string(),
        request_id: "req-confirm-1".into(),
        installation_id: "install-demo".into(),
        provider_id: "extracta/acquisition/v1".into(),
        offer_id: "offer-1".into(),
        work_id: "EXTRACTOR_RUN:821".into(),
        snapshot_revision: 1009,
        result: OfferConfirmationResult::Confirmed,
        claim_epoch: None,
        claim_id: None,
        lease_until_ms: None,
        assigned_worker_capability: None,
        operation_keys: vec![],
    };
    assert!(
        validate_external_work_message(&message)
            .unwrap_err()
            .contains("missing claim fields")
    );
}

#[test]
fn verified_success_requires_receipt_and_valid_digest() {
    let value = json!({
        "op": "verified_outcome",
        "protocol": EXTERNAL_WORK_PROTOCOL_VERSION,
        "request_id": "req-outcome-1",
        "installation_id": "install-demo",
        "provider_id": "extracta/acquisition/v1",
        "work_id": "EXTRACTOR_RUN:821",
        "attempt_id": "attempt-2",
        "claim_epoch": 22,
        "status": "SUCCEEDED_VERIFIED",
        "receipt_id": "receipt-991",
        "operation_key": "extractor-run-821/capture/primary",
        "result_digest": format!("sha256:{}", "a".repeat(64))
    });
    let message: ExternalWorkMessage = serde_json::from_value(value).unwrap();
    validate_external_work_message(&message).unwrap();
}
