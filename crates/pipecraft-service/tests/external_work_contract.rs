use serde_json::Value;

fn schema() -> Value {
    serde_json::from_str(include_str!(
        "../../../schemas/external-work-v1.schema.json"
    ))
    .unwrap()
}

fn golden() -> Value {
    serde_json::from_str(include_str!(
        "../../../tests/golden/external-work-v1-contract.json"
    ))
    .unwrap()
}

#[test]
fn canonical_external_work_contract_matches_golden_vocabulary() {
    let schema = schema();
    let golden = golden();
    let defs = schema.get("$defs").and_then(Value::as_object).unwrap();

    let pairs = [
        ("work_state", "work_states"),
        ("offer_confirmation_result", "offer_confirmation_results"),
        ("verified_outcome_status", "verified_outcome_statuses"),
        ("semantic_error_code", "semantic_error_codes"),
    ];

    for (definition, golden_key) in pairs {
        assert_eq!(
            defs[definition].get("enum"),
            golden.get(golden_key),
            "{definition} drifted from the accepted golden contract"
        );
    }
}

#[test]
fn canonical_identity_types_keep_domain_and_security_invariants() {
    let schema = schema();
    let golden = golden();
    let defs = schema.get("$defs").and_then(Value::as_object).unwrap();
    let ids = golden.get("identities").and_then(Value::as_object).unwrap();

    for required in [
        "installation_id",
        "provider_id",
        "work_id",
        "pipeline_run_id",
        "attempt_id",
        "offer_id",
        "claim_epoch",
        "leadership_epoch",
        "operation_key",
        "receipt_id",
        "snapshot_revision",
    ] {
        assert!(
            ids.contains_key(required),
            "golden identity missing {required}"
        );
        assert!(
            defs.contains_key(required),
            "schema definition missing {required}"
        );
    }

    assert!(ids["provider_id"].as_str().unwrap().contains('/'));
    assert!(ids["work_id"].as_str().unwrap().contains(':'));
    assert!(ids["claim_epoch"].as_u64().unwrap() > 0);
    assert!(ids["leadership_epoch"].as_u64().unwrap() > 0);

    let owner_token = &defs["owner_token"];
    assert_eq!(
        owner_token.get("writeOnly").and_then(Value::as_bool),
        Some(true)
    );
    assert_eq!(
        owner_token.get("x-sensitive").and_then(Value::as_bool),
        Some(true)
    );
    assert!(
        !ids.contains_key("owner_token"),
        "secret owner_token must never appear in the generic golden decision fixture"
    );
}

#[test]
fn external_work_success_is_verified_not_process_exit_success() {
    let golden = golden();
    let outcomes = golden["verified_outcome_statuses"].as_array().unwrap();
    assert!(outcomes.iter().any(|v| v == "SUCCEEDED_VERIFIED"));
    assert!(outcomes.iter().any(|v| v == "UNCERTAIN"));
    assert!(!outcomes.iter().any(|v| v == "SUCCEEDED"));
}

#[test]
fn resource_and_replay_golden_stays_in_sync_with_schema() {
    let schema = schema();
    let golden = golden();
    let defs = schema.get("$defs").and_then(Value::as_object).unwrap();
    let resource = golden
        .get("resource_contract")
        .and_then(Value::as_object)
        .expect("golden resource_contract");

    let schema_modes = defs["resource_share_mode"]["enum"].as_array().unwrap();
    let demand_mode = resource["demand"]["share_mode"].as_str().unwrap();
    assert!(
        schema_modes.iter().any(|value| value == demand_mode),
        "golden resource share mode is not allowed by schema"
    );

    assert_eq!(
        defs["replay_disposition"].get("enum"),
        resource.get("replay_dispositions"),
        "replay dispositions drifted from golden contract"
    );

    let claim_fence = defs["claim_fence"].as_object().unwrap();
    let required = claim_fence["required"].as_array().unwrap();
    assert!(required.iter().any(|value| value == "owner_token"));
    assert_eq!(
        defs["owner_token"]
            .get("writeOnly")
            .and_then(Value::as_bool),
        Some(true)
    );
}
