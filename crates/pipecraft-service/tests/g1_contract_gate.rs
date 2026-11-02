use serde_json::Value;
use std::fs;
use std::path::PathBuf;

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf()
}

fn read_json(path: &str) -> Value {
    serde_json::from_str(&fs::read_to_string(root().join(path)).unwrap()).unwrap()
}

#[test]
fn g1_gate_accepts_all_required_p0_adrs() {
    let gate = read_json("docs/gates/G1_EXTERNAL_WORK_CONTRACT.json");
    let golden = read_json("tests/golden/g1-external-work-acceptance.json");

    assert_eq!(gate["gate"], "G1");
    assert_eq!(gate["status"], "PASS_CANDIDATE");
    assert_eq!(gate["cutover_enabled"], false);

    let required = ["ADR-01", "ADR-03", "ADR-04", "ADR-05", "ADR-12"];
    let accepted = gate["accepted_adrs"].as_array().unwrap();
    for adr in required {
        assert!(accepted.iter().any(|value| value == adr), "{adr} missing");
        assert!(
            golden["accepted_adrs"].get(adr).is_some(),
            "{adr} missing from golden acceptance"
        );
    }
}

#[test]
fn accepted_adr_documents_are_not_pending() {
    for path in [
        "docs/adrs/ADR-01-external-provider-transport.md",
        "docs/adrs/ADR-03-unified-resource-semantics.md",
        "docs/adrs/ADR-04-durable-claim-owner.md",
        "docs/adrs/ADR-05-leader-fencing.md",
        "docs/adrs/ADR-12-api-compatibility.md",
    ] {
        let text = fs::read_to_string(root().join(path)).unwrap();
        assert!(
            text.contains("Status: **accepted**"),
            "{path} is not accepted"
        );
        assert!(
            !text.contains("Status: **pending**"),
            "{path} reverted to pending"
        );
    }
}

#[test]
fn g1_golden_preserves_authority_and_replay_invariants() {
    let golden = read_json("tests/golden/g1-external-work-acceptance.json");
    assert_eq!(golden["protocol"]["legacy"], "pipecraft.ipc/v1");
    assert_eq!(golden["protocol"]["external"], "pipecraft.external-work/v1");
    assert_eq!(golden["protocol"]["contract_only"], true);
    assert_eq!(golden["protocol"]["side_effects"], false);
    assert_eq!(golden["authority"]["durable_claim"], "PostgreSQL");
    assert_eq!(golden["authority"]["leadership_epoch"], "PostgreSQL");
    assert_eq!(golden["authority"]["claim_epoch"], "PostgreSQL");
    assert_eq!(golden["replay"]["uncertain_auto_retry"], false);
    assert_eq!(golden["replay"]["receipt_authoritative"], true);
}
