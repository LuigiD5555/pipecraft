//! Cross-repository first-offer parity against Extracta SchedulingEngine::decide.
use pipecraft_runtime::admission::{decide, AdmissionSnapshot};
use serde::Deserialize;

#[derive(Deserialize)]
struct Golden {
    cases: Vec<Case>,
}
#[derive(Deserialize)]
struct Case {
    id: String,
    snapshot: AdmissionSnapshot,
    expected_first: Option<String>,
}
#[test]
fn extracta_first_offer_matches_fair_admission() {
    let golden: Golden = serde_json::from_str(include_str!(
        "../../../tests/golden/extracta-admission-v1.json"
    ))
    .expect("valid A03 cross-repository fixtures");
    assert!(golden.cases.len() >= 15);
    for case in golden.cases {
        let decision = decide(&case.snapshot).unwrap_or_else(|e| panic!("{}: {e}", case.id));
        assert_eq!(
            decision.offered.first().cloned(),
            case.expected_first,
            "first-offer parity: {}",
            case.id
        );
        assert_eq!(
            decision,
            decide(&case.snapshot).unwrap(),
            "determinism: {}",
            case.id
        );
        assert!(
            decision.requires_confirmation,
            "{}: offer is advisory",
            case.id
        );
    }
}
