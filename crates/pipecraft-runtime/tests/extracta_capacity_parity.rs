//! Cross-repository golden vectors: the same file is exercised by Extracta's
//! Python effective_capacity oracle on its integration branch. Both repositories
//! must retain identical case data; neither implementation is the test oracle
//! for itself.
use pipecraft_runtime::capacity::{calculate, CapacityEvidence, CapacityProfile, CapacityResult};
use serde::Deserialize;

#[derive(Deserialize)]
struct Golden {
    cases: Vec<Case>,
}
#[derive(Deserialize)]
struct Case {
    id: String,
    profile: CapacityProfile,
    evidence: CapacityEvidence,
    expected: CapacityResult,
}

#[test]
fn extracta_capacity_golden_matches_python_reference() {
    let golden: Golden = serde_json::from_str(include_str!(
        "../../../tests/golden/extracta-capacity-v1.json"
    ))
    .expect("capacity golden must deserialize");
    assert!(golden.cases.len() >= 25);
    for case in golden.cases {
        let actual = calculate(&case.profile, &case.evidence)
            .unwrap_or_else(|error| panic!("{}: {error}", case.id));
        assert_eq!(actual, case.expected, "capacity parity case {}", case.id);
        // The algorithm is pure: identical observations must reproduce the
        // same result without sampling a live machine or system clock.
        assert_eq!(
            calculate(&case.profile, &case.evidence).unwrap(),
            case.expected,
            "determinism case {}",
            case.id
        );
    }
}
