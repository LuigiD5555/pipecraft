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

#[test]
fn persisted_capacity_evidence_replays_identically_after_restart() {
    use pipecraft_runtime::coordinator::RuntimeCoordinator;

    // Extracta owns persistence; PipeCraft reuses the exact observations
    // supplied by that authority. This represents two fresh runtimes reading
    // the same serialized evidence, not a shared in-memory semaphore.
    let persisted = r#"{
        "clean": 10,
        "bad": 1,
        "bad_groups": 1,
        "memory_available_mib": 8192,
        "cpu_load_ratio": 0.2
    }"#;
    let profile = CapacityProfile::default();
    let first = RuntimeCoordinator::with_resource_points(4, 4);
    let original: CapacityEvidence = serde_json::from_str(persisted).unwrap();
    let first_result = first.apply_capacity_evidence(&profile, &original).unwrap();

    let restarted = RuntimeCoordinator::with_resource_points(4, 4);
    let restored: CapacityEvidence = serde_json::from_str(persisted).unwrap();
    let replayed = restarted
        .apply_capacity_evidence(&profile, &restored)
        .unwrap();

    assert_eq!(first_result, replayed);
    assert_eq!(first_result.points, 3);
    assert_eq!(first.resources.snapshot().points_limit, 3);
    assert_eq!(restarted.resources.snapshot().points_limit, 3);
}
