//! A03: each frozen scenario is also executed by Extracta's Python
//! SchedulingEngine.decide in the integration-branch CI. Compare a single
//! admission per iteration; Python claims one unit at a time, while PipeCraft
//! computes an advisory batch of offers.
use pipecraft_runtime::admission::{decide, AdmissionSnapshot};
use serde::Deserialize;

#[derive(Deserialize)]
struct Golden {
    schema_version: String,
    cases: Vec<Case>,
}

#[derive(Deserialize)]
struct Case {
    id: String,
    snapshot: AdmissionSnapshot,
    expected_first: Option<String>,
    expected_wakeup: Option<u64>,
}

#[test]
fn a03_extracta_first_offer_and_wakeup_parity() {
    let fixture: Golden = serde_json::from_str(include_str!(
        "../../../tests/golden/extracta-admission-v1.json"
    ))
    .expect("admission golden fixture must deserialize");
    assert_eq!(
        fixture.schema_version,
        "extracta.pipecraft.g2.admission-parity/v1"
    );
    assert!(fixture.cases.len() >= 15);
    for case in fixture.cases {
        let first = decide(&case.snapshot).unwrap_or_else(|err| panic!("{}: {err}", case.id));
        assert_eq!(
            first.offered.first().cloned(),
            case.expected_first,
            "first offered work must match Extracta oracle: {}",
            case.id
        );
        assert_eq!(
            first.next_wakeup_at_ms, case.expected_wakeup,
            "timer wakeup must match Extracta oracle: {}",
            case.id
        );
        assert_eq!(
            first,
            decide(&case.snapshot).unwrap(),
            "admission must be deterministic: {}",
            case.id
        );
        assert!(first.requires_confirmation, "{}", case.id);
    }
}
