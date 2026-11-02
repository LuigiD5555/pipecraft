use std::collections::BTreeMap;

use pipecraft_runtime::admission::{
    decide, ActiveClaim, AdmissionPolicy, AdmissionSettings, AdmissionSnapshot, WorkCandidate,
};

fn candidate(
    id: &str,
    group: &str,
    priority: i32,
    eligible_since_ms: u64,
    weight: u32,
) -> WorkCandidate {
    WorkCandidate {
        id: id.into(),
        group: group.into(),
        priority,
        eligible_since_ms,
        ready_at_ms: eligible_since_ms,
        not_before_ms: None,
        weight,
        scopes: vec![],
        exclusive_resources: vec![],
        shared_resources: vec![],
        hard_block: None,
        dependencies_met: true,
        group_limit: 1,
    }
}

fn settings(policy: AdmissionPolicy) -> AdmissionSettings {
    AdmissionSettings {
        policy,
        aging_ms: 1_000,
        heavy_wait_guard_ms: 2_000,
        initial_capacity: 4,
        ceiling_capacity: 8,
        floor_capacity: 1,
        allow_idle_overweight: false,
    }
}

#[test]
fn fair_admission_ages_from_eligible_since() {
    let snapshot = AdmissionSnapshot {
        revision: 1,
        now_ms: 10_000,
        candidates: vec![
            candidate("newer", "newer", 100, 9_000, 1),
            candidate("older", "older", 105, 1_000, 1),
        ],
        active: vec![],
        last_dispatch_ms: BTreeMap::new(),
        settings: settings(AdmissionPolicy::Fair),
        capacity: Some(1),
        adaptive_capacity: None,
    };
    let decision = decide(&snapshot).unwrap();
    assert_eq!(decision.offered, vec!["older"]);
    assert_eq!(decision.decision_trace[0].id, "older");
    assert!(decision.requires_confirmation);
}

#[test]
fn heavy_guard_publishes_exact_activation_wakeup() {
    let snapshot = AdmissionSnapshot {
        revision: 2,
        now_ms: 10_000,
        candidates: vec![
            candidate("heavy", "heavy", 100, 9_000, 4),
            candidate("light", "light", 101, 10_000, 1),
        ],
        active: vec![ActiveClaim {
            id: "active".into(),
            group: "active".into(),
            weight: 1,
            scopes: vec![],
            exclusive_resources: vec![],
            shared_resources: vec![],
        }],
        last_dispatch_ms: BTreeMap::new(),
        settings: settings(AdmissionPolicy::Fair),
        capacity: Some(4),
        adaptive_capacity: None,
    };
    let decision = decide(&snapshot).unwrap();
    assert_eq!(decision.next_wakeup_at_ms, Some(11_000));
    assert_eq!(decision.offered, vec!["light"]);

    let guarded = decide(&AdmissionSnapshot {
        now_ms: 11_000,
        ..snapshot
    })
    .unwrap();
    assert!(guarded.offered.is_empty());
    assert!(guarded
        .blocked
        .iter()
        .any(|item| item.id == "light" && item.reason == "HEAVY_BACKFILL_GUARD"));
}

#[test]
fn legacy_admission_preserves_input_order_for_dag_compatibility() {
    let snapshot = AdmissionSnapshot {
        revision: 3,
        now_ms: 0,
        candidates: vec![candidate("z", "z", 100, 0, 1), candidate("a", "a", 1, 0, 1)],
        active: vec![],
        last_dispatch_ms: BTreeMap::new(),
        settings: settings(AdmissionPolicy::Legacy),
        capacity: Some(2),
        adaptive_capacity: None,
    };
    assert_eq!(decide(&snapshot).unwrap().offered, vec!["z", "a"]);
}
