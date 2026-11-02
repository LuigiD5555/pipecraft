//! Deterministic, side-effect-free admission decisions for DAG steps and external providers.
//!
//! This module computes *proposals*. It never claims a job or authorizes an
//! external side effect. An external provider must independently confirm an
//! offer through its durable, fenced transaction before executing anything.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

fn one() -> u32 {
    1
}
fn yes() -> bool {
    true
}
fn default_initial() -> u32 {
    4
}
fn default_max() -> u32 {
    8
}
fn default_aging() -> u64 {
    120_000
}
fn default_backfill() -> u64 {
    300_000
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdmissionPolicy {
    Legacy,
    Fair,
}
impl Default for AdmissionPolicy {
    fn default() -> Self {
        Self::Fair
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdmissionSettings {
    #[serde(default)]
    pub policy: AdmissionPolicy,
    #[serde(default = "default_aging")]
    pub aging_ms: u64,
    #[serde(default = "default_backfill")]
    pub heavy_wait_guard_ms: u64,
    #[serde(default = "default_initial")]
    pub initial_capacity: u32,
    #[serde(default = "default_max")]
    pub ceiling_capacity: u32,
    #[serde(default = "one")]
    pub floor_capacity: u32,
    #[serde(default)]
    pub allow_idle_overweight: bool,
}
impl Default for AdmissionSettings {
    fn default() -> Self {
        Self {
            policy: AdmissionPolicy::Fair,
            aging_ms: default_aging(),
            heavy_wait_guard_ms: default_backfill(),
            initial_capacity: default_initial(),
            ceiling_capacity: default_max(),
            floor_capacity: 1,
            allow_idle_overweight: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkCandidate {
    pub id: String,
    #[serde(default)]
    pub group: String,
    #[serde(default)]
    pub priority: i32,
    #[serde(default)]
    pub ready_at_ms: u64,
    #[serde(default)]
    pub not_before_ms: Option<u64>,
    #[serde(default = "one")]
    pub weight: u32,
    #[serde(default)]
    pub scopes: Vec<String>,
    #[serde(default)]
    pub exclusive_resources: Vec<String>,
    #[serde(default)]
    pub shared_resources: Vec<String>,
    #[serde(default)]
    pub hard_block: Option<String>,
    #[serde(default = "yes")]
    pub dependencies_met: bool,
    #[serde(default = "one")]
    pub group_limit: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActiveClaim {
    pub id: String,
    #[serde(default)]
    pub group: String,
    #[serde(default = "one")]
    pub weight: u32,
    #[serde(default)]
    pub scopes: Vec<String>,
    #[serde(default)]
    pub exclusive_resources: Vec<String>,
    #[serde(default)]
    pub shared_resources: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapacityInput {
    pub profile: crate::capacity::CapacityProfile,
    pub evidence: crate::capacity::CapacityEvidence,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdmissionSnapshot {
    pub revision: u64,
    pub now_ms: u64,
    #[serde(default)]
    pub candidates: Vec<WorkCandidate>,
    #[serde(default)]
    pub active: Vec<ActiveClaim>,
    #[serde(default)]
    pub last_dispatch_ms: BTreeMap<String, u64>,
    #[serde(default)]
    pub settings: AdmissionSettings,
    /// Explicit effective capacity provided by the host or a pure capacity policy.
    /// The admission function does not fetch system signals or mutate counters.
    #[serde(default)]
    pub capacity: Option<u32>,
    #[serde(default)]
    pub adaptive_capacity: Option<CapacityInput>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BlockedCandidate {
    pub id: String,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AdmissionDecision {
    pub revision: u64,
    pub offered: Vec<String>,
    pub blocked: Vec<BlockedCandidate>,
    pub capacity_used: u32,
    pub capacity_total: u32,
    pub next_wakeup_at_ms: Option<u64>,
    /// Offers are advisory and have NOT acquired leases or effects.
    pub requires_confirmation: bool,
}

/// A stable comparator shared by DAG-ready nodes and external work candidates.
/// `legacy` preserves input order; `fair` uses aging, LRU, readiness, then id.
pub fn ranked_ids(
    candidates: &[WorkCandidate],
    now_ms: u64,
    settings: &AdmissionSettings,
    dispatch_history: &BTreeMap<String, u64>,
) -> Vec<usize> {
    let mut indexes: Vec<usize> = (0..candidates.len()).collect();
    if matches!(settings.policy, AdmissionPolicy::Legacy) {
        return indexes;
    }
    indexes.sort_by(|&a, &b| {
        let x = &candidates[a];
        let y = &candidates[b];
        let effective = |unit: &WorkCandidate| -> i64 {
            let elapsed = now_ms.saturating_sub(unit.ready_at_ms);
            let aged = elapsed / settings.aging_ms.max(1);
            i64::from(unit.priority).saturating_sub(aged.min(i64::MAX as u64) as i64)
        };
        effective(x)
            .cmp(&effective(y))
            .then_with(|| {
                dispatch_history
                    .get(&x.group)
                    .copied()
                    .unwrap_or(0)
                    .cmp(&dispatch_history.get(&y.group).copied().unwrap_or(0))
            })
            .then_with(|| x.ready_at_ms.cmp(&y.ready_at_ms))
            .then_with(|| x.id.cmp(&y.id))
            .then_with(|| a.cmp(&b))
    });
    indexes
}

fn conflicts(candidate: &WorkCandidate, active: &[ActiveClaim]) -> bool {
    active.iter().any(|claim| {
        candidate.scopes.iter().any(|s| claim.scopes.contains(s))
            || candidate.exclusive_resources.iter().any(|s| {
                claim.exclusive_resources.contains(s) || claim.shared_resources.contains(s)
            })
            || candidate
                .shared_resources
                .iter()
                .any(|s| claim.exclusive_resources.contains(s))
    })
}

/// Compute a reproducible, non-mutating batch of proposed admissions.
/// Never treat `offered` as authorization to execute or persist effects.
pub fn decide(snapshot: &AdmissionSnapshot) -> Result<AdmissionDecision, String> {
    let calculated = snapshot
        .adaptive_capacity
        .as_ref()
        .map(|input| crate::capacity::calculate(&input.profile, &input.evidence))
        .transpose()?;
    let capacity = snapshot
        .capacity
        .or_else(|| calculated.as_ref().map(|result| result.points))
        .unwrap_or(snapshot.settings.initial_capacity)
        .max(snapshot.settings.floor_capacity)
        .min(
            snapshot
                .settings
                .ceiling_capacity
                .max(snapshot.settings.floor_capacity),
        );
    let mut seen = BTreeSet::new();
    for candidate in &snapshot.candidates {
        if candidate.id.trim().is_empty() || !seen.insert(&candidate.id) {
            return Err("candidate IDs must be non-empty and unique".into());
        }
        if candidate.weight == 0 || candidate.group_limit == 0 {
            return Err(format!("invalid weight/group limit for {}", candidate.id));
        }
        if candidate
            .exclusive_resources
            .iter()
            .any(|r| candidate.shared_resources.contains(r))
        {
            return Err(format!(
                "conflicting resource declaration for {}",
                candidate.id
            ));
        }
    }
    let mut active = snapshot.active.clone();
    let mut used = active.iter().fold(0u32, |n, c| n.saturating_add(c.weight));
    let mut offered = Vec::new();
    let mut reasons: BTreeMap<String, String> = BTreeMap::new();
    let mut next_wakeup = None::<u64>;
    let order = ranked_ids(
        &snapshot.candidates,
        snapshot.now_ms,
        &snapshot.settings,
        &snapshot.last_dispatch_ms,
    );
    let heavy_waiting = snapshot.candidates.iter().any(|c| {
        c.weight > capacity.saturating_sub(used)
            && c.hard_block.is_none()
            && c.dependencies_met
            && c.ready_at_ms <= snapshot.now_ms
            && c.not_before_ms.unwrap_or(0) <= snapshot.now_ms
            && snapshot.now_ms.saturating_sub(c.ready_at_ms)
                >= snapshot.settings.heavy_wait_guard_ms
    });
    for index in order {
        let unit = &snapshot.candidates[index];
        let gate = unit.ready_at_ms.max(unit.not_before_ms.unwrap_or(0));
        let reason = if !unit.dependencies_met {
            Some("DEPENDENCY_PENDING")
        } else if unit.hard_block.is_some() {
            Some("POLICY_BLOCKED")
        } else if gate > snapshot.now_ms {
            next_wakeup = Some(next_wakeup.map_or(gate, |old| old.min(gate)));
            Some("NOT_READY")
        } else if active.iter().filter(|a| a.group == unit.group).count() as u32 >= unit.group_limit
        {
            Some("GROUP_BUSY")
        } else if conflicts(unit, &active) {
            Some("RESOURCE_BUSY")
        } else if unit.weight > capacity.saturating_sub(used)
            && !(snapshot.settings.allow_idle_overweight && used == 0 && offered.is_empty())
        {
            Some("CAPACITY_BUSY")
        } else if heavy_waiting && unit.weight < capacity && used > 0 {
            Some("HEAVY_BACKFILL_GUARD")
        } else {
            None
        };
        if let Some(reason) = reason {
            reasons.insert(
                unit.id.clone(),
                unit.hard_block.clone().unwrap_or_else(|| reason.into()),
            );
            continue;
        }
        used = used.saturating_add(unit.weight);
        offered.push(unit.id.clone());
        active.push(ActiveClaim {
            id: unit.id.clone(),
            group: unit.group.clone(),
            weight: unit.weight,
            scopes: unit.scopes.clone(),
            exclusive_resources: unit.exclusive_resources.clone(),
            shared_resources: unit.shared_resources.clone(),
        });
    }
    let blocked = snapshot
        .candidates
        .iter()
        .filter_map(|c| {
            reasons.get(&c.id).map(|reason| BlockedCandidate {
                id: c.id.clone(),
                reason: reason.clone(),
            })
        })
        .collect();
    Ok(AdmissionDecision {
        revision: snapshot.revision,
        offered,
        blocked,
        capacity_used: used,
        capacity_total: capacity,
        next_wakeup_at_ms: next_wakeup,
        requires_confirmation: true,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn work(id: &str, group: &str, weight: u32) -> WorkCandidate {
        WorkCandidate {
            id: id.into(),
            group: group.into(),
            priority: 100,
            ready_at_ms: 1000,
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
    fn snap(candidates: Vec<WorkCandidate>) -> AdmissionSnapshot {
        AdmissionSnapshot {
            revision: 7,
            now_ms: 2000,
            candidates,
            active: vec![],
            last_dispatch_ms: BTreeMap::new(),
            settings: AdmissionSettings::default(),
            capacity: Some(4),
            adaptive_capacity: None,
        }
    }
    #[test]
    fn deterministic_and_fair_across_groups() {
        let mut s = snap(vec![work("a", "group-a", 1), work("b", "group-b", 1)]);
        s.last_dispatch_ms.insert("group-a".into(), 1999);
        s.last_dispatch_ms.insert("group-b".into(), 1000);
        let first = decide(&s).unwrap();
        assert_eq!(first.offered, vec!["b", "a"]);
        assert_eq!(first, decide(&s).unwrap());
        assert!(first.requires_confirmation);
    }
    #[test]
    fn scope_cooldown_and_capacity_block_without_mutating_input() {
        let mut a = work("a", "one", 3);
        a.scopes.push("origin:shared".into());
        let mut b = work("b", "two", 1);
        b.scopes.push("origin:shared".into());
        let mut c = work("c", "three", 1);
        c.not_before_ms = Some(5000);
        let s = snap(vec![a, b, c]);
        let result = decide(&s).unwrap();
        assert_eq!(result.offered.len(), 1);
        assert!(result.blocked.iter().any(|e| e.reason == "RESOURCE_BUSY"));
        assert_eq!(result.next_wakeup_at_ms, Some(5000));
    }
    #[test]
    fn bad_payload_rejected_and_blocked_never_offered() {
        let mut s = snap(vec![work("a", "x", 1), work("a", "y", 1)]);
        assert!(decide(&s).is_err());
        s.candidates[1].id = "b".into();
        s.candidates[1].hard_block = Some("NEEDS_HUMAN".into());
        let result = decide(&s).unwrap();
        assert_eq!(result.offered, vec!["a"]);
        assert_eq!(result.blocked[0].reason, "NEEDS_HUMAN");
    }
    #[test]
    fn active_claims_block_group_and_exclusive_resources() {
        let mut reserved = work("new", "one", 1);
        reserved.exclusive_resources.push("disk:shared".into());
        let mut s = snap(vec![reserved]);
        s.active.push(ActiveClaim {
            id: "in-progress".into(),
            group: "another".into(),
            weight: 1,
            scopes: vec![],
            exclusive_resources: vec!["disk:shared".into()],
            shared_resources: vec![],
        });
        let result = decide(&s).unwrap();
        assert!(result.offered.is_empty());
        assert_eq!(result.blocked[0].reason, "RESOURCE_BUSY");
    }
    #[test]
    fn legacy_keeps_order() {
        let mut s = snap(vec![work("z", "z", 1), work("a", "a", 1)]);
        s.settings.policy = AdmissionPolicy::Legacy;
        assert_eq!(decide(&s).unwrap().offered, vec!["z", "a"]);
    }
}
