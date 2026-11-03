//! S01 snapshot-to-admission driver.
//!
//! This is a Tokio-serialized, advisory driver. It makes no provider claim,
//! creates no lease/epoch/receipt, and never executes a work unit. Extracta
//! retains the authority to confirm every proposed work id.
use std::collections::{BTreeMap, BTreeSet};

use pipecraft_runtime::admission::{
    decide, ActiveClaim, AdmissionDecision, AdmissionSettings, AdmissionSnapshot, WorkCandidate,
};
use serde::Serialize;
use tokio::sync::Mutex;

use crate::protocol::{
    validate_external_work_message, ExternalWorkCandidate, ExternalWorkMessage, ResourceShareMode,
};

#[derive(Debug, Clone, Serialize)]
pub struct ProvisionalOffer {
    pub work_id: String,
    pub snapshot_revision: u64,
    pub required_worker_capability: Option<String>,
    pub requires_confirmation: bool,
}

#[derive(Debug, Serialize)]
pub struct ExternalFeedPlan {
    pub snapshot_revision: u64,
    pub decision: AdmissionDecision,
    pub provisional_offers: Vec<ProvisionalOffer>,
    pub side_effects: bool,
}

#[derive(Default)]
pub struct ExternalFeedDriver {
    // Volatile watermark only. Persistent provider revision and fencing are
    // owned by Extracta, not by PipeCraft. S02 adds durable deltas/worker TTL.
    revisions: Mutex<BTreeMap<(String, String), u64>>,
}

impl ExternalFeedDriver {
    pub async fn plan(
        &self,
        message: &ExternalWorkMessage,
        settings: AdmissionSettings,
        capacity: Option<u32>,
        last_dispatch_ms: BTreeMap<String, u64>,
    ) -> Result<ExternalFeedPlan, String> {
        validate_external_work_message(message)?;
        let ExternalWorkMessage::ProviderSnapshot {
            installation_id,
            provider_id,
            snapshot_revision,
            observed_at_ms,
            work,
            active_claims,
            ..
        } = message
        else {
            return Err("S01 requires a complete provider_snapshot, not a delta or offer".into());
        };
        if *snapshot_revision == 0 || *observed_at_ms == 0 {
            return Err("snapshot revision and observed timestamp must be positive".into());
        }
        if capacity == Some(0) {
            return Err("capacity must be positive".into());
        }
        let key = (installation_id.clone(), provider_id.clone());
        // One serialized Tokio admission driver per service for this provider.
        // Reject stale snapshots rather than producing offers on old evidence.
        let mut revisions = self.revisions.lock().await;
        if revisions
            .get(&key)
            .is_some_and(|previous| *snapshot_revision < *previous)
        {
            return Err("stale provider snapshot revision".into());
        }

        let mut active_ids = BTreeSet::new();
        let mut active = Vec::with_capacity(active_claims.len());
        for claim in active_claims {
            if !active_ids.insert(claim.work_id.as_str()) {
                return Err("duplicate active work claim".into());
            }
            // A pending-work feed alone does not identify the owner/scopes of
            // a running unit. In S01 require explicit source metadata instead
            // of guessing an owner's identity and over-offering a busy scope.
            let source = work
                .iter()
                .find(|candidate| candidate.work_id == claim.work_id)
                .ok_or_else(|| {
                    format!(
                        "active work {} lacks owner/scope metadata in snapshot",
                        claim.work_id
                    )
                })?;
            let (exclusive_resources, shared_resources) = resource_keys(&claim.resources);
            active.push(ActiveClaim {
                id: claim.work_id.clone(),
                group: source.owner_group.clone(),
                weight: source.weight,
                scopes: source.scopes.clone(),
                exclusive_resources,
                shared_resources,
            });
        }

        let candidates = work
            .iter()
            .map(|candidate| {
                let (exclusive_resources, shared_resources) = resource_keys(&candidate.resources);
                Ok(WorkCandidate {
                    id: candidate.work_id.clone(),
                    group: candidate.owner_group.clone(),
                    priority: i32::try_from(candidate.priority)
                        .map_err(|_| "priority outside admission i32 range".to_string())?,
                    eligible_since_ms: candidate.eligible_since_ms,
                    ready_at_ms: candidate.ready_at_ms,
                    not_before_ms: candidate.not_before_ms,
                    weight: candidate.weight,
                    scopes: candidate.scopes.clone(),
                    exclusive_resources,
                    shared_resources,
                    hard_block: candidate.hard_block.clone(),
                    dependencies_met: true,
                    group_limit: candidate.source_limit,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        let admission = AdmissionSnapshot {
            revision: *snapshot_revision,
            now_ms: *observed_at_ms,
            candidates,
            active,
            last_dispatch_ms,
            settings,
            capacity,
            adaptive_capacity: None,
        };
        let decision = decide(&admission)?;
        let provisional_offers = decision
            .offered
            .iter()
            .map(|id| {
                let source: &ExternalWorkCandidate = work
                    .iter()
                    .find(|candidate| &candidate.work_id == id)
                    .expect("admission offered an existing work id");
                ProvisionalOffer {
                    work_id: id.clone(),
                    snapshot_revision: *snapshot_revision,
                    required_worker_capability: source.required_worker_capability.clone(),
                    requires_confirmation: true,
                }
            })
            .collect();
        revisions.insert(key, *snapshot_revision);
        Ok(ExternalFeedPlan {
            snapshot_revision: *snapshot_revision,
            decision,
            provisional_offers,
            side_effects: false,
        })
    }
}

fn resource_keys(
    resources: &[crate::protocol::ExternalResourceDemand],
) -> (Vec<String>, Vec<String>) {
    let mut exclusive = Vec::new();
    let mut shared = Vec::new();
    for item in resources {
        match item.share_mode {
            ResourceShareMode::Exclusive => exclusive.push(item.resource_key.clone()),
            ResourceShareMode::Shared => shared.push(item.resource_key.clone()),
        }
    }
    (exclusive, shared)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::ExternalWorkMessage;
    use serde_json::json;

    fn snapshot(revision: u64) -> ExternalWorkMessage {
        serde_json::from_value(json!({
            "op": "provider_snapshot",
            "protocol": "pipecraft.external-work/v1",
            "request_id": "s01-test-1",
            "installation_id": "install-test",
            "provider_id": "extracta/acquisition/v1",
            "snapshot_revision": revision,
            "observed_at_ms": 1_000_000,
            "resource_evidence_revision": 5,
            "work": [
                {
                    "work_id": "EXTRACTOR_RUN:1",
                    "owner_group": "source:1",
                    "queue_kind": "EXTRACTOR_RUN",
                    "priority": 1,
                    "eligible_since_ms": 900_000,
                    "ready_at_ms": 900_000,
                    "weight": 3,
                    "source_limit": 1,
                    "scopes": ["origin:shared"],
                    "resources": [{
                        "resource_key": "browser:1",
                        "points": 3,
                        "quantity": 1,
                        "share_mode": "EXCLUSIVE",
                        "limit": 1
                    }]
                },
                {
                    "work_id": "EXTRACTOR_RUN:2",
                    "owner_group": "source:2",
                    "queue_kind": "EXTRACTOR_RUN",
                    "priority": 20,
                    "eligible_since_ms": 900_000,
                    "ready_at_ms": 900_000,
                    "weight": 1,
                    "source_limit": 1,
                    "scopes": ["origin:independent"],
                    "resources": []
                }
            ],
            "active_claims": [{
                "work_id": "EXTRACTOR_RUN:1",
                "attempt_id": "attempt-1",
                "claim_epoch": 3,
                "leadership_epoch": 7,
                "lease_until_ms": 1_100_000,
                "resources": [{
                    "resource_key": "browser:1",
                    "points": 3,
                    "quantity": 1,
                    "share_mode": "EXCLUSIVE",
                    "limit": 1
                }]
            }]
        }))
        .unwrap()
    }

    #[tokio::test]
    async fn driver_proposes_only_unoccupied_work_without_provider_effects() {
        let driver = ExternalFeedDriver::default();
        let result = driver
            .plan(
                &snapshot(6),
                AdmissionSettings::default(),
                Some(4),
                BTreeMap::new(),
            )
            .await
            .unwrap();
        assert_eq!(result.provisional_offers.len(), 1);
        assert_eq!(result.provisional_offers[0].work_id, "EXTRACTOR_RUN:2");
        assert!(result.provisional_offers[0].requires_confirmation);
        assert!(!result.side_effects);
        assert!(result.decision.requires_confirmation);
        assert_eq!(result.decision.capacity_used, 4);
        assert!(result
            .decision
            .blocked
            .iter()
            .any(|item| item.id == "EXTRACTOR_RUN:1"));
    }

    #[tokio::test]
    async fn driver_rejects_stale_snapshots_and_is_repeatable() {
        let driver = ExternalFeedDriver::default();
        let first = driver
            .plan(
                &snapshot(12),
                AdmissionSettings::default(),
                Some(4),
                BTreeMap::new(),
            )
            .await
            .unwrap();
        let repeated = driver
            .plan(
                &snapshot(12),
                AdmissionSettings::default(),
                Some(4),
                BTreeMap::new(),
            )
            .await
            .unwrap();
        assert_eq!(first.decision, repeated.decision);
        assert_eq!(
            driver
                .plan(
                    &snapshot(11),
                    AdmissionSettings::default(),
                    Some(4),
                    BTreeMap::new(),
                )
                .await
                .unwrap_err(),
            "stale provider snapshot revision"
        );
    }

    #[tokio::test]
    async fn driver_rejects_non_snapshot_and_unresolved_claims() {
        let driver = ExternalFeedDriver::default();
        let mut data = serde_json::to_value(snapshot(20)).unwrap();
        data["active_claims"][0]["work_id"] = json!("EXTRACTOR_RUN:999");
        let unknown: ExternalWorkMessage = serde_json::from_value(data).unwrap();
        assert!(driver
            .plan(
                &unknown,
                AdmissionSettings::default(),
                None,
                BTreeMap::new(),
            )
            .await
            .unwrap_err()
            .contains("lacks owner/scope metadata"));
        let mut other = snapshot(20);
        if let ExternalWorkMessage::ProviderSnapshot { work, .. } = &mut other {
            work[0].priority = i64::MAX;
        }
        assert_eq!(
            driver
                .plan(&other, AdmissionSettings::default(), None, BTreeMap::new())
                .await
                .unwrap_err(),
            "priority outside admission i32 range"
        );
    }
}
