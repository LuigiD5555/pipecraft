//! S02 durable, bounded provider feed and advisory admission.
//! PostgreSQL remains authoritative for epochs, claims and work outcomes.
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use pipecraft_runtime::admission::{
    decide, ActiveClaim, AdmissionDecision, AdmissionSettings, AdmissionSnapshot, WorkCandidate,
};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::protocol::{
    validate_external_work_message, ExternalWorkCandidate, ExternalWorkMessage, ResourceShareMode,
};

const MAX_PROVIDERS: usize = 128;
const FEED_TTL_MS: u64 = 60_000;

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

#[derive(Clone, Serialize, Deserialize)]
struct PersistedProvider {
    installation_id: String,
    provider_id: String,
    snapshot: ExternalWorkMessage,
}

#[derive(Serialize, Deserialize)]
struct FeedFile {
    format_version: u32,
    providers: Vec<PersistedProvider>,
}

#[derive(Default)]
pub struct ExternalFeedDriver {
    feeds: Mutex<BTreeMap<(String, String), ExternalWorkMessage>>,
    persistence_file: Option<PathBuf>,
}

impl ExternalFeedDriver {
    /// Durable feed cache, not provider authority. A corrupt file refuses startup.
    pub async fn open(dir: PathBuf) -> Result<Self, String> {
        tokio::fs::create_dir_all(&dir).await.map_err(|e| e.to_string())?;
        let path = dir.join("provider-feeds-v1.json");
        let mut feeds = BTreeMap::new();
        match tokio::fs::read(&path).await {
            Ok(data) => {
                let state: FeedFile = serde_json::from_slice(&data)
                    .map_err(|e| format!("corrupt external feed state: {e}"))?;
                if state.format_version != 1 || state.providers.len() > MAX_PROVIDERS {
                    return Err("invalid external feed state version or cardinality".into());
                }
                for saved in state.providers {
                    validate_external_work_message(&saved.snapshot)?;
                    match &saved.snapshot {
                        ExternalWorkMessage::ProviderSnapshot {
                            installation_id, provider_id, snapshot_revision, ..
                        } if installation_id == &saved.installation_id
                            && provider_id == &saved.provider_id && *snapshot_revision > 0 => {}
                        _ => return Err("invalid persisted provider snapshot identity".into()),
                    };
                    if feeds.insert(
                        (saved.installation_id, saved.provider_id), saved.snapshot
                    ).is_some() {
                        return Err("duplicate persisted provider feed".into());
                    }
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.to_string()),
        }
        Ok(Self { feeds: Mutex::new(feeds), persistence_file: Some(path) })
    }

    async fn persist(
        &self,
        state: &BTreeMap<(String, String), ExternalWorkMessage>
    ) -> Result<(), String> {
        let Some(path) = &self.persistence_file else { return Ok(()); };
        let providers = state.iter().map(|((installation_id, provider_id), snapshot)| {
            PersistedProvider {
                installation_id: installation_id.clone(),
                provider_id: provider_id.clone(),
                snapshot: snapshot.clone(),
            }
        }).collect();
        let bytes = serde_json::to_vec(&FeedFile {format_version: 1, providers})
            .map_err(|e| e.to_string())?;
        let tmp = path.with_extension(format!("tmp-{}", uuid::Uuid::new_v4().simple()));
        let result = async {
            let mut file = tokio::fs::File::create(&tmp).await.map_err(|e| e.to_string())?;
            use tokio::io::AsyncWriteExt;
            file.write_all(&bytes).await.map_err(|e| e.to_string())?;
            file.sync_all().await.map_err(|e| e.to_string())?;
            drop(file);
            tokio::fs::rename(&tmp, path).await.map_err(|e| e.to_string())?;
            if let Some(dir) = path.parent() {
                if let Ok(handle) = std::fs::File::open(dir) {
                    let _ = handle.sync_all();
                }
            }
            Ok::<(), String>(())
        }.await;
        if result.is_err() { let _ = tokio::fs::remove_file(&tmp).await; }
        result
    }

    pub async fn plan(
        &self,
        message: &ExternalWorkMessage,
        settings: AdmissionSettings,
        capacity: Option<u32>,
        last_dispatch_ms: BTreeMap<String, u64>,
    ) -> Result<ExternalFeedPlan, String> {
        self.plan_with_workers(message, settings, capacity, last_dispatch_ms, None).await
    }

    /// Registered capabilities are optional to preserve the S01 old-client
    /// advisory contract. When enabled, absence of a matching worker blocks
    /// offers, but never grants execution rights.
    pub async fn plan_with_workers(
        &self,
        message: &ExternalWorkMessage,
        settings: AdmissionSettings,
        capacity: Option<u32>,
        last_dispatch_ms: BTreeMap<String, u64>,
        registered_capabilities: Option<&BTreeSet<String>>,
    ) -> Result<ExternalFeedPlan, String> {
        validate_external_work_message(message)?;
        if capacity == Some(0) { return Err("capacity must be positive".into()); }
        let (installation_id, provider_id) = match message {
            ExternalWorkMessage::ProviderSnapshot { installation_id, provider_id, .. }
            | ExternalWorkMessage::ProviderDelta { installation_id, provider_id, .. } =>
                (installation_id, provider_id),
            _ => return Err("external feed requires provider_snapshot or provider_delta".into()),
        };
        let key = (installation_id.clone(), provider_id.clone());
        let mut guard = self.feeds.lock().await;
        let previous = guard.get(&key);
        let effective = match message {
            ExternalWorkMessage::ProviderSnapshot {
                snapshot_revision, observed_at_ms, ..
            } => {
                if *snapshot_revision == 0 || *observed_at_ms == 0 {
                    return Err("snapshot revision and observed timestamp must be positive".into());
                }
                if let Some(prior) = previous {
                    if let ExternalWorkMessage::ProviderSnapshot { snapshot_revision: old, .. } = prior {
                        if snapshot_revision < old { return Err("stale provider snapshot revision".into()); }
                        if snapshot_revision == old
                            && serde_json::to_value(prior).map_err(|e| e.to_string())?
                            != serde_json::to_value(message).map_err(|e| e.to_string())?
                        { return Err("conflicting provider snapshot revision".into()); }
                    }
                }
                message.clone()
            }
            ExternalWorkMessage::ProviderDelta {
                base_revision, snapshot_revision, observed_at_ms,
                upsert, removed_work_ids, ..
            } => {
                let prior = previous.ok_or("delta requires a complete provider snapshot")?;
                let mut full = prior.clone();
                let ExternalWorkMessage::ProviderSnapshot {
                    snapshot_revision: old_revision,
                    observed_at_ms: old_time,
                    work, active_claims, ..
                } = &mut full else { return Err("invalid cached provider snapshot".into()); };
                if *old_revision != *base_revision {
                    return Err("provider delta base revision mismatch; resync required".into());
                }
                if *observed_at_ms <= *old_time || *observed_at_ms - *old_time > FEED_TTL_MS {
                    return Err("provider delta outside feed TTL; resync required".into());
                }
                // Deltas lack resource/claim evidence; do not reuse possibly stale
                // active-claim occupancy across a feed update.
                if !active_claims.is_empty() {
                    return Err("delta with active claims requires full snapshot resync".into());
                }
                let mut index: BTreeMap<String, ExternalWorkCandidate> =
                    work.iter().map(|w| (w.work_id.clone(), w.clone())).collect();
                for id in removed_work_ids { index.remove(id); }
                for candidate in upsert { index.insert(candidate.work_id.clone(), candidate.clone()); }
                if index.len() > 10_000 { return Err("provider delta exceeds bounded feed".into()); }
                *work = index.into_values().collect();
                *old_revision = *snapshot_revision;
                *old_time = *observed_at_ms;
                full
            }
            _ => unreachable!(),
        };
        validate_external_work_message(&effective)?;
        let ExternalWorkMessage::ProviderSnapshot {
            snapshot_revision, observed_at_ms, work, active_claims, ..
        } = &effective else { unreachable!() };
        let mut active_ids = BTreeSet::new();
        let mut active = Vec::with_capacity(active_claims.len());
        for claim in active_claims {
            if !active_ids.insert(claim.work_id.as_str()) {
                return Err("duplicate active work claim".into());
            }
            let source = work.iter().find(|candidate| candidate.work_id == claim.work_id)
                .ok_or_else(|| format!("active work {} lacks owner/scope metadata in snapshot", claim.work_id))?;
            let (exclusive_resources, shared_resources) = resource_keys(&claim.resources);
            active.push(ActiveClaim {
                id: claim.work_id.clone(), group: source.owner_group.clone(),
                weight: source.weight, scopes: source.scopes.clone(),
                exclusive_resources, shared_resources,
            });
        }
        let candidates = work.iter().map(|candidate| {
            let (exclusive_resources, shared_resources) = resource_keys(&candidate.resources);
            let worker_unavailable = registered_capabilities.is_some_and(|caps| {
                candidate.required_worker_capability.as_ref().map_or(
                    !caps.contains("*"), |cap| !caps.contains(cap))
            });
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
                exclusive_resources, shared_resources,
                hard_block: if worker_unavailable {
                    Some("NO_REGISTERED_WORKER".into())
                } else { candidate.hard_block.clone() },
                dependencies_met: true,
                group_limit: candidate.source_limit,
            })
        }).collect::<Result<Vec<_>, String>>()?;
        let admission = AdmissionSnapshot {
            revision: *snapshot_revision, now_ms: *observed_at_ms,
            candidates, active, last_dispatch_ms, settings, capacity,
            adaptive_capacity: None,
        };
        let decision = decide(&admission)?;
        let provisional_offers = decision.offered.iter().map(|id| {
            let source = work.iter().find(|candidate| &candidate.work_id == id)
                .expect("admission offered an existing work id");
            ProvisionalOffer {
                work_id: id.clone(),
                snapshot_revision: *snapshot_revision,
                required_worker_capability: source.required_worker_capability.clone(),
                requires_confirmation: true,
            }
        }).collect();

        if !guard.contains_key(&key) && guard.len() >= MAX_PROVIDERS {
            return Err("external feed backpressure: provider limit reached".into());
        }
        let mut updated = guard.clone();
        updated.insert(key, effective);
        self.persist(&updated).await?;
        *guard = updated;
        Ok(ExternalFeedPlan {
            snapshot_revision: *snapshot_revision, decision,
            provisional_offers, side_effects: false,
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
