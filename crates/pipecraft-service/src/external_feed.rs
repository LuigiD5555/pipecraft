//! S02 durable snapshot/delta admission driver.
//!
//! An advisory provider feed. This module NEVER grants claims, epochs,
//! receipts or execution. The provider remains the source of business truth.
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use pipecraft_runtime::admission::{
    decide, ActiveClaim, AdmissionDecision, AdmissionSettings, AdmissionSnapshot, WorkCandidate,
};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

use crate::protocol::{
    validate_external_work_message, ExternalWorkCandidate, ExternalWorkMessage, ResourceShareMode,
};

const MAX_PROVIDERS: usize = 128;
const MAX_WORK_PER_PROVIDER: usize = 10_000;

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

#[derive(Debug, Serialize, Deserialize)]
struct StoredFeed {
    installation_id: String,
    provider_id: String,
    message: ExternalWorkMessage,
}

#[derive(Default)]
pub struct ExternalFeedDriver {
    feeds: Mutex<BTreeMap<(String, String), ExternalWorkMessage>>,
    storage: Option<PathBuf>,
}

impl ExternalFeedDriver {
    /// Fail closed on corrupt durable evidence, rather than forgetting a
    /// revision watermark and accidentally accepting an old provider delta.
    pub async fn open(storage: PathBuf) -> Result<Self, String> {
        let mut feeds = BTreeMap::new();
        match tokio::fs::read(&storage).await {
            Ok(bytes) => {
                let saved: Vec<StoredFeed> = serde_json::from_slice(&bytes)
                    .map_err(|error| format!("invalid durable provider feed: {error}"))?;
                if saved.len() > MAX_PROVIDERS {
                    return Err("durable provider feed exceeds capacity".into());
                }
                for entry in saved {
                    validate_external_work_message(&entry.message)?;
                    let ExternalWorkMessage::ProviderSnapshot {
                        installation_id,
                        provider_id,
                        work,
                        ..
                    } = &entry.message
                    else {
                        return Err("durable provider feed is not a snapshot".into());
                    };
                    if entry.installation_id != *installation_id
                        || entry.provider_id != *provider_id
                        || work.len() > MAX_WORK_PER_PROVIDER
                    {
                        return Err("durable provider feed identity/capacity mismatch".into());
                    }
                    let key = (entry.installation_id, entry.provider_id);
                    if feeds.insert(key, entry.message).is_some() {
                        return Err("duplicate durable provider feed".into());
                    }
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(format!("unable to read durable provider feed: {error}")),
        }
        Ok(Self {
            feeds: Mutex::new(feeds),
            storage: Some(storage),
        })
    }

    async fn persist(
        &self,
        feeds: &BTreeMap<(String, String), ExternalWorkMessage>,
    ) -> Result<(), String> {
        let Some(path) = &self.storage else {
            return Ok(());
        };
        let saved: Vec<_> = feeds
            .iter()
            .map(|((installation_id, provider_id), message)| StoredFeed {
                installation_id: installation_id.clone(),
                provider_id: provider_id.clone(),
                message: message.clone(),
            })
            .collect();
        let bytes = serde_json::to_vec(&saved).map_err(|error| error.to_string())?;
        let parent = path
            .parent()
            .ok_or("provider feed has no parent directory")?;
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|error| error.to_string())?;
        let tmp = path.with_extension(format!("json.tmp-{}", uuid::Uuid::new_v4().simple()));
        let result: Result<(), String> = async {
            let mut file = tokio::fs::File::create(&tmp)
                .await
                .map_err(|error| error.to_string())?;
            file.write_all(&bytes)
                .await
                .map_err(|error| error.to_string())?;
            file.sync_all().await.map_err(|error| error.to_string())?;
            drop(file);
            tokio::fs::rename(&tmp, path)
                .await
                .map_err(|error| error.to_string())?;
            if let Ok(dir) = std::fs::File::open(parent) {
                let _ = dir.sync_all();
            }
            Ok(())
        }
        .await;
        if result.is_err() {
            let _ = tokio::fs::remove_file(&tmp).await;
        }
        result
    }

    fn materialize(
        message: &ExternalWorkMessage,
        previous: Option<&ExternalWorkMessage>,
    ) -> Result<ExternalWorkMessage, String> {
        match message {
            ExternalWorkMessage::ProviderSnapshot {
                snapshot_revision, ..
            } => {
                if let Some(ExternalWorkMessage::ProviderSnapshot {
                    snapshot_revision: old_revision,
                    ..
                }) = previous
                {
                    if snapshot_revision < old_revision {
                        return Err("stale provider snapshot revision".into());
                    }
                    if snapshot_revision == old_revision && previous != Some(message) {
                        return Err("conflicting same-revision provider snapshot".into());
                    }
                }
                Ok(message.clone())
            }
            ExternalWorkMessage::ProviderDelta {
                request_id,
                base_revision,
                snapshot_revision,
                observed_at_ms,
                upsert,
                removed_work_ids,
                ..
            } => {
                let Some(ExternalWorkMessage::ProviderSnapshot {
                    protocol,
                    installation_id,
                    provider_id,
                    snapshot_revision: current_revision,
                    observed_at_ms: current_observed,
                    work,
                    active_claims,
                    resource_evidence_revision,
                    ..
                }) = previous
                else {
                    return Err("provider delta requires durable baseline snapshot".into());
                };
                if base_revision != current_revision {
                    return Err("provider delta base_revision mismatch".into());
                }
                if *observed_at_ms < *current_observed || *observed_at_ms == 0 {
                    return Err("provider delta observed_at_ms went backwards".into());
                }
                let mut merged: BTreeMap<String, ExternalWorkCandidate> = work
                    .iter()
                    .cloned()
                    .map(|candidate| (candidate.work_id.clone(), candidate))
                    .collect();
                let mut removals = BTreeSet::new();
                for work_id in removed_work_ids {
                    if !removals.insert(work_id) {
                        return Err("duplicate removed work_id".into());
                    }
                    if active_claims.iter().any(|claim| &claim.work_id == work_id) {
                        return Err("cannot remove active claim metadata with delta".into());
                    }
                    merged.remove(work_id);
                }
                for candidate in upsert {
                    merged.insert(candidate.work_id.clone(), candidate.clone());
                }
                if merged.len() > MAX_WORK_PER_PROVIDER {
                    return Err("provider feed backpressure: too many candidates".into());
                }
                Ok(ExternalWorkMessage::ProviderSnapshot {
                    protocol: protocol.clone(),
                    request_id: request_id.clone(),
                    installation_id: installation_id.clone(),
                    provider_id: provider_id.clone(),
                    snapshot_revision: *snapshot_revision,
                    observed_at_ms: *observed_at_ms,
                    work: merged.into_values().collect(),
                    active_claims: active_claims.clone(),
                    resource_evidence_revision: *resource_evidence_revision,
                })
            }
            _ => Err("provider feed requires snapshot or delta".into()),
        }
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

    pub async fn plan_with_workers(
        &self,
        message: &ExternalWorkMessage,
        settings: AdmissionSettings,
        capacity: Option<u32>,
        last_dispatch_ms: BTreeMap<String, u64>,
        registered_capabilities: Option<&BTreeSet<String>>,
    ) -> Result<ExternalFeedPlan, String> {
        validate_external_work_message(message)?;
        if capacity == Some(0) {
            return Err("capacity must be positive".into());
        }
        let (installation_id, provider_id) = match message {
            ExternalWorkMessage::ProviderSnapshot {
                installation_id,
                provider_id,
                ..
            }
            | ExternalWorkMessage::ProviderDelta {
                installation_id,
                provider_id,
                ..
            } => (installation_id, provider_id),
            _ => return Err("provider feed requires snapshot or delta".into()),
        };
        let key = (installation_id.clone(), provider_id.clone());
        let mut feeds = self.feeds.lock().await;
        let next = Self::materialize(message, feeds.get(&key))?;
        let ExternalWorkMessage::ProviderSnapshot {
            snapshot_revision,
            observed_at_ms,
            work,
            active_claims,
            ..
        } = &next
        else {
            return Err("internal provider feed invariant".into());
        };
        if *snapshot_revision == 0 || *observed_at_ms == 0 {
            return Err("snapshot revision and timestamp must be positive".into());
        }
        if work.len() > MAX_WORK_PER_PROVIDER {
            return Err("provider feed backpressure: too many candidates".into());
        }
        if !feeds.contains_key(&key) && feeds.len() >= MAX_PROVIDERS {
            return Err("provider feed backpressure: too many providers".into());
        }

        let mut active_ids = BTreeSet::new();
        let mut active = Vec::with_capacity(active_claims.len());
        for claim in active_claims {
            if !active_ids.insert(claim.work_id.as_str()) {
                return Err("duplicate active work claim".into());
            }
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
                    hard_block: if registered_capabilities.is_some_and(|caps| {
                        candidate.required_worker_capability.as_ref().map_or(
                            caps.is_empty(),
                            |needed| !caps.contains(needed),
                        )
                    }) {
                        Some("NO_REGISTERED_WORKER".into())
                    } else {
                        candidate.hard_block.clone()
                    },
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

        if feeds.get(&key) != Some(&next) {
            let mut next_feeds = feeds.clone();
            next_feeds.insert(key, next);
            self.persist(&next_feeds).await?;
            *feeds = next_feeds;
        }
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
