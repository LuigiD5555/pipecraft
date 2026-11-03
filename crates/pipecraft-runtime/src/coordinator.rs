//! Shared concurrency controls for one or many pipelines.

use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::capacity::{calculate, CapacityEvidence, CapacityProfile, CapacityResult};
use crate::resources::{GlobalResourceManager, ResourceDemand, ResourceLease};

#[derive(Clone, Debug)]
pub struct RuntimeCoordinator {
    task_slots: Arc<Semaphore>,
    pub resources: GlobalResourceManager,
    max_tasks: usize,
}

impl RuntimeCoordinator {
    pub fn new(max_tasks: usize) -> Self {
        Self::with_resource_points(max_tasks, u32::MAX)
    }

    pub fn with_resource_points(max_tasks: usize, resource_points: u32) -> Self {
        let max_tasks = max_tasks.max(1);
        Self {
            task_slots: Arc::new(Semaphore::new(max_tasks)),
            resources: GlobalResourceManager::with_point_capacity(resource_points),
            max_tasks,
        }
    }

    /// Refresh the shared weighted ledger from explicit, reproducible host
    /// observations. This does not sample hardware, preempt active work,
    /// claim provider jobs, or grant authority to perform external effects.
    pub fn apply_capacity_evidence(
        &self,
        profile: &CapacityProfile,
        evidence: &CapacityEvidence,
    ) -> Result<CapacityResult, String> {
        let result = calculate(profile, evidence)?;
        self.resources.set_point_capacity(result.points)?;
        Ok(result)
    }

    pub fn max_tasks(&self) -> usize {
        self.max_tasks
    }

    pub fn try_task_slot(&self) -> Option<OwnedSemaphorePermit> {
        self.task_slots.clone().try_acquire_owned().ok()
    }

    pub async fn wait_for_task_slot(&self) {
        if let Ok(permit) = self.task_slots.acquire().await {
            drop(permit);
        }
    }

    pub fn try_external_resources(
        &self,
        owner: &str,
        demands: &[ResourceDemand],
    ) -> Result<Option<ResourceLease>, String> {
        self.resources.try_external_occupancy(owner, demands)
    }

    pub fn cancel_external_resource_wait(&self, owner: &str) {
        self.resources.cancel_wait(owner);
    }

    pub async fn wait_for_progress(&self, resource_generation: Option<u64>, task_blocked: bool) {
        match (resource_generation, task_blocked) {
            (Some(generation), true) => {
                tokio::select! {
                    _ = self.resources.changed_since(generation) => {},
                    _ = self.wait_for_task_slot() => {},
                }
            }
            (Some(generation), false) => self.resources.changed_since(generation).await,
            (None, true) => self.wait_for_task_slot().await,
            (None, false) => tokio::task::yield_now().await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resources::{ResourceDemand, ResourceShareMode};

    #[test]
    fn adaptive_evidence_updates_ledger_without_preempting_external_occupancy() {
        let coordinator = RuntimeCoordinator::with_resource_points(4, 4);
        let profile = CapacityProfile::default();
        let mut evidence = CapacityEvidence::default();
        let heavy = [ResourceDemand {
            resource_key: "browser".into(),
            points: 3,
            quantity: 1,
            share_mode: ResourceShareMode::Shared,
            limit: 2,
        }];
        let light = [ResourceDemand {
            resource_key: "http".into(),
            points: 2,
            quantity: 1,
            share_mode: ResourceShareMode::Shared,
            limit: 2,
        }];
        let first = coordinator
            .try_external_resources("browser-a", &heavy)
            .unwrap()
            .unwrap();
        assert!(coordinator
            .try_external_resources("http-a", &light)
            .unwrap()
            .is_none());

        evidence.clean = 5;
        let grown = coordinator
            .apply_capacity_evidence(&profile, &evidence)
            .unwrap();
        assert_eq!(grown.points, 5);
        assert_eq!(grown.reason, "GROWN_BY_EVIDENCE");
        let second = coordinator
            .try_external_resources("http-a", &light)
            .unwrap()
            .unwrap();

        evidence.memory_available_mib = Some(1_724);
        let reduced = coordinator
            .apply_capacity_evidence(&profile, &evidence)
            .unwrap();
        assert_eq!(reduced.points, 1);
        assert_eq!(reduced.reason, "MEMORY_PRESSURE");
        assert_eq!(coordinator.resources.snapshot().points_used, 5);
        drop((first, second));
        assert_eq!(coordinator.resources.snapshot().points_used, 0);
        assert_eq!(coordinator.resources.snapshot().points_limit, 1);
        assert!(coordinator
            .try_external_resources("browser-b", &heavy)
            .unwrap()
            .is_none());
    }

    #[test]
    fn coordinator_exposes_one_weighted_ledger_for_external_work() {
        let coordinator = RuntimeCoordinator::with_resource_points(4, 4);
        let three = [ResourceDemand {
            resource_key: "browser:pool".into(),
            points: 3,
            quantity: 1,
            share_mode: ResourceShareMode::Shared,
            limit: 2,
        }];
        let two = [ResourceDemand {
            resource_key: "network:heavy".into(),
            points: 2,
            quantity: 1,
            share_mode: ResourceShareMode::Shared,
            limit: 2,
        }];
        let lease = coordinator
            .try_external_resources("external-a", &three)
            .unwrap()
            .unwrap();
        assert!(coordinator
            .try_external_resources("external-b", &two)
            .unwrap()
            .is_none());
        drop(lease);
        assert!(coordinator
            .try_external_resources("external-b", &two)
            .unwrap()
            .is_some());
    }
}
