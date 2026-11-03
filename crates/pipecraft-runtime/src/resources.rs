//! Unified named-resource coordination for DAG steps and external occupancy.
//!
//! Legacy DAG resources and typed external demands share this single ledger.
//! Exclusive waiters are queued so a stream of new shared readers cannot starve
//! a waiting writer.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use pipecraft_core::model::PipelineStep;
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResourceShareMode {
    Shared,
    Exclusive,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResourceDemand {
    pub resource_key: String,
    pub points: u32,
    pub quantity: u32,
    pub share_mode: ResourceShareMode,
    pub limit: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResourceOccupancy {
    pub shared_quantity: u32,
    pub exclusive: bool,
    pub waiting_exclusive: usize,
    pub limit: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResourceLedgerSnapshot {
    pub points_used: u32,
    pub points_limit: u32,
    pub resources: BTreeMap<String, ResourceOccupancy>,
}

#[derive(Debug, Default)]
struct NamedResourceState {
    shared_quantity: u32,
    exclusive: bool,
    limit: Option<u32>,
    waiting_exclusive: VecDeque<String>,
}

impl NamedResourceState {
    fn is_empty(&self) -> bool {
        self.shared_quantity == 0 && !self.exclusive && self.waiting_exclusive.is_empty()
    }
}

#[derive(Debug)]
struct ResourceState {
    named: BTreeMap<String, NamedResourceState>,
    points_used: u32,
    points_limit: u32,
}

impl ResourceState {
    fn new(points_limit: u32) -> Self {
        Self {
            named: BTreeMap::new(),
            points_used: 0,
            points_limit: points_limit.max(1),
        }
    }
}

#[derive(Clone, Debug)]
pub struct GlobalResourceManager {
    state: Arc<Mutex<ResourceState>>,
    notify: Arc<Notify>,
    generation: Arc<AtomicU64>,
}

impl Default for GlobalResourceManager {
    fn default() -> Self {
        Self::with_point_capacity(u32::MAX)
    }
}

impl GlobalResourceManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_point_capacity(points_limit: u32) -> Self {
        Self {
            state: Arc::new(Mutex::new(ResourceState::new(points_limit))),
            notify: Arc::new(Notify::new()),
            generation: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Apply a host-supplied point budget to the same ledger used by DAG and
    /// external work. A reduction never revokes existing leases; it only
    /// prevents new admissions until occupancy falls below the new limit.
    /// Readers waiting on resources are notified when the budget changes.
    pub fn set_point_capacity(&self, points_limit: u32) -> Result<(), String> {
        if points_limit == 0 {
            return Err("resource point capacity must be positive".into());
        }
        let changed = {
            let mut state = self.state.lock().expect("resource state poisoned");
            if state.points_limit == points_limit {
                false
            } else {
                state.points_limit = points_limit;
                true
            }
        };
        if changed {
            self.signal_change();
        }
        Ok(())
    }

    /// Preserve the legacy DAG resource API while routing it through the same
    /// ledger used by external occupancy.
    pub fn try_acquire(&self, step: &PipelineStep) -> Option<ResourceLease> {
        // A blocked DAG node retries acquisition. Its waiter identity must stay
        // stable across retries or an abandoned earlier identity can remain at
        // the head of the exclusive FIFO forever.
        let owner = format!("dag:{}", step.id);
        let mut demands = Vec::new();
        for key in &step.exclusive_resources {
            demands.push(ResourceDemand {
                resource_key: key.clone(),
                points: 1,
                quantity: 1,
                share_mode: ResourceShareMode::Exclusive,
                limit: 1,
            });
        }
        for key in &step.shared_resources {
            demands.push(ResourceDemand {
                resource_key: key.clone(),
                points: 1,
                quantity: 1,
                share_mode: ResourceShareMode::Shared,
                limit: u32::MAX,
            });
        }
        self.try_acquire_demands(&owner, &demands).ok().flatten()
    }

    /// Try to reserve typed resources for confirmed external work or physical
    /// occupancy. This never performs scheduling or domain authorization.
    pub fn try_acquire_demands(
        &self,
        owner: &str,
        demands: &[ResourceDemand],
    ) -> Result<Option<ResourceLease>, String> {
        validate_request(owner, demands)?;
        let points = demand_points(demands);
        let mut state = self.state.lock().expect("resource state poisoned");

        if state.points_used.saturating_add(points) > state.points_limit {
            return Ok(None);
        }

        let mut blocked = false;
        for demand in demands {
            let entry = state.named.entry(demand.resource_key.clone()).or_default();

            if let Some(limit) = entry.limit {
                if limit != demand.limit {
                    return Err(format!(
                        "resource {} declared inconsistent limits",
                        demand.resource_key
                    ));
                }
            }

            match demand.share_mode {
                ResourceShareMode::Exclusive => {
                    let queue_blocks = entry
                        .waiting_exclusive
                        .front()
                        .is_some_and(|front| front != owner);
                    if entry.exclusive || entry.shared_quantity > 0 || queue_blocks {
                        if !entry.waiting_exclusive.iter().any(|queued| queued == owner) {
                            entry.waiting_exclusive.push_back(owner.to_string());
                        }
                        blocked = true;
                    }
                }
                ResourceShareMode::Shared => {
                    if entry.exclusive
                        || !entry.waiting_exclusive.is_empty()
                        || entry.shared_quantity.saturating_add(demand.quantity) > demand.limit
                    {
                        blocked = true;
                    }
                }
            }
        }
        if blocked {
            return Ok(None);
        }

        for demand in demands {
            let entry = state.named.entry(demand.resource_key.clone()).or_default();
            entry.limit.get_or_insert(demand.limit);
            match demand.share_mode {
                ResourceShareMode::Exclusive => {
                    if entry
                        .waiting_exclusive
                        .front()
                        .is_some_and(|front| front == owner)
                    {
                        entry.waiting_exclusive.pop_front();
                    }
                    entry.exclusive = true;
                }
                ResourceShareMode::Shared => {
                    entry.shared_quantity = entry.shared_quantity.saturating_add(demand.quantity);
                }
            }
        }
        state.points_used = state.points_used.saturating_add(points);

        Ok(Some(ResourceLease {
            manager: self.clone(),
            owner: owner.to_string(),
            demands: demands.to_vec(),
            points,
            released: false,
        }))
    }

    pub fn try_external_occupancy(
        &self,
        owner: &str,
        demands: &[ResourceDemand],
    ) -> Result<Option<ResourceLease>, String> {
        self.try_acquire_demands(owner, demands)
    }

    pub fn cancel_wait(&self, owner: &str) {
        let mut changed = false;
        {
            let mut state = self.state.lock().expect("resource state poisoned");
            for entry in state.named.values_mut() {
                let before = entry.waiting_exclusive.len();
                entry.waiting_exclusive.retain(|queued| queued != owner);
                changed |= before != entry.waiting_exclusive.len();
            }
            state.named.retain(|_, entry| !entry.is_empty());
        }
        if changed {
            self.signal_change();
        }
    }

    pub fn snapshot(&self) -> ResourceLedgerSnapshot {
        let state = self.state.lock().expect("resource state poisoned");
        ResourceLedgerSnapshot {
            points_used: state.points_used,
            points_limit: state.points_limit,
            resources: state
                .named
                .iter()
                .map(|(key, entry)| {
                    (
                        key.clone(),
                        ResourceOccupancy {
                            shared_quantity: entry.shared_quantity,
                            exclusive: entry.exclusive,
                            waiting_exclusive: entry.waiting_exclusive.len(),
                            limit: entry.limit,
                        },
                    )
                })
                .collect(),
        }
    }

    /// Monotonic generation used to avoid lost wakeups between a failed
    /// acquisition and registering a waiter.
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    pub async fn changed_since(&self, observed: u64) {
        loop {
            if self.generation() != observed {
                return;
            }
            let notified = self.notify.notified();
            if self.generation() != observed {
                return;
            }
            notified.await;
        }
    }

    fn release(&self, owner: &str, demands: &[ResourceDemand], points: u32) {
        {
            let mut state = self.state.lock().expect("resource state poisoned");
            for demand in demands {
                if let Some(entry) = state.named.get_mut(&demand.resource_key) {
                    match demand.share_mode {
                        ResourceShareMode::Exclusive => {
                            entry.exclusive = false;
                        }
                        ResourceShareMode::Shared => {
                            entry.shared_quantity =
                                entry.shared_quantity.saturating_sub(demand.quantity);
                        }
                    }
                    entry.waiting_exclusive.retain(|queued| queued != owner);
                }
            }
            state.points_used = state.points_used.saturating_sub(points);
            state.named.retain(|_, entry| !entry.is_empty());
        }
        self.signal_change();
    }

    fn signal_change(&self) {
        self.generation.fetch_add(1, Ordering::AcqRel);
        self.notify.notify_waiters();
    }
}

fn demand_points(demands: &[ResourceDemand]) -> u32 {
    demands
        .iter()
        .map(|demand| demand.points)
        .max()
        .unwrap_or(0)
}

fn validate_request(owner: &str, demands: &[ResourceDemand]) -> Result<(), String> {
    if owner.trim().is_empty() {
        return Err("resource owner must be non-empty".into());
    }
    let mut keys = BTreeSet::new();
    for demand in demands {
        if demand.resource_key.trim().is_empty()
            || !keys.insert(demand.resource_key.as_str())
            || demand.points == 0
            || demand.quantity == 0
            || demand.limit == 0
            || demand.quantity > demand.limit
        {
            return Err("invalid or duplicate resource demand".into());
        }
    }
    Ok(())
}

#[derive(Debug)]
pub struct ResourceLease {
    manager: GlobalResourceManager,
    owner: String,
    demands: Vec<ResourceDemand>,
    points: u32,
    released: bool,
}

impl ResourceLease {
    pub fn release(mut self) {
        if !self.released {
            self.manager
                .release(&self.owner, &self.demands, self.points);
            self.released = true;
        }
    }
}

impl Drop for ResourceLease {
    fn drop(&mut self) {
        if !self.released {
            self.manager
                .release(&self.owner, &self.demands, self.points);
            self.released = true;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn demand(
        key: &str,
        points: u32,
        quantity: u32,
        mode: ResourceShareMode,
        limit: u32,
    ) -> ResourceDemand {
        ResourceDemand {
            resource_key: key.into(),
            points,
            quantity,
            share_mode: mode,
            limit,
        }
    }

    #[test]
    fn capacity_changes_are_non_preemptive_and_wake_waiters() {
        let resources = GlobalResourceManager::with_point_capacity(4);
        let held = resources
            .try_acquire_demands(
                "held",
                &[demand("browser", 3, 1, ResourceShareMode::Shared, 2)],
            )
            .unwrap()
            .unwrap();

        let generation = resources.generation();
        resources.set_point_capacity(1).unwrap();
        assert!(resources.generation() > generation);
        assert_eq!(resources.snapshot().points_used, 3);
        assert_eq!(resources.snapshot().points_limit, 1);
        assert!(resources
            .try_acquire_demands(
                "new",
                &[demand("cpu", 1, 1, ResourceShareMode::Shared, 2)],
            )
            .unwrap()
            .is_none());
        drop(held);
        assert!(resources
            .try_acquire_demands(
                "new",
                &[demand("cpu", 1, 1, ResourceShareMode::Shared, 2)],
            )
            .unwrap()
            .is_some());
        let stable_generation = resources.generation();
        resources.set_point_capacity(1).unwrap();
        assert_eq!(resources.generation(), stable_generation);
        assert!(resources.set_point_capacity(0).is_err());
        assert_eq!(resources.snapshot().points_limit, 1);
    }

    #[test]
    fn weighted_points_prevent_oversubscription() {
        let resources = GlobalResourceManager::with_point_capacity(4);
        let first = resources
            .try_acquire_demands("one", &[demand("cpu", 3, 1, ResourceShareMode::Shared, 4)])
            .unwrap()
            .unwrap();
        assert!(resources
            .try_acquire_demands(
                "two",
                &[demand("memory", 2, 1, ResourceShareMode::Shared, 4)],
            )
            .unwrap()
            .is_none());
        assert_eq!(resources.snapshot().points_used, 3);
        drop(first);
        assert_eq!(resources.snapshot().points_used, 0);
    }

    #[test]
    fn shared_quantity_obeys_limit() {
        let resources = GlobalResourceManager::new();
        let a = resources
            .try_acquire_demands(
                "a",
                &[demand("browser-pool", 1, 1, ResourceShareMode::Shared, 2)],
            )
            .unwrap()
            .unwrap();
        let b = resources
            .try_acquire_demands(
                "b",
                &[demand("browser-pool", 1, 1, ResourceShareMode::Shared, 2)],
            )
            .unwrap()
            .unwrap();
        assert!(resources
            .try_acquire_demands(
                "c",
                &[demand("browser-pool", 1, 1, ResourceShareMode::Shared, 2)],
            )
            .unwrap()
            .is_none());
        drop((a, b));
    }

    #[test]
    fn waiting_exclusive_writer_blocks_new_shared_readers() {
        let resources = GlobalResourceManager::new();
        let reader = resources
            .try_acquire_demands(
                "reader-1",
                &[demand("profile:19", 1, 1, ResourceShareMode::Shared, 8)],
            )
            .unwrap()
            .unwrap();

        assert!(resources
            .try_acquire_demands(
                "writer",
                &[demand("profile:19", 1, 1, ResourceShareMode::Exclusive, 8)],
            )
            .unwrap()
            .is_none());
        assert!(resources
            .try_acquire_demands(
                "reader-2",
                &[demand("profile:19", 1, 1, ResourceShareMode::Shared, 8)],
            )
            .unwrap()
            .is_none());

        drop(reader);
        let writer = resources
            .try_acquire_demands(
                "writer",
                &[demand("profile:19", 1, 1, ResourceShareMode::Exclusive, 8)],
            )
            .unwrap()
            .unwrap();
        assert!(resources.snapshot().resources["profile:19"].exclusive);
        drop(writer);
    }

    #[test]
    fn external_occupancy_and_dag_share_named_resource_state() {
        let resources = GlobalResourceManager::new();
        let external = resources
            .try_external_occupancy(
                "human-session",
                &[demand("gpu", 1, 1, ResourceShareMode::Exclusive, 1)],
            )
            .unwrap()
            .unwrap();

        let step = PipelineStep {
            id: "dag".into(),
            step_type: "command".into(),
            description: String::new(),
            repo: String::new(),
            command: String::new(),
            risk: String::new(),
            requires_approval: false,
            required: true,
            needs: vec![],
            run_if: "all_success".into(),
            requires: vec![],
            provides: vec![],
            exclusive_resources: vec!["gpu".into()],
            shared_resources: vec![],
            barrier: false,
            rules: Default::default(),
            with: Default::default(),
        };
        assert!(resources.try_acquire(&step).is_none());
        drop(external);
        assert!(resources.try_acquire(&step).is_some());
    }
}
