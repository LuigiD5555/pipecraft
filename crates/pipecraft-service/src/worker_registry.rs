//! S02: bounded, advisory worker registry. This registry does not allocate
//! provider claims, mint epochs or authorize execution.
use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;
use tokio::sync::Mutex;

const MAX_WORKERS: usize = 1024;
const MAX_CAPABILITIES: usize = 32;
const MIN_TTL_MS: u64 = 1_000;
const MAX_TTL_MS: u64 = 3_600_000;

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct WorkerView {
    pub worker_id: String,
    pub capabilities: Vec<String>,
    pub expires_at_ms: u64,
    pub cancelled: bool,
}

#[derive(Default)]
pub struct WorkerRegistry {
    workers: Mutex<BTreeMap<String, WorkerView>>,
}

impl WorkerRegistry {
    pub async fn register(
        &self,
        worker_id: String,
        capabilities: Vec<String>,
        now_ms: u64,
        ttl_ms: u64,
    ) -> Result<WorkerView, String> {
        if worker_id.is_empty()
            || worker_id.len() > 128
            || !worker_id
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || "-_:.".contains(c))
        {
            return Err("invalid worker id".into());
        }
        if now_ms == 0 || !(MIN_TTL_MS..=MAX_TTL_MS).contains(&ttl_ms) {
            return Err("invalid worker TTL or timestamp".into());
        }
        let expires_at_ms = now_ms.checked_add(ttl_ms).ok_or("worker TTL overflow")?;
        if capabilities.is_empty() || capabilities.len() > MAX_CAPABILITIES {
            return Err("invalid worker capabilities".into());
        }
        let unique = capabilities.iter().cloned().collect::<BTreeSet<_>>();
        if unique.len() != capabilities.len()
            || unique.iter().any(|cap| {
                cap.is_empty()
                    || cap.len() > 128
                    || !cap
                        .chars()
                        .all(|c| c.is_ascii_alphanumeric() || "-_:.".contains(c))
            })
        {
            return Err("invalid or duplicate worker capability".into());
        }
        let mut workers = self.workers.lock().await;
        workers.retain(|_, worker| worker.expires_at_ms > now_ms);
        if !workers.contains_key(&worker_id) && workers.len() >= MAX_WORKERS {
            return Err("worker registry backpressure: capacity reached".into());
        }
        let view = WorkerView {
            worker_id: worker_id.clone(),
            capabilities,
            expires_at_ms,
            cancelled: false,
        };
        workers.insert(worker_id, view.clone());
        Ok(view)
    }

    pub async fn cancel(&self, worker_id: &str, now_ms: u64) -> Result<WorkerView, String> {
        let mut workers = self.workers.lock().await;
        let worker = workers.get_mut(worker_id).ok_or("worker not registered")?;
        if worker.expires_at_ms <= now_ms {
            return Err("worker registration expired".into());
        }
        worker.cancelled = true;
        Ok(worker.clone())
    }

    pub async fn available(&self, capability: &str, now_ms: u64) -> Vec<WorkerView> {
        let mut workers = self.workers.lock().await;
        workers.retain(|_, worker| worker.expires_at_ms > now_ms);
        workers
            .values()
            .filter(|worker| {
                !worker.cancelled && worker.capabilities.iter().any(|cap| cap == capability)
            })
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn ttl_cancel_and_capability_are_fail_closed() {
        let registry = WorkerRegistry::default();
        registry
            .register("w1".into(), vec!["browser".into()], 1000, 1000)
            .await
            .unwrap();
        assert_eq!(registry.available("browser", 1999).await.len(), 1);
        assert!(registry.available("http", 1999).await.is_empty());
        assert!(registry.available("browser", 2000).await.is_empty());
        registry
            .register("w1".into(), vec!["browser".into()], 3000, 1000)
            .await
            .unwrap();
        assert!(registry.cancel("w1", 3100).await.unwrap().cancelled);
        assert!(registry.available("browser", 3100).await.is_empty());
        assert!(registry.cancel("w1", 4000).await.is_err());
    }

    #[tokio::test]
    async fn invalid_registration_never_replaces_valid_worker() {
        let registry = WorkerRegistry::default();
        registry
            .register("w1".into(), vec!["http".into()], 1000, 5000)
            .await
            .unwrap();
        assert!(registry
            .register("w1".into(), vec!["http".into(), "http".into()], 1100, 5000)
            .await
            .is_err());
        assert!(registry
            .register("bad/id".into(), vec!["http".into()], 1100, 5000)
            .await
            .is_err());
        assert_eq!(registry.available("http", 1100).await.len(), 1);
    }
}
