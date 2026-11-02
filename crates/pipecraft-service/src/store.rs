use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::protocol::{ExternalWorkState, ReplayDisposition};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobRequest {
    pub pipeline: String,
    pub execute: bool,
    pub approve: bool,
    pub labels: Vec<String>,
    pub max_workers: usize,
    pub from_step: Option<String>,
    pub only: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Queued,
    Running,
    Succeeded,
    Failed,
    Cancelled,
    Interrupted,
}

impl JobStatus {
    pub fn terminal(&self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Cancelled | Self::Interrupted
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobRecord {
    pub protocol: String,
    pub run_id: String,
    pub status: JobStatus,
    pub request: JobRequest,
    #[serde(default = "default_runs_dir")]
    pub runs_dir: String,
    #[serde(default)]
    pub pipeline_snapshot_path: String,
    #[serde(default)]
    pub runtime_version: String,
    pub submitted_at: String,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub updated_at: String,
    pub message: String,
    pub attempts: u32,
    pub resume_requested: bool,
    pub report_path: Option<String>,
    #[serde(default)]
    pub attempt_reports: Vec<String>,
    pub events_path: String,
    pub state_path: String,
    pub warning: Option<String>,
}

fn default_runs_dir() -> String {
    ".pipelines/runs".into()
}

impl JobRecord {
    pub fn new(
        run_id: String,
        request: JobRequest,
        runs_dir: &str,
        root: &Path,
        pipeline_snapshot_path: String,
    ) -> Self {
        let now = Utc::now().to_rfc3339();
        let run_dir = root.join(runs_dir).join(&run_id);
        Self {
            protocol: "pipecraft.job/v1".into(),
            run_id,
            status: JobStatus::Queued,
            request,
            runs_dir: runs_dir.into(),
            pipeline_snapshot_path,
            runtime_version: env!("CARGO_PKG_VERSION").into(),
            submitted_at: now.clone(),
            started_at: None,
            finished_at: None,
            updated_at: now,
            message: "queued".into(),
            attempts: 0,
            resume_requested: false,
            report_path: None,
            attempt_reports: Vec::new(),
            events_path: run_dir.join("events.jsonl").display().to_string(),
            state_path: run_dir.join("state.json").display().to_string(),
            warning: None,
        }
    }

    pub fn touch(&mut self) {
        self.updated_at = Utc::now().to_rfc3339();
    }
}

#[derive(Debug, Clone)]
pub struct JobStore {
    dir: PathBuf,
}

impl JobStore {
    pub fn new(dir: PathBuf) -> Self {
        Self { dir }
    }
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub async fn prepare(&self) -> std::io::Result<()> {
        tokio::fs::create_dir_all(&self.dir).await
    }

    pub fn path(&self, run_id: &str) -> PathBuf {
        self.dir.join(format!("{run_id}.json"))
    }

    pub async fn save(&self, record: &JobRecord) -> std::io::Result<()> {
        self.prepare().await?;
        let path = self.path(&record.run_id);
        let tmp = path.with_extension(format!("json.tmp-{}", uuid::Uuid::new_v4().simple()));
        use tokio::io::AsyncWriteExt;
        let bytes = serde_json::to_vec_pretty(record)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
        let mut file = tokio::fs::File::create(&tmp).await?;
        file.write_all(&bytes).await?;
        file.flush().await?;
        file.sync_all().await?;
        drop(file);
        if let Err(first) = tokio::fs::rename(&tmp, &path).await {
            if tokio::fs::try_exists(&path).await.unwrap_or(false) {
                tokio::fs::remove_file(&path).await?;
                tokio::fs::rename(&tmp, &path).await?;
            } else {
                return Err(first);
            }
        }
        if let Some(parent) = path.parent() {
            if let Ok(dir) = std::fs::File::open(parent) {
                let _ = dir.sync_all();
            }
        }
        Ok(())
    }

    pub async fn load(&self, run_id: &str) -> std::io::Result<JobRecord> {
        let bytes = tokio::fs::read(self.path(run_id)).await?;
        serde_json::from_slice(&bytes)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))
    }

    pub async fn load_all(&self) -> std::io::Result<BTreeMap<String, JobRecord>> {
        self.prepare().await?;
        let mut out = BTreeMap::new();
        let mut entries = tokio::fs::read_dir(&self.dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.extension().and_then(|value| value.to_str()) != Some("json") {
                continue;
            }
            let bytes = tokio::fs::read(&path).await?;
            let record = serde_json::from_slice::<JobRecord>(&bytes).map_err(|error| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("invalid durable job record {}: {error}", path.display()),
                )
            })?;
            let expected = path.file_stem().and_then(|name| name.to_str());
            if expected != Some(record.run_id.as_str()) || out.contains_key(&record.run_id) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("job identity mismatch or duplicate in {}", path.display()),
                ));
            }
            out.insert(record.run_id.clone(), record);
        }
        Ok(out)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExternalReconcileObservation {
    pub protocol: String,
    pub work_id: String,
    pub operation_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempt_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claim_epoch: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub leadership_epoch: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receipt_id: Option<String>,
    pub observed_state: ExternalWorkState,
    pub replay: ReplayDisposition,
    pub observed_at: String,
}

impl ExternalReconcileObservation {
    pub fn new(
        work_id: String,
        operation_key: String,
        attempt_id: Option<String>,
        claim_epoch: Option<u64>,
        leadership_epoch: Option<u64>,
        receipt_id: Option<String>,
        observed_state: ExternalWorkState,
        replay: ReplayDisposition,
    ) -> Result<Self, String> {
        if work_id.trim().is_empty() || operation_key.trim().is_empty() {
            return Err("external reconcile work_id and operation_key must be non-empty".into());
        }
        if claim_epoch == Some(0) || leadership_epoch == Some(0) {
            return Err("external reconcile epochs must be positive when present".into());
        }
        if matches!(replay, ReplayDisposition::ReceiptAlreadyCommitted) && receipt_id.is_none() {
            return Err("RECEIPT_ALREADY_COMMITTED requires receipt_id".into());
        }
        Ok(Self {
            protocol: "pipecraft.external-reconcile-observation/v1".into(),
            work_id,
            operation_key,
            attempt_id,
            claim_epoch,
            leadership_epoch,
            receipt_id,
            observed_state,
            replay,
            observed_at: Utc::now().to_rfc3339(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct ExternalRecoveryStore {
    dir: PathBuf,
}

impl ExternalRecoveryStore {
    pub fn new(dir: PathBuf) -> Self {
        Self { dir }
    }

    pub async fn prepare(&self) -> std::io::Result<()> {
        tokio::fs::create_dir_all(&self.dir).await
    }

    fn encode_key(value: &str) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut out = String::with_capacity(value.len() * 2);
        for byte in value.bytes() {
            out.push(HEX[(byte >> 4) as usize] as char);
            out.push(HEX[(byte & 0x0f) as usize] as char);
        }
        out
    }

    pub fn path(&self, work_id: &str, operation_key: &str) -> PathBuf {
        self.dir.join(format!(
            "{}--{}.json",
            Self::encode_key(work_id),
            Self::encode_key(operation_key)
        ))
    }

    pub async fn load(
        &self,
        work_id: &str,
        operation_key: &str,
    ) -> std::io::Result<Option<ExternalReconcileObservation>> {
        let path = self.path(work_id, operation_key);
        let bytes = match tokio::fs::read(&path).await {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        let observation = serde_json::from_slice(&bytes)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
        Ok(Some(observation))
    }

    pub async fn observe(&self, observation: &ExternalReconcileObservation) -> Result<(), String> {
        self.prepare().await.map_err(|error| error.to_string())?;

        if observation.protocol != "pipecraft.external-reconcile-observation/v1"
            || observation.work_id.trim().is_empty()
            || observation.operation_key.trim().is_empty()
            || observation.claim_epoch == Some(0)
            || observation.leadership_epoch == Some(0)
        {
            return Err("invalid external reconcile observation".into());
        }
        if matches!(
            observation.replay,
            ReplayDisposition::ReceiptAlreadyCommitted
        ) && observation.receipt_id.is_none()
        {
            return Err("RECEIPT_ALREADY_COMMITTED requires receipt_id".into());
        }

        if let Some(previous) = self
            .load(&observation.work_id, &observation.operation_key)
            .await
            .map_err(|error| error.to_string())?
        {
            if let (Some(old), Some(new)) = (previous.claim_epoch, observation.claim_epoch) {
                if new < old {
                    return Err("stale claim_epoch in external reconcile observation".into());
                }
            }
            if let (Some(old), Some(new)) =
                (previous.leadership_epoch, observation.leadership_epoch)
            {
                if new < old {
                    return Err("stale leadership_epoch in external reconcile observation".into());
                }
            }
            if let Some(old_receipt) = previous.receipt_id.as_deref() {
                match observation.receipt_id.as_deref() {
                    Some(new_receipt) if new_receipt == old_receipt => {}
                    Some(_) => {
                        return Err(
                            "conflicting receipt_id for external reconcile observation".into()
                        )
                    }
                    None => {
                        return Err(
                            "external reconcile observation cannot discard known receipt_id".into(),
                        )
                    }
                }
            }
            if matches!(previous.replay, ReplayDisposition::ReceiptAlreadyCommitted)
                && !matches!(
                    observation.replay,
                    ReplayDisposition::ReceiptAlreadyCommitted
                )
            {
                return Err(
                    "external reconcile observation cannot regress committed receipt state".into(),
                );
            }
        }

        let path = self.path(&observation.work_id, &observation.operation_key);
        let tmp = path.with_extension(format!("json.tmp-{}", uuid::Uuid::new_v4().simple()));
        let bytes = serde_json::to_vec_pretty(observation).map_err(|error| error.to_string())?;
        let mut file = tokio::fs::File::create(&tmp)
            .await
            .map_err(|error| error.to_string())?;
        use tokio::io::AsyncWriteExt;
        file.write_all(&bytes)
            .await
            .map_err(|error| error.to_string())?;
        file.flush().await.map_err(|error| error.to_string())?;
        file.sync_all().await.map_err(|error| error.to_string())?;
        drop(file);
        tokio::fs::rename(&tmp, &path)
            .await
            .map_err(|error| error.to_string())?;
        if let Some(parent) = path.parent() {
            if let Ok(dir) = std::fs::File::open(parent) {
                let _ = dir.sync_all();
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn corrupt_job_blocks_startup_instead_of_disappearing() {
        let temp = tempfile::tempdir().unwrap();
        let store = JobStore::new(temp.path().join("jobs"));
        store.prepare().await.unwrap();
        tokio::fs::write(store.path("orphan"), b"{broken")
            .await
            .unwrap();
        let error = store.load_all().await.unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("orphan.json"));
    }

    #[tokio::test]
    async fn job_record_round_trips_atomically() {
        let dir = tempfile::tempdir().unwrap();
        let store = JobStore::new(dir.path().join("jobs"));
        let request = JobRequest {
            pipeline: "demo".into(),
            execute: false,
            approve: false,
            labels: vec!["test".into()],
            max_workers: 2,
            from_step: None,
            only: vec![],
        };
        let record = JobRecord::new(
            "run-1".into(),
            request,
            ".pipelines/runs",
            dir.path(),
            "snapshot.yaml".into(),
        );
        store.save(&record).await.unwrap();
        let loaded = store.load("run-1").await.unwrap();
        assert_eq!(loaded.run_id, "run-1");
        assert_eq!(loaded.status, JobStatus::Queued);
        assert_eq!(loaded.request.pipeline, "demo");
    }
    #[tokio::test]
    async fn external_reconcile_store_rejects_stale_epochs_and_receipt_conflicts() {
        use crate::protocol::{ExternalWorkState, ReplayDisposition};

        let dir = tempfile::tempdir().unwrap();
        let store = ExternalRecoveryStore::new(dir.path().join("external-recovery"));
        let committed = ExternalReconcileObservation::new(
            "EXTRACTOR_RUN:821".into(),
            "extractor-run-821/capture/primary".into(),
            Some("attempt-2".into()),
            Some(22),
            Some(7),
            Some("receipt-991".into()),
            ExternalWorkState::SucceededVerified,
            ReplayDisposition::ReceiptAlreadyCommitted,
        )
        .unwrap();
        store.observe(&committed).await.unwrap();

        let loaded = store
            .load("EXTRACTOR_RUN:821", "extractor-run-821/capture/primary")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.receipt_id.as_deref(), Some("receipt-991"));

        let stale = ExternalReconcileObservation::new(
            "EXTRACTOR_RUN:821".into(),
            "extractor-run-821/capture/primary".into(),
            Some("attempt-3".into()),
            Some(21),
            Some(7),
            Some("receipt-991".into()),
            ExternalWorkState::SucceededVerified,
            ReplayDisposition::ReceiptAlreadyCommitted,
        )
        .unwrap();
        assert!(store
            .observe(&stale)
            .await
            .unwrap_err()
            .contains("stale claim_epoch"));

        let conflicting = ExternalReconcileObservation::new(
            "EXTRACTOR_RUN:821".into(),
            "extractor-run-821/capture/primary".into(),
            Some("attempt-3".into()),
            Some(23),
            Some(8),
            Some("receipt-other".into()),
            ExternalWorkState::SucceededVerified,
            ReplayDisposition::ReceiptAlreadyCommitted,
        )
        .unwrap();
        assert!(store
            .observe(&conflicting)
            .await
            .unwrap_err()
            .contains("conflicting receipt_id"));
    }
}
