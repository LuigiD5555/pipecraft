use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const PROTOCOL_VERSION: &str = "pipecraft.ipc/v1";

// G1 contract-only transport: typed validation/roundtrip, never execution.
pub const EXTERNAL_WORK_PROTOCOL_VERSION: &str = "pipecraft.external-work/v1";
pub const EXTERNAL_WORK_CAPABILITY: &str = "external_work_contract_v1";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ExternalWorkState {
    Queued,
    Blocked,
    Offered,
    Claimed,
    Running,
    SucceededVerified,
    Deferred,
    NeedsHuman,
    FailedRetryable,
    FailedFinal,
    Cancelled,
    Uncertain,
    LeaseExpired,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OfferConfirmationResult {
    Confirmed,
    StaleOffer,
    StaleSnapshot,
    PolicyBlocked,
    SourceBusy,
    ScopeBusy,
    NotFound,
    NoPrimary,
    ReconciliationRequired,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ExternalOutcomeStatus {
    SucceededVerified,
    Deferred,
    NeedsHuman,
    FailedRetryable,
    FailedFinal,
    Cancelled,
    Uncertain,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResourceShareMode {
    Shared,
    Exclusive,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ReplayDisposition {
    SafeToReplay,
    ReceiptAlreadyCommitted,
    ReconciliationRequired,
    ForbiddenUncertain,
    StaleFence,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExternalResourceDemand {
    pub resource_key: String,
    pub points: u32,
    pub quantity: u32,
    pub share_mode: ResourceShareMode,
    pub limit: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExternalWorkCandidate {
    pub work_id: String,
    pub owner_group: String,
    pub queue_kind: String,
    pub priority: i64,
    pub eligible_since_ms: u64,
    pub ready_at_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub not_before_ms: Option<u64>,
    pub weight: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hard_block: Option<String>,
    #[serde(default)]
    pub scopes: Vec<String>,
    #[serde(default)]
    pub resources: Vec<ExternalResourceDemand>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub required_worker_capability: Option<String>,
    pub source_limit: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExternalActiveClaim {
    pub work_id: String,
    pub attempt_id: String,
    pub claim_epoch: u64,
    pub leadership_epoch: u64,
    pub lease_until_ms: u64,
    #[serde(default)]
    pub resources: Vec<ExternalResourceDemand>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum ExternalWorkMessage {
    ProviderSnapshot {
        protocol: String,
        request_id: String,
        installation_id: String,
        provider_id: String,
        snapshot_revision: u64,
        observed_at_ms: u64,
        work: Vec<ExternalWorkCandidate>,
        #[serde(default)]
        active_claims: Vec<ExternalActiveClaim>,
        resource_evidence_revision: u64,
    },
    ProviderDelta {
        protocol: String,
        request_id: String,
        installation_id: String,
        provider_id: String,
        base_revision: u64,
        snapshot_revision: u64,
        observed_at_ms: u64,
        #[serde(default)]
        upsert: Vec<ExternalWorkCandidate>,
        #[serde(default)]
        removed_work_ids: Vec<String>,
    },
    WorkOffer {
        protocol: String,
        request_id: String,
        installation_id: String,
        provider_id: String,
        offer_id: String,
        work_id: String,
        snapshot_revision: u64,
        leadership_epoch: u64,
        offer_deadline_ms: u64,
        #[serde(default)]
        required_capabilities: Vec<String>,
        decision_trace_id: String,
    },
    OfferConfirmation {
        protocol: String,
        request_id: String,
        installation_id: String,
        provider_id: String,
        offer_id: String,
        work_id: String,
        snapshot_revision: u64,
        result: OfferConfirmationResult,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        claim_epoch: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        claim_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        lease_until_ms: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        assigned_worker_capability: Option<String>,
        #[serde(default)]
        operation_keys: Vec<String>,
    },
    Reconcile {
        protocol: String,
        request_id: String,
        installation_id: String,
        provider_id: String,
        work_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        attempt_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        claim_epoch: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        receipt_id: Option<String>,
        observed_state: ExternalWorkState,
        replay: ReplayDisposition,
    },
    VerifiedOutcome {
        protocol: String,
        request_id: String,
        installation_id: String,
        provider_id: String,
        work_id: String,
        attempt_id: String,
        claim_epoch: u64,
        status: ExternalOutcomeStatus,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        receipt_id: Option<String>,
        operation_key: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        result_digest: Option<String>,
    },
}

impl ExternalWorkMessage {
    fn common(&self) -> (&str, &str, &str, &str) {
        match self {
            Self::ProviderSnapshot {
                protocol,
                request_id,
                installation_id,
                provider_id,
                ..
            }
            | Self::ProviderDelta {
                protocol,
                request_id,
                installation_id,
                provider_id,
                ..
            }
            | Self::WorkOffer {
                protocol,
                request_id,
                installation_id,
                provider_id,
                ..
            }
            | Self::OfferConfirmation {
                protocol,
                request_id,
                installation_id,
                provider_id,
                ..
            }
            | Self::Reconcile {
                protocol,
                request_id,
                installation_id,
                provider_id,
                ..
            }
            | Self::VerifiedOutcome {
                protocol,
                request_id,
                installation_id,
                provider_id,
                ..
            } => (protocol, request_id, installation_id, provider_id),
        }
    }
}

fn valid_token(value: &str, max: usize) -> bool {
    !value.is_empty()
        && value.len() <= max
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._:/-".contains(&byte))
}

fn valid_provider(value: &str) -> bool {
    valid_token(value, 128)
        && value.contains('/')
        && !value.starts_with('/')
        && !value.ends_with('/')
}

fn validate_resource(resource: &ExternalResourceDemand) -> Result<(), String> {
    if !valid_token(&resource.resource_key, 256) {
        return Err("invalid resource_key".into());
    }
    if resource.points == 0 || resource.quantity == 0 || resource.limit == 0 {
        return Err("resource points, quantity and limit must be positive".into());
    }
    if resource.quantity > resource.limit {
        return Err("resource quantity exceeds its declared limit".into());
    }
    Ok(())
}

fn validate_candidate(candidate: &ExternalWorkCandidate) -> Result<(), String> {
    if !valid_token(&candidate.work_id, 192) || !candidate.work_id.contains(':') {
        return Err("invalid work_id".into());
    }
    if !valid_token(&candidate.owner_group, 192) || !valid_token(&candidate.queue_kind, 64) {
        return Err("invalid owner_group or queue_kind".into());
    }
    if candidate.weight == 0 || candidate.source_limit == 0 {
        return Err("weight and source_limit must be positive".into());
    }
    if let Some(capability) = &candidate.required_worker_capability {
        if !valid_token(capability, 128) {
            return Err("invalid worker capability".into());
        }
    }
    for scope in &candidate.scopes {
        if !valid_token(scope, 256) {
            return Err("invalid scope".into());
        }
    }
    for resource in &candidate.resources {
        validate_resource(resource)?;
    }
    Ok(())
}

pub fn validate_external_work_message(message: &ExternalWorkMessage) -> Result<(), String> {
    let (protocol, request_id, installation_id, provider_id) = message.common();
    if protocol != EXTERNAL_WORK_PROTOCOL_VERSION {
        return Err("unsupported external-work protocol".into());
    }
    if !valid_token(request_id, 128)
        || !valid_token(installation_id, 128)
        || !valid_provider(provider_id)
    {
        return Err("invalid request, installation or provider identity".into());
    }
    match message {
        ExternalWorkMessage::ProviderSnapshot {
            work,
            active_claims,
            ..
        } => {
            if work.len() > 10_000 || active_claims.len() > 10_000 {
                return Err("external-work snapshot exceeds bounded cardinality".into());
            }
            let mut ids = std::collections::BTreeSet::new();
            for candidate in work {
                validate_candidate(candidate)?;
                if !ids.insert(candidate.work_id.as_str()) {
                    return Err("duplicate work_id in provider snapshot".into());
                }
            }
            for claim in active_claims {
                if !valid_token(&claim.work_id, 192)
                    || !valid_token(&claim.attempt_id, 128)
                    || claim.claim_epoch == 0
                    || claim.leadership_epoch == 0
                {
                    return Err("invalid active claim".into());
                }
                for resource in &claim.resources {
                    validate_resource(resource)?;
                }
            }
        }
        ExternalWorkMessage::ProviderDelta {
            base_revision,
            snapshot_revision,
            upsert,
            removed_work_ids,
            ..
        } => {
            if snapshot_revision <= base_revision {
                return Err("provider delta revision must advance".into());
            }
            let mut ids = std::collections::BTreeSet::new();
            for candidate in upsert {
                validate_candidate(candidate)?;
                if !ids.insert(candidate.work_id.as_str()) {
                    return Err("duplicate work_id in provider delta".into());
                }
            }
            for work_id in removed_work_ids {
                if !valid_token(work_id, 192) || !work_id.contains(':') {
                    return Err("invalid removed work_id".into());
                }
                if ids.contains(work_id.as_str()) {
                    return Err("provider delta cannot upsert and remove the same work_id".into());
                }
            }
        }
        ExternalWorkMessage::WorkOffer {
            offer_id,
            work_id,
            leadership_epoch,
            offer_deadline_ms,
            required_capabilities,
            decision_trace_id,
            ..
        } => {
            if !valid_token(offer_id, 128)
                || !valid_token(work_id, 192)
                || !work_id.contains(':')
                || *leadership_epoch == 0
                || *offer_deadline_ms == 0
                || !valid_token(decision_trace_id, 128)
            {
                return Err("invalid work offer".into());
            }
            let mut seen = std::collections::BTreeSet::new();
            for capability in required_capabilities {
                if !valid_token(capability, 128) || !seen.insert(capability.as_str()) {
                    return Err("invalid or duplicate required capability".into());
                }
            }
        }
        ExternalWorkMessage::OfferConfirmation {
            offer_id,
            work_id,
            result,
            claim_epoch,
            claim_id,
            lease_until_ms,
            assigned_worker_capability,
            operation_keys,
            ..
        } => {
            if !valid_token(offer_id, 128) || !valid_token(work_id, 192) || !work_id.contains(':') {
                return Err("invalid offer confirmation identity".into());
            }
            if matches!(result, OfferConfirmationResult::Confirmed)
                && (claim_epoch.unwrap_or(0) == 0
                    || claim_id
                        .as_deref()
                        .map(|value| !valid_token(value, 128))
                        .unwrap_or(true)
                    || lease_until_ms.unwrap_or(0) == 0
                    || assigned_worker_capability
                        .as_deref()
                        .map(|value| !valid_token(value, 128))
                        .unwrap_or(true)
                    || operation_keys.is_empty()
                    || operation_keys.iter().any(|value| !valid_token(value, 256)))
            {
                return Err("confirmed offer is missing claim fields".into());
            }
        }
        ExternalWorkMessage::Reconcile {
            work_id,
            attempt_id,
            claim_epoch,
            receipt_id,
            ..
        } => {
            if !valid_token(work_id, 192) || !work_id.contains(':') {
                return Err("invalid reconcile work_id".into());
            }
            if attempt_id
                .as_deref()
                .map(|value| !valid_token(value, 128))
                .unwrap_or(false)
                || claim_epoch.map(|value| value == 0).unwrap_or(false)
                || receipt_id
                    .as_deref()
                    .map(|value| !valid_token(value, 128))
                    .unwrap_or(false)
            {
                return Err("invalid reconcile identity".into());
            }
        }
        ExternalWorkMessage::VerifiedOutcome {
            work_id,
            attempt_id,
            claim_epoch,
            status,
            receipt_id,
            operation_key,
            result_digest,
            ..
        } => {
            if !valid_token(work_id, 192)
                || !work_id.contains(':')
                || !valid_token(attempt_id, 128)
                || *claim_epoch == 0
                || !valid_token(operation_key, 256)
            {
                return Err("invalid verified outcome identity".into());
            }
            if matches!(status, ExternalOutcomeStatus::SucceededVerified)
                && receipt_id
                    .as_deref()
                    .map(|value| !valid_token(value, 128))
                    .unwrap_or(true)
            {
                return Err("SUCCEEDED_VERIFIED requires a durable receipt_id".into());
            }
            if let Some(value) = result_digest {
                if !value.starts_with("sha256:")
                    || value.len() != 71
                    || !value[7..].bytes().all(|byte| byte.is_ascii_hexdigit())
                {
                    return Err("result_digest must be sha256:<64 hex chars>".into());
                }
            }
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum IpcRequest {
    Ping,
    /// Advisory, read-only scheduling: no execution or claims.
    PlanAdmission {
        snapshot: pipecraft_runtime::admission::AdmissionSnapshot,
    },
    ExternalWorkContract {
        message: ExternalWorkMessage,
    },
    /// S01: admit one validated provider snapshot into the resident Tokio
    /// scheduling driver. Resulting offers are provisional and side-effect-free.
    PlanExternalFeed {
        message: ExternalWorkMessage,
        #[serde(default)]
        settings: pipecraft_runtime::admission::AdmissionSettings,
        #[serde(default)]
        capacity: Option<u32>,
        #[serde(default)]
        last_dispatch_ms: std::collections::BTreeMap<String, u64>,
        #[serde(default)]
        enforce_worker_registration: bool,
    },
    /// S02: worker registration is advisory, never an execution claim.
    RegisterWorker {
        worker_id: String,
        capabilities: Vec<String>,
        now_ms: u64,
        ttl_ms: u64,
    },
    CancelWorker {
        worker_id: String,
        now_ms: u64,
    },
    AvailableWorkers {
        capability: String,
        now_ms: u64,
    },
    Submit {
        pipeline: String,
        #[serde(default)]
        execute: bool,
        #[serde(default)]
        approve: bool,
        #[serde(default)]
        labels: Vec<String>,
        #[serde(default = "default_workers")]
        max_workers: usize,
        #[serde(default)]
        from_step: Option<String>,
        #[serde(default)]
        only: Vec<String>,
    },
    SubmitSpec {
        spec: Value,
        #[serde(default)]
        execute: bool,
        #[serde(default)]
        approve: bool,
        #[serde(default)]
        labels: Vec<String>,
        #[serde(default = "default_workers")]
        max_workers: usize,
        #[serde(default)]
        from_step: Option<String>,
        #[serde(default)]
        only: Vec<String>,
    },
    ValidateSpec {
        spec: Value,
    },
    PlanSpec {
        spec: Value,
        #[serde(default)]
        from_step: Option<String>,
        #[serde(default)]
        only: Vec<String>,
    },
    Status {
        run_id: String,
    },
    Jobs,
    Cancel {
        run_id: String,
    },
    Resume {
        run_id: String,
    },
    Events {
        run_id: String,
        #[serde(default)]
        after: usize,
        #[serde(default = "default_event_limit")]
        limit: usize,
    },
    Report {
        run_id: String,
    },
}

fn default_workers() -> usize {
    1
}
fn default_event_limit() -> usize {
    200
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IpcResponse {
    pub protocol: String,
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<IpcError>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IpcError {
    pub code: String,
    pub message: String,
}

impl IpcResponse {
    pub fn ok(data: Value) -> Self {
        Self {
            protocol: PROTOCOL_VERSION.into(),
            ok: true,
            data: Some(data),
            error: None,
        }
    }

    pub fn error(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            protocol: PROTOCOL_VERSION.into(),
            ok: false,
            data: None,
            error: Some(IpcError {
                code: code.into(),
                message: message.into(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protocol_is_tagged_and_versioned() {
        let request: IpcRequest = serde_json::from_value(serde_json::json!({
            "op": "submit",
            "pipeline": "demo"
        }))
        .unwrap();
        match request {
            IpcRequest::Submit {
                pipeline,
                max_workers,
                ..
            } => {
                assert_eq!(pipeline, "demo");
                assert_eq!(max_workers, 1);
            }
            _ => panic!("wrong request variant"),
        }
        let snapshot_request: IpcRequest = serde_json::from_value(serde_json::json!({
            "op": "plan_admission",
            "snapshot": {"revision": 1, "now_ms": 100, "capacity": 2, "candidates": []}
        }))
        .unwrap();
        assert!(matches!(snapshot_request, IpcRequest::PlanAdmission { .. }));
        let response = IpcResponse::ok(serde_json::json!({"pong": true}));
        assert_eq!(response.protocol, PROTOCOL_VERSION);
    }
}
