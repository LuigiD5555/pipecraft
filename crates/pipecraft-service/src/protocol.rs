use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const PROTOCOL_VERSION: &str = "pipecraft.ipc/v1";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum IpcRequest {
    Ping,
    /// Advisory, read-only scheduling: no execution or claims.
    PlanAdmission { snapshot: pipecraft_runtime::admission::AdmissionSnapshot },
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
    ValidateSpec { spec: Value },
    PlanSpec {
        spec: Value,
        #[serde(default)]
        from_step: Option<String>,
        #[serde(default)]
        only: Vec<String>,
    },
    Status { run_id: String },
    Jobs,
    Cancel { run_id: String },
    Resume { run_id: String },
    Events {
        run_id: String,
        #[serde(default)]
        after: usize,
        #[serde(default = "default_event_limit")]
        limit: usize,
    },
    Report { run_id: String },
}

fn default_workers() -> usize { 1 }
fn default_event_limit() -> usize { 200 }

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
        Self { protocol: PROTOCOL_VERSION.into(), ok: true, data: Some(data), error: None }
    }

    pub fn error(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            protocol: PROTOCOL_VERSION.into(),
            ok: false,
            data: None,
            error: Some(IpcError { code: code.into(), message: message.into() }),
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
        })).unwrap();
        match request {
            IpcRequest::Submit { pipeline, max_workers, .. } => {
                assert_eq!(pipeline, "demo");
                assert_eq!(max_workers, 1);
            }
            _ => panic!("wrong request variant"),
        }
        let snapshot_request: IpcRequest = serde_json::from_value(serde_json::json!({
            "op": "plan_admission",
            "snapshot": {"revision": 1, "now_ms": 100, "capacity": 2, "candidates": []}
        })).unwrap();
        assert!(matches!(snapshot_request, IpcRequest::PlanAdmission { .. }));
        let response = IpcResponse::ok(serde_json::json!({"pong": true}));
        assert_eq!(response.protocol, PROTOCOL_VERSION);
    }
}
