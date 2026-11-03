//! A03: the real Tokio DAG may offer only READY nodes, never a dependent
//! node whose parent has not reached the required terminal state.
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pipecraft_core::{
    load_pipeline,
    model::{PipelineDefinition, PipelineStep},
};
use pipecraft_report::{status, StepResult};
use pipecraft_runtime::{ExecutionContext, ExecutorRegistry, PipelineEngine, StepExecutor};

struct TraceExecutor {
    starts: Arc<Mutex<Vec<String>>>,
}
impl StepExecutor for TraceExecutor {
    fn step_type(&self) -> &'static str {
        "trace"
    }
    fn run(
        &self,
        step: &PipelineStep,
        _pipeline: &PipelineDefinition,
        _ctx: &ExecutionContext,
    ) -> StepResult {
        self.starts.lock().unwrap().push(step.id.clone());
        if step.id == "parent" {
            std::thread::sleep(Duration::from_millis(30));
        }
        StepResult::new(&step.id, &step.step_type, true, status::OK, "completed")
    }
}

#[test]
fn dependency_pending_never_bypasses_ready_admission() {
    let tmp = tempfile::tempdir().unwrap();
    let input = tmp.path().join("pipeline.yaml");
    std::fs::write(
        &input,
        r#"
schema_version: pipecraft/v1
name: dag-admission-regression
steps:
  - id: child
    type: trace
    needs: [parent]
  - id: independent
    type: trace
  - id: parent
    type: trace
"#,
    )
    .unwrap();
    let pipeline = load_pipeline(&input).unwrap();
    let starts = Arc::new(Mutex::new(Vec::new()));
    let mut registry = ExecutorRegistry::empty();
    registry.register(Box::new(TraceExecutor {
        starts: starts.clone(),
    }));
    let engine = PipelineEngine::new(registry);
    let run = engine.run(
        &pipeline,
        &ExecutionContext::new(tmp.path().to_path_buf())
            .dry_run(false)
            .max_workers(2),
    );
    assert!(run.success);
    let started = starts.lock().unwrap();
    assert_eq!(started.len(), 3);
    assert!(
        started.iter().position(|id| id == "parent").unwrap()
            < started.iter().position(|id| id == "child").unwrap()
    );
    assert!(started.iter().any(|id| id == "independent"));
}
