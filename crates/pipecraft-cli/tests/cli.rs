//! End-to-end CLI tests. These invoke the compiled `pipecraft` binary against
//! the bundled examples and assert on its exit code and stdout.
//!
//! Examples live at the workspace root (`../../examples` from this crate).

use assert_cmd::Command;
use predicates::prelude::*;
use std::fs;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

/// Absolute path to an example workspace root.
fn example(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("examples")
        .join(name)
}

fn pipecraft() -> Command {
    Command::cargo_bin("pipecraft").expect("binary `pipecraft` should build")
}

fn copy_tree(source: &Path, destination: &Path) {
    fs::create_dir_all(destination).expect("temporary example directory should be created");
    for entry in fs::read_dir(source).expect("example directory should be readable") {
        let entry = entry.expect("example entry should be readable");
        let target = destination.join(entry.file_name());
        if entry
            .file_type()
            .expect("example entry type should be readable")
            .is_dir()
        {
            copy_tree(&entry.path(), &target);
        } else {
            fs::copy(entry.path(), target).expect("example file should be copied");
        }
    }
}

fn isolated_example(name: &str) -> (TempDir, PathBuf) {
    let temp = tempfile::tempdir().expect("temporary workspace should be created");
    let root = temp.path().join(name);
    copy_tree(&example(name), &root);
    (temp, root)
}

#[test]
fn list_shows_hello_world() {
    pipecraft()
        .arg("--root")
        .arg(example("01-hello-world"))
        .arg("list")
        .assert()
        .success()
        .stdout(predicate::str::contains("hello-world"));
}

#[test]
fn validate_hello_world_is_valid() {
    pipecraft()
        .arg("--root")
        .arg(example("01-hello-world"))
        .args(["validate", "hello-world"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Pipeline valid"));
}

#[test]
fn validate_unknown_pipeline_fails_cleanly() {
    pipecraft()
        .arg("--root")
        .arg(example("01-hello-world"))
        .args(["validate", "does-not-exist"])
        .assert()
        .failure();
}

#[test]
fn plan_lists_execution_order() {
    pipecraft()
        .arg("--root")
        .arg(example("01-hello-world"))
        .args(["plan", "hello-world"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Execution order"));
}

#[test]
fn run_is_dry_run_by_default() {
    pipecraft()
        .arg("--root")
        .arg(example("01-hello-world"))
        .args(["run", "hello-world"])
        .assert()
        .success()
        .stdout(predicate::str::contains("dry-run"));
}

#[test]
fn ci_example_routes_to_build_pipeline() {
    pipecraft()
        .arg("--root")
        .arg(example("02-ci-build-test-deploy"))
        .args(["route", "ci"])
        .assert()
        .success()
        .stdout(predicate::str::contains("build-test-deploy"));
}

#[test]
fn release_pipeline_validates() {
    pipecraft()
        .arg("--root")
        .arg(example("04-release-management"))
        .args(["validate", "release"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Pipeline valid"));
}

#[test]
fn open_core_boundary_pipeline_validates() {
    pipecraft()
        .arg("--root")
        .arg(example("07-open-core-boundaries"))
        .args(["validate", "lite-boundary"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Pipeline valid"));
}

#[test]
fn manual_approval_blocks_without_approve_flag() {
    // The deploy/publish steps require approval; a plain `run` should not
    // report overall success because a gated step is left needing approval.
    pipecraft()
        .arg("--root")
        .arg(example("02-ci-build-test-deploy"))
        .args(["run", "build-test-deploy"])
        .assert()
        .failure()
        .stdout(predicate::str::contains("needs_approval"));
}

#[test]
fn bare_label_routes_to_scraper_pipeline() {
    pipecraft()
        .arg("--root")
        .arg(example("03-maintenance-scraper"))
        .args(["route", "scraper"])
        .assert()
        .success()
        .stdout(predicate::str::contains("scraper-maintenance"));
}

#[test]
fn run_from_filters_steps() {
    let (_temp, root) = isolated_example("01-hello-world");
    pipecraft()
        .arg("--root")
        .arg(root)
        .args(["run", "hello-world", "--from", "show_date"])
        .assert()
        .success()
        .stdout(predicate::str::contains("From"));
}
