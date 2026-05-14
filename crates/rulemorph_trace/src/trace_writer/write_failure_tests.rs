use super::*;
use serde_json::json;
use std::fs;

#[test]
fn write_trace_bundle_cleans_up_on_manifest_write_failure() {
    let temp_dir = std::env::temp_dir().join("rulemorph-trace-write-failure-cleanup");
    let _ = fs::remove_dir_all(&temp_dir);
    fs::create_dir_all(&temp_dir).expect("create temp dir");

    let trace = json!({
        "trace_id": "trace-fail-cleanup",
        "records": [
            { "index": 0, "status": "ok", "output": { "value": 1 } }
        ]
    });

    let _guard = fail_write_for_trace_id("trace-fail-cleanup", Some("trace.json"));
    let result = write_trace_bundle_sync(&temp_dir, &trace, &TraceWriteOptions::default());
    assert!(result.is_err());

    let now = Utc::now();
    let trace_dir = temp_dir
        .join("traces")
        .join(format!("{:04}", now.year()))
        .join(format!("{:02}", now.month()))
        .join(format!("{:02}", now.day()))
        .join("trace-fail-cleanup");
    assert!(
        !trace_dir.exists(),
        "trace dir should be removed after failure"
    );
}
