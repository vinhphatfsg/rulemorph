use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

use rulemorph_trace::{
    TraceCompression, TraceManifest, TraceStore, TraceWriteOptions, write_trace_bundle,
};
use serde_json::json;

fn unique_temp_dir() -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    std::env::temp_dir().join(format!("rulemorph-trace-test-{nanos}"))
}

#[tokio::test]
async fn write_trace_bundle_downgrades_on_budget_exceeded() -> anyhow::Result<()> {
    let temp_dir = unique_temp_dir();
    fs::create_dir_all(&temp_dir)?;

    let trace = json!({
        "trace_id": "trace-test",
        "records": [
            { "id": 1, "payload": "x".repeat(1024) }
        ],
        "finalize": { "output": "done" }
    });

    let options = TraceWriteOptions {
        max_bytes_per_trace: 1,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let manifest_payload = fs::read_to_string(&manifest_path)?;
    let manifest: TraceManifest = serde_json::from_str(&manifest_payload)?;
    let detail = manifest.detail.expect("detail should exist");

    assert_eq!(detail.status, "basic");
    assert!(
        detail
            .reason
            .iter()
            .any(|reason| reason == "budget_exceeded")
    );
    assert!(detail.records.is_empty());
    assert!(detail.finalize.is_none());

    let trace_dir = manifest_path.parent().expect("trace dir should exist");
    for entry in fs::read_dir(trace_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        assert!(
            !name.starts_with("records-")
                && !name.starts_with("nodes-")
                && !name.starts_with("finalize.json")
                && !name.contains(".tmp-"),
            "unexpected detail file: {name}"
        );
    }

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_sampling_skips_success() -> anyhow::Result<()> {
    let temp_dir = unique_temp_dir();
    fs::create_dir_all(&temp_dir)?;

    let trace = json!({
        "trace_id": "trace-sample-success",
        "status": "ok",
        "records": [
            { "index": 0, "status": "ok", "value": 1 }
        ],
        "summary": {
            "record_total": 1,
            "record_success": 1,
            "record_failed": 0,
            "duration_us": 10
        }
    });

    let options = TraceWriteOptions {
        sampling_rate: 0.0,
        compression: TraceCompression::None,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let manifest_payload = fs::read_to_string(&manifest_path)?;
    let manifest: TraceManifest = serde_json::from_str(&manifest_payload)?;
    let detail = manifest.detail.expect("detail should exist");

    assert_eq!(detail.status, "basic");
    assert!(detail.reason.iter().any(|reason| reason == "sampled_out"));
    assert!(detail.records.is_empty());
    assert!(detail.nodes.is_empty());
    assert!(detail.finalize.is_none());

    let trace_dir = manifest_path.parent().expect("trace dir should exist");
    for entry in fs::read_dir(trace_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        assert!(
            !name.starts_with("records-")
                && !name.starts_with("nodes-")
                && !name.starts_with("finalize.json")
                && !name.contains(".tmp-"),
            "unexpected detail file: {name}"
        );
    }

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_sampling_keeps_error() -> anyhow::Result<()> {
    let temp_dir = unique_temp_dir();
    fs::create_dir_all(&temp_dir)?;

    let trace = json!({
        "trace_id": "trace-sample-error",
        "status": "error",
        "records": [
            { "index": 0, "status": "error", "value": 1 }
        ],
        "summary": {
            "record_total": 1,
            "record_success": 0,
            "record_failed": 1,
            "duration_us": 20
        }
    });

    let options = TraceWriteOptions {
        sampling_rate: 0.0,
        compression: TraceCompression::None,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let manifest_payload = fs::read_to_string(&manifest_path)?;
    let manifest: TraceManifest = serde_json::from_str(&manifest_payload)?;
    let detail = manifest.detail.expect("detail should exist");

    assert_eq!(detail.status, "full");
    assert!(!detail.records.is_empty());

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_splits_nodes() -> anyhow::Result<()> {
    let temp_dir = unique_temp_dir();
    fs::create_dir_all(&temp_dir)?;

    let trace = json!({
        "trace_id": "trace-nodes",
        "records": [
            {
                "index": 0,
                "status": "ok",
                "nodes": [
                    { "id": "n1", "kind": "mappings", "status": "ok" }
                ]
            },
            {
                "index": 1,
                "status": "ok",
                "nodes": [
                    { "id": "n2", "kind": "branch", "status": "ok" }
                ]
            }
        ],
        "summary": {
            "record_total": 2,
            "record_success": 2,
            "record_failed": 0
        }
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        split_nodes: true,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let manifest_payload = fs::read_to_string(&manifest_path)?;
    let manifest: TraceManifest = serde_json::from_str(&manifest_payload)?;
    let detail = manifest.detail.expect("detail should exist");

    assert_eq!(detail.status, "full");
    assert_eq!(detail.layout, "records_nodes_split");
    assert!(!detail.records.is_empty());
    assert!(!detail.nodes.is_empty());

    let trace_dir = manifest_path.parent().expect("trace dir should exist");
    let record_chunk = &detail.records[0];
    let record_payload = fs::read_to_string(trace_dir.join(&record_chunk.path))?;
    for line in record_payload.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let record: serde_json::Value = serde_json::from_str(line)?;
        assert!(record.get("nodes").is_none());
    }

    let node_chunk = &detail.nodes[0];
    let node_payload = fs::read_to_string(trace_dir.join(&node_chunk.path))?;
    for line in node_payload.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let node: serde_json::Value = serde_json::from_str(line)?;
        assert!(node.get("record_index").is_some());
        assert!(node.get("node").is_some());
    }

    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store.get("trace-nodes").await?.expect("trace should load");
    let records = loaded
        .get("records")
        .and_then(|value| value.as_array())
        .expect("records should exist");
    assert!(records[0].get("nodes").is_some());
    let nodes = records[0]
        .get("nodes")
        .and_then(|value| value.as_array())
        .expect("nodes should be array");
    assert!(nodes[0].get("record_index").is_none());

    Ok(())
}
