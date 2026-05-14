mod common;

use std::collections::HashMap;
use std::fs;

use common::trace_writer::{
    create_temp_dir, read_manifest, read_manifest_payload, read_manifest_value, sampling_bucket,
    trace_dir, unique_temp_dir,
};
use rulemorph_trace::{
    TRACE_CHUNK_COUNT_HARD_MAX, TRACE_JSON_MAX_BYTES, TraceCompression, TraceDetailLevel,
    TraceManifest, TraceStore, TraceWriteOptions, write_trace_bundle,
};
use serde_json::json;

#[tokio::test]
async fn write_trace_bundle_downgrades_on_budget_exceeded() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

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
    let manifest = read_manifest(&manifest_path)?;
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

    let trace_dir = trace_dir(&manifest_path);
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
async fn write_trace_bundle_clamps_max_chunk_bytes_uncompressed() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-clamp",
        "records": [
            { "index": 0, "status": "ok", "value": 1 }
        ]
    });

    let options = TraceWriteOptions {
        max_chunk_bytes_uncompressed: 64 * 1024 * 1024,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let manifest = read_manifest(&manifest_path)?;
    assert_eq!(
        manifest.max_chunk_bytes_uncompressed,
        Some(16 * 1024 * 1024)
    );

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_downgrades_on_oversized_record_line() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-oversize-line",
        "records": [
            { "index": 0, "payload": "x".repeat(1024) }
        ]
    });

    let options = TraceWriteOptions {
        max_chunk_bytes_uncompressed: 64,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let manifest = read_manifest(&manifest_path)?;
    let detail = manifest.detail.expect("detail should exist");

    assert_eq!(detail.status, "basic");
    assert!(
        detail
            .reason
            .iter()
            .any(|reason| reason == "chunk_too_large")
    );
    assert!(detail.records.is_empty());
    assert!(detail.nodes.is_empty());
    assert!(detail.finalize.is_none());

    let trace_dir = trace_dir(&manifest_path);
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
async fn write_trace_bundle_downgrades_on_chunk_count_exceeded() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let record_count = TRACE_CHUNK_COUNT_HARD_MAX + 1;
    let records: Vec<_> = (0..record_count)
        .map(|index| json!({ "index": index, "value": index }))
        .collect();
    let trace = json!({
        "trace_id": "trace-chunk-count",
        "records": records
    });

    let options = TraceWriteOptions {
        max_records_per_chunk: 1,
        compression: TraceCompression::None,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let manifest = read_manifest(&manifest_path)?;
    let detail = manifest.detail.expect("detail should exist");

    assert_eq!(detail.status, "basic");
    assert!(
        detail
            .reason
            .iter()
            .any(|reason| reason == "budget_exceeded")
    );
    assert!(detail.records.is_empty());
    assert!(detail.nodes.is_empty());
    assert!(detail.finalize.is_none());

    let trace_dir = trace_dir(&manifest_path);
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
async fn write_trace_bundle_drops_rule_source_when_manifest_too_large() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-large-manifest",
        "records": [],
        "rule_source": {
            "raw": "x".repeat(TRACE_JSON_MAX_BYTES as usize)
        }
    });

    let options = TraceWriteOptions {
        max_bytes_per_trace: TRACE_JSON_MAX_BYTES as usize + 1024,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let manifest_payload = read_manifest_payload(&manifest_path)?;
    assert!(manifest_payload.len() as u64 <= TRACE_JSON_MAX_BYTES);
    let manifest: TraceManifest = serde_json::from_str(&manifest_payload)?;
    let detail = manifest.detail.expect("detail should exist");
    assert!(
        detail
            .reason
            .iter()
            .any(|reason| reason == "rule_source_dropped")
    );
    assert!(manifest.rule_source.is_none());

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_sampling_skips_success() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

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
    let manifest = read_manifest(&manifest_path)?;
    let detail = manifest.detail.expect("detail should exist");

    assert_eq!(detail.status, "basic");
    assert!(detail.reason.iter().any(|reason| reason == "sampled_out"));
    assert!(detail.records.is_empty());
    assert!(detail.nodes.is_empty());
    assert!(detail.finalize.is_none());

    let trace_dir = trace_dir(&manifest_path);
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
    let temp_dir = create_temp_dir()?;

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
    let manifest = read_manifest(&manifest_path)?;
    let detail = manifest.detail.expect("detail should exist");

    assert_eq!(detail.status, "full");
    assert!(!detail.records.is_empty());

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_sampling_keeps_slow_trace() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-sample-slow",
        "status": "ok",
        "records": [
            { "index": 0, "status": "ok", "value": 1 }
        ],
        "summary": {
            "record_total": 1,
            "record_success": 1,
            "record_failed": 0,
            "duration_us": 250
        }
    });

    let options = TraceWriteOptions {
        sampling_rate: 0.0,
        sampling_slow_threshold_us: Some(100),
        compression: TraceCompression::None,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let manifest = read_manifest(&manifest_path)?;
    let detail = manifest.detail.expect("detail should exist");

    assert_eq!(detail.status, "full");
    assert!(!detail.reason.iter().any(|reason| reason == "sampled_out"));
    assert!(!detail.records.is_empty());

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_sampling_uses_generated_trace_id_for_placeholder() -> anyhow::Result<()>
{
    let temp_dir = create_temp_dir()?;

    let rate = 0.5;
    let placeholder_keep = sampling_bucket("trace") < rate;

    for _ in 0..10 {
        let trace = json!({
            "trace_id": "trace",
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
            sampling_rate: rate,
            compression: TraceCompression::None,
            ..Default::default()
        };

        let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
        let manifest = read_manifest(&manifest_path)?;
        let detail = manifest.detail.expect("detail should exist");
        let keep = sampling_bucket(&manifest.trace_id) < rate;
        if keep != placeholder_keep {
            let expected = if keep { "full" } else { "basic" };
            assert_eq!(detail.status, expected);
            return Ok(());
        }
    }

    panic!("failed to find sampling bucket differing from placeholder");
}

#[tokio::test]
async fn write_trace_bundle_splits_nodes() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-nodes",
        "records": [
            {
                "status": "ok",
                "nodes": [
                    { "id": "n1", "kind": "mappings", "status": "ok" }
                ]
            },
            {
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
    let manifest = read_manifest(&manifest_path)?;
    let detail = manifest.detail.expect("detail should exist");

    assert_eq!(detail.status, "full");
    assert_eq!(detail.layout, "records_nodes_split");
    assert!(!detail.records.is_empty());
    assert!(!detail.nodes.is_empty());

    let trace_dir = trace_dir(&manifest_path);
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
        assert!(node.get("id").is_some());
        assert!(node.get("kind").is_some());
        assert!(node.get("node").is_none());
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
    assert_eq!(
        nodes[0].get("id").and_then(|value| value.as_str()),
        Some("n1")
    );
    assert!(nodes[0].get("record_index").is_none());
    let nodes_second = records[1]
        .get("nodes")
        .and_then(|value| value.as_array())
        .expect("second nodes should be array");
    assert_eq!(
        nodes_second[0].get("id").and_then(|value| value.as_str()),
        Some("n2")
    );

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_recovers_nodes_without_record_index() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-nodes-missing-index",
        "records": [
            {
                "status": "ok",
                "nodes": [
                    { "id": "n1", "kind": "mappings", "status": "ok" },
                    { "id": "n2", "kind": "branch", "status": "ok" }
                ]
            }
        ]
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        split_nodes: true,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let manifest = read_manifest(&manifest_path)?;
    let detail = manifest.detail.expect("detail should exist");
    let trace_dir = trace_dir(&manifest_path);
    let node_chunk = &detail.nodes[0];
    let node_chunk_path = trace_dir.join(&node_chunk.path);

    let node_payload = fs::read_to_string(&node_chunk_path)?;
    let mut lines: Vec<String> = node_payload
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| line.to_string())
        .collect();
    let mut second: serde_json::Value = serde_json::from_str(&lines[1])?;
    if let Some(obj) = second.as_object_mut() {
        obj.remove("record_index");
    }
    lines[1] = serde_json::to_string(&second)?;
    fs::write(&node_chunk_path, format!("{}\n", lines.join("\n")))?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store
        .get("trace-nodes-missing-index")
        .await?
        .expect("trace should load");
    let records = loaded
        .get("records")
        .and_then(|value| value.as_array())
        .expect("records should exist");
    let nodes_first = records[0]
        .get("nodes")
        .and_then(|value| value.as_array())
        .expect("first nodes should be array");
    assert_eq!(
        nodes_first[0].get("id").and_then(|value| value.as_str()),
        Some("n1")
    );
    assert_eq!(
        nodes_first[1].get("id").and_then(|value| value.as_str()),
        Some("n2")
    );

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_preserves_node_owned_record_index() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-node-owned-record-index",
        "records": [
            {
                "status": "ok",
                "nodes": [
                    {
                        "id": "n1",
                        "kind": "mappings",
                        "status": "ok",
                        "record_index": "node-owned"
                    }
                ]
            }
        ]
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        split_nodes: true,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let manifest = read_manifest(&manifest_path)?;
    let detail = manifest.detail.expect("detail should exist");
    let trace_dir = trace_dir(&manifest_path);
    let node_chunk = &detail.nodes[0];
    let node_payload = fs::read_to_string(trace_dir.join(&node_chunk.path))?;
    let first_line = node_payload
        .lines()
        .find(|line| !line.trim().is_empty())
        .expect("node line should exist");
    let raw_entry: serde_json::Value = serde_json::from_str(first_line)?;
    assert_eq!(
        raw_entry
            .get("record_index")
            .and_then(|value| value.as_u64()),
        Some(0)
    );
    assert_eq!(
        raw_entry
            .get("node")
            .and_then(|value| value.get("record_index"))
            .and_then(|value| value.as_str()),
        Some("node-owned")
    );

    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store
        .get("trace-node-owned-record-index")
        .await?
        .expect("trace should load");
    let records = loaded
        .get("records")
        .and_then(|value| value.as_array())
        .expect("records should exist");
    let nodes = records[0]
        .get("nodes")
        .and_then(|value| value.as_array())
        .expect("nodes should be array");
    assert_eq!(
        nodes[0].get("id").and_then(|value| value.as_str()),
        Some("n1")
    );
    assert_eq!(
        nodes[0]
            .get("record_index")
            .and_then(|value| value.as_str()),
        Some("node-owned")
    );

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_reads_legacy_node_wrapper() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-nodes-legacy-wrapper",
        "records": [
            {
                "status": "ok",
                "nodes": [
                    { "id": "n1", "kind": "mappings", "status": "ok" }
                ]
            }
        ]
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        split_nodes: true,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let manifest = read_manifest(&manifest_path)?;
    let detail = manifest.detail.expect("detail should exist");
    let trace_dir = trace_dir(&manifest_path);
    let node_chunk = &detail.nodes[0];
    let node_chunk_path = trace_dir.join(&node_chunk.path);

    let node_payload = fs::read_to_string(&node_chunk_path)?;
    let mut lines: Vec<String> = node_payload
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| line.to_string())
        .collect();
    let original: serde_json::Value = serde_json::from_str(&lines[0])?;
    let record_index = original
        .get("record_index")
        .and_then(|value| value.as_u64())
        .unwrap_or(0);
    let mut node = original.clone();
    if let Some(obj) = node.as_object_mut() {
        obj.remove("record_index");
    }
    let wrapped = json!({
        "record_index": record_index.to_string(),
        "node": node
    });
    lines[0] = serde_json::to_string(&wrapped)?;
    fs::write(&node_chunk_path, format!("{}\n", lines.join("\n")))?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store
        .get("trace-nodes-legacy-wrapper")
        .await?
        .expect("trace should load");
    let records = loaded
        .get("records")
        .and_then(|value| value.as_array())
        .expect("records should exist");
    let nodes_first = records[0]
        .get("nodes")
        .and_then(|value| value.as_array())
        .expect("first nodes should be array");
    assert_eq!(
        nodes_first[0].get("id").and_then(|value| value.as_str()),
        Some("n1")
    );

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_keeps_node_key_with_sibling_fields() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-nodes-legacy-wrapper-extra-keys",
        "records": [
            {
                "status": "ok",
                "nodes": [
                    { "id": "n1", "kind": "mappings", "status": "ok" }
                ]
            }
        ]
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        split_nodes: true,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let manifest = read_manifest(&manifest_path)?;
    let detail = manifest.detail.expect("detail should exist");
    let trace_dir = trace_dir(&manifest_path);
    let node_chunk = &detail.nodes[0];
    let node_chunk_path = trace_dir.join(&node_chunk.path);

    let node_payload = fs::read_to_string(&node_chunk_path)?;
    let mut lines: Vec<String> = node_payload
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| line.to_string())
        .collect();
    let original: serde_json::Value = serde_json::from_str(&lines[0])?;
    let record_index = original
        .get("record_index")
        .and_then(|value| value.as_u64())
        .unwrap_or(0);
    let mut node = original.clone();
    if let Some(obj) = node.as_object_mut() {
        obj.remove("record_index");
    }
    let wrapped = json!({
        "record_index": record_index.to_string(),
        "node": node,
        "extra": "ignored"
    });
    lines[0] = serde_json::to_string(&wrapped)?;
    fs::write(&node_chunk_path, format!("{}\n", lines.join("\n")))?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store
        .get("trace-nodes-legacy-wrapper-extra-keys")
        .await?
        .expect("trace should load");
    let records = loaded
        .get("records")
        .and_then(|value| value.as_array())
        .expect("records should exist");
    let nodes_first = records[0]
        .get("nodes")
        .and_then(|value| value.as_array())
        .expect("first nodes should be array");
    assert!(nodes_first[0].get("id").is_none());
    assert_eq!(
        nodes_first[0].get("extra").and_then(|value| value.as_str()),
        Some("ignored")
    );
    assert_eq!(
        nodes_first[0]
            .get("node")
            .and_then(|value| value.get("id"))
            .and_then(|value| value.as_str()),
        Some("n1")
    );
    assert!(nodes_first[0].get("record_index").is_none());

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_skips_malformed_ndjson_line() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-nodes-malformed-ndjson",
        "records": [
            {
                "status": "ok",
                "nodes": [
                    { "id": "n1", "kind": "mappings", "status": "ok" }
                ]
            }
        ]
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        split_nodes: true,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let manifest = read_manifest(&manifest_path)?;
    let detail = manifest.detail.expect("detail should exist");
    let trace_dir = trace_dir(&manifest_path);
    let node_chunk = &detail.nodes[0];
    let node_chunk_path = trace_dir.join(&node_chunk.path);

    let mut node_payload = fs::read_to_string(&node_chunk_path)?;
    node_payload.push_str("not-json\n");
    fs::write(&node_chunk_path, node_payload)?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store
        .get("trace-nodes-malformed-ndjson")
        .await?
        .expect("trace should load");
    let detail = loaded
        .get("detail")
        .and_then(|value| value.as_object())
        .expect("detail should exist");
    assert_eq!(
        detail.get("status").and_then(|value| value.as_str()),
        Some("basic")
    );
    let reasons = detail
        .get("reason")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(
        reasons
            .iter()
            .any(|value| value.as_str() == Some("chunk_error"))
    );
    let detail_records = detail
        .get("records")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(detail_records.is_empty());
    let detail_nodes = detail
        .get("nodes")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(detail_nodes.is_empty());
    assert!(detail.get("finalize").is_none());
    let records = loaded
        .get("records")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(records.is_empty());
    assert!(loaded.get("finalize").is_none());

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_inherits_record_index_for_legacy_wrapper() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-legacy-wrapper-inherit",
        "records": [
            {
                "status": "ok",
                "nodes": [
                    { "id": "n1", "kind": "mappings", "status": "ok" }
                ]
            }
        ]
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        split_nodes: true,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let manifest = read_manifest(&manifest_path)?;
    let detail = manifest.detail.expect("detail should exist");
    let trace_dir = trace_dir(&manifest_path);
    let node_chunk = &detail.nodes[0];
    let node_chunk_path = trace_dir.join(&node_chunk.path);

    let node_payload = fs::read_to_string(&node_chunk_path)?;
    let mut lines: Vec<String> = node_payload
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| line.to_string())
        .collect();
    let first: serde_json::Value = serde_json::from_str(&lines[0])?;
    let wrapper_first = json!({
        "record_index": first.get("record_index").cloned().unwrap_or(json!(0)),
        "node": first
    });
    let mut wrapper_second = json!({
        "node": { "id": "n2", "kind": "branch", "status": "ok" }
    });
    if let Some(obj) = wrapper_second.as_object_mut() {
        obj.remove("record_index");
    }
    lines = vec![
        serde_json::to_string(&wrapper_first)?,
        serde_json::to_string(&wrapper_second)?,
    ];
    fs::write(&node_chunk_path, format!("{}\n", lines.join("\n")))?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store
        .get("trace-legacy-wrapper-inherit")
        .await?
        .expect("trace should load");
    let records = loaded
        .get("records")
        .and_then(|value| value.as_array())
        .expect("records should exist");
    let nodes_first = records[0]
        .get("nodes")
        .and_then(|value| value.as_array())
        .expect("first nodes should be array");
    assert_eq!(nodes_first.len(), 2);
    assert_eq!(
        nodes_first[0].get("id").and_then(|value| value.as_str()),
        Some("n1")
    );
    assert_eq!(
        nodes_first[1].get("id").and_then(|value| value.as_str()),
        Some("n2")
    );

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_skips_invalid_utf8_chunk() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-invalid-utf8-chunk",
        "records": [
            {
                "status": "ok",
                "nodes": [
                    { "id": "n1", "kind": "mappings", "status": "ok" }
                ]
            }
        ]
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        split_nodes: true,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let manifest = read_manifest(&manifest_path)?;
    let detail = manifest.detail.expect("detail should exist");
    let trace_dir = trace_dir(&manifest_path);
    let record_chunk = &detail.records[0];
    let record_chunk_path = trace_dir.join(&record_chunk.path);
    fs::write(&record_chunk_path, vec![0xff, 0xfe, 0xfd])?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store
        .get("trace-invalid-utf8-chunk")
        .await?
        .expect("trace should load");
    let detail = loaded
        .get("detail")
        .and_then(|value| value.as_object())
        .expect("detail object");
    assert_eq!(
        detail.get("status").and_then(|value| value.as_str()),
        Some("basic")
    );
    let reasons = detail
        .get("reason")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(
        reasons
            .iter()
            .any(|value| value.as_str() == Some("chunk_error"))
    );
    let records = loaded
        .get("records")
        .and_then(|value| value.as_array())
        .expect("records should exist");
    assert!(records.is_empty());

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_skips_unsupported_compression_chunk() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-unsupported-compression",
        "records": [
            {
                "status": "ok",
                "nodes": [
                    { "id": "n1", "kind": "mappings", "status": "ok" }
                ]
            }
        ]
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        split_nodes: true,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let mut manifest_value = read_manifest_value(&manifest_path)?;
    if let Some(detail) = manifest_value
        .get_mut("detail")
        .and_then(|v| v.as_object_mut())
    {
        if let Some(records) = detail.get_mut("records").and_then(|v| v.as_array_mut()) {
            if let Some(record) = records.get_mut(0).and_then(|v| v.as_object_mut()) {
                record.insert(
                    "compression".to_string(),
                    serde_json::Value::String("gzip".to_string()),
                );
            }
        }
    }
    fs::write(&manifest_path, serde_json::to_string(&manifest_value)?)?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store
        .get("trace-unsupported-compression")
        .await?
        .expect("trace should load");
    let detail = loaded
        .get("detail")
        .and_then(|value| value.as_object())
        .expect("detail object");
    assert_eq!(
        detail.get("status").and_then(|value| value.as_str()),
        Some("basic")
    );
    let reasons = detail
        .get("reason")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(
        reasons
            .iter()
            .any(|value| value.as_str() == Some("chunk_error"))
    );
    let records = loaded
        .get("records")
        .and_then(|value| value.as_array())
        .expect("records should exist");
    assert!(records.is_empty());

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_skips_unsupported_format_chunk() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-unsupported-format",
        "records": [
            {
                "status": "ok",
                "nodes": [
                    { "id": "n1", "kind": "mappings", "status": "ok" }
                ]
            }
        ]
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        split_nodes: true,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let mut manifest_value = read_manifest_value(&manifest_path)?;
    if let Some(detail) = manifest_value
        .get_mut("detail")
        .and_then(|v| v.as_object_mut())
    {
        if let Some(records) = detail.get_mut("records").and_then(|v| v.as_array_mut()) {
            if let Some(record) = records.get_mut(0).and_then(|v| v.as_object_mut()) {
                record.insert(
                    "format".to_string(),
                    serde_json::Value::String("json".to_string()),
                );
            }
        }
    }
    fs::write(&manifest_path, serde_json::to_string(&manifest_value)?)?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store
        .get("trace-unsupported-format")
        .await?
        .expect("trace should load");
    let detail = loaded
        .get("detail")
        .and_then(|value| value.as_object())
        .expect("detail object");
    assert_eq!(
        detail.get("status").and_then(|value| value.as_str()),
        Some("basic")
    );
    let reasons = detail
        .get("reason")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(
        reasons
            .iter()
            .any(|value| value.as_str() == Some("chunk_error"))
    );
    let records = loaded
        .get("records")
        .and_then(|value| value.as_array())
        .expect("records should exist");
    assert!(records.is_empty());

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_skips_zstd_decode_failure() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-zstd-decode-failure",
        "records": [
            {
                "status": "ok",
                "nodes": [
                    { "id": "n1", "kind": "mappings", "status": "ok" }
                ]
            }
        ]
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        split_nodes: true,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let mut manifest_value = read_manifest_value(&manifest_path)?;
    if let Some(detail) = manifest_value
        .get_mut("detail")
        .and_then(|v| v.as_object_mut())
    {
        if let Some(records) = detail.get_mut("records").and_then(|v| v.as_array_mut()) {
            if let Some(record) = records.get_mut(0).and_then(|v| v.as_object_mut()) {
                record.insert(
                    "compression".to_string(),
                    serde_json::Value::String("zstd".to_string()),
                );
            }
        }
    }
    fs::write(&manifest_path, serde_json::to_string(&manifest_value)?)?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store
        .get("trace-zstd-decode-failure")
        .await?
        .expect("trace should load");
    let detail = loaded
        .get("detail")
        .and_then(|value| value.as_object())
        .expect("detail object");
    assert_eq!(
        detail.get("status").and_then(|value| value.as_str()),
        Some("basic")
    );
    let reasons = detail
        .get("reason")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(
        reasons
            .iter()
            .any(|value| value.as_str() == Some("chunk_error"))
    );
    let records = loaded
        .get("records")
        .and_then(|value| value.as_array())
        .expect("records should exist");
    assert!(records.is_empty());

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_roundtrips_finalize_none() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-finalize-roundtrip-none",
        "records": [],
        "finalize": { "output": "done" }
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        ..Default::default()
    };

    let _manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store
        .get("trace-finalize-roundtrip-none")
        .await?
        .expect("trace should load");
    assert_eq!(loaded.get("finalize"), Some(&json!({ "output": "done" })));

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_roundtrips_finalize_zstd() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-finalize-roundtrip-zstd",
        "records": [],
        "finalize": { "output": "done" }
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::Zstd,
        ..Default::default()
    };

    let _manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store
        .get("trace-finalize-roundtrip-zstd")
        .await?
        .expect("trace should load");
    assert_eq!(loaded.get("finalize"), Some(&json!({ "output": "done" })));

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_downgrades_on_oversized_finalize() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-finalize-oversized",
        "records": [],
        "finalize": { "output": "x".repeat(200) }
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        max_chunk_bytes_uncompressed: 64,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let manifest = read_manifest(&manifest_path)?;
    let detail = manifest.detail.expect("detail should exist");
    assert_eq!(detail.status, "basic");
    assert!(
        detail
            .reason
            .iter()
            .any(|reason| reason == "chunk_too_large")
    );
    assert!(detail.records.is_empty());
    assert!(detail.nodes.is_empty());
    assert!(detail.finalize.is_none());

    let trace_dir = trace_dir(&manifest_path);
    assert!(!trace_dir.join("finalize.json").exists());

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_skips_finalize_unsupported_format() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-finalize-unsupported-format",
        "records": [],
        "finalize": { "output": "done" }
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let mut manifest_value = read_manifest_value(&manifest_path)?;
    if let Some(detail) = manifest_value
        .get_mut("detail")
        .and_then(|v| v.as_object_mut())
    {
        if let Some(finalize) = detail.get_mut("finalize").and_then(|v| v.as_object_mut()) {
            finalize.insert(
                "format".to_string(),
                serde_json::Value::String("ndjson".to_string()),
            );
        }
    }
    fs::write(&manifest_path, serde_json::to_string(&manifest_value)?)?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store
        .get("trace-finalize-unsupported-format")
        .await?
        .expect("trace should load");
    assert!(loaded.get("finalize").is_none());

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_skips_finalize_unsupported_compression() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-finalize-unsupported-compression",
        "records": [],
        "finalize": { "output": "done" }
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let mut manifest_value = read_manifest_value(&manifest_path)?;
    if let Some(detail) = manifest_value
        .get_mut("detail")
        .and_then(|v| v.as_object_mut())
    {
        if let Some(finalize) = detail.get_mut("finalize").and_then(|v| v.as_object_mut()) {
            finalize.insert(
                "compression".to_string(),
                serde_json::Value::String("gzip".to_string()),
            );
        }
    }
    fs::write(&manifest_path, serde_json::to_string(&manifest_value)?)?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store
        .get("trace-finalize-unsupported-compression")
        .await?
        .expect("trace should load");
    assert!(loaded.get("finalize").is_none());

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_skips_finalize_invalid_json() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-finalize-invalid-json",
        "records": [],
        "finalize": { "output": "done" }
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let manifest = read_manifest(&manifest_path)?;
    let detail = manifest.detail.expect("detail should exist");
    let trace_dir = trace_dir(&manifest_path);
    let finalize = detail.finalize.expect("finalize chunk should exist");
    let finalize_path = trace_dir.join(&finalize.path);
    fs::write(&finalize_path, b"{invalid json")?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store
        .get("trace-finalize-invalid-json")
        .await?
        .expect("trace should load");
    assert!(loaded.get("finalize").is_none());

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_skips_finalize_zstd_decode_failure() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-finalize-zstd-decode-failure",
        "records": [],
        "finalize": { "output": "done" }
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let mut manifest_value = read_manifest_value(&manifest_path)?;
    if let Some(detail) = manifest_value
        .get_mut("detail")
        .and_then(|v| v.as_object_mut())
    {
        if let Some(finalize) = detail.get_mut("finalize").and_then(|v| v.as_object_mut()) {
            finalize.insert(
                "compression".to_string(),
                serde_json::Value::String("zstd".to_string()),
            );
        }
    }
    fs::write(&manifest_path, serde_json::to_string(&manifest_value)?)?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store
        .get("trace-finalize-zstd-decode-failure")
        .await?
        .expect("trace should load");
    assert!(loaded.get("finalize").is_none());

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_parses_string_record_index() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-nodes-string-index",
        "records": [
            {
                "index": 0,
                "status": "ok",
                "nodes": [
                    { "id": "n1", "kind": "mappings", "status": "ok" }
                ]
            }
        ]
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        split_nodes: true,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let manifest = read_manifest(&manifest_path)?;
    let detail = manifest.detail.expect("detail should exist");
    let trace_dir = trace_dir(&manifest_path);
    let record_chunk = &detail.records[0];
    let record_chunk_path = trace_dir.join(&record_chunk.path);

    let record_payload = fs::read_to_string(&record_chunk_path)?;
    let mut lines: Vec<String> = record_payload
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| line.to_string())
        .collect();
    let mut record: serde_json::Value = serde_json::from_str(&lines[0])?;
    if let Some(obj) = record.as_object_mut() {
        obj.insert(
            "index".to_string(),
            serde_json::Value::String("0".to_string()),
        );
    }
    lines[0] = serde_json::to_string(&record)?;
    fs::write(&record_chunk_path, format!("{}\n", lines.join("\n")))?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store
        .get("trace-nodes-string-index")
        .await?
        .expect("trace should load");
    let records = loaded
        .get("records")
        .and_then(|value| value.as_array())
        .expect("records should exist");
    let nodes_first = records[0]
        .get("nodes")
        .and_then(|value| value.as_array())
        .expect("first nodes should be array");
    assert_eq!(
        nodes_first[0].get("id").and_then(|value| value.as_str()),
        Some("n1")
    );

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_normalizes_inline_nodes() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-inline-nodes",
        "records": [
            {
                "index": 0,
                "status": "ok",
                "nodes": { "id": "n1", "kind": "mappings", "status": "ok" }
            }
        ]
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        split_nodes: false,
        ..Default::default()
    };

    let _manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store
        .get("trace-inline-nodes")
        .await?
        .expect("trace should load");
    let records = loaded
        .get("records")
        .and_then(|value| value.as_array())
        .expect("records should exist");
    let nodes = records[0]
        .get("nodes")
        .and_then(|value| value.as_array())
        .expect("nodes should be array");
    assert_eq!(nodes.len(), 1);
    assert_eq!(
        nodes[0].get("id").and_then(|value| value.as_str()),
        Some("n1")
    );

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_skips_invalid_record_index() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-nodes-invalid-index",
        "records": [
            {
                "status": "ok",
                "nodes": [
                    { "id": "n1", "kind": "mappings", "status": "ok" },
                    { "id": "n2", "kind": "branch", "status": "ok" }
                ]
            }
        ]
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        split_nodes: true,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let manifest = read_manifest(&manifest_path)?;
    let detail = manifest.detail.expect("detail should exist");
    let trace_dir = trace_dir(&manifest_path);
    let node_chunk = &detail.nodes[0];
    let node_chunk_path = trace_dir.join(&node_chunk.path);

    let node_payload = fs::read_to_string(&node_chunk_path)?;
    let mut lines: Vec<String> = node_payload
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| line.to_string())
        .collect();
    let mut second: serde_json::Value = serde_json::from_str(&lines[1])?;
    if let Some(obj) = second.as_object_mut() {
        obj.insert(
            "record_index".to_string(),
            serde_json::Value::String("invalid".to_string()),
        );
    }
    lines[1] = serde_json::to_string(&second)?;
    fs::write(&node_chunk_path, format!("{}\n", lines.join("\n")))?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store
        .get("trace-nodes-invalid-index")
        .await?
        .expect("trace should load");
    let records = loaded
        .get("records")
        .and_then(|value| value.as_array())
        .expect("records should exist");
    let nodes_first = records[0]
        .get("nodes")
        .and_then(|value| value.as_array())
        .expect("first nodes should be array");
    assert_eq!(nodes_first.len(), 1);
    assert_eq!(
        nodes_first[0].get("id").and_then(|value| value.as_str()),
        Some("n1")
    );

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_skips_null_record_index_keeps_inheritance() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-nodes-null-index",
        "records": [
            {
                "status": "ok",
                "nodes": [
                    { "id": "n1", "kind": "mappings", "status": "ok" },
                    { "id": "n2", "kind": "branch", "status": "ok" }
                ]
            }
        ]
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        split_nodes: true,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let manifest = read_manifest(&manifest_path)?;
    let detail = manifest.detail.expect("detail should exist");
    let trace_dir = trace_dir(&manifest_path);
    let node_chunk = &detail.nodes[0];
    let node_chunk_path = trace_dir.join(&node_chunk.path);

    let node_payload = fs::read_to_string(&node_chunk_path)?;
    let mut lines: Vec<String> = node_payload
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| line.to_string())
        .collect();
    let first: serde_json::Value = serde_json::from_str(&lines[0])?;
    let mut second: serde_json::Value = serde_json::from_str(&lines[1])?;
    if let Some(obj) = second.as_object_mut() {
        obj.insert("record_index".to_string(), serde_json::Value::Null);
        obj.insert(
            "id".to_string(),
            serde_json::Value::String("n2-invalid".to_string()),
        );
    }
    let mut third = first.clone();
    if let Some(obj) = third.as_object_mut() {
        obj.remove("record_index");
        obj.insert(
            "id".to_string(),
            serde_json::Value::String("n3".to_string()),
        );
    }
    lines = vec![
        serde_json::to_string(&first)?,
        serde_json::to_string(&second)?,
        serde_json::to_string(&third)?,
    ];
    fs::write(&node_chunk_path, format!("{}\n", lines.join("\n")))?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store
        .get("trace-nodes-null-index")
        .await?
        .expect("trace should load");
    let records = loaded
        .get("records")
        .and_then(|value| value.as_array())
        .expect("records should exist");
    let nodes_first = records[0]
        .get("nodes")
        .and_then(|value| value.as_array())
        .expect("first nodes should be array");
    assert_eq!(nodes_first.len(), 2);
    assert_eq!(
        nodes_first[0].get("id").and_then(|value| value.as_str()),
        Some("n1")
    );
    assert_eq!(
        nodes_first[1].get("id").and_then(|value| value.as_str()),
        Some("n3")
    );

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_treats_node_key_with_core_fields_as_normal_node() -> anyhow::Result<()>
{
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-nodes-node-key",
        "records": [
            {
                "status": "ok",
                "nodes": [
                    { "id": "n1", "kind": "mappings", "status": "ok" }
                ]
            }
        ]
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        split_nodes: true,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let manifest = read_manifest(&manifest_path)?;
    let detail = manifest.detail.expect("detail should exist");
    let trace_dir = trace_dir(&manifest_path);
    let node_chunk = &detail.nodes[0];
    let node_chunk_path = trace_dir.join(&node_chunk.path);

    let node_payload = fs::read_to_string(&node_chunk_path)?;
    let mut lines: Vec<String> = node_payload
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| line.to_string())
        .collect();
    let mut first: serde_json::Value = serde_json::from_str(&lines[0])?;
    if let Some(obj) = first.as_object_mut() {
        obj.insert(
            "id".to_string(),
            serde_json::Value::String("top".to_string()),
        );
        obj.insert(
            "kind".to_string(),
            serde_json::Value::String("mappings".to_string()),
        );
        obj.insert(
            "status".to_string(),
            serde_json::Value::String("ok".to_string()),
        );
        obj.insert(
            "node".to_string(),
            json!({ "id": "wrapped", "kind": "branch", "status": "ok" }),
        );
    }
    lines[0] = serde_json::to_string(&first)?;
    fs::write(&node_chunk_path, format!("{}\n", lines.join("\n")))?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store
        .get("trace-nodes-node-key")
        .await?
        .expect("trace should load");
    let records = loaded
        .get("records")
        .and_then(|value| value.as_array())
        .expect("records should exist");
    let nodes_first = records[0]
        .get("nodes")
        .and_then(|value| value.as_array())
        .expect("first nodes should be array");
    assert_eq!(
        nodes_first[0].get("id").and_then(|value| value.as_str()),
        Some("top")
    );

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_wraps_non_object_nodes() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-nodes-non-object",
        "records": [
            {
                "status": "ok",
                "nodes": ["raw-node"]
            }
        ]
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        split_nodes: true,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let manifest = read_manifest(&manifest_path)?;
    let detail = manifest.detail.expect("detail should exist");
    let trace_dir = trace_dir(&manifest_path);
    let node_chunk = &detail.nodes[0];
    let node_payload = fs::read_to_string(trace_dir.join(&node_chunk.path))?;
    let first_line = node_payload
        .lines()
        .find(|line| !line.trim().is_empty())
        .expect("node line should exist");
    let node: serde_json::Value = serde_json::from_str(first_line)?;
    assert_eq!(
        node.get("value").and_then(|value| value.as_str()),
        Some("raw-node")
    );
    assert_eq!(
        node.get("record_index").and_then(|value| value.as_u64()),
        Some(0)
    );

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_dedupes_duplicate_record_index() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-duplicate-index",
        "records": [
            {
                "index": 1,
                "status": "ok",
                "nodes": [{ "id": "n1", "kind": "mappings", "status": "ok" }]
            },
            {
                "index": 1,
                "status": "ok",
                "nodes": [{ "id": "n2", "kind": "branch", "status": "ok" }]
            }
        ]
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        split_nodes: true,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let manifest = read_manifest(&manifest_path)?;
    let detail = manifest.detail.expect("detail should exist");
    let trace_dir = trace_dir(&manifest_path);

    let record_chunk = &detail.records[0];
    let record_payload = fs::read_to_string(trace_dir.join(&record_chunk.path))?;
    let mut record_indices: Vec<u64> = record_payload
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str::<serde_json::Value>(line))
        .collect::<Result<Vec<_>, _>>()?
        .iter()
        .filter_map(|record| record.get("index").and_then(|value| value.as_u64()))
        .collect();
    record_indices.sort_unstable();
    assert_eq!(record_indices, vec![1, 2]);

    let node_chunk = &detail.nodes[0];
    let node_payload = fs::read_to_string(trace_dir.join(&node_chunk.path))?;
    let mut node_indices: Vec<u64> = node_payload
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str::<serde_json::Value>(line))
        .collect::<Result<Vec<_>, _>>()?
        .iter()
        .filter_map(|node| node.get("record_index").and_then(|value| value.as_u64()))
        .collect();
    node_indices.sort_unstable();
    assert_eq!(node_indices, vec![1, 2]);

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_skips_missing_first_record_index() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-nodes-missing-first-index",
        "records": [
            {
                "status": "ok",
                "nodes": [
                    { "id": "n1", "kind": "mappings", "status": "ok" },
                    { "id": "n2", "kind": "branch", "status": "ok" }
                ]
            }
        ]
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        split_nodes: true,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let manifest = read_manifest(&manifest_path)?;
    let detail = manifest.detail.expect("detail should exist");
    let trace_dir = trace_dir(&manifest_path);
    let node_chunk = &detail.nodes[0];
    let node_chunk_path = trace_dir.join(&node_chunk.path);

    let node_payload = fs::read_to_string(&node_chunk_path)?;
    let mut lines: Vec<String> = node_payload
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| line.to_string())
        .collect();
    let mut first: serde_json::Value = serde_json::from_str(&lines[0])?;
    if let Some(obj) = first.as_object_mut() {
        obj.remove("record_index");
    }
    lines[0] = serde_json::to_string(&first)?;
    fs::write(&node_chunk_path, format!("{}\n", lines.join("\n")))?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store
        .get("trace-nodes-missing-first-index")
        .await?
        .expect("trace should load");
    let records = loaded
        .get("records")
        .and_then(|value| value.as_array())
        .expect("records should exist");
    let nodes_first = records[0]
        .get("nodes")
        .and_then(|value| value.as_array())
        .expect("nodes should be array");
    assert_eq!(nodes_first.len(), 1);
    assert_eq!(
        nodes_first[0].get("id").and_then(|value| value.as_str()),
        Some("n2")
    );

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_accepts_string_record_index() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-string-index-input",
        "records": [
            {
                "index": "2",
                "status": "ok",
                "nodes": [
                    { "id": "n1", "kind": "mappings", "status": "ok" }
                ]
            }
        ]
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        split_nodes: true,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let manifest = read_manifest(&manifest_path)?;
    let detail = manifest.detail.expect("detail should exist");
    let trace_dir = trace_dir(&manifest_path);

    let record_chunk = &detail.records[0];
    let record_payload = fs::read_to_string(trace_dir.join(&record_chunk.path))?;
    let record_line = record_payload
        .lines()
        .find(|line| !line.trim().is_empty())
        .expect("record line should exist");
    let record: serde_json::Value = serde_json::from_str(record_line)?;
    assert_eq!(
        record.get("index").and_then(|value| value.as_u64()),
        Some(2)
    );

    let node_chunk = &detail.nodes[0];
    let node_payload = fs::read_to_string(trace_dir.join(&node_chunk.path))?;
    let node_line = node_payload
        .lines()
        .find(|line| !line.trim().is_empty())
        .expect("node line should exist");
    let node: serde_json::Value = serde_json::from_str(node_line)?;
    assert_eq!(
        node.get("record_index").and_then(|value| value.as_u64()),
        Some(2)
    );

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_merges_inline_nodes_with_chunk() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-nodes-merge",
        "records": [
            {
                "status": "ok",
                "nodes": [{ "id": "n1", "kind": "mappings", "status": "ok" }]
            }
        ]
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        split_nodes: true,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let manifest = read_manifest(&manifest_path)?;
    let detail = manifest.detail.expect("detail should exist");
    let trace_dir = trace_dir(&manifest_path);
    let record_chunk = &detail.records[0];
    let record_chunk_path = trace_dir.join(&record_chunk.path);

    let record_payload = fs::read_to_string(&record_chunk_path)?;
    let mut lines: Vec<String> = record_payload
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| line.to_string())
        .collect();
    let mut record: serde_json::Value = serde_json::from_str(&lines[0])?;
    if let Some(obj) = record.as_object_mut() {
        obj.insert(
            "nodes".to_string(),
            serde_json::Value::Array(vec![json!({
                "id": "inline",
                "kind": "mappings",
                "status": "ok"
            })]),
        );
    }
    lines[0] = serde_json::to_string(&record)?;
    fs::write(&record_chunk_path, format!("{}\n", lines.join("\n")))?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store
        .get("trace-nodes-merge")
        .await?
        .expect("trace should load");
    let records = loaded
        .get("records")
        .and_then(|value| value.as_array())
        .expect("records should exist");
    let nodes = records[0]
        .get("nodes")
        .and_then(|value| value.as_array())
        .expect("nodes should be array");
    let ids: Vec<&str> = nodes
        .iter()
        .filter_map(|node| node.get("id").and_then(|value| value.as_str()))
        .collect();
    assert!(ids.contains(&"inline"));
    assert!(ids.contains(&"n1"));

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_first_wins_duplicate_record_index() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-dup-index-first-wins",
        "records": [
            {
                "status": "ok",
                "nodes": [{ "id": "n1", "kind": "mappings", "status": "ok" }]
            },
            {
                "status": "ok",
                "nodes": [{ "id": "n2", "kind": "branch", "status": "ok" }]
            }
        ]
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        split_nodes: true,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let manifest = read_manifest(&manifest_path)?;
    let detail = manifest.detail.expect("detail should exist");
    let trace_dir = trace_dir(&manifest_path);

    let record_chunk = &detail.records[0];
    let record_chunk_path = trace_dir.join(&record_chunk.path);
    let record_payload = fs::read_to_string(&record_chunk_path)?;
    let mut record_lines: Vec<String> = record_payload
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| line.to_string())
        .collect();
    for line in &mut record_lines {
        let mut record: serde_json::Value = serde_json::from_str(line)?;
        if let Some(obj) = record.as_object_mut() {
            obj.insert("index".to_string(), serde_json::Value::from(0));
        }
        *line = serde_json::to_string(&record)?;
    }
    fs::write(&record_chunk_path, format!("{}\n", record_lines.join("\n")))?;

    let node_chunk = &detail.nodes[0];
    let node_chunk_path = trace_dir.join(&node_chunk.path);
    let node_payload = fs::read_to_string(&node_chunk_path)?;
    let mut node_lines: Vec<String> = node_payload
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| line.to_string())
        .collect();
    for line in &mut node_lines {
        let mut node: serde_json::Value = serde_json::from_str(line)?;
        if let Some(obj) = node.as_object_mut() {
            obj.insert("record_index".to_string(), serde_json::Value::from(0));
        }
        *line = serde_json::to_string(&node)?;
    }
    fs::write(&node_chunk_path, format!("{}\n", node_lines.join("\n")))?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store
        .get("trace-dup-index-first-wins")
        .await?
        .expect("trace should load");
    let records = loaded
        .get("records")
        .and_then(|value| value.as_array())
        .expect("records should exist");
    let nodes_first = records[0]
        .get("nodes")
        .and_then(|value| value.as_array())
        .expect("first nodes should be array");
    let nodes_second = records[1].get("nodes");
    assert_eq!(nodes_first.len(), 2);
    assert!(nodes_second.is_none());

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_skips_missing_record_index_across_chunks() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-missing-index-across-chunks",
        "records": [
            {
                "status": "ok",
                "nodes": [
                    { "id": "n1", "kind": "mappings", "status": "ok" },
                    { "id": "n2", "kind": "branch", "status": "ok" }
                ]
            }
        ]
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        split_nodes: true,
        max_nodes_per_chunk: 1,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let manifest = read_manifest(&manifest_path)?;
    let detail = manifest.detail.expect("detail should exist");
    assert!(detail.nodes.len() >= 2);
    let trace_dir = trace_dir(&manifest_path);
    let second_chunk = &detail.nodes[1];
    let second_chunk_path = trace_dir.join(&second_chunk.path);
    let node_payload = fs::read_to_string(&second_chunk_path)?;
    let mut lines: Vec<String> = node_payload
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| line.to_string())
        .collect();
    let mut node: serde_json::Value = serde_json::from_str(&lines[0])?;
    if let Some(obj) = node.as_object_mut() {
        obj.remove("record_index");
    }
    lines[0] = serde_json::to_string(&node)?;
    fs::write(&second_chunk_path, format!("{}\n", lines.join("\n")))?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store
        .get("trace-missing-index-across-chunks")
        .await?
        .expect("trace should load");
    let records = loaded
        .get("records")
        .and_then(|value| value.as_array())
        .expect("records should exist");
    let nodes = records[0]
        .get("nodes")
        .and_then(|value| value.as_array())
        .expect("nodes should be array");
    assert_eq!(nodes.len(), 1);
    assert_eq!(
        nodes[0].get("id").and_then(|value| value.as_str()),
        Some("n1")
    );

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_parses_string_record_index_in_nodes() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-nodes-string-index-line",
        "records": [
            {
                "status": "ok",
                "nodes": [
                    { "id": "n1", "kind": "mappings", "status": "ok" }
                ]
            }
        ]
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        split_nodes: true,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let manifest = read_manifest(&manifest_path)?;
    let detail = manifest.detail.expect("detail should exist");
    let trace_dir = trace_dir(&manifest_path);
    let node_chunk = &detail.nodes[0];
    let node_chunk_path = trace_dir.join(&node_chunk.path);

    let node_payload = fs::read_to_string(&node_chunk_path)?;
    let mut lines: Vec<String> = node_payload
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| line.to_string())
        .collect();
    let mut node: serde_json::Value = serde_json::from_str(&lines[0])?;
    if let Some(obj) = node.as_object_mut() {
        obj.insert(
            "record_index".to_string(),
            serde_json::Value::String("0".to_string()),
        );
    }
    lines[0] = serde_json::to_string(&node)?;
    fs::write(&node_chunk_path, format!("{}\n", lines.join("\n")))?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store
        .get("trace-nodes-string-index-line")
        .await?
        .expect("trace should load");
    let records = loaded
        .get("records")
        .and_then(|value| value.as_array())
        .expect("records should exist");
    let nodes_first = records[0]
        .get("nodes")
        .and_then(|value| value.as_array())
        .expect("nodes should be array");
    assert_eq!(nodes_first.len(), 1);
    assert_eq!(
        nodes_first[0].get("id").and_then(|value| value.as_str()),
        Some("n1")
    );

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_skips_invalid_record_index_in_record() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-invalid-record-index",
        "records": [
            {
                "status": "ok",
                "nodes": [
                    { "id": "n1", "kind": "mappings", "status": "ok" }
                ]
            }
        ]
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        split_nodes: true,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let manifest = read_manifest(&manifest_path)?;
    let detail = manifest.detail.expect("detail should exist");
    let trace_dir = trace_dir(&manifest_path);
    let record_chunk = &detail.records[0];
    let record_chunk_path = trace_dir.join(&record_chunk.path);

    let record_payload = fs::read_to_string(&record_chunk_path)?;
    let mut lines: Vec<String> = record_payload
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| line.to_string())
        .collect();
    let mut record: serde_json::Value = serde_json::from_str(&lines[0])?;
    if let Some(obj) = record.as_object_mut() {
        obj.insert(
            "index".to_string(),
            serde_json::Value::String("invalid".to_string()),
        );
    }
    lines[0] = serde_json::to_string(&record)?;
    fs::write(&record_chunk_path, format!("{}\n", lines.join("\n")))?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store
        .get("trace-invalid-record-index")
        .await?
        .expect("trace should load");
    let records = loaded
        .get("records")
        .and_then(|value| value.as_array())
        .expect("records should exist");
    assert!(records[0].get("nodes").is_none());

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_emits_chunk_metadata_offsets() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-chunk-metadata",
        "records": [
            { "status": "ok", "nodes": [{ "id": "n1", "kind": "mappings", "status": "ok" }] },
            { "status": "ok", "nodes": [{ "id": "n2", "kind": "branch", "status": "ok" }] },
            { "status": "ok", "nodes": [{ "id": "n3", "kind": "branch", "status": "ok" }] }
        ]
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        split_nodes: true,
        max_records_per_chunk: 2,
        max_nodes_per_chunk: 2,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let manifest = read_manifest(&manifest_path)?;
    let detail = manifest.detail.expect("detail should exist");

    assert_eq!(detail.records.len(), 2);
    assert_eq!(detail.records[0].record_start, Some(0));
    assert_eq!(detail.records[0].record_end, Some(1));
    assert_eq!(detail.records[1].record_start, Some(2));
    assert_eq!(detail.records[1].record_end, Some(2));

    assert_eq!(detail.nodes.len(), 2);
    assert_eq!(detail.nodes[0].node_start, Some(0));
    assert_eq!(detail.nodes[0].node_end, Some(1));
    assert_eq!(detail.nodes[1].node_start, Some(2));
    assert_eq!(detail.nodes[1].node_end, Some(2));

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_emits_chunk_metadata_offsets_with_non_sequential_index()
-> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-chunk-metadata-non-seq",
        "records": [
            { "index": 10, "status": "ok", "nodes": [{ "id": "n1", "kind": "mappings", "status": "ok" }] },
            { "index": 20, "status": "ok", "nodes": [{ "id": "n2", "kind": "branch", "status": "ok" }] },
            { "index": 30, "status": "ok", "nodes": [{ "id": "n3", "kind": "branch", "status": "ok" }] }
        ]
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        split_nodes: true,
        max_records_per_chunk: 2,
        max_nodes_per_chunk: 2,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let manifest = read_manifest(&manifest_path)?;
    let detail = manifest.detail.expect("detail should exist");

    assert_eq!(detail.records.len(), 2);
    assert_eq!(detail.records[0].record_start, Some(0));
    assert_eq!(detail.records[0].record_end, Some(1));
    assert_eq!(detail.records[1].record_start, Some(2));
    assert_eq!(detail.records[1].record_end, Some(2));

    assert_eq!(detail.nodes.len(), 2);
    assert_eq!(detail.nodes[0].node_start, Some(0));
    assert_eq!(detail.nodes[0].node_end, Some(1));
    assert_eq!(detail.nodes[1].node_start, Some(2));
    assert_eq!(detail.nodes[1].node_end, Some(2));

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_wraps_inline_non_object_nodes() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-inline-non-object",
        "records": [
            {
                "status": "ok",
                "nodes": ["raw-node"]
            }
        ]
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        split_nodes: false,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let manifest = read_manifest(&manifest_path)?;
    let detail = manifest.detail.expect("detail should exist");
    let trace_dir = trace_dir(&manifest_path);
    let record_chunk = &detail.records[0];
    let record_payload = fs::read_to_string(trace_dir.join(&record_chunk.path))?;
    let record_line = record_payload
        .lines()
        .find(|line| !line.trim().is_empty())
        .expect("record line should exist");
    let record: serde_json::Value = serde_json::from_str(record_line)?;
    let nodes = record
        .get("nodes")
        .and_then(|value| value.as_array())
        .expect("nodes should be array");
    assert_eq!(
        nodes[0].get("value").and_then(|value| value.as_str()),
        Some("raw-node")
    );

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_wraps_inline_scalar_nodes_value() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-inline-scalar-nodes",
        "records": [
            {
                "status": "ok",
                "nodes": "raw-node"
            }
        ]
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        split_nodes: false,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let manifest = read_manifest(&manifest_path)?;
    let detail = manifest.detail.expect("detail should exist");
    let trace_dir = trace_dir(&manifest_path);
    let record_chunk = &detail.records[0];
    let record_payload = fs::read_to_string(trace_dir.join(&record_chunk.path))?;
    let record_line = record_payload
        .lines()
        .find(|line| !line.trim().is_empty())
        .expect("record line should exist");
    let record: serde_json::Value = serde_json::from_str(record_line)?;
    let nodes = record
        .get("nodes")
        .and_then(|value| value.as_array())
        .expect("nodes should be array");
    assert_eq!(
        nodes[0].get("value").and_then(|value| value.as_str()),
        Some("raw-node")
    );

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_wraps_inline_non_object_nodes_on_read() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-inline-non-object-read",
        "records": [
            {
                "status": "ok",
                "nodes": [
                    { "id": "n1", "kind": "mappings", "status": "ok" }
                ]
            }
        ]
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        split_nodes: false,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let manifest = read_manifest(&manifest_path)?;
    let detail = manifest.detail.expect("detail should exist");
    let trace_dir = trace_dir(&manifest_path);
    let record_chunk = &detail.records[0];
    let record_chunk_path = trace_dir.join(&record_chunk.path);

    let record_payload = fs::read_to_string(&record_chunk_path)?;
    let mut lines: Vec<String> = record_payload
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| line.to_string())
        .collect();
    let mut record: serde_json::Value = serde_json::from_str(&lines[0])?;
    if let Some(obj) = record.as_object_mut() {
        obj.insert(
            "nodes".to_string(),
            serde_json::Value::Array(vec![serde_json::Value::String("raw".to_string())]),
        );
    }
    lines[0] = serde_json::to_string(&record)?;
    fs::write(&record_chunk_path, format!("{}\n", lines.join("\n")))?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store
        .get("trace-inline-non-object-read")
        .await?
        .expect("trace should load");
    let records = loaded
        .get("records")
        .and_then(|value| value.as_array())
        .expect("records should exist");
    let nodes = records[0]
        .get("nodes")
        .and_then(|value| value.as_array())
        .expect("nodes should be array");
    assert_eq!(
        nodes[0].get("value").and_then(|value| value.as_str()),
        Some("raw")
    );

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_invalid_record_index_keeps_inheritance() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-invalid-index-keeps",
        "records": [
            {
                "status": "ok",
                "nodes": [
                    { "id": "n1", "kind": "mappings", "status": "ok" },
                    { "id": "n2", "kind": "branch", "status": "ok" },
                    { "id": "n3", "kind": "branch", "status": "ok" }
                ]
            }
        ]
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        split_nodes: true,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let manifest = read_manifest(&manifest_path)?;
    let detail = manifest.detail.expect("detail should exist");
    let trace_dir = trace_dir(&manifest_path);
    let node_chunk = &detail.nodes[0];
    let node_chunk_path = trace_dir.join(&node_chunk.path);

    let node_payload = fs::read_to_string(&node_chunk_path)?;
    let mut lines: Vec<String> = node_payload
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| line.to_string())
        .collect();
    let mut second: serde_json::Value = serde_json::from_str(&lines[1])?;
    if let Some(obj) = second.as_object_mut() {
        obj.insert(
            "record_index".to_string(),
            serde_json::Value::String("invalid".to_string()),
        );
    }
    lines[1] = serde_json::to_string(&second)?;
    let mut third: serde_json::Value = serde_json::from_str(&lines[2])?;
    if let Some(obj) = third.as_object_mut() {
        obj.remove("record_index");
    }
    lines[2] = serde_json::to_string(&third)?;
    fs::write(&node_chunk_path, format!("{}\n", lines.join("\n")))?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store
        .get("trace-invalid-index-keeps")
        .await?
        .expect("trace should load");
    let records = loaded
        .get("records")
        .and_then(|value| value.as_array())
        .expect("records should exist");
    let nodes = records[0]
        .get("nodes")
        .and_then(|value| value.as_array())
        .expect("nodes should be array");
    let ids: Vec<&str> = nodes
        .iter()
        .filter_map(|node| node.get("id").and_then(|value| value.as_str()))
        .collect();
    assert_eq!(ids.len(), 2);
    assert!(ids.contains(&"n1"));
    assert!(ids.contains(&"n3"));

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_default_compression_loads() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-default-compression",
        "records": [
            {
                "status": "ok",
                "nodes": [{ "id": "n1", "kind": "mappings", "status": "ok" }]
            }
        ]
    });

    let _manifest_path = write_trace_bundle(&temp_dir, &trace, None).await?;
    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store
        .get("trace-default-compression")
        .await?
        .expect("trace should load");
    let records = loaded
        .get("records")
        .and_then(|value| value.as_array())
        .expect("records should exist");
    let nodes = records[0]
        .get("nodes")
        .and_then(|value| value.as_array())
        .expect("nodes should be array");
    assert_eq!(
        nodes[0].get("id").and_then(|value| value.as_str()),
        Some("n1")
    );

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_generates_trace_id_when_missing() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "records": []
    });

    let manifest_path = write_trace_bundle(&temp_dir, &trace, None).await?;
    let manifest = read_manifest(&manifest_path)?;

    assert!(manifest.trace_id.starts_with("trace-"));

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_generates_trace_id_when_empty() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "",
        "records": []
    });

    let manifest_path = write_trace_bundle(&temp_dir, &trace, None).await?;
    let manifest = read_manifest(&manifest_path)?;

    assert!(manifest.trace_id.starts_with("trace-"));

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_sanitizes_trace_id() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "../unsafe/trace",
        "records": []
    });

    let manifest_path = write_trace_bundle(&temp_dir, &trace, None).await?;
    let manifest = read_manifest(&manifest_path)?;

    assert!(!manifest.trace_id.contains('/'));
    assert!(!manifest.trace_id.contains('\\'));
    assert!(manifest.trace_id.contains(".."));

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_sanitizes_trace_id_general_case() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "abc DEF/ghi あ",
        "records": []
    });

    let manifest_path = write_trace_bundle(&temp_dir, &trace, None).await?;
    let manifest = read_manifest(&manifest_path)?;

    assert_eq!(manifest.trace_id, "abc_DEF_ghi__");

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_rejects_dot_trace_id() -> anyhow::Result<()> {
    for raw in [".", ".."] {
        let temp_dir = create_temp_dir()?;

        let trace = json!({
            "trace_id": raw,
            "records": []
        });

        let manifest_path = write_trace_bundle(&temp_dir, &trace, None).await?;
        let manifest = read_manifest(&manifest_path)?;

        assert!(manifest.trace_id.starts_with("trace-"));
        assert_ne!(manifest.trace_id, raw);
    }

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_trace_trace_id_falls_back_to_generated() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace",
        "records": []
    });

    let manifest_path = write_trace_bundle(&temp_dir, &trace, None).await?;
    let manifest = read_manifest(&manifest_path)?;

    assert!(manifest.trace_id.starts_with("trace-"));
    assert_ne!(manifest.trace_id, "trace");

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_preserves_trailing_underscore_trace_id() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace_",
        "records": []
    });

    let manifest_path = write_trace_bundle(&temp_dir, &trace, None).await?;
    let manifest = read_manifest(&manifest_path)?;

    assert_eq!(manifest.trace_id, "trace_");

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_sanitizes_legacy_trace_id() -> anyhow::Result<()> {
    let temp_dir = unique_temp_dir();
    let traces_dir = temp_dir.join("traces");
    fs::create_dir_all(&traces_dir)?;

    let legacy_path = traces_dir.join("legacy.json");
    fs::write(
        &legacy_path,
        serde_json::to_string(&json!({
            "trace_id": "legacy id/非",
            "status": "ok"
        }))?,
    )?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let items = store.list().await?;
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].trace_id, "legacy_id__");
    let loaded = store
        .get(&items[0].trace_id)
        .await?
        .expect("trace should load");
    assert_eq!(
        loaded.get("trace_id").and_then(|value| value.as_str()),
        Some("legacy_id__")
    );

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_legacy_missing_trace_id_trace_filename_falls_back_to_hash()
-> anyhow::Result<()> {
    let temp_dir = unique_temp_dir();
    let traces_dir = temp_dir.join("traces");
    fs::create_dir_all(&traces_dir)?;

    let legacy_path = traces_dir.join("trace.json");
    fs::write(
        &legacy_path,
        serde_json::to_string(&json!({
            "records": [],
            "status": "ok"
        }))?,
    )?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let items = store.list().await?;
    assert_eq!(items.len(), 1);
    assert!(items[0].trace_id.starts_with("trace-"));
    assert_ne!(items[0].trace_id, "trace");

    let loaded = store
        .get(&items[0].trace_id)
        .await?
        .expect("trace should load");
    assert_eq!(
        loaded.get("trace_id").and_then(|value| value.as_str()),
        Some(items[0].trace_id.as_str())
    );

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_legacy_trace_id_trace_falls_back_to_hash() -> anyhow::Result<()> {
    let temp_dir = unique_temp_dir();
    let traces_dir = temp_dir.join("traces");
    fs::create_dir_all(&traces_dir)?;

    let legacy_path = traces_dir.join("legacy-trace.json");
    fs::write(
        &legacy_path,
        serde_json::to_string(&json!({
            "trace_id": "trace",
            "records": [],
            "status": "ok"
        }))?,
    )?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let items = store.list().await?;
    assert_eq!(items.len(), 1);
    assert!(items[0].trace_id.starts_with("trace-"));
    assert_ne!(items[0].trace_id, "trace");

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_avoids_trace_id_collision_on_sanitize() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace_a = json!({
        "trace_id": "a/b",
        "timestamp": "2026-02-01T00:00:00Z",
        "records": []
    });
    let trace_b = json!({
        "trace_id": "a_b",
        "timestamp": "2026-02-01T00:00:00Z",
        "records": []
    });

    let manifest_a_path = write_trace_bundle(&temp_dir, &trace_a, None).await?;
    let manifest_a = read_manifest(&manifest_a_path)?;

    let manifest_b_path = write_trace_bundle(&temp_dir, &trace_b, None).await?;
    let manifest_b = read_manifest(&manifest_b_path)?;

    assert_ne!(manifest_a.trace_id, manifest_b.trace_id);

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_disambiguates_legacy_trace_id_collisions() -> anyhow::Result<()> {
    let temp_dir = unique_temp_dir();
    let traces_dir = temp_dir.join("traces");
    fs::create_dir_all(&traces_dir)?;

    fs::write(
        traces_dir.join("legacy-a.json"),
        serde_json::to_string(&json!({ "trace_id": "a/b", "status": "ok" }))?,
    )?;
    fs::write(
        traces_dir.join("legacy-b.json"),
        serde_json::to_string(&json!({ "trace_id": "a_b", "status": "ok" }))?,
    )?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let items = store.list().await?;
    let legacy_items: Vec<_> = items
        .into_iter()
        .filter(|item| item.path.ends_with("legacy-a.json") || item.path.ends_with("legacy-b.json"))
        .collect();
    assert_eq!(legacy_items.len(), 2);
    let ids: std::collections::HashSet<_> = legacy_items
        .iter()
        .map(|item| item.trace_id.as_str())
        .collect();
    assert_eq!(ids.len(), 2);
    assert!(ids.iter().all(|id| id.starts_with("a_b-dup-")));

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_sanitizes_manifest_trace_id_on_list_get() -> anyhow::Result<()> {
    let temp_dir = unique_temp_dir();
    let traces_dir = temp_dir.join("traces");
    fs::create_dir_all(&traces_dir)?;

    let manifest_path = traces_dir.join("manifest.json");
    fs::write(
        &manifest_path,
        serde_json::to_string(&json!({
            "trace_schema_version": 1,
            "trace_id": "a/b"
        }))?,
    )?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let items = store.list().await?;
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].trace_id, "a_b");

    let loaded = store
        .get(&items[0].trace_id)
        .await?
        .expect("trace should load");
    assert_eq!(
        loaded.get("trace_id").and_then(|value| value.as_str()),
        Some("a_b")
    );

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_manifest_empty_trace_id_falls_back_to_hash() -> anyhow::Result<()> {
    let temp_dir = unique_temp_dir();
    let traces_dir = temp_dir.join("traces");
    fs::create_dir_all(&traces_dir)?;

    let manifest_path = traces_dir.join("trace.json");
    fs::write(
        &manifest_path,
        serde_json::to_string(&json!({
            "trace_schema_version": 1,
            "trace_id": ""
        }))?,
    )?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let items = store.list().await?;
    assert_eq!(items.len(), 1);
    assert!(items[0].trace_id.starts_with("trace-"));

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_manifest_empty_trace_id_uses_file_stem() -> anyhow::Result<()> {
    let temp_dir = unique_temp_dir();
    let traces_dir = temp_dir.join("traces");
    fs::create_dir_all(&traces_dir)?;

    let manifest_path = traces_dir.join("custom-id.json");
    fs::write(
        &manifest_path,
        serde_json::to_string(&json!({
            "trace_schema_version": 1,
            "trace_id": ""
        }))?,
    )?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let items = store.list().await?;
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].trace_id, "custom-id");

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_legacy_empty_trace_id_uses_file_stem() -> anyhow::Result<()> {
    let temp_dir = unique_temp_dir();
    let traces_dir = temp_dir.join("traces");
    fs::create_dir_all(&traces_dir)?;

    let legacy_path = traces_dir.join("legacy-custom.json");
    fs::write(
        &legacy_path,
        serde_json::to_string(&json!({
            "trace_id": "",
            "status": "ok"
        }))?,
    )?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let items = store.list().await?;
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].trace_id, "legacy-custom");

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_legacy_dot_trace_id_uses_file_stem() -> anyhow::Result<()> {
    let temp_dir = unique_temp_dir();
    let traces_dir = temp_dir.join("traces");
    fs::create_dir_all(&traces_dir)?;

    for raw in [".", ".."] {
        let legacy_path = traces_dir.join(format!("legacy-dot-{raw}.json"));
        fs::write(
            &legacy_path,
            serde_json::to_string(&json!({
                "trace_id": raw,
                "status": "ok"
            }))?,
        )?;
    }

    let store = TraceStore::new(temp_dir.clone()).await?;
    let items = store.list().await?;
    let trace_ids: std::collections::HashSet<_> =
        items.iter().map(|item| item.trace_id.as_str()).collect();
    assert!(trace_ids.contains("legacy-dot-."));
    assert!(trace_ids.contains("legacy-dot-.."));

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_manifest_trace_id_trace_falls_back_to_hash() -> anyhow::Result<()> {
    let temp_dir = unique_temp_dir();
    let traces_dir = temp_dir.join("traces");
    fs::create_dir_all(&traces_dir)?;

    let manifest_path = traces_dir.join("manifest-trace.json");
    fs::write(
        &manifest_path,
        serde_json::to_string(&json!({
            "trace_schema_version": 1,
            "trace_id": "trace"
        }))?,
    )?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let items = store.list().await?;
    assert_eq!(items.len(), 1);
    assert!(items[0].trace_id.starts_with("trace-"));
    assert_ne!(items[0].trace_id, "trace");

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_manifest_dot_trace_id_uses_file_stem() -> anyhow::Result<()> {
    let temp_dir = unique_temp_dir();
    let traces_dir = temp_dir.join("traces");
    fs::create_dir_all(&traces_dir)?;

    for raw in [".", ".."] {
        let manifest_path = traces_dir.join(format!("manifest-dot-{raw}.json"));
        fs::write(
            &manifest_path,
            serde_json::to_string(&json!({
                "trace_schema_version": 1,
                "trace_id": raw
            }))?,
        )?;
    }

    let store = TraceStore::new(temp_dir.clone()).await?;
    let items = store.list().await?;
    let trace_ids: std::collections::HashSet<_> =
        items.iter().map(|item| item.trace_id.as_str()).collect();
    assert!(trace_ids.contains("manifest-dot-."));
    assert!(trace_ids.contains("manifest-dot-.."));

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_disambiguates_manifest_trace_id_collisions() -> anyhow::Result<()> {
    let temp_dir = unique_temp_dir();
    let traces_dir = temp_dir.join("traces");
    fs::create_dir_all(&traces_dir)?;

    fs::write(
        traces_dir.join("manifest-a.json"),
        serde_json::to_string(&json!({
            "trace_schema_version": 1,
            "trace_id": "a/b"
        }))?,
    )?;
    fs::write(
        traces_dir.join("manifest-b.json"),
        serde_json::to_string(&json!({
            "trace_schema_version": 1,
            "trace_id": "a_b"
        }))?,
    )?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let items = store.list().await?;
    let manifest_items: Vec<_> = items
        .into_iter()
        .filter(|item| {
            item.path.ends_with("manifest-a.json") || item.path.ends_with("manifest-b.json")
        })
        .collect();
    assert_eq!(manifest_items.len(), 2);
    let ids: std::collections::HashSet<_> = manifest_items
        .iter()
        .map(|item| item.trace_id.as_str())
        .collect();
    assert_eq!(ids.len(), 2);
    assert!(ids.iter().all(|id| id.starts_with("a_b-dup-")));

    for item in &manifest_items {
        let loaded = store.get(&item.trace_id).await?.expect("trace should load");
        assert_eq!(
            loaded.get("trace_id").and_then(|value| value.as_str()),
            Some(item.trace_id.as_str())
        );
    }

    Ok(())
}

#[tokio::test]
async fn trace_store_collision_resolution_is_stable_across_refresh() -> anyhow::Result<()> {
    let temp_dir = unique_temp_dir();
    let traces_dir = temp_dir.join("traces");
    fs::create_dir_all(&traces_dir)?;

    fs::write(
        traces_dir.join("b.json"),
        serde_json::to_string(&json!({
            "trace_id": "same",
            "records": [],
            "status": "ok"
        }))?,
    )?;
    fs::write(
        traces_dir.join("a.json"),
        serde_json::to_string(&json!({
            "trace_id": "same",
            "records": [],
            "status": "ok"
        }))?,
    )?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let first = store.list().await?;
    let first_map: HashMap<String, String> = first
        .iter()
        .map(|item| (item.path.clone(), item.trace_id.clone()))
        .collect();

    let second = store.list().await?;
    let second_map: HashMap<String, String> = second
        .iter()
        .map(|item| (item.path.clone(), item.trace_id.clone()))
        .collect();

    assert_eq!(first_map, second_map);

    let a_path = traces_dir.join("a.json").display().to_string();
    let b_path = traces_dir.join("b.json").display().to_string();
    let a_id = first_map.get(&a_path).expect("a.json should exist");
    let b_id = first_map.get(&b_path).expect("b.json should exist");
    assert_ne!(a_id, b_id);
    assert!(a_id.starts_with("same-dup-"));
    assert!(b_id.starts_with("same-dup-"));

    Ok(())
}

#[tokio::test]
async fn trace_store_collision_adds_counter_when_candidate_exists() -> anyhow::Result<()> {
    fn fnv1a_hash(bytes: &[u8]) -> u64 {
        const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
        const FNV_PRIME: u64 = 0x100000001b3;
        let mut hash = FNV_OFFSET_BASIS;
        for byte in bytes {
            hash ^= *byte as u64;
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        hash
    }

    let temp_dir = unique_temp_dir();
    let traces_dir = temp_dir.join("traces");
    fs::create_dir_all(&traces_dir)?;

    let base_path = traces_dir.join("a.json");
    fs::write(
        &base_path,
        serde_json::to_string(&json!({
            "trace_id": "same",
            "records": [],
            "status": "ok"
        }))?,
    )?;
    fs::write(
        traces_dir.join("b.json"),
        serde_json::to_string(&json!({
            "trace_id": "same",
            "records": [],
            "status": "ok"
        }))?,
    )?;

    let hash = fnv1a_hash("a.json".as_bytes());
    let conflict_id = format!("same-dup-{hash:x}");
    fs::write(
        traces_dir.join("conflict.json"),
        serde_json::to_string(&json!({
            "trace_id": conflict_id,
            "records": [],
            "status": "ok"
        }))?,
    )?;

    let store = TraceStore::new(temp_dir.clone()).await?;
    let items = store.list().await?;
    let a_item = items
        .iter()
        .find(|item| item.path.ends_with("a.json"))
        .expect("a.json should exist");
    assert_eq!(a_item.trace_id, format!("same-dup-{hash:x}-1"));

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_masks_sensitive_fields() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-mask",
        "records": [
            {
                "index": 0,
                "input": {
                    "authorization": "Bearer abc",
                    "password": "secret",
                    "nested": { "token": "abc" },
                    "ok": 1
                },
                "output": { "secret": "value" }
            }
        ]
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        ..Default::default()
    };

    let _manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store.get("trace-mask").await?.expect("trace should load");

    let record = loaded
        .get("records")
        .and_then(|value| value.as_array())
        .and_then(|records| records.first())
        .and_then(|value| value.as_object())
        .expect("record object");
    let input = record
        .get("input")
        .and_then(|value| value.as_object())
        .expect("input object");
    let nested = input
        .get("nested")
        .and_then(|value| value.as_object())
        .expect("nested object");
    let output = record
        .get("output")
        .and_then(|value| value.as_object())
        .expect("output object");

    assert_eq!(
        input.get("authorization").and_then(|value| value.as_str()),
        Some("[masked]")
    );
    assert_eq!(
        input.get("password").and_then(|value| value.as_str()),
        Some("[masked]")
    );
    assert_eq!(
        nested.get("token").and_then(|value| value.as_str()),
        Some("[masked]")
    );
    assert_eq!(
        output.get("secret").and_then(|value| value.as_str()),
        Some("[masked]")
    );

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_masks_rule_source_when_basic() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-mask-basic-rule-source",
        "status": "ok",
        "summary": {
            "record_total": 1,
            "record_success": 1,
            "record_failed": 0
        },
        "rule_source": {
            "token": "secret",
            "headers": {
                "Authorization": "Bearer 123"
            }
        },
        "records": [
            { "index": 0, "input": { "token": "secret" } }
        ]
    });

    let options = TraceWriteOptions {
        detail_level: TraceDetailLevel::Basic,
        compression: TraceCompression::None,
        ..Default::default()
    };

    let _manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store
        .get("trace-mask-basic-rule-source")
        .await?
        .expect("trace should load");

    let detail = loaded
        .get("detail")
        .and_then(|value| value.as_object())
        .expect("detail object");
    assert_eq!(
        detail.get("status").and_then(|value| value.as_str()),
        Some("basic")
    );
    let detail_records = detail
        .get("records")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(detail_records.is_empty());
    let detail_nodes = detail
        .get("nodes")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(detail_nodes.is_empty());
    assert!(detail.get("finalize").is_none());

    let records = loaded
        .get("records")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(records.is_empty());

    let rule_source = loaded
        .get("rule_source")
        .and_then(|value| value.as_object())
        .expect("rule_source object");
    assert_eq!(
        rule_source.get("token").and_then(|value| value.as_str()),
        Some("[masked]")
    );
    let headers = rule_source
        .get("headers")
        .and_then(|value| value.as_object())
        .expect("headers object");
    assert_eq!(
        headers
            .get("Authorization")
            .and_then(|value| value.as_str()),
        Some("[masked]")
    );

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_masks_rule_source_when_off() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-mask-off-rule-source",
        "status": "ok",
        "summary": {
            "record_total": 1,
            "record_success": 1,
            "record_failed": 0
        },
        "rule_source": {
            "token": "secret"
        },
        "records": [
            { "index": 0, "input": { "token": "secret" } }
        ]
    });

    let options = TraceWriteOptions {
        detail_level: TraceDetailLevel::Off,
        compression: TraceCompression::None,
        ..Default::default()
    };

    let _manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store
        .get("trace-mask-off-rule-source")
        .await?
        .expect("trace should load");

    let detail = loaded
        .get("detail")
        .and_then(|value| value.as_object())
        .expect("detail object");
    assert_eq!(
        detail.get("status").and_then(|value| value.as_str()),
        Some("dropped")
    );
    let detail_records = detail
        .get("records")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(detail_records.is_empty());
    let detail_nodes = detail
        .get("nodes")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(detail_nodes.is_empty());
    assert!(detail.get("finalize").is_none());

    let records = loaded
        .get("records")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(records.is_empty());

    let rule_source = loaded
        .get("rule_source")
        .and_then(|value| value.as_object())
        .expect("rule_source object");
    assert_eq!(
        rule_source.get("token").and_then(|value| value.as_str()),
        Some("[masked]")
    );

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_basic_without_masking_strips_detail() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-basic-no-mask",
        "status": "ok",
        "summary": {
            "record_total": 1,
            "record_success": 1,
            "record_failed": 0
        },
        "rule_source": {
            "token": "secret"
        },
        "records": [
            { "index": 0, "input": { "token": "secret" } }
        ]
    });

    let options = TraceWriteOptions {
        detail_level: TraceDetailLevel::Basic,
        masking_enabled: false,
        compression: TraceCompression::None,
        ..Default::default()
    };

    let _manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store
        .get("trace-basic-no-mask")
        .await?
        .expect("trace should load");

    let detail = loaded
        .get("detail")
        .and_then(|value| value.as_object())
        .expect("detail object");
    assert_eq!(
        detail.get("status").and_then(|value| value.as_str()),
        Some("basic")
    );
    let detail_records = detail
        .get("records")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(detail_records.is_empty());
    let detail_nodes = detail
        .get("nodes")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(detail_nodes.is_empty());
    assert!(detail.get("finalize").is_none());

    let records = loaded
        .get("records")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(records.is_empty());

    let rule_source = loaded
        .get("rule_source")
        .and_then(|value| value.as_object())
        .expect("rule_source object");
    assert_eq!(
        rule_source.get("token").and_then(|value| value.as_str()),
        Some("secret")
    );

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_off_without_masking_strips_detail() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-off-no-mask",
        "status": "ok",
        "summary": {
            "record_total": 1,
            "record_success": 1,
            "record_failed": 0
        },
        "rule_source": {
            "token": "secret"
        },
        "records": [
            { "index": 0, "input": { "token": "secret" } }
        ]
    });

    let options = TraceWriteOptions {
        detail_level: TraceDetailLevel::Off,
        masking_enabled: false,
        compression: TraceCompression::None,
        ..Default::default()
    };

    let _manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store
        .get("trace-off-no-mask")
        .await?
        .expect("trace should load");

    let detail = loaded
        .get("detail")
        .and_then(|value| value.as_object())
        .expect("detail object");
    assert_eq!(
        detail.get("status").and_then(|value| value.as_str()),
        Some("dropped")
    );
    let detail_records = detail
        .get("records")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(detail_records.is_empty());
    let detail_nodes = detail
        .get("nodes")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(detail_nodes.is_empty());
    assert!(detail.get("finalize").is_none());

    let records = loaded
        .get("records")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(records.is_empty());

    let rule_source = loaded
        .get("rule_source")
        .and_then(|value| value.as_object())
        .expect("rule_source object");
    assert_eq!(
        rule_source.get("token").and_then(|value| value.as_str()),
        Some("secret")
    );

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_masks_url_query_params() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-mask-url",
        "records": [
            {
                "index": 0,
                "input": {
                    "url": "https://example.com/path?token=abc&ok=1#frag"
                }
            }
        ]
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        ..Default::default()
    };

    let _manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store
        .get("trace-mask-url")
        .await?
        .expect("trace should load");

    let record = loaded
        .get("records")
        .and_then(|value| value.as_array())
        .and_then(|records| records.first())
        .and_then(|value| value.as_object())
        .expect("record object");
    let input = record
        .get("input")
        .and_then(|value| value.as_object())
        .expect("input object");

    assert_eq!(
        input.get("url").and_then(|value| value.as_str()),
        Some("https://example.com/path?token=[masked]&ok=1#frag")
    );

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_masks_url_fragment_params() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-mask-url-fragment",
        "records": [
            {
                "index": 0,
                "input": {
                    "fragment_only": "https://example.com/callback#access_token=abc&ok=1",
                    "query_and_fragment": "https://example.com/path?ok=1#token=abc",
                    "hash_route": "https://example.com/#/callback?token=abc&ok=1"
                }
            }
        ]
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        ..Default::default()
    };

    let _manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store
        .get("trace-mask-url-fragment")
        .await?
        .expect("trace should load");

    let input = loaded
        .get("records")
        .and_then(|value| value.as_array())
        .and_then(|records| records.first())
        .and_then(|value| value.as_object())
        .and_then(|record| record.get("input"))
        .and_then(|value| value.as_object())
        .expect("input object");

    assert_eq!(
        input.get("fragment_only").and_then(|value| value.as_str()),
        Some("https://example.com/callback#access_token=[masked]&ok=1")
    );
    assert_eq!(
        input
            .get("query_and_fragment")
            .and_then(|value| value.as_str()),
        Some("https://example.com/path?ok=1#token=[masked]")
    );
    assert_eq!(
        input.get("hash_route").and_then(|value| value.as_str()),
        Some("https://example.com/#/callback?token=[masked]&ok=1")
    );

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_drops_oversized_rule_source() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-rule-source-drop",
        "rule_source": {
            "text": "x".repeat(2048)
        },
        "records": [
            { "index": 0, "status": "ok" }
        ]
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        max_bytes_per_trace: 64,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let manifest = read_manifest(&manifest_path)?;

    assert!(manifest.rule_source.is_none());
    let detail = manifest.detail.expect("detail should exist");
    assert!(
        detail
            .reason
            .iter()
            .any(|reason| reason == "rule_source_dropped")
    );

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_externalizes_large_payloads() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-blob",
        "records": [
            { "index": 0, "input": { "data": "x".repeat(200) } }
        ]
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        max_payload_bytes: 32,
        payload_preview_bytes: 8,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store.get("trace-blob").await?.expect("trace should load");
    let record = loaded
        .get("records")
        .and_then(|value| value.as_array())
        .and_then(|records| records.first())
        .and_then(|value| value.as_object())
        .expect("record object");
    let input = record
        .get("input")
        .and_then(|value| value.as_object())
        .expect("input object");

    let blob_ref = input
        .get("blob_ref")
        .and_then(|value| value.as_str())
        .expect("blob_ref");
    let size_bytes = input
        .get("size_bytes")
        .and_then(|value| value.as_u64())
        .expect("size_bytes");
    assert!(size_bytes > 32);

    let trace_dir = trace_dir(&manifest_path);
    let blob_path = trace_dir.join(blob_ref);
    assert!(blob_path.exists(), "blob file should exist");

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_counts_blob_bytes_in_budget() -> anyhow::Result<()> {
    let trace = json!({
        "trace_id": "trace-blob-budget",
        "records": [
            { "index": 0, "input": { "data": "x".repeat(5000) } }
        ]
    });

    let options_full = TraceWriteOptions {
        compression: TraceCompression::None,
        max_payload_bytes: 32,
        payload_preview_bytes: 16,
        max_bytes_per_trace: 10_000_000,
        ..Default::default()
    };

    let temp_dir_full = create_temp_dir()?;
    let manifest_path_full = write_trace_bundle(&temp_dir_full, &trace, Some(options_full)).await?;
    let manifest_full = read_manifest(&manifest_path_full)?;
    let detail_full = manifest_full.detail.expect("detail should exist");
    assert_eq!(detail_full.status, "full");

    let chunk_total: u64 = detail_full
        .records
        .iter()
        .filter_map(|chunk| chunk.bytes)
        .sum::<u64>()
        .saturating_add(
            detail_full
                .nodes
                .iter()
                .filter_map(|chunk| chunk.bytes)
                .sum::<u64>(),
        )
        .saturating_add(
            detail_full
                .finalize
                .as_ref()
                .and_then(|c| c.bytes)
                .unwrap_or(0),
        );

    let trace_dir_full = trace_dir(&manifest_path_full);
    let blobs_dir = trace_dir_full.join("blobs");
    let mut blob_total = 0u64;
    if blobs_dir.exists() {
        for entry in fs::read_dir(&blobs_dir)? {
            let entry = entry?;
            blob_total = blob_total.saturating_add(entry.metadata()?.len());
        }
    }
    assert!(blob_total > 0, "expected blob files to be written");

    let options_budget = TraceWriteOptions {
        compression: TraceCompression::None,
        max_payload_bytes: 32,
        payload_preview_bytes: 16,
        max_bytes_per_trace: (chunk_total as usize) + 1,
        ..Default::default()
    };

    let temp_dir_budget = create_temp_dir()?;
    let manifest_path_budget =
        write_trace_bundle(&temp_dir_budget, &trace, Some(options_budget)).await?;
    let manifest_budget = read_manifest(&manifest_path_budget)?;
    let detail_budget = manifest_budget.detail.expect("detail should exist");
    assert_eq!(detail_budget.status, "basic");
    assert!(
        detail_budget
            .reason
            .iter()
            .any(|reason| reason == "budget_exceeded")
    );

    Ok(())
}

#[tokio::test]
async fn write_trace_bundle_masks_preview_for_externalized_payloads() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-blob-mask-preview",
        "records": [
            {
                "index": 0,
                "input": {
                    "token": "secret",
                    "pad": "x".repeat(500)
                }
            }
        ]
    });

    let options = TraceWriteOptions {
        compression: TraceCompression::None,
        max_payload_bytes: 32,
        payload_preview_bytes: 200,
        ..Default::default()
    };

    let manifest_path = write_trace_bundle(&temp_dir, &trace, Some(options)).await?;
    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store
        .get("trace-blob-mask-preview")
        .await?
        .expect("trace should load");
    let record = loaded
        .get("records")
        .and_then(|value| value.as_array())
        .and_then(|records| records.first())
        .and_then(|value| value.as_object())
        .expect("record object");
    let input = record
        .get("input")
        .and_then(|value| value.as_object())
        .expect("input object");
    let preview = input
        .get("preview")
        .and_then(|value| value.as_str())
        .expect("preview should exist");
    assert!(!preview.contains("secret"));

    let blob_ref = input
        .get("blob_ref")
        .and_then(|value| value.as_str())
        .expect("blob_ref should exist");
    let blob_path = trace_dir(&manifest_path).join(blob_ref);
    let blob_payload = fs::read_to_string(blob_path)?;
    let blob_json: serde_json::Value = serde_json::from_str(&blob_payload)?;
    let token = blob_json
        .get("token")
        .and_then(|value| value.as_str())
        .expect("token should exist");
    assert_eq!(token, "[masked]");

    Ok(())
}
