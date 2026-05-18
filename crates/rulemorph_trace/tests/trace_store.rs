#[path = "common/trace_store.rs"]
mod trace_store_common;

use anyhow::Result;
use rulemorph_trace::{
    TRACE_CHUNK_BYTES_COMPRESSED_OVERHEAD_MAX, TRACE_JSON_MAX_BYTES, TRACE_NODE_COUNT_HARD_MAX,
    TRACE_RECORD_COUNT_HARD_MAX, TraceStore,
};
use serde_json::json;
use std::fs;
#[cfg(unix)]
use std::os::unix::fs::symlink;
use tempfile::tempdir;
use trace_store_common::{
    assert_detail_array_empty, assert_detail_reason, assert_detail_status, assert_finalize_absent,
    assert_top_level_array_empty, create_trace_dir, write_records_inline_trace_json,
    write_trace_json,
};

include!("trace_store/import_bundle.rs");

#[tokio::test]
async fn trace_store_ignores_blobs_in_index() -> Result<()> {
    let temp = tempdir()?;
    let data_dir = temp.path();

    let trace_dir = create_trace_dir(data_dir, "traces/2026/01/01/trace-main")?;
    write_trace_json(
        &trace_dir,
        &json!({
            "trace_schema_version": 1,
            "trace_id": "trace-main",
            "status": "ok",
            "timestamp": "2026-01-01T00:00:00Z"
        }),
    )?;

    let blob_dir = trace_dir.join("blobs");
    fs::create_dir_all(&blob_dir)?;
    fs::write(
        blob_dir.join("sha256-deadbeef.json"),
        serde_json::to_vec(&json!({
            "trace_id": "trace-blob",
            "status": "ok",
            "records": [],
            "summary": { "record_total": 0 }
        }))?,
    )?;

    let legacy_dir = data_dir.join("traces/2026/01/01");
    fs::create_dir_all(&legacy_dir)?;
    fs::write(
        legacy_dir.join("legacy.json"),
        serde_json::to_vec(&json!({
            "trace_id": "trace-legacy",
            "status": "ok",
            "records": [],
            "summary": { "record_total": 0 }
        }))?,
    )?;

    let store = TraceStore::new(data_dir.to_path_buf()).await?;
    let items = store.list().await?;

    assert!(items.iter().any(|item| item.trace_id == "trace-main"));
    assert!(items.iter().any(|item| item.trace_id == "trace-legacy"));
    assert!(!items.iter().any(|item| item.trace_id == "trace-blob"));

    Ok(())
}

#[tokio::test]
async fn trace_store_ignores_oversized_trace_json() -> Result<()> {
    let temp = tempdir()?;
    let data_dir = temp.path();

    let trace_dir = create_trace_dir(data_dir, "traces/2026/01/03/trace-large")?;
    let padding_len = TRACE_JSON_MAX_BYTES as usize;
    let payload = format!(
        "{{\"trace_id\":\"trace-large\",\"status\":\"ok\",\"records\":[],\"padding\":\"{}\"}}",
        "x".repeat(padding_len)
    );
    fs::write(trace_dir.join("trace.json"), payload)?;

    let small_dir = create_trace_dir(data_dir, "traces/2026/01/03/trace-small")?;
    write_trace_json(
        &small_dir,
        &json!({
            "trace_schema_version": 1,
            "trace_id": "trace-small",
            "status": "ok",
            "timestamp": "2026-01-03T00:00:00Z"
        }),
    )?;

    let store = TraceStore::new(data_dir.to_path_buf()).await?;
    let items = store.list().await?;

    assert!(items.iter().any(|item| item.trace_id == "trace-small"));
    assert!(!items.iter().any(|item| item.trace_id == "trace-large"));

    Ok(())
}

#[tokio::test]
async fn trace_store_downgrades_on_record_count_limit() -> Result<()> {
    let temp = tempdir()?;
    let data_dir = temp.path();

    let trace_dir = data_dir.join("traces/2026/01/04/trace-record-limit");
    fs::create_dir_all(&trace_dir)?;

    let record_count = TRACE_RECORD_COUNT_HARD_MAX + 1;
    let mut lines = String::new();
    for index in 0..record_count {
        lines.push_str(&format!("{{\"index\":{index}}}\n"));
    }
    fs::write(trace_dir.join("records-0001.ndjson"), lines)?;

    fs::write(
        trace_dir.join("trace.json"),
        serde_json::to_vec(&json!({
            "trace_schema_version": 1,
            "trace_id": "trace-record-limit",
            "status": "ok",
            "max_chunk_bytes_uncompressed": 4 * 1024 * 1024,
            "detail": {
                "layout": "records_nodes_split",
                "status": "full",
                "records": [
                    {
                        "path": "records-0001.ndjson",
                        "format": "ndjson",
                        "compression": "none"
                    }
                ],
                "nodes": []
            }
        }))?,
    )?;

    let store = TraceStore::new(data_dir.to_path_buf()).await?;
    let trace = store
        .get("trace-record-limit")
        .await?
        .expect("trace should exist");
    assert_detail_status(&trace, "basic");
    assert_detail_reason(&trace, "budget_exceeded");
    assert_detail_array_empty(&trace, "records");

    Ok(())
}

#[tokio::test]
async fn trace_store_downgrades_on_node_count_limit() -> Result<()> {
    let temp = tempdir()?;
    let data_dir = temp.path();

    let trace_dir = data_dir.join("traces/2026/01/04/trace-node-limit");
    fs::create_dir_all(&trace_dir)?;

    let node_count = TRACE_NODE_COUNT_HARD_MAX + 1;
    let mut lines = String::new();
    for _ in 0..node_count {
        lines.push_str("{\"record_index\":0,\"node\":0}\n");
    }
    fs::write(trace_dir.join("nodes-0001.ndjson"), lines)?;

    fs::write(
        trace_dir.join("trace.json"),
        serde_json::to_vec(&json!({
            "trace_schema_version": 1,
            "trace_id": "trace-node-limit",
            "status": "ok",
            "max_chunk_bytes_uncompressed": 16 * 1024 * 1024,
            "detail": {
                "layout": "records_nodes_split",
                "status": "full",
                "records": [],
                "nodes": [
                    {
                        "path": "nodes-0001.ndjson",
                        "format": "ndjson",
                        "compression": "none"
                    }
                ]
            }
        }))?,
    )?;

    let store = TraceStore::new(data_dir.to_path_buf()).await?;
    let trace = store
        .get("trace-node-limit")
        .await?
        .expect("trace should exist");
    assert_detail_status(&trace, "basic");
    assert_detail_reason(&trace, "budget_exceeded");

    Ok(())
}

#[tokio::test]
async fn trace_store_downgrades_on_inline_node_count_limit() -> Result<()> {
    let temp = tempdir()?;
    let data_dir = temp.path();

    let trace_dir = data_dir.join("traces/2026/01/04/trace-inline-node-limit");
    fs::create_dir_all(&trace_dir)?;

    let node_count = TRACE_NODE_COUNT_HARD_MAX + 1;
    let mut nodes = Vec::with_capacity(node_count);
    for _ in 0..node_count {
        nodes.push(json!({}));
    }
    let record = json!({
        "index": 0,
        "status": "ok",
        "nodes": nodes
    });
    let line = serde_json::to_string(&record)?;
    fs::write(trace_dir.join("records-0001.ndjson"), format!("{line}\n"))?;

    fs::write(
        trace_dir.join("trace.json"),
        serde_json::to_vec(&json!({
            "trace_schema_version": 1,
            "trace_id": "trace-inline-node-limit",
            "status": "ok",
            "max_chunk_bytes_uncompressed": 16 * 1024 * 1024,
            "detail": {
                "layout": "records_nodes_split",
                "status": "full",
                "records": [
                    {
                        "path": "records-0001.ndjson",
                        "format": "ndjson",
                        "compression": "none"
                    }
                ],
                "nodes": []
            }
        }))?,
    )?;

    let store = TraceStore::new(data_dir.to_path_buf()).await?;
    let trace = store
        .get("trace-inline-node-limit")
        .await?
        .expect("trace should exist");
    assert_detail_status(&trace, "basic");
    assert_detail_reason(&trace, "budget_exceeded");

    Ok(())
}

include!("trace_store/chunk_downgrade.rs");
