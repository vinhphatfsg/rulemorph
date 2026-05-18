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

#[tokio::test]
async fn trace_store_downgrades_detail_on_missing_chunk() -> Result<()> {
    let temp = tempdir()?;
    let data_dir = temp.path();
    let trace_dir = data_dir.join("traces/2026/01/02/trace-missing");
    fs::create_dir_all(&trace_dir)?;

    write_records_inline_trace_json(
        &trace_dir,
        "trace-missing",
        "records-0001.ndjson",
        "ndjson",
        "none",
        None,
    )?;

    let store = TraceStore::new(data_dir.to_path_buf()).await?;
    let trace = store
        .get("trace-missing")
        .await?
        .expect("trace should exist");
    assert_detail_status(&trace, "basic");
    assert_detail_reason(&trace, "chunk_error");
    assert_detail_array_empty(&trace, "records");
    assert_detail_array_empty(&trace, "nodes");
    assert_top_level_array_empty(&trace, "records");
    assert_finalize_absent(&trace);

    Ok(())
}

#[tokio::test]
async fn trace_store_accepts_compressed_chunk_overhead() -> Result<()> {
    let temp = tempdir()?;
    let data_dir = temp.path();
    let trace_dir = data_dir.join("traces/2026/01/02/trace-compressed-overhead");
    fs::create_dir_all(&trace_dir)?;

    let payload = "0\n";
    let compressed = zstd::stream::encode_all(payload.as_bytes(), 3)?;
    assert!(compressed.len() > payload.len());
    let max_chunk_bytes_uncompressed = compressed.len().saturating_sub(1);
    assert!(max_chunk_bytes_uncompressed >= payload.len());
    fs::write(trace_dir.join("records-0001.ndjson.zst"), compressed)?;

    write_records_inline_trace_json(
        &trace_dir,
        "trace-compressed-overhead",
        "records-0001.ndjson.zst",
        "ndjson",
        "zstd",
        Some(max_chunk_bytes_uncompressed),
    )?;

    let store = TraceStore::new(data_dir.to_path_buf()).await?;
    let trace = store
        .get("trace-compressed-overhead")
        .await?
        .expect("trace should exist");
    assert_detail_status(&trace, "full");
    let records = trace
        .get("records")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    assert_eq!(records.len(), 1);

    Ok(())
}

#[tokio::test]
async fn trace_store_downgrades_detail_on_oversized_uncompressed_chunk() -> Result<()> {
    let temp = tempdir()?;
    let data_dir = temp.path();
    let trace_dir = data_dir.join("traces/2026/01/02/trace-oversized-none");
    fs::create_dir_all(&trace_dir)?;

    let payload = format!("{{\"index\":0,\"payload\":\"{}\"}}\n", "x".repeat(64));
    fs::write(trace_dir.join("records-0001.ndjson"), payload)?;

    write_records_inline_trace_json(
        &trace_dir,
        "trace-oversized-none",
        "records-0001.ndjson",
        "ndjson",
        "none",
        Some(32),
    )?;

    let store = TraceStore::new(data_dir.to_path_buf()).await?;
    let trace = store
        .get("trace-oversized-none")
        .await?
        .expect("trace should exist");
    assert_detail_status(&trace, "basic");
    assert_detail_reason(&trace, "chunk_error");
    assert_detail_array_empty(&trace, "records");
    assert_detail_array_empty(&trace, "nodes");
    assert_top_level_array_empty(&trace, "records");
    assert_finalize_absent(&trace);

    Ok(())
}

#[tokio::test]
async fn trace_store_downgrades_on_oversized_compressed_chunk() -> Result<()> {
    let temp = tempdir()?;
    let data_dir = temp.path();
    let trace_dir = data_dir.join("traces/2026/01/03/trace-compressed-oversize");
    fs::create_dir_all(&trace_dir)?;

    let oversized = TRACE_CHUNK_BYTES_COMPRESSED_OVERHEAD_MAX + 2;
    fs::write(
        trace_dir.join("records-0001.ndjson.zst"),
        vec![0u8; oversized],
    )?;

    write_records_inline_trace_json(
        &trace_dir,
        "trace-compressed-oversize",
        "records-0001.ndjson.zst",
        "ndjson",
        "zstd",
        Some(1),
    )?;

    let store = TraceStore::new(data_dir.to_path_buf()).await?;
    let trace = store
        .get("trace-compressed-oversize")
        .await?
        .expect("trace should exist");
    assert_detail_status(&trace, "basic");
    assert_detail_reason(&trace, "chunk_too_large");

    Ok(())
}

#[tokio::test]
async fn trace_store_downgrades_on_invalid_utf8_chunk() -> Result<()> {
    let temp = tempdir()?;
    let data_dir = temp.path();
    let trace_dir = data_dir.join("traces/2026/01/04/trace-invalid-utf8");
    fs::create_dir_all(&trace_dir)?;

    fs::write(
        trace_dir.join("records-0001.ndjson"),
        vec![0xff, 0xfe, 0xfd],
    )?;

    write_records_inline_trace_json(
        &trace_dir,
        "trace-invalid-utf8",
        "records-0001.ndjson",
        "ndjson",
        "none",
        None,
    )?;

    let store = TraceStore::new(data_dir.to_path_buf()).await?;
    let trace = store
        .get("trace-invalid-utf8")
        .await?
        .expect("trace should exist");
    assert_detail_status(&trace, "basic");
    assert_detail_reason(&trace, "chunk_error");

    Ok(())
}

#[tokio::test]
async fn trace_store_downgrades_on_invalid_ndjson() -> Result<()> {
    let temp = tempdir()?;
    let data_dir = temp.path();
    let trace_dir = data_dir.join("traces/2026/01/04/trace-invalid-ndjson");
    fs::create_dir_all(&trace_dir)?;

    fs::write(trace_dir.join("records-0001.ndjson"), b"not_json\n")?;

    write_records_inline_trace_json(
        &trace_dir,
        "trace-invalid-ndjson",
        "records-0001.ndjson",
        "ndjson",
        "none",
        None,
    )?;

    let store = TraceStore::new(data_dir.to_path_buf()).await?;
    let trace = store
        .get("trace-invalid-ndjson")
        .await?
        .expect("trace should exist");
    assert_detail_status(&trace, "basic");
    assert_detail_reason(&trace, "chunk_error");

    Ok(())
}

#[tokio::test]
async fn trace_store_downgrades_on_unsupported_compression() -> Result<()> {
    let temp = tempdir()?;
    let data_dir = temp.path();
    let trace_dir = data_dir.join("traces/2026/01/05/trace-unsupported-compression");
    fs::create_dir_all(&trace_dir)?;

    fs::write(trace_dir.join("records-0001.ndjson"), b"0\n")?;

    write_records_inline_trace_json(
        &trace_dir,
        "trace-unsupported-compression",
        "records-0001.ndjson",
        "ndjson",
        "gzip",
        None,
    )?;

    let store = TraceStore::new(data_dir.to_path_buf()).await?;
    let trace = store
        .get("trace-unsupported-compression")
        .await?
        .expect("trace should exist");
    assert_detail_status(&trace, "basic");
    assert_detail_reason(&trace, "chunk_error");

    Ok(())
}

#[tokio::test]
async fn trace_store_downgrades_on_unsupported_format() -> Result<()> {
    let temp = tempdir()?;
    let data_dir = temp.path();
    let trace_dir = data_dir.join("traces/2026/01/06/trace-unsupported-format");
    fs::create_dir_all(&trace_dir)?;

    fs::write(trace_dir.join("records-0001.json"), b"0\n")?;

    write_records_inline_trace_json(
        &trace_dir,
        "trace-unsupported-format",
        "records-0001.json",
        "json",
        "none",
        None,
    )?;

    let store = TraceStore::new(data_dir.to_path_buf()).await?;
    let trace = store
        .get("trace-unsupported-format")
        .await?
        .expect("trace should exist");
    assert_detail_status(&trace, "basic");
    assert_detail_reason(&trace, "chunk_error");

    Ok(())
}

#[tokio::test]
async fn trace_store_downgrades_detail_on_chunk_budget_exceeded() -> Result<()> {
    let temp = tempdir()?;
    let data_dir = temp.path();
    let trace_dir = data_dir.join("traces/2026/01/02/trace-chunk-budget");
    fs::create_dir_all(&trace_dir)?;

    let max_chunks = 128usize;
    let mut chunks = Vec::new();
    for index in 0..=max_chunks {
        let filename = format!("records-{index:04}.ndjson");
        fs::write(trace_dir.join(&filename), "{\"index\":0}\n")?;
        chunks.push(json!({
            "path": filename,
            "format": "ndjson",
            "compression": "none"
        }));
    }

    fs::write(
        trace_dir.join("trace.json"),
        serde_json::to_vec(&json!({
            "trace_schema_version": 1,
            "trace_id": "trace-chunk-budget",
            "status": "ok",
            "detail": {
                "layout": "records_inline",
                "status": "full",
                "records": chunks,
                "nodes": []
            }
        }))?,
    )?;

    let store = TraceStore::new(data_dir.to_path_buf()).await?;
    let trace = store
        .get("trace-chunk-budget")
        .await?
        .expect("trace should exist");
    assert_detail_status(&trace, "basic");
    assert_detail_reason(&trace, "chunk_error");
    assert_detail_reason(&trace, "budget_exceeded");
    assert_detail_array_empty(&trace, "records");
    assert_detail_array_empty(&trace, "nodes");
    assert_top_level_array_empty(&trace, "records");

    Ok(())
}
