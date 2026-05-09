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

#[tokio::test]
async fn trace_store_ignores_blobs_in_index() -> Result<()> {
    let temp = tempdir()?;
    let data_dir = temp.path();

    let trace_dir = data_dir.join("traces/2026/01/01/trace-main");
    fs::create_dir_all(&trace_dir)?;
    fs::write(
        trace_dir.join("trace.json"),
        serde_json::to_vec(&json!({
            "trace_schema_version": 1,
            "trace_id": "trace-main",
            "status": "ok",
            "timestamp": "2026-01-01T00:00:00Z"
        }))?,
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

    let trace_dir = data_dir.join("traces/2026/01/03/trace-large");
    fs::create_dir_all(&trace_dir)?;
    let padding_len = TRACE_JSON_MAX_BYTES as usize;
    let payload = format!(
        "{{\"trace_id\":\"trace-large\",\"status\":\"ok\",\"records\":[],\"padding\":\"{}\"}}",
        "x".repeat(padding_len)
    );
    fs::write(trace_dir.join("trace.json"), payload)?;

    let small_dir = data_dir.join("traces/2026/01/03/trace-small");
    fs::create_dir_all(&small_dir)?;
    fs::write(
        small_dir.join("trace.json"),
        serde_json::to_vec(&json!({
            "trace_schema_version": 1,
            "trace_id": "trace-small",
            "status": "ok",
            "timestamp": "2026-01-03T00:00:00Z"
        }))?,
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
    let detail = trace
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
            .any(|value| value.as_str() == Some("budget_exceeded"))
    );
    let detail_records = detail
        .get("records")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(detail_records.is_empty());

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
    let detail = trace
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
            .any(|value| value.as_str() == Some("budget_exceeded"))
    );

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
    let detail = trace
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
            .any(|value| value.as_str() == Some("budget_exceeded"))
    );

    Ok(())
}

#[tokio::test]
async fn trace_store_downgrades_detail_on_missing_chunk() -> Result<()> {
    let temp = tempdir()?;
    let data_dir = temp.path();
    let trace_dir = data_dir.join("traces/2026/01/02/trace-missing");
    fs::create_dir_all(&trace_dir)?;

    fs::write(
        trace_dir.join("trace.json"),
        serde_json::to_vec(&json!({
            "trace_schema_version": 1,
            "trace_id": "trace-missing",
            "status": "ok",
            "detail": {
                "layout": "records_inline",
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
        .get("trace-missing")
        .await?
        .expect("trace should exist");
    let detail = trace
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
    let records = trace
        .get("records")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(records.is_empty());
    assert!(trace.get("finalize").is_none());

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

    fs::write(
        trace_dir.join("trace.json"),
        serde_json::to_vec(&json!({
            "trace_schema_version": 1,
            "trace_id": "trace-compressed-overhead",
            "status": "ok",
            "max_chunk_bytes_uncompressed": max_chunk_bytes_uncompressed,
            "detail": {
                "layout": "records_inline",
                "status": "full",
                "records": [
                    {
                        "path": "records-0001.ndjson.zst",
                        "format": "ndjson",
                        "compression": "zstd"
                    }
                ],
                "nodes": []
            }
        }))?,
    )?;

    let store = TraceStore::new(data_dir.to_path_buf()).await?;
    let trace = store
        .get("trace-compressed-overhead")
        .await?
        .expect("trace should exist");
    let detail = trace
        .get("detail")
        .and_then(|value| value.as_object())
        .expect("detail object");
    let detail_status = detail.get("status").and_then(|value| value.as_str());
    assert_eq!(detail_status, Some("full"));
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

    fs::write(
        trace_dir.join("trace.json"),
        serde_json::to_vec(&json!({
            "trace_schema_version": 1,
            "trace_id": "trace-oversized-none",
            "status": "ok",
            "max_chunk_bytes_uncompressed": 32,
            "detail": {
                "layout": "records_inline",
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
        .get("trace-oversized-none")
        .await?
        .expect("trace should exist");
    let detail = trace
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
    let records = trace
        .get("records")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(records.is_empty());
    assert!(trace.get("finalize").is_none());

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

    fs::write(
        trace_dir.join("trace.json"),
        serde_json::to_vec(&json!({
            "trace_schema_version": 1,
            "trace_id": "trace-compressed-oversize",
            "status": "ok",
            "max_chunk_bytes_uncompressed": 1,
            "detail": {
                "layout": "records_inline",
                "status": "full",
                "records": [
                    {
                        "path": "records-0001.ndjson.zst",
                        "format": "ndjson",
                        "compression": "zstd"
                    }
                ],
                "nodes": []
            }
        }))?,
    )?;

    let store = TraceStore::new(data_dir.to_path_buf()).await?;
    let trace = store
        .get("trace-compressed-oversize")
        .await?
        .expect("trace should exist");
    let detail = trace
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
            .any(|value| value.as_str() == Some("chunk_too_large"))
    );

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

    fs::write(
        trace_dir.join("trace.json"),
        serde_json::to_vec(&json!({
            "trace_schema_version": 1,
            "trace_id": "trace-invalid-utf8",
            "status": "ok",
            "detail": {
                "layout": "records_inline",
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
        .get("trace-invalid-utf8")
        .await?
        .expect("trace should exist");
    let detail = trace
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

    Ok(())
}

#[tokio::test]
async fn trace_store_downgrades_on_invalid_ndjson() -> Result<()> {
    let temp = tempdir()?;
    let data_dir = temp.path();
    let trace_dir = data_dir.join("traces/2026/01/04/trace-invalid-ndjson");
    fs::create_dir_all(&trace_dir)?;

    fs::write(trace_dir.join("records-0001.ndjson"), b"not_json\n")?;

    fs::write(
        trace_dir.join("trace.json"),
        serde_json::to_vec(&json!({
            "trace_schema_version": 1,
            "trace_id": "trace-invalid-ndjson",
            "status": "ok",
            "detail": {
                "layout": "records_inline",
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
        .get("trace-invalid-ndjson")
        .await?
        .expect("trace should exist");
    let detail = trace
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

    Ok(())
}

#[tokio::test]
async fn trace_store_downgrades_on_unsupported_compression() -> Result<()> {
    let temp = tempdir()?;
    let data_dir = temp.path();
    let trace_dir = data_dir.join("traces/2026/01/05/trace-unsupported-compression");
    fs::create_dir_all(&trace_dir)?;

    fs::write(trace_dir.join("records-0001.ndjson"), b"0\n")?;

    fs::write(
        trace_dir.join("trace.json"),
        serde_json::to_vec(&json!({
            "trace_schema_version": 1,
            "trace_id": "trace-unsupported-compression",
            "status": "ok",
            "detail": {
                "layout": "records_inline",
                "status": "full",
                "records": [
                    {
                        "path": "records-0001.ndjson",
                        "format": "ndjson",
                        "compression": "gzip"
                    }
                ],
                "nodes": []
            }
        }))?,
    )?;

    let store = TraceStore::new(data_dir.to_path_buf()).await?;
    let trace = store
        .get("trace-unsupported-compression")
        .await?
        .expect("trace should exist");
    let detail = trace
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

    Ok(())
}

#[tokio::test]
async fn trace_store_downgrades_on_unsupported_format() -> Result<()> {
    let temp = tempdir()?;
    let data_dir = temp.path();
    let trace_dir = data_dir.join("traces/2026/01/06/trace-unsupported-format");
    fs::create_dir_all(&trace_dir)?;

    fs::write(trace_dir.join("records-0001.json"), b"0\n")?;

    fs::write(
        trace_dir.join("trace.json"),
        serde_json::to_vec(&json!({
            "trace_schema_version": 1,
            "trace_id": "trace-unsupported-format",
            "status": "ok",
            "detail": {
                "layout": "records_inline",
                "status": "full",
                "records": [
                    {
                        "path": "records-0001.json",
                        "format": "json",
                        "compression": "none"
                    }
                ],
                "nodes": []
            }
        }))?,
    )?;

    let store = TraceStore::new(data_dir.to_path_buf()).await?;
    let trace = store
        .get("trace-unsupported-format")
        .await?
        .expect("trace should exist");
    let detail = trace
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
    let detail = trace
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
    assert!(
        reasons
            .iter()
            .any(|value| value.as_str() == Some("budget_exceeded"))
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
    let records = trace
        .get("records")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(records.is_empty());

    Ok(())
}

#[tokio::test]
async fn import_bundle_ignores_blobs_for_metadata() -> Result<()> {
    let temp = tempdir()?;
    let data_dir = temp.path().join("data");
    let bundle_dir = temp.path().join("bundle");
    let bundle_traces = bundle_dir.join("traces/2026/01/03/trace-bundle");
    fs::create_dir_all(&bundle_traces)?;

    fs::write(
        bundle_traces.join("trace.json"),
        serde_json::to_vec(&json!({
            "trace_schema_version": 1,
            "trace_id": "trace-bundle",
            "status": "ok",
            "timestamp": "2026-01-03T00:00:00Z"
        }))?,
    )?;

    let blob_dir = bundle_traces.join("blobs");
    fs::create_dir_all(&blob_dir)?;
    fs::write(
        blob_dir.join("sha256-cafe.json"),
        serde_json::to_vec(&json!({
            "trace_id": "trace-bundle-blob",
            "status": "ok",
            "records": [],
            "summary": { "record_total": 0 }
        }))?,
    )?;

    let store = TraceStore::new(data_dir.clone()).await?;
    let result = store.import_bundle(&bundle_dir).await?;

    assert_eq!(result.imported, 1);
    assert_eq!(result.trace_ids, vec!["trace-bundle".to_string()]);

    Ok(())
}

#[tokio::test]
async fn import_bundle_returns_index_ids_for_sanitized_trace_id() -> Result<()> {
    let temp = tempdir()?;
    let data_dir = temp.path().join("data");
    let bundle_dir = temp.path().join("bundle");
    let bundle_traces = bundle_dir.join("traces/2026/01/05/trace-blank");
    fs::create_dir_all(&bundle_traces)?;

    fs::write(
        bundle_traces.join("trace.json"),
        serde_json::to_vec(&json!({
            "trace_schema_version": 1,
            "trace_id": "",
            "status": "ok",
            "timestamp": "2026-01-05T00:00:00Z"
        }))?,
    )?;

    let store = TraceStore::new(data_dir.clone()).await?;
    let result = store.import_bundle(&bundle_dir).await?;

    assert_eq!(result.imported, 1);
    assert_eq!(result.trace_ids.len(), 1);
    let trace = store.get(&result.trace_ids[0]).await?;
    assert!(trace.is_some());

    Ok(())
}

#[tokio::test]
async fn import_bundle_rejects_overwrite() -> Result<()> {
    let temp = tempdir()?;
    let data_dir = temp.path().join("data");
    let existing_dir = data_dir.join("traces/2026/01/06/trace-existing");
    fs::create_dir_all(&existing_dir)?;
    fs::write(
        existing_dir.join("trace.json"),
        serde_json::to_vec(&json!({
            "trace_schema_version": 1,
            "trace_id": "trace-existing",
            "status": "ok"
        }))?,
    )?;

    let bundle_dir = temp.path().join("bundle");
    let bundle_traces = bundle_dir.join("traces/2026/01/06/trace-existing");
    fs::create_dir_all(&bundle_traces)?;
    fs::write(
        bundle_traces.join("trace.json"),
        serde_json::to_vec(&json!({
            "trace_schema_version": 1,
            "trace_id": "trace-overwrite",
            "status": "ok"
        }))?,
    )?;

    let store = TraceStore::new(data_dir.clone()).await?;
    let err = store
        .import_bundle(&bundle_dir)
        .await
        .expect_err("overwrite should be rejected");
    assert!(err.to_string().contains("overwrite"));

    Ok(())
}

#[tokio::test]
async fn import_bundle_rolls_back_on_rule_conflict() -> Result<()> {
    let temp = tempdir()?;
    let data_dir = temp.path().join("data");
    let bundle_dir = temp.path().join("bundle");

    let conflict_path = data_dir.join("rules/conflict.yaml");
    fs::create_dir_all(conflict_path.parent().expect("rules dir"))?;
    fs::write(&conflict_path, "existing: true")?;

    let bundle_traces = bundle_dir.join("traces/2026/01/08/trace-rollback");
    fs::create_dir_all(&bundle_traces)?;
    fs::write(
        bundle_traces.join("trace.json"),
        serde_json::to_vec(&json!({
            "trace_schema_version": 1,
            "trace_id": "trace-rollback",
            "status": "ok",
            "timestamp": "2026-01-08T00:00:00Z"
        }))?,
    )?;
    let bundle_rules = bundle_dir.join("rules");
    fs::create_dir_all(&bundle_rules)?;
    fs::write(bundle_rules.join("conflict.yaml"), "bundle: true")?;

    let store = TraceStore::new(data_dir.clone()).await?;
    let err = store
        .import_bundle(&bundle_dir)
        .await
        .expect_err("rule conflict should fail");
    assert!(err.to_string().contains("overwrite"));

    let imported_trace = data_dir.join("traces/2026/01/08/trace-rollback/trace.json");
    assert!(
        !imported_trace.exists(),
        "trace should be rolled back on import failure"
    );

    Ok(())
}

#[tokio::test]
async fn import_bundle_rejects_oversized_file() -> Result<()> {
    let temp = tempdir()?;
    let data_dir = temp.path().join("data");
    let bundle_dir = temp.path().join("bundle");
    let bundle_traces = bundle_dir.join("traces/2026/01/09/trace-oversized");
    fs::create_dir_all(&bundle_traces)?;

    fs::write(
        bundle_traces.join("trace.json"),
        serde_json::to_vec(&json!({
            "trace_schema_version": 1,
            "trace_id": "trace-oversized",
            "status": "ok",
            "timestamp": "2026-01-09T00:00:00Z"
        }))?,
    )?;

    let oversized_bytes = (20 * 1024 * 1024) + 1;
    let payload = vec![b'x'; oversized_bytes];
    fs::write(bundle_traces.join("records-0001.ndjson"), payload)?;

    let store = TraceStore::new(data_dir).await?;
    let err = store
        .import_bundle(&bundle_dir)
        .await
        .expect_err("oversized file should be rejected");
    assert!(err.to_string().contains("max bytes"));

    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn import_bundle_rejects_symlinks() -> Result<()> {
    let temp = tempdir()?;
    let data_dir = temp.path().join("data");
    let bundle_dir = temp.path().join("bundle");
    let traces_dir = bundle_dir.join("traces/2026/01/04");
    fs::create_dir_all(&traces_dir)?;

    let target = bundle_dir.join("payload.json");
    fs::write(&target, r#"{"trace_id":"trace-symlink"}"#)?;
    symlink(&target, traces_dir.join("link.json"))?;

    let store = TraceStore::new(data_dir).await?;
    let err = store
        .import_bundle(&bundle_dir)
        .await
        .expect_err("symlink should be rejected");
    assert!(err.to_string().contains("symlink"));

    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn import_bundle_rejects_destination_symlink() -> Result<()> {
    let temp = tempdir()?;
    let data_dir = temp.path().join("data");
    let bundle_dir = temp.path().join("bundle");
    let bundle_traces = bundle_dir.join("traces/2026/01/07/trace-escape");
    fs::create_dir_all(&bundle_traces)?;

    fs::write(
        bundle_traces.join("trace.json"),
        serde_json::to_vec(&json!({
            "trace_schema_version": 1,
            "trace_id": "trace-escape",
            "status": "ok",
            "timestamp": "2026-01-07T00:00:00Z"
        }))?,
    )?;

    let traces_dir = data_dir.join("traces");
    fs::create_dir_all(&traces_dir)?;
    let outside = temp.path().join("outside");
    fs::create_dir_all(&outside)?;
    symlink(&outside, traces_dir.join("2026"))?;

    let store = TraceStore::new(data_dir).await?;
    let err = store
        .import_bundle(&bundle_dir)
        .await
        .expect_err("destination symlink should be rejected");
    assert!(err.to_string().contains("symlink"));

    Ok(())
}
