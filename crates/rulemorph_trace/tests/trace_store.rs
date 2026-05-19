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

include!("trace_store/count_limits.rs");

include!("trace_store/chunk_downgrade.rs");
