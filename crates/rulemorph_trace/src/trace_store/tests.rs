use super::{
    ChunkBudget, FileTraceBackend, PurgeReport, Result, TraceManifest, TraceMeta, TraceStore,
    apply_legacy_limits_with_thresholds, build_trace_from_manifest_with_budget,
    fallback_trace_id_for_path, path_hash_for_trace_id, resolve_chunk_path,
    resolve_trace_timestamp,
};
use crate::trace_id::sanitize_trace_id;
use crate::trace_schema::{
    TRACE_CHUNK_BYTES_UNCOMPRESSED_HARD_MAX, TRACE_CHUNK_COUNT_HARD_MAX, TRACE_NODE_COUNT_HARD_MAX,
    TRACE_RECORD_COUNT_HARD_MAX, TRACE_TOTAL_CHUNK_BYTES_UNCOMPRESSED_HARD_MAX, TraceChunkRef,
};
use crate::{ImportResult, TraceBackend, TraceDetailRef, TraceNodeChunkEntry};
use chrono::{Duration as ChronoDuration, SecondsFormat, Utc};
use serde_json::{Value, json};
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;

struct DummyBackend {
    data_dir: std::path::PathBuf,
    list: Vec<TraceMeta>,
}

#[async_trait::async_trait]
impl TraceBackend for DummyBackend {
    async fn list(&self) -> Result<Vec<TraceMeta>> {
        Ok(self.list.clone())
    }

    async fn get(&self, _trace_id: &str) -> Result<Option<Value>> {
        Ok(None)
    }

    async fn get_manifest(&self, _trace_id: &str) -> Result<Option<TraceManifest>> {
        Ok(None)
    }

    async fn get_records_chunk(
        &self,
        _trace_id: &str,
        _chunk_index: usize,
    ) -> Result<Option<Vec<Value>>> {
        Ok(None)
    }

    async fn get_nodes_chunk(
        &self,
        _trace_id: &str,
        _chunk_index: usize,
    ) -> Result<Option<Vec<TraceNodeChunkEntry>>> {
        Ok(None)
    }

    async fn get_finalize_chunk(&self, _trace_id: &str) -> Result<Option<Value>> {
        Ok(None)
    }

    async fn import_bundle(&self, _bundle_path: &Path) -> Result<ImportResult> {
        Ok(ImportResult {
            imported: 0,
            trace_ids: Vec::new(),
            rules_imported: 0,
        })
    }

    async fn purge_traces(&self, _retention: Duration, _dry_run: bool) -> Result<PurgeReport> {
        Ok(PurgeReport {
            purged: Vec::new(),
            failed: Vec::new(),
        })
    }

    fn data_root(&self) -> &Path {
        &self.data_dir
    }
}

include!("tests/path_safety.rs");

#[tokio::test]
async fn trace_store_with_backend_uses_backend_list() -> Result<()> {
    let temp = tempdir()?;
    let data_dir = temp.path().to_path_buf();
    let meta = TraceMeta {
        trace_id: "trace-001".to_string(),
        status: "ok".to_string(),
        timestamp: None,
        duration_us: None,
        rule: None,
        summary: None,
        path: "traces/2026/02/03/trace-001/trace.json".to_string(),
    };
    let backend = DummyBackend {
        data_dir: data_dir.clone(),
        list: vec![meta.clone()],
    };
    let store = TraceStore::with_backend(Arc::new(backend));
    let traces = store.list().await?;
    assert_eq!(traces.len(), 1);
    assert_eq!(traces[0].trace_id, "trace-001");
    assert_eq!(store.data_dir(), data_dir.as_path());
    Ok(())
}

#[tokio::test]
async fn manifest_downgrades_when_chunk_budget_exceeded() -> Result<()> {
    let temp = tempdir()?;
    let trace_dir = temp.path().join("traces/2026/02/03/trace-over-budget");
    fs::create_dir_all(&trace_dir)?;

    let mut records = Vec::new();
    for index in 0..(TRACE_CHUNK_COUNT_HARD_MAX + 1) {
        records.push(json!({
            "path": format!("records-{index:04}.ndjson"),
            "format": "ndjson",
            "compression": "none",
            "record_start": 0,
            "record_end": 0
        }));
    }

    fs::write(
        trace_dir.join("trace.json"),
        serde_json::to_vec(&json!({
            "trace_schema_version": 1,
            "trace_id": "trace-over-budget",
            "status": "ok",
            "detail": {
                "layout": "records_inline",
                "status": "full",
                "reason": [],
                "records": records,
                "nodes": []
            }
        }))?,
    )?;

    let store = TraceStore::new(temp.path().to_path_buf()).await?;
    let manifest = store
        .get_manifest("trace-over-budget")
        .await?
        .expect("manifest");
    let detail = manifest.detail.expect("detail");
    assert_eq!(detail.status, "basic");
    assert!(
        detail
            .reason
            .iter()
            .any(|reason| reason == "budget_exceeded")
    );
    assert!(detail.records.is_empty());

    Ok(())
}

#[tokio::test]
async fn manifest_downgrades_when_total_bytes_exceeded() -> Result<()> {
    let temp = tempdir()?;
    let trace_dir = temp.path().join("traces/2026/02/03/trace-bytes-budget");
    fs::create_dir_all(&trace_dir)?;

    fs::write(
        trace_dir.join("trace.json"),
        serde_json::to_vec(&json!({
            "trace_schema_version": 1,
            "trace_id": "trace-bytes-budget",
            "status": "ok",
            "detail": {
                "layout": "records_inline",
                "status": "full",
                "reason": [],
                "records": [
                    {
                        "path": "records-0001.ndjson",
                        "format": "ndjson",
                        "compression": "none",
                        "record_start": 0,
                        "record_end": 0,
                        "bytes": TRACE_TOTAL_CHUNK_BYTES_UNCOMPRESSED_HARD_MAX as u64 + 1
                    }
                ],
                "nodes": []
            }
        }))?,
    )?;

    let store = TraceStore::new(temp.path().to_path_buf()).await?;
    let manifest = store
        .get_manifest("trace-bytes-budget")
        .await?
        .expect("manifest");
    let detail = manifest.detail.expect("detail");
    assert_eq!(detail.status, "basic");
    assert!(
        detail
            .reason
            .iter()
            .any(|reason| reason == "budget_exceeded")
    );

    Ok(())
}

#[tokio::test]
async fn purge_traces_removes_old_entries() -> Result<()> {
    let temp = tempdir()?;
    let data_dir = temp.path().to_path_buf();
    let old_dir = data_dir.join("traces/2026/01/01/trace-old");
    let new_dir = data_dir.join("traces/2026/01/01/trace-new");
    fs::create_dir_all(&old_dir)?;
    fs::create_dir_all(&new_dir)?;

    let old_ts =
        (Utc::now() - chrono::Duration::days(20)).to_rfc3339_opts(SecondsFormat::Secs, true);
    let new_ts =
        (Utc::now() - chrono::Duration::days(2)).to_rfc3339_opts(SecondsFormat::Secs, true);

    fs::write(
        old_dir.join("trace.json"),
        serde_json::to_vec(&json!({
            "trace_schema_version": 1,
            "trace_id": "trace-old",
            "timestamp": old_ts,
            "status": "ok"
        }))?,
    )?;
    fs::write(
        new_dir.join("trace.json"),
        serde_json::to_vec(&json!({
            "trace_schema_version": 1,
            "trace_id": "trace-new",
            "timestamp": new_ts,
            "status": "ok"
        }))?,
    )?;

    let store = TraceStore::new(data_dir.clone()).await?;
    let dry_run = store
        .purge_traces(Duration::from_secs(10 * 86_400), true)
        .await?;
    assert!(
        dry_run
            .purged
            .iter()
            .any(|meta| meta.trace_id == "trace-old")
    );
    assert!(
        !dry_run
            .purged
            .iter()
            .any(|meta| meta.trace_id == "trace-new")
    );
    assert!(dry_run.failed.is_empty());
    assert!(old_dir.exists());

    let purged = store
        .purge_traces(Duration::from_secs(10 * 86_400), false)
        .await?;
    assert!(
        purged
            .purged
            .iter()
            .any(|meta| meta.trace_id == "trace-old")
    );
    assert!(purged.failed.is_empty());
    assert!(!old_dir.exists());
    assert!(new_dir.exists());

    Ok(())
}

#[tokio::test]
async fn resolve_trace_timestamp_ignores_far_future() -> Result<()> {
    let temp = tempdir()?;
    let trace_dir = temp.path().join("traces/2026/01/01/trace-future");
    fs::create_dir_all(&trace_dir)?;
    let future_dt = Utc::now() + ChronoDuration::days(365);
    let future_ts = future_dt.to_rfc3339_opts(SecondsFormat::Secs, true);
    let trace_path = trace_dir.join("trace.json");
    fs::write(
        &trace_path,
        serde_json::to_vec(&json!({
            "trace_schema_version": 1,
            "trace_id": "trace-future",
            "timestamp": future_ts,
            "status": "ok"
        }))?,
    )?;

    let meta = TraceMeta {
        trace_id: "trace-future".to_string(),
        status: "ok".to_string(),
        timestamp: Some(future_ts),
        duration_us: None,
        rule: None,
        summary: None,
        path: trace_path.to_string_lossy().to_string(),
    };

    let resolved = resolve_trace_timestamp(&meta, &trace_path)
        .await
        .expect("timestamp");
    assert!(resolved < std::time::SystemTime::from(future_dt));
    Ok(())
}

#[tokio::test]
async fn manifest_downgrades_when_unknown_uncompressed_bytes_exceed_budget() -> Result<()> {
    let temp = tempdir()?;
    let trace_dir = temp.path().join("traces/2026/02/03/trace-unknown-bytes");
    fs::create_dir_all(&trace_dir)?;

    let unknown_chunks =
        TRACE_TOTAL_CHUNK_BYTES_UNCOMPRESSED_HARD_MAX / TRACE_CHUNK_BYTES_UNCOMPRESSED_HARD_MAX + 1;
    let records: Vec<Value> = (0..unknown_chunks)
        .map(|index| {
            json!({
                "path": format!("records-{index:04}.ndjson.zst"),
                "format": "ndjson",
                "compression": "zstd",
                "record_start": index as u64,
                "record_end": index as u64,
                "bytes": 1024
            })
        })
        .collect();

    fs::write(
        trace_dir.join("trace.json"),
        serde_json::to_vec(&json!({
            "trace_schema_version": 1,
            "trace_id": "trace-unknown-bytes",
            "status": "ok",
            "max_chunk_bytes_uncompressed": TRACE_CHUNK_BYTES_UNCOMPRESSED_HARD_MAX as u64,
            "detail": {
                "layout": "records_inline",
                "status": "full",
                "reason": [],
                "records": records,
                "nodes": []
            }
        }))?,
    )?;

    let store = TraceStore::new(temp.path().to_path_buf()).await?;
    let manifest = store
        .get_manifest("trace-unknown-bytes")
        .await?
        .expect("manifest");
    let detail = manifest.detail.expect("detail");
    assert_eq!(detail.status, "basic");
    assert!(
        detail
            .reason
            .iter()
            .any(|reason| reason == "budget_exceeded")
    );

    Ok(())
}

#[tokio::test]
async fn manifest_downgrades_when_record_total_exceeded() -> Result<()> {
    let temp = tempdir()?;
    let trace_dir = temp.path().join("traces/2026/02/03/trace-record-budget");
    fs::create_dir_all(&trace_dir)?;

    fs::write(
        trace_dir.join("trace.json"),
        serde_json::to_vec(&json!({
            "trace_schema_version": 1,
            "trace_id": "trace-record-budget",
            "status": "ok",
            "summary": {
                "record_total": TRACE_RECORD_COUNT_HARD_MAX as u64 + 1
            },
            "detail": {
                "layout": "records_inline",
                "status": "full",
                "reason": [],
                "records": [],
                "nodes": []
            }
        }))?,
    )?;

    let store = TraceStore::new(temp.path().to_path_buf()).await?;
    let manifest = store
        .get_manifest("trace-record-budget")
        .await?
        .expect("manifest");
    let detail = manifest.detail.expect("detail");
    assert_eq!(detail.status, "basic");
    assert!(
        detail
            .reason
            .iter()
            .any(|reason| reason == "budget_exceeded")
    );

    let trace = store.get("trace-record-budget").await?.expect("trace");
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
async fn manifest_downgrades_when_node_total_exceeded() -> Result<()> {
    let temp = tempdir()?;
    let trace_dir = temp.path().join("traces/2026/02/03/trace-node-budget");
    fs::create_dir_all(&trace_dir)?;

    fs::write(
        trace_dir.join("trace.json"),
        serde_json::to_vec(&json!({
            "trace_schema_version": 1,
            "trace_id": "trace-node-budget",
            "status": "ok",
            "detail": {
                "layout": "records_nodes_split",
                "status": "full",
                "reason": [],
                "records": [],
                "nodes": [
                    {
                        "path": "nodes-0001.ndjson",
                        "format": "ndjson",
                        "compression": "none",
                        "node_start": 0,
                        "node_end": TRACE_NODE_COUNT_HARD_MAX as u64
                    }
                ]
            }
        }))?,
    )?;

    let store = TraceStore::new(temp.path().to_path_buf()).await?;
    let manifest = store
        .get_manifest("trace-node-budget")
        .await?
        .expect("manifest");
    let detail = manifest.detail.expect("detail");
    assert_eq!(detail.status, "basic");
    assert!(
        detail
            .reason
            .iter()
            .any(|reason| reason == "budget_exceeded")
    );

    Ok(())
}

#[test]
fn legacy_trace_downgrades_on_record_limit() -> Result<()> {
    let mut legacy = json!({
        "trace_id": "legacy-over-limit",
        "records": [
            { "index": 0, "status": "ok" },
            { "index": 1, "status": "ok" }
        ]
    });

    apply_legacy_limits_with_thresholds(&mut legacy, 1, 10);

    let detail = legacy
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
    let records = legacy
        .get("records")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(records.is_empty());

    Ok(())
}

#[tokio::test]
async fn trace_store_downgrades_detail_on_oversized_chunk() -> Result<()> {
    let temp = tempdir()?;
    let data_dir = temp.path();
    let trace_dir = data_dir.join("traces/2026/01/07/trace-oversized");
    fs::create_dir_all(&trace_dir)?;

    let oversized = TRACE_CHUNK_BYTES_UNCOMPRESSED_HARD_MAX + 1024;
    let payload = format!(
        "{{\"index\":0,\"payload\":\"{}\"}}\n",
        "x".repeat(oversized)
    );
    let compressed = zstd::stream::encode_all(payload.as_bytes(), 3)?;
    fs::write(trace_dir.join("records-0001.ndjson.zst"), compressed)?;

    fs::write(
        trace_dir.join("trace.json"),
        serde_json::to_vec(&json!({
            "trace_schema_version": 1,
            "trace_id": "trace-oversized",
            "status": "ok",
            "detail": {
                "layout": "records_inline",
                "status": "full",
                "records": [
                    {
                        "path": "records-0001.ndjson.zst",
                        "format": "ndjson",
                        "compression": "zstd",
                        "bytes": 1024,
                        "record_start": 0,
                        "record_end": 0
                    }
                ],
                "nodes": []
            }
        }))?,
    )?;

    let store = TraceStore::new(data_dir.to_path_buf()).await?;
    let trace = store
        .get("trace-oversized")
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
            .any(|value| value.as_str() == Some("chunk_too_large"))
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

#[test]
fn trace_store_downgrades_on_total_bytes_budget() -> Result<()> {
    let temp = tempdir()?;
    let trace_dir = temp.path().join("trace-budget");
    fs::create_dir_all(&trace_dir)?;

    let payload = format!("{{\"index\":0,\"payload\":\"{}\"}}\n", "x".repeat(64));
    let payload_len = payload.as_bytes().len();
    fs::write(trace_dir.join("records-0001.ndjson"), payload.as_bytes())?;
    fs::write(trace_dir.join("records-0002.ndjson"), payload.as_bytes())?;

    let max_chunk_bytes = payload_len + 8;
    let manifest = TraceManifest {
        trace_schema_version: 1,
        trace_id: "trace-budget".to_string(),
        timestamp: None,
        status: Some("ok".to_string()),
        rule: None,
        input_format: None,
        summary: None,
        max_chunk_bytes_uncompressed: Some(max_chunk_bytes as u64),
        detail: Some(TraceDetailRef {
            layout: "records_inline".to_string(),
            status: "full".to_string(),
            reason: Vec::new(),
            records: vec![
                TraceChunkRef {
                    path: "records-0001.ndjson".to_string(),
                    format: "ndjson".to_string(),
                    compression: "none".to_string(),
                    record_start: None,
                    record_end: None,
                    node_start: None,
                    node_end: None,
                    bytes: None,
                    bytes_uncompressed: None,
                },
                TraceChunkRef {
                    path: "records-0002.ndjson".to_string(),
                    format: "ndjson".to_string(),
                    compression: "none".to_string(),
                    record_start: None,
                    record_end: None,
                    node_start: None,
                    node_end: None,
                    bytes: None,
                    bytes_uncompressed: None,
                },
            ],
            nodes: Vec::new(),
            finalize: None,
        }),
        masking: None,
        rule_source: None,
    };

    let budget = ChunkBudget {
        remaining_bytes: payload_len + 1,
        remaining_chunks: 10,
    };
    let trace = build_trace_from_manifest_with_budget(&manifest, &trace_dir, budget)?;
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
async fn import_bundle_rejects_total_bytes_limit() -> Result<()> {
    let temp = tempdir()?;
    let data_dir = temp.path().join("data");
    let bundle_dir = temp.path().join("bundle");
    let bundle_traces = bundle_dir.join("traces/2026/01/10/trace-total-limit");
    fs::create_dir_all(&bundle_traces)?;

    let trace_payload = serde_json::to_vec(&json!({
        "trace_schema_version": 1,
        "trace_id": "trace-total-limit",
        "status": "ok",
        "timestamp": "2026-01-10T00:00:00Z"
    }))?;
    fs::write(bundle_traces.join("trace.json"), &trace_payload)?;
    let records_payload = vec![b'x'; 64];
    fs::write(bundle_traces.join("records-0001.ndjson"), &records_payload)?;

    let max_total_bytes =
        (trace_payload.len().saturating_add(records_payload.len()) as u64).saturating_sub(1);
    let store = FileTraceBackend::new(data_dir).await?;
    let err = store
        .import_bundle_with_limit(&bundle_dir, max_total_bytes)
        .await
        .expect_err("total bytes should be rejected");
    assert!(err.to_string().contains("max total bytes"));

    Ok(())
}
