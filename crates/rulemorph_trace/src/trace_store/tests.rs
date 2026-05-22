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

include!("tests/manifest_budget.rs");
include!("tests/detail_budget.rs");

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
