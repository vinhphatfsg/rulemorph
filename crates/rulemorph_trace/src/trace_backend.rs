use std::path::Path;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;

use crate::trace_store::PurgeReport;
use crate::{ImportResult, TraceManifest, TraceMeta, TraceNodeChunkEntry};

#[async_trait]
pub trait TraceBackend: Send + Sync {
    async fn list(&self) -> Result<Vec<TraceMeta>>;
    async fn get(&self, trace_id: &str) -> Result<Option<Value>>;
    async fn get_manifest(&self, trace_id: &str) -> Result<Option<TraceManifest>>;
    async fn get_records_chunk(
        &self,
        trace_id: &str,
        chunk_index: usize,
    ) -> Result<Option<Vec<Value>>>;
    async fn get_nodes_chunk(
        &self,
        trace_id: &str,
        chunk_index: usize,
    ) -> Result<Option<Vec<TraceNodeChunkEntry>>>;
    async fn get_finalize_chunk(&self, trace_id: &str) -> Result<Option<Value>>;
    async fn import_bundle(&self, bundle_path: &Path) -> Result<ImportResult>;
    async fn purge_traces(&self, retention: Duration, dry_run: bool) -> Result<PurgeReport>;
    fn data_root(&self) -> &Path;
}

pub trait TraceWriteBackend: Send + Sync {
    fn write_trace_bundle(&self, trace: &Value, options: &crate::TraceWriteOptions) -> Result<()>;
}
