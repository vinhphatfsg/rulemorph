use super::*;

#[derive(Clone)]
pub struct TraceStore {
    backend: Arc<dyn TraceBackend>,
}

impl TraceStore {
    pub async fn new(data_dir: PathBuf) -> Result<Self> {
        let backend = FileTraceBackend::new(data_dir).await?;
        Ok(Self::with_backend(Arc::new(backend)))
    }

    pub fn with_backend(backend: Arc<dyn TraceBackend>) -> Self {
        Self { backend }
    }

    pub async fn list(&self) -> Result<Vec<TraceMeta>> {
        self.backend.list().await
    }

    pub async fn get(&self, trace_id: &str) -> Result<Option<Value>> {
        self.backend.get(trace_id).await
    }

    pub async fn get_manifest(&self, trace_id: &str) -> Result<Option<TraceManifest>> {
        self.backend.get_manifest(trace_id).await
    }

    pub async fn get_records_chunk(
        &self,
        trace_id: &str,
        chunk_index: usize,
    ) -> Result<Option<Vec<Value>>> {
        self.backend.get_records_chunk(trace_id, chunk_index).await
    }

    pub async fn get_nodes_chunk(
        &self,
        trace_id: &str,
        chunk_index: usize,
    ) -> Result<Option<Vec<TraceNodeChunkEntry>>> {
        self.backend.get_nodes_chunk(trace_id, chunk_index).await
    }

    pub async fn get_finalize_chunk(&self, trace_id: &str) -> Result<Option<Value>> {
        self.backend.get_finalize_chunk(trace_id).await
    }

    pub async fn import_bundle(&self, bundle_path: &Path) -> Result<ImportResult> {
        self.backend.import_bundle(bundle_path).await
    }

    pub async fn purge_traces(&self, retention: Duration, dry_run: bool) -> Result<PurgeReport> {
        self.backend.purge_traces(retention, dry_run).await
    }

    pub async fn seed_sample(&self) -> Result<()> {
        let _ = self.list().await?;
        Ok(())
    }

    pub fn data_dir(&self) -> &Path {
        self.backend.data_root()
    }
}
