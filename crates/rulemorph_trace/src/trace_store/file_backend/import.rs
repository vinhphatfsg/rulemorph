use std::path::Path;

use anyhow::{Context, Result};
use tracing::warn;

use super::FileTraceBackend;
use crate::trace_store::ImportResult;
use crate::trace_store::import_bundle::{IMPORT_MAX_TOTAL_BYTES, import_bundle_files};

impl FileTraceBackend {
    pub async fn import_bundle(&self, bundle_path: &Path) -> Result<ImportResult> {
        self.import_bundle_inner(bundle_path, IMPORT_MAX_TOTAL_BYTES)
            .await
    }

    #[cfg(test)]
    pub async fn import_bundle_with_limit(
        &self,
        bundle_path: &Path,
        max_total_bytes: u64,
    ) -> Result<ImportResult> {
        self.import_bundle_inner(bundle_path, max_total_bytes).await
    }

    async fn import_bundle_inner(
        &self,
        bundle_path: &Path,
        max_total_bytes: u64,
    ) -> Result<ImportResult> {
        let bundle_path = bundle_path
            .canonicalize()
            .with_context(|| format!("failed to resolve bundle path: {}", bundle_path.display()))?;
        if !bundle_path.is_dir() {
            return Err(anyhow::anyhow!(
                "bundle path is not a directory: {}",
                bundle_path.display()
            ));
        }

        let data_dir = self.data_dir.clone();
        let result = tokio::task::spawn_blocking(move || {
            import_bundle_files(&data_dir, &bundle_path, max_total_bytes)
        })
        .await??;

        self.refresh_index().await?;

        let index = self.index.read().await;
        let mut trace_ids = Vec::new();
        for path in result.imported_paths {
            let path_string = path.display().to_string();
            if let Some(meta) = index.values().find(|meta| meta.path == path_string) {
                trace_ids.push(meta.trace_id.clone());
            } else {
                warn!(
                    "imported trace metadata not found in index: {}",
                    path.display()
                );
            }
        }

        Ok(ImportResult {
            imported: trace_ids.len(),
            trace_ids,
            rules_imported: result.rules_imported,
        })
    }
}
