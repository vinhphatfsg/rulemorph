use std::path::PathBuf;

use anyhow::Result;

pub(super) fn ensure_detail_files_exist(
    detail_status: &str,
    record_files: Vec<PathBuf>,
    node_files: Vec<PathBuf>,
    finalize_file: Option<PathBuf>,
) -> Result<()> {
    if detail_status != "full" {
        return Ok(());
    }

    for path in record_files {
        if !path.exists() {
            return Err(anyhow::anyhow!("record chunk missing: {}", path.display()));
        }
    }
    for path in node_files {
        if !path.exists() {
            return Err(anyhow::anyhow!("node chunk missing: {}", path.display()));
        }
    }
    if let Some(path) = finalize_file
        && !path.exists()
    {
        return Err(anyhow::anyhow!(
            "finalize chunk missing: {}",
            path.display()
        ));
    }

    Ok(())
}
