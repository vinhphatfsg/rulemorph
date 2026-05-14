use std::fs;
use std::path::PathBuf;

use tracing::warn;

pub(super) struct TraceDirGuard {
    path: PathBuf,
    committed: bool,
}

impl TraceDirGuard {
    pub(super) fn new(path: PathBuf) -> Self {
        Self {
            path,
            committed: false,
        }
    }

    pub(super) fn commit(&mut self) {
        self.committed = true;
    }
}

impl Drop for TraceDirGuard {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        let _ = fs::remove_dir_all(&self.path);
    }
}

pub(super) fn cleanup_detail_files(
    record_files: &mut Vec<PathBuf>,
    node_files: &mut Vec<PathBuf>,
    finalize_file: &mut Option<PathBuf>,
    blob_files: &mut Vec<PathBuf>,
) {
    for path in record_files.iter() {
        if let Err(err) = fs::remove_file(path) {
            warn!(
                "failed to remove record chunk file {}: {}",
                path.display(),
                err
            );
        }
    }
    for path in node_files.iter() {
        if let Err(err) = fs::remove_file(path) {
            warn!(
                "failed to remove node chunk file {}: {}",
                path.display(),
                err
            );
        }
    }
    if let Some(path) = finalize_file.as_ref() {
        if let Err(err) = fs::remove_file(path) {
            warn!(
                "failed to remove finalize chunk file {}: {}",
                path.display(),
                err
            );
        }
    }
    for path in blob_files.iter() {
        if let Err(err) = fs::remove_file(path) {
            warn!("failed to remove blob file {}: {}", path.display(), err);
        }
    }
    record_files.clear();
    node_files.clear();
    *finalize_file = None;
    blob_files.clear();
}
