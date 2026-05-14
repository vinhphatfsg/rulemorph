use std::cmp::Reverse;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use anyhow::Result;
use tracing::warn;
use walkdir::WalkDir;

use super::import_path::{
    copy_file_create_new, ensure_import_base_dir, ensure_import_target_parent,
};
use super::{is_trace_meta_candidate, parse_trace_meta, rules_dir, traces_dir};

const IMPORT_MAX_FILE_BYTES: u64 = 20 * 1024 * 1024;
pub(super) const IMPORT_MAX_TOTAL_BYTES: u64 = 256 * 1024 * 1024;

pub(super) struct ImportWorkResult {
    pub(super) imported_paths: Vec<PathBuf>,
    pub(super) rules_imported: usize,
}

pub(super) fn import_bundle_files(
    data_dir: &Path,
    bundle_path: &Path,
    max_total_bytes: u64,
) -> Result<ImportWorkResult> {
    let traces_src = bundle_path.join("traces");
    let rules_src = bundle_path.join("rules");

    let mut copied_paths = Vec::new();
    let mut created_dirs = BTreeSet::new();

    let work = (|| -> Result<ImportWorkResult> {
        let mut imported_paths = Vec::new();
        let mut total_bytes: u64 = 0;
        if traces_src.exists() {
            let dest = traces_dir(data_dir);
            let dest_canon = ensure_import_base_dir(&dest)?;
            let mut entries: Vec<(PathBuf, PathBuf)> = Vec::new();
            for entry in WalkDir::new(&traces_src)
                .follow_links(false)
                .into_iter()
                .filter_map(|e| e.ok())
            {
                if entry.file_type().is_symlink() {
                    return Err(anyhow::anyhow!(
                        "bundle contains symlink: {}",
                        entry.path().display()
                    ));
                }
                if entry.file_type().is_file() {
                    let rel = entry.path().strip_prefix(&traces_src).unwrap();
                    let target = dest.join(rel);
                    let file_bytes = entry.metadata().map(|meta| meta.len())?;
                    if file_bytes > IMPORT_MAX_FILE_BYTES {
                        return Err(anyhow::anyhow!(
                            "bundle file exceeds max bytes: {} > {} ({})",
                            file_bytes,
                            IMPORT_MAX_FILE_BYTES,
                            entry.path().display()
                        ));
                    }
                    total_bytes = total_bytes.saturating_add(file_bytes);
                    if total_bytes > max_total_bytes {
                        return Err(anyhow::anyhow!(
                            "bundle exceeds max total bytes: {} > {}",
                            total_bytes,
                            max_total_bytes
                        ));
                    }
                    entries.push((entry.path().to_path_buf(), target));
                }
            }
            for (_, target) in &entries {
                if target.exists() {
                    return Err(anyhow::anyhow!(
                        "bundle would overwrite existing file: {}",
                        target.display()
                    ));
                }
            }
            for (source, target) in entries {
                ensure_import_target_parent(&dest, &dest_canon, &target, &mut created_dirs)?;
                copy_file_create_new(&source, &target, &dest_canon)?;
                copied_paths.push(target.clone());
                if is_trace_meta_candidate(&target) {
                    if parse_trace_meta(&target).is_ok() {
                        imported_paths.push(target);
                    }
                }
            }
        }

        let mut rules_imported = 0usize;
        if rules_src.exists() {
            let dest = rules_dir(data_dir);
            let dest_canon = ensure_import_base_dir(&dest)?;
            let mut entries: Vec<(PathBuf, PathBuf)> = Vec::new();
            for entry in WalkDir::new(&rules_src)
                .follow_links(false)
                .into_iter()
                .filter_map(|e| e.ok())
            {
                if entry.file_type().is_symlink() {
                    return Err(anyhow::anyhow!(
                        "bundle contains symlink: {}",
                        entry.path().display()
                    ));
                }
                if entry.file_type().is_file() {
                    let rel = entry.path().strip_prefix(&rules_src).unwrap();
                    let target = dest.join(rel);
                    let file_bytes = entry.metadata().map(|meta| meta.len())?;
                    if file_bytes > IMPORT_MAX_FILE_BYTES {
                        return Err(anyhow::anyhow!(
                            "bundle file exceeds max bytes: {} > {} ({})",
                            file_bytes,
                            IMPORT_MAX_FILE_BYTES,
                            entry.path().display()
                        ));
                    }
                    total_bytes = total_bytes.saturating_add(file_bytes);
                    if total_bytes > max_total_bytes {
                        return Err(anyhow::anyhow!(
                            "bundle exceeds max total bytes: {} > {}",
                            total_bytes,
                            max_total_bytes
                        ));
                    }
                    entries.push((entry.path().to_path_buf(), target));
                }
            }
            for (_, target) in &entries {
                if target.exists() {
                    return Err(anyhow::anyhow!(
                        "bundle would overwrite existing file: {}",
                        target.display()
                    ));
                }
            }
            for (source, target) in entries {
                ensure_import_target_parent(&dest, &dest_canon, &target, &mut created_dirs)?;
                copy_file_create_new(&source, &target, &dest_canon)?;
                copied_paths.push(target);
                rules_imported += 1;
            }
        }

        Ok(ImportWorkResult {
            imported_paths,
            rules_imported,
        })
    })();

    if let Err(err) = work {
        rollback_import(&copied_paths, &created_dirs);
        return Err(err);
    }

    work
}

fn rollback_import(copied_paths: &[PathBuf], created_dirs: &BTreeSet<PathBuf>) {
    for path in copied_paths.iter().rev() {
        if let Err(err) = std::fs::remove_file(path) {
            if err.kind() != std::io::ErrorKind::NotFound {
                warn!(
                    "failed to rollback imported file {}: {}",
                    path.display(),
                    err
                );
            }
        }
    }

    let mut dirs: Vec<&PathBuf> = created_dirs.iter().collect();
    dirs.sort_by_key(|path| Reverse(path.components().count()));
    for dir in dirs {
        if let Err(err) = std::fs::remove_dir(dir) {
            if err.kind() != std::io::ErrorKind::NotFound {
                warn!("failed to rollback import dir {}: {}", dir.display(), err);
            }
        }
    }
}
