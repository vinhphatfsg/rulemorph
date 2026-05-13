use std::collections::BTreeSet;
use std::fs::OpenOptions;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

pub(super) fn ensure_import_base_dir(base_dir: &Path) -> Result<PathBuf> {
    std::fs::create_dir_all(base_dir)
        .with_context(|| format!("failed to create import base dir: {}", base_dir.display()))?;
    let meta = std::fs::symlink_metadata(base_dir)
        .with_context(|| format!("failed to stat import base dir: {}", base_dir.display()))?;
    if meta.file_type().is_symlink() {
        return Err(anyhow::anyhow!(
            "bundle destination base is symlink: {}",
            base_dir.display()
        ));
    }
    if !meta.is_dir() {
        return Err(anyhow::anyhow!(
            "bundle destination base is not a directory: {}",
            base_dir.display()
        ));
    }
    base_dir.canonicalize().with_context(|| {
        format!(
            "failed to canonicalize import base dir: {}",
            base_dir.display()
        )
    })
}

fn ensure_relative_dir_safe(
    base_dir: &Path,
    target_dir: &Path,
    created_dirs: &mut BTreeSet<PathBuf>,
) -> Result<()> {
    let relative = target_dir.strip_prefix(base_dir).with_context(|| {
        format!(
            "bundle destination escapes base dir: {}",
            target_dir.display()
        )
    })?;
    let mut current = base_dir.to_path_buf();
    for component in relative.components() {
        current.push(component.as_os_str());
        match std::fs::create_dir(&current) {
            Ok(()) => {
                created_dirs.insert(current.clone());
            }
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                let meta = std::fs::symlink_metadata(&current).with_context(|| {
                    format!("failed to stat bundle destination: {}", current.display())
                })?;
                if meta.file_type().is_symlink() {
                    return Err(anyhow::anyhow!(
                        "bundle destination contains symlink: {}",
                        current.display()
                    ));
                }
                if !meta.is_dir() {
                    return Err(anyhow::anyhow!(
                        "bundle destination is not a directory: {}",
                        current.display()
                    ));
                }
            }
            Err(err) => {
                return Err(anyhow::anyhow!(
                    "failed to create bundle destination dir {}: {}",
                    current.display(),
                    err
                ));
            }
        }
    }
    Ok(())
}

pub(super) fn ensure_import_target_parent(
    base_dir: &Path,
    base_canon: &Path,
    target: &Path,
    created_dirs: &mut BTreeSet<PathBuf>,
) -> Result<()> {
    if !target.starts_with(base_dir) {
        return Err(anyhow::anyhow!(
            "bundle destination escapes base dir: {}",
            target.display()
        ));
    }
    let parent = target
        .parent()
        .ok_or_else(|| anyhow::anyhow!("bundle destination has no parent: {}", target.display()))?;
    ensure_relative_dir_safe(base_dir, parent, created_dirs)?;
    let parent_canon = parent.canonicalize().with_context(|| {
        format!(
            "failed to canonicalize bundle destination parent: {}",
            parent.display()
        )
    })?;
    if !parent_canon.starts_with(base_canon) {
        return Err(anyhow::anyhow!(
            "bundle destination escapes base dir: {}",
            target.display()
        ));
    }
    Ok(())
}

pub(super) fn copy_file_create_new(source: &Path, target: &Path, base_canon: &Path) -> Result<()> {
    let mut source_file = std::fs::File::open(source)
        .with_context(|| format!("failed to open bundle source: {}", source.display()))?;
    let mut target_file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(target)
        .with_context(|| format!("failed to create bundle target: {}", target.display()))?;
    let target_canon = target
        .canonicalize()
        .with_context(|| format!("failed to canonicalize bundle target: {}", target.display()))?;
    if !target_canon.starts_with(base_canon) {
        drop(target_file);
        let _ = std::fs::remove_file(target);
        return Err(anyhow::anyhow!(
            "bundle target escapes base dir: {}",
            target.display()
        ));
    }
    std::io::copy(&mut source_file, &mut target_file).with_context(|| {
        format!(
            "failed to copy bundle file {} -> {}",
            source.display(),
            target.display()
        )
    })?;
    Ok(())
}
