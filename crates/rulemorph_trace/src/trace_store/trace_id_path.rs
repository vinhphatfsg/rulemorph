use std::path::{Path, PathBuf};

use crate::trace_id::{sanitize_trace_id, trace_id_is_insufficient};

fn fnv1a_hash(bytes: &[u8]) -> u64 {
    const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;
    let mut hash = FNV_OFFSET_BASIS;
    for byte in bytes {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

fn relative_trace_path(path: &Path) -> Option<PathBuf> {
    for ancestor in path.ancestors() {
        if ancestor.file_name().and_then(|name| name.to_str()) == Some("traces") {
            if let Ok(rel) = path.strip_prefix(ancestor) {
                if !rel.as_os_str().is_empty() {
                    return Some(rel.to_path_buf());
                }
            }
        }
    }
    None
}

#[cfg(unix)]
fn path_bytes_for_hash(path: &Path) -> Vec<u8> {
    use std::os::unix::ffi::OsStrExt;
    let candidate = relative_trace_path(path).unwrap_or_else(|| path.to_path_buf());
    candidate.as_os_str().as_bytes().to_vec()
}

#[cfg(not(unix))]
fn path_bytes_for_hash(path: &Path) -> Vec<u8> {
    let candidate = relative_trace_path(path).unwrap_or_else(|| path.to_path_buf());
    candidate.to_string_lossy().into_owned().into_bytes()
}

pub(super) fn path_hash_for_trace_id(path: &Path) -> u64 {
    fnv1a_hash(&path_bytes_for_hash(path))
}

pub(super) fn hash_trace_id_for_path(path: &Path) -> String {
    let hash = path_hash_for_trace_id(path);
    format!("trace-{hash:x}")
}

pub(super) fn fallback_trace_id_for_path(path: &Path) -> String {
    let stem = path.file_stem().and_then(|s| s.to_str());
    let sanitized = stem.map(sanitize_trace_id).unwrap_or_default();
    if !trace_id_is_insufficient(&sanitized) {
        return sanitized;
    }
    hash_trace_id_for_path(path)
}
