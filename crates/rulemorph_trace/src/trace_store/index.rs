use std::collections::{HashMap, HashSet};
use std::path::Path;

use anyhow::Result;
use tracing::warn;
use walkdir::WalkDir;

use super::TraceMeta;
use super::meta::parse_trace_meta;
use super::trace_id_path::path_hash_for_trace_id;

pub(super) fn build_trace_index(traces_dir: &Path) -> Result<HashMap<String, TraceMeta>> {
    let mut metas = Vec::new();
    if !traces_dir.exists() {
        return Ok(HashMap::new());
    }
    for entry in WalkDir::new(traces_dir)
        .into_iter()
        .filter_entry(|entry| {
            !(entry.file_type().is_dir() && entry.file_name().to_string_lossy() == "blobs")
        })
        .filter_map(|e| e.ok())
    {
        if !entry.file_type().is_file() {
            continue;
        }
        let path = entry.path();
        if !is_trace_meta_candidate(path) {
            continue;
        }
        match parse_trace_meta(path) {
            Ok(meta) => metas.push(meta),
            Err(err) => {
                warn!("failed to parse trace metadata {}: {}", path.display(), err);
            }
        }
    }

    disambiguate_trace_ids(metas)
}

fn disambiguate_trace_ids(metas: Vec<TraceMeta>) -> Result<HashMap<String, TraceMeta>> {
    let original_ids: HashSet<String> = metas.iter().map(|meta| meta.trace_id.clone()).collect();
    let mut by_id: HashMap<String, Vec<TraceMeta>> = HashMap::new();
    for meta in metas {
        by_id.entry(meta.trace_id.clone()).or_default().push(meta);
    }

    let mut used_ids = HashSet::new();
    let mut map = HashMap::new();
    let mut keys: Vec<String> = by_id.keys().cloned().collect();
    keys.sort();

    for base_id in keys {
        let mut group = by_id.remove(&base_id).unwrap_or_default();
        group.sort_by(|a, b| a.path.cmp(&b.path));
        if group.len() == 1 {
            let meta = group.pop().expect("single meta");
            used_ids.insert(base_id.clone());
            map.insert(base_id, meta);
            continue;
        }

        for mut meta in group {
            let hash = path_hash_for_trace_id(Path::new(&meta.path));
            let mut candidate = format!("{base_id}-dup-{hash:x}");
            let mut counter = 0usize;
            while used_ids.contains(&candidate) || original_ids.contains(&candidate) {
                counter = counter.saturating_add(1);
                candidate = format!("{base_id}-dup-{hash:x}-{counter}");
            }
            warn!(
                "trace_id collision {} at {}; using {}",
                base_id, meta.path, candidate
            );
            meta.trace_id = candidate.clone();
            used_ids.insert(candidate.clone());
            map.insert(candidate, meta);
        }
    }

    Ok(map)
}

pub(super) fn is_trace_meta_candidate(path: &Path) -> bool {
    if path.extension().and_then(|s| s.to_str()) != Some("json") {
        return false;
    }
    if path
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name == "finalize.json")
    {
        return false;
    }
    if path
        .components()
        .any(|component| component.as_os_str() == "blobs")
    {
        return false;
    }
    true
}
