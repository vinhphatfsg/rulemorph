use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use walkdir::WalkDir;

use super::{ApiGraphEdge, ApiGraphNode};

pub(super) fn collect_rule_files(data_dir: &Path) -> Vec<PathBuf> {
    WalkDir::new(data_dir)
        .into_iter()
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_type().is_file())
        .filter(|entry| {
            entry
                .path()
                .extension()
                .and_then(|ext| ext.to_str())
                .map(|ext| ext == "yaml" || ext == "yml" || ext == "json")
                .unwrap_or(false)
        })
        .map(|entry| entry.path().to_path_buf())
        .collect()
}

pub(super) fn rule_id(data_dir: &Path, path: &Path) -> String {
    let path = normalize_path(path);
    let data_dir = normalize_path(data_dir);
    if let Ok(rel) = path.strip_prefix(&data_dir) {
        rel.to_string_lossy().replace('\\', "/")
    } else {
        path.to_string_lossy().replace('\\', "/")
    }
}

pub(super) fn rule_path_display(data_dir: &Path, path: &Path) -> String {
    rule_id(data_dir, path)
}

pub(super) fn rule_label(path: &Path) -> String {
    path.file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or("rule")
        .to_string()
}

pub(super) fn insert_placeholder(
    nodes: &mut HashMap<String, ApiGraphNode>,
    data_dir: &Path,
    path: &Path,
) {
    let id = rule_id(data_dir, path);
    nodes.entry(id.clone()).or_insert(ApiGraphNode {
        id,
        label: format!("missing · {}", rule_label(path)),
        kind: "missing".to_string(),
        path: rule_path_display(data_dir, path),
        ops: Vec::new(),
    });
}

pub(super) fn normalize_path(path: &Path) -> PathBuf {
    let mut result = PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                result.pop();
            }
            other => result.push(other.as_os_str()),
        }
    }
    result
}

pub(super) fn push_edge(
    edges: &mut Vec<ApiGraphEdge>,
    edge_keys: &mut HashSet<String>,
    source: &str,
    target: &str,
    label: Option<String>,
    kind: &str,
) {
    let key = format!("{}::{}::{}", source, target, label.as_deref().unwrap_or(""));
    if edge_keys.contains(&key) {
        return;
    }
    edge_keys.insert(key);
    edges.push(ApiGraphEdge {
        source: source.to_string(),
        target: target.to_string(),
        label,
        kind: kind.to_string(),
    });
}
