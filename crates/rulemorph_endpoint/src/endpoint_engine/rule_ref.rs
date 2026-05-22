use std::path::{Path, PathBuf};

pub(super) fn resolve_rule_path(base_dir: &Path, rule: &str) -> PathBuf {
    let path = PathBuf::from(rule);
    if path.is_absolute() {
        path
    } else {
        base_dir.join(path)
    }
}

pub(super) fn rule_ref_from_rule(base_dir: &Path, rule: &str) -> String {
    let resolved = resolve_rule_path(base_dir, rule);
    rule_ref_from_path(base_dir, &resolved)
}

pub(super) fn safe_rule_ref_from_path(base_dir: &Path, path: &Path) -> Option<String> {
    if path.strip_prefix(base_dir).is_ok() {
        Some(rule_ref_from_path(base_dir, path))
    } else {
        None
    }
}

pub(super) fn rule_ref_from_path(base_dir: &Path, path: &Path) -> String {
    if let Ok(rel) = path.strip_prefix(base_dir) {
        let rel = rel.to_string_lossy().replace('\\', "/");
        if rel.starts_with("rules/") {
            rel
        } else {
            format!("rules/{}", rel)
        }
    } else {
        path.display().to_string()
    }
}

pub(super) fn rule_display_name(path: &Path) -> String {
    path.file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("rule")
        .to_string()
}
