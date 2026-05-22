pub(crate) fn canonical_input_path(path: &str) -> String {
    canonical_at_path("@input", &["@input", "input"], path)
}

pub(crate) fn canonical_item_path(path: &str) -> String {
    canonical_at_path("@item", &["@item", "item"], path)
}

pub(crate) fn canonical_acc_path(path: &str) -> String {
    canonical_at_path("@acc", &["@acc", "acc"], path)
}

pub(crate) fn canonical_context_path(path: &str) -> String {
    canonical_at_path("@context", &["@context", "context"], path)
}

pub(crate) fn canonical_out_path(path: &str) -> String {
    canonical_at_path("@out", &["@out", "out"], path)
}

pub(crate) fn canonical_output_path(path: &str) -> String {
    let trimmed = path.trim();
    if trimmed == "$" || trimmed.starts_with("$.") || trimmed.starts_with("$[") {
        return trimmed.to_string();
    }
    let stripped = trimmed
        .strip_prefix("output.")
        .or_else(|| trimmed.strip_prefix("out."))
        .unwrap_or(trimmed)
        .trim_start_matches('.');
    if stripped.is_empty() {
        "$".to_string()
    } else if stripped.starts_with('[') {
        format!("${stripped}")
    } else {
        format!("$.{stripped}")
    }
}

fn canonical_at_path(prefix: &'static str, strip_prefixes: &[&str], path: &str) -> String {
    let trimmed = path.trim();
    if trimmed == prefix
        || trimmed.starts_with(&format!("{prefix}."))
        || trimmed.starts_with(&format!("{prefix}["))
    {
        return trimmed.to_string();
    }
    for other_prefix in ["@input", "@item", "@acc", "@context", "@out"] {
        debug_assert!(
            other_prefix == prefix
                || !(trimmed == other_prefix
                    || trimmed.starts_with(&format!("{other_prefix}."))
                    || trimmed.starts_with(&format!("{other_prefix}["))),
            "semantic path namespace mismatch: expected {prefix}, got {trimmed}"
        );
    }
    let suffix = strip_prefixes
        .iter()
        .find_map(|candidate| {
            if trimmed == *candidate {
                Some("")
            } else if let Some(rest) = trimmed.strip_prefix(candidate) {
                (rest.starts_with('.') || rest.starts_with('[')).then_some(rest)
            } else {
                None
            }
        })
        .unwrap_or(trimmed);
    if suffix.is_empty() {
        prefix.to_string()
    } else if suffix.starts_with('.') || suffix.starts_with('[') {
        format!("{prefix}{suffix}")
    } else {
        format!("{prefix}.{suffix}")
    }
}
