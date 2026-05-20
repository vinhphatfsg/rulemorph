pub(super) fn normalize_domain(domain: &str) -> String {
    domain.trim().trim_end_matches('.').to_ascii_lowercase()
}

pub(super) fn host_equals(host: &str, allow: &str) -> bool {
    if allow.is_empty() {
        return false;
    }
    host == allow
}

pub(super) fn host_matches(host: &str, allow: &str) -> bool {
    if allow.is_empty() {
        return false;
    }
    if host == allow {
        return true;
    }
    host.ends_with(&format!(".{allow}"))
}
