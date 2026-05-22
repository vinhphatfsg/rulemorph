pub(super) fn internal_hosts_match(target: Option<&str>, base: Option<&str>) -> bool {
    let Some(target) = target.map(normalize_internal_host) else {
        return false;
    };
    let Some(base) = base.map(normalize_internal_host) else {
        return false;
    };
    target == base || is_loopback_host(&target) && is_loopback_host(&base)
}

pub(super) fn normalize_internal_host(host: &str) -> String {
    host.trim().trim_end_matches('.').to_ascii_lowercase()
}

pub(super) fn is_loopback_host(host: &str) -> bool {
    matches!(host, "localhost" | "127.0.0.1" | "::1")
}
