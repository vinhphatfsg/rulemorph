pub(crate) fn sanitize_trace_id(raw: &str) -> String {
    let mut sanitized = String::with_capacity(raw.len());
    for ch in raw.chars() {
        if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' || ch == '.' {
            sanitized.push(ch);
        } else {
            sanitized.push('_');
        }
    }
    if sanitized == "." || sanitized == ".." {
        String::new()
    } else {
        sanitized
    }
}

pub(crate) fn trace_id_is_insufficient(trace_id: &str) -> bool {
    trace_id.is_empty() || trace_id_is_placeholder(trace_id)
}

pub(crate) fn trace_id_is_placeholder(trace_id: &str) -> bool {
    trace_id == "trace"
}
