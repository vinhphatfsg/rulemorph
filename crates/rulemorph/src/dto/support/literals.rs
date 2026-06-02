pub(in crate::dto) fn rust_string_literal(value: &str) -> String {
    format!("{:?}", value)
}

pub(in crate::dto) fn json_string_literal(value: &str) -> String {
    serde_json::to_string(value).expect("string serialization should not fail")
}

pub(in crate::dto) fn swift_string_literal(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            ch if ch.is_control() => out.push_str(&format!("\\u{{{:x}}}", ch as u32)),
            ch => out.push(ch),
        }
    }
    out.push('"');
    out
}

pub(in crate::dto) fn safe_comment_text(value: &str) -> String {
    value.replace("*/", "* /").replace(['\r', '\n'], "\\n")
}

pub(in crate::dto) fn go_json_tag_literal(key: &str, optional: bool) -> String {
    if !key.contains('`')
        && !key.contains('"')
        && !key.contains('\\')
        && !key.contains('\r')
        && !key.contains('\n')
        && !key.contains('\t')
        && !key.chars().any(char::is_control)
    {
        if optional {
            return format!("`json:\"{},omitempty\"`", key);
        }
        return format!("`json:\"{}\"`", key);
    }
    let mut tag_key = String::with_capacity(key.len());
    for ch in key.chars() {
        match ch {
            '\\' => tag_key.push_str("\\\\"),
            '"' => tag_key.push_str("\\\""),
            '\n' => tag_key.push_str("\\n"),
            '\r' => tag_key.push_str("\\r"),
            '\t' => tag_key.push_str("\\t"),
            ch => tag_key.push(ch),
        }
    }
    let tag = if optional {
        format!("json:\"{},omitempty\"", tag_key)
    } else {
        format!("json:\"{}\"", tag_key)
    };
    json_string_literal(&tag)
}
