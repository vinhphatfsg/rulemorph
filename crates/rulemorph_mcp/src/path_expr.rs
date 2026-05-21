use serde_json::Value;

mod parser;

use self::parser::{PathToken, get_value_by_tokens, parse_path_tokens};

pub(crate) fn append_path(prefix: &str, key: &str) -> String {
    let needs_quote = key
        .chars()
        .any(|ch| ch == '.' || ch == '[' || ch == ']' || ch == '"' || ch == '\'' || ch == '\\');
    let segment = if needs_quote {
        let escaped = key.replace('\\', "\\\\").replace('"', "\\\"");
        format!("[\"{}\"]", escaped)
    } else {
        key.to_string()
    };
    if prefix.is_empty() {
        segment
    } else if segment.starts_with('[') {
        format!("{}{}", prefix, segment)
    } else {
        format!("{}.{}", prefix, segment)
    }
}

pub(crate) fn leaf_from_path(path: &str) -> Option<String> {
    match parse_path_tokens(path) {
        Ok(tokens) => {
            for token in tokens.iter().rev() {
                if let PathToken::Key(key) = token {
                    return Some(key.clone());
                }
            }
            None
        }
        Err(_) => Some(path.to_string()),
    }
}

pub(crate) fn get_value_at_path<'a>(
    value: &'a Value,
    path: &str,
) -> Result<Option<&'a Value>, String> {
    let tokens = parse_path_tokens(path)?;
    Ok(get_value_by_tokens(value, &tokens))
}
