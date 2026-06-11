use super::profiles::profile_supports_root_type;
use super::*;

pub(super) fn validate_hint_path(raw: &str, base_path: &str, ctx: &mut ValidationCtx<'_>) {
    if let Err(message) = parse_hint_path(raw) {
        ctx.push(ErrorCode::InvalidExprShape, message, base_path);
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
struct ParsedHintPath(Vec<ParsedPathPart>);

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
enum ParsedPathPart {
    Key(String),
    AnyIndex,
}

fn parse_hint_path(raw: &str) -> Result<ParsedHintPath, &'static str> {
    if raw.len() > MAX_TYPED_VALUE_HINT_PATH_BYTES {
        return Err("typed value hint path bytes exceed configured limit");
    }
    if raw == "." {
        return Ok(ParsedHintPath(Vec::new()));
    }
    if raw.is_empty() {
        return Err("hint path must not be empty");
    }
    let chars: Vec<char> = raw.chars().collect();
    let mut parts = Vec::new();
    let mut i = 0usize;
    while i < chars.len() {
        if chars[i] == '.' {
            if i == 0 || i + 1 == chars.len() || chars[i + 1] == '.' {
                return Err("hint path contains an empty segment");
            }
            i += 1;
            continue;
        }
        if chars[i] == '[' {
            if i + 2 < chars.len() && chars[i + 1] == '*' && chars[i + 2] == ']' {
                parts.push(ParsedPathPart::AnyIndex);
                i += 3;
                continue;
            }
            if i + 2 < chars.len() && chars[i + 1] == '"' {
                let mut key = String::new();
                i += 2;
                let mut closed = false;
                while i < chars.len() {
                    match chars[i] {
                        '"' if i + 1 < chars.len() && chars[i + 1] == ']' => {
                            i += 2;
                            closed = true;
                            if key.is_empty() {
                                return Err("hint path contains an empty segment");
                            }
                            parts.push(ParsedPathPart::Key(key));
                            break;
                        }
                        '\\' if i + 1 < chars.len() => {
                            i += 1;
                            key.push(chars[i]);
                            i += 1;
                        }
                        ch => {
                            key.push(ch);
                            i += 1;
                        }
                    }
                }
                if !closed {
                    return Err("invalid quoted hint path");
                }
                continue;
            }
            return Err("invalid hint path bracket syntax");
        }
        let start = i;
        while i < chars.len() && chars[i] != '.' && chars[i] != '[' {
            i += 1;
        }
        let key: String = chars[start..i].iter().collect();
        if key.is_empty() {
            return Err("hint path contains an empty segment");
        }
        parts.push(ParsedPathPart::Key(key));
    }
    if parts.len() > MAX_TYPED_VALUE_HINT_PATH_TOKENS {
        return Err("typed value hint path token count exceeds configured limit");
    }
    Ok(ParsedHintPath(parts))
}

pub(super) fn validate_duplicate_hint_paths(
    map: &JsonMap<String, JsonValue>,
    base_path: &str,
    ctx: &mut ValidationCtx<'_>,
) {
    let mut seen = BTreeSet::new();
    for raw_path in collect_hint_paths(map) {
        let Ok(path) = parse_hint_path(&raw_path) else {
            continue;
        };
        if !seen.insert(path) {
            ctx.push(
                ErrorCode::InvalidExprShape,
                "duplicate field type path",
                base_path,
            );
        }
    }
}

pub(super) fn validate_root_hint_paths(
    map: &JsonMap<String, JsonValue>,
    base_path: &str,
    ctx: &mut ValidationCtx<'_>,
) {
    let Some(profile) = map.get("profile").and_then(JsonValue::as_str) else {
        return;
    };
    if profile_supports_root_type(profile) {
        return;
    }
    for raw_path in collect_hint_paths(map) {
        let Ok(path) = parse_hint_path(&raw_path) else {
            continue;
        };
        if path.0.is_empty() {
            ctx.push(
                ErrorCode::InvalidExprShape,
                &format!(
                    "root field type path is not supported by {} profile",
                    profile
                ),
                base_path,
            );
        }
    }
}

pub(super) fn validate_root_type_hint_conflicts(
    map: &JsonMap<String, JsonValue>,
    base_path: &str,
    ctx: &mut ValidationCtx<'_>,
) {
    if !map.contains_key("type") {
        return;
    }
    for raw_path in collect_hint_paths(map) {
        let Ok(path) = parse_hint_path(&raw_path) else {
            continue;
        };
        if path.0.is_empty() {
            ctx.push(
                ErrorCode::InvalidExprShape,
                "root type and root field type path cannot be used together",
                base_path,
            );
        }
    }
}

fn collect_hint_paths(map: &JsonMap<String, JsonValue>) -> Vec<String> {
    let mut paths = Vec::new();
    if let Some(field_types) = map.get("field_types").and_then(JsonValue::as_object) {
        paths.extend(field_types.keys().cloned());
    }
    if let Some(hints) = map.get("hints").and_then(JsonValue::as_array) {
        for hint in hints {
            if let Some(path) = hint
                .as_object()
                .and_then(|hint| hint.get("path"))
                .and_then(JsonValue::as_str)
            {
                paths.push(path.to_string());
            }
        }
    }
    if let Some(sets) = map.get("sets").and_then(JsonValue::as_object) {
        for set_paths in sets.values() {
            if let Some(items) = set_paths.as_array() {
                paths.extend(
                    items
                        .iter()
                        .filter_map(JsonValue::as_str)
                        .map(ToOwned::to_owned),
                );
            }
        }
    }
    for key in ["number_strings", "binary_base64"] {
        if let Some(items) = map.get(key).and_then(JsonValue::as_array) {
            paths.extend(
                items
                    .iter()
                    .filter_map(JsonValue::as_str)
                    .map(ToOwned::to_owned),
            );
        }
    }
    paths
}
