use super::*;

pub(in crate::transform) fn flatten_object(
    map: &Map<String, JsonValue>,
    tokens: &mut Vec<PathToken>,
    output: &mut Map<String, JsonValue>,
    base_path: &str,
) -> Result<(), TransformError> {
    for (key, value) in map {
        if key.is_empty() {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "object_flatten does not support empty keys",
            )
            .with_path(format!("{}.args[0]", base_path)));
        }
        if key.contains('[') || key.contains(']') {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "object_flatten does not support keys with '[' or ']'",
            )
            .with_path(format!("{}.args[0]", base_path)));
        }
        tokens.push(PathToken::Key(key.clone()));
        match value {
            JsonValue::Object(child) => {
                if child.is_empty() {
                    let path = format_path_tokens(tokens);
                    output.insert(path, JsonValue::Object(Map::new()));
                } else {
                    flatten_object(child, tokens, output, base_path)?;
                }
            }
            _ => {
                let path = format_path_tokens(tokens);
                output.insert(path, value.clone());
            }
        }
        tokens.pop();
    }
    Ok(())
}

fn format_path_tokens(tokens: &[PathToken]) -> String {
    let mut path = String::new();
    for token in tokens {
        match token {
            PathToken::Key(key) => {
                if needs_bracket_quote(key) {
                    let escaped = key.replace('\\', "\\\\").replace('"', "\\\"");
                    path.push('[');
                    path.push('"');
                    path.push_str(&escaped);
                    path.push('"');
                    path.push(']');
                } else {
                    if !path.is_empty() {
                        path.push('.');
                    }
                    path.push_str(key);
                }
            }
            PathToken::Index(index) => {
                path.push('[');
                path.push_str(&index.to_string());
                path.push(']');
            }
        }
    }
    path
}

fn needs_bracket_quote(key: &str) -> bool {
    key.contains('.')
}
