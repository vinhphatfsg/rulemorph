use super::*;

pub(super) fn has_duplicate_path(paths: &[Vec<PathToken>], tokens: &[PathToken]) -> bool {
    paths.iter().any(|existing| existing == tokens)
}

pub(super) fn has_path_conflict(paths: &[Vec<PathToken>], tokens: &[PathToken]) -> bool {
    paths
        .iter()
        .any(|existing| is_path_prefix(existing, tokens) || is_path_prefix(tokens, existing))
}

pub(super) fn is_path_prefix(prefix: &[PathToken], tokens: &[PathToken]) -> bool {
    if prefix.len() > tokens.len() {
        return false;
    }
    prefix.iter().zip(tokens).all(|(left, right)| left == right)
}

pub(super) fn merge_object(
    target: &mut Map<String, JsonValue>,
    incoming: &Map<String, JsonValue>,
    deep: bool,
) {
    for (key, value) in incoming {
        if deep {
            if let (Some(JsonValue::Object(target_obj)), JsonValue::Object(incoming_obj)) =
                (target.get_mut(key), value)
            {
                merge_object(target_obj, incoming_obj, true);
                continue;
            }
        }
        target.insert(key.clone(), value.clone());
    }
}

pub(super) fn flatten_object(
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

pub(super) fn format_path_tokens(tokens: &[PathToken]) -> String {
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

pub(super) fn needs_bracket_quote(key: &str) -> bool {
    key.contains('.')
}

pub(super) fn set_path_object_only(
    root: &mut JsonValue,
    tokens: &[PathToken],
    value: JsonValue,
    base_path: &str,
) -> Result<(), TransformError> {
    if tokens.is_empty() {
        return Err(
            TransformError::new(TransformErrorKind::ExprError, "path is empty")
                .with_path(format!("{}.args[0]", base_path)),
        );
    }

    let mut current = root;
    for (index, token) in tokens.iter().enumerate() {
        let key = match token {
            PathToken::Key(key) => key,
            PathToken::Index(_) => {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "array indexes are not allowed in path",
                )
                .with_path(format!("{}.args[0]", base_path)));
            }
        };
        let is_last = index == tokens.len() - 1;

        match current {
            JsonValue::Object(map) => {
                if is_last {
                    if map.contains_key(key) {
                        return Err(TransformError::new(
                            TransformErrorKind::ExprError,
                            "path conflicts with existing value",
                        )
                        .with_path(format!("{}.args[0]", base_path)));
                    }
                    map.insert(key.clone(), value);
                    return Ok(());
                }

                let entry = map
                    .entry(key.clone())
                    .or_insert_with(|| JsonValue::Object(Map::new()));
                if !entry.is_object() {
                    return Err(TransformError::new(
                        TransformErrorKind::ExprError,
                        "path conflicts with non-object value",
                    )
                    .with_path(format!("{}.args[0]", base_path)));
                }
                current = entry;
            }
            _ => {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "path conflicts with non-object value",
                )
                .with_path(format!("{}.args[0]", base_path)));
            }
        }
    }

    Ok(())
}

pub(super) fn set_path_with_indexes(
    root: &mut JsonValue,
    tokens: &[PathToken],
    value: JsonValue,
    base_path: &str,
) -> Result<(), TransformError> {
    if tokens.is_empty() {
        return Err(
            TransformError::new(TransformErrorKind::ExprError, "path is empty")
                .with_path(format!("{}.args[1]", base_path)),
        );
    }

    let mut current = root;
    for (index, token) in tokens.iter().enumerate() {
        let is_last = index == tokens.len() - 1;
        match token {
            PathToken::Key(key) => {
                let next_token = tokens.get(index + 1);
                match current {
                    JsonValue::Object(map) => {
                        if is_last {
                            map.insert(key.clone(), value);
                            return Ok(());
                        }
                        let entry = map.entry(key.clone()).or_insert_with(|| match next_token {
                            Some(PathToken::Index(_)) => JsonValue::Array(Vec::new()),
                            _ => JsonValue::Object(Map::new()),
                        });
                        let expect_index = matches!(next_token, Some(PathToken::Index(_)));
                        let entry_is_array = matches!(entry, JsonValue::Array(_));
                        let entry_is_object = matches!(entry, JsonValue::Object(_));
                        if !(expect_index && entry_is_array || !expect_index && entry_is_object) {
                            return Err(TransformError::new(
                                TransformErrorKind::ExprError,
                                "path conflicts with non-object value",
                            )
                            .with_path(format!("{}.args[1]", base_path)));
                        }
                        current = entry;
                    }
                    _ => {
                        return Err(TransformError::new(
                            TransformErrorKind::ExprError,
                            "path conflicts with non-object value",
                        )
                        .with_path(format!("{}.args[1]", base_path)));
                    }
                }
            }
            PathToken::Index(path_index) => {
                let next_token = tokens.get(index + 1);
                match current {
                    JsonValue::Array(items) => {
                        if items.len() <= *path_index {
                            items.resize_with(path_index + 1, || JsonValue::Null);
                        }
                        if is_last {
                            items[*path_index] = value;
                            return Ok(());
                        }
                        let entry = &mut items[*path_index];
                        if entry.is_null() {
                            *entry = match next_token {
                                Some(PathToken::Index(_)) => JsonValue::Array(Vec::new()),
                                _ => JsonValue::Object(Map::new()),
                            };
                        }
                        let expect_index = matches!(next_token, Some(PathToken::Index(_)));
                        let entry_is_array = matches!(entry, JsonValue::Array(_));
                        let entry_is_object = matches!(entry, JsonValue::Object(_));
                        if !(expect_index && entry_is_array || !expect_index && entry_is_object) {
                            return Err(TransformError::new(
                                TransformErrorKind::ExprError,
                                "path conflicts with non-object value",
                            )
                            .with_path(format!("{}.args[1]", base_path)));
                        }
                        current = entry;
                    }
                    _ => {
                        return Err(TransformError::new(
                            TransformErrorKind::ExprError,
                            "path conflicts with non-object value",
                        )
                        .with_path(format!("{}.args[1]", base_path)));
                    }
                }
            }
        }
    }

    Ok(())
}

pub(super) fn remove_path(root: &mut JsonValue, tokens: &[PathToken]) {
    if tokens.is_empty() {
        return;
    }

    let (first, rest) = tokens.split_first().unwrap();
    match first {
        PathToken::Key(key) => {
            if let JsonValue::Object(map) = root {
                if rest.is_empty() {
                    map.remove(key);
                    return;
                }
                if let Some(next) = map.get_mut(key) {
                    remove_path(next, rest);
                }
            }
        }
        PathToken::Index(index) => {
            if let JsonValue::Array(items) = root {
                if let Some(next) = items.get_mut(*index) {
                    remove_path(next, rest);
                }
            }
        }
    }
}

pub(super) fn parse_source(source: &str) -> Result<(Namespace, &str), TransformError> {
    if let Some((prefix, path)) = source.split_once('.') {
        if path.is_empty() {
            return Err(TransformError::new(
                TransformErrorKind::InvalidRef,
                "reference path is empty",
            ));
        }
        let namespace = match prefix {
            "input" => Namespace::Input,
            "context" => Namespace::Context,
            "out" => Namespace::Out,
            _ => {
                return Err(TransformError::new(
                    TransformErrorKind::InvalidRef,
                    "ref namespace must be input|context|out",
                ));
            }
        };
        Ok((namespace, path))
    } else {
        if source.is_empty() {
            return Err(TransformError::new(
                TransformErrorKind::InvalidRef,
                "reference path is empty",
            ));
        }
        Ok((Namespace::Input, source))
    }
}

pub(super) fn parse_ref(value: &str) -> Result<(Namespace, &str), TransformError> {
    let (prefix, path) = value.split_once('.').ok_or_else(|| {
        TransformError::new(TransformErrorKind::InvalidRef, "ref must include namespace")
    })?;

    if path.is_empty() {
        return Err(TransformError::new(
            TransformErrorKind::InvalidRef,
            "ref path is empty",
        ));
    }

    let namespace = match prefix {
        "input" => Namespace::Input,
        "context" => Namespace::Context,
        "out" => Namespace::Out,
        "item" => Namespace::Item,
        "acc" => Namespace::Acc,
        "pipe" => Namespace::Pipe,
        "local" => Namespace::Local,
        _ => {
            return Err(TransformError::new(
                TransformErrorKind::InvalidRef,
                "ref namespace must be input|context|out|item|acc|pipe|local",
            ));
        }
    };

    Ok((namespace, path))
}

pub(super) fn parse_path_tokens(
    path: &str,
    kind: TransformErrorKind,
    error_path: impl Into<String>,
) -> Result<Vec<PathToken>, TransformError> {
    parse_path(path)
        .map_err(|err| TransformError::new(kind, err.message()).with_path(error_path.into()))
}

pub(super) fn set_path(
    root: &mut JsonValue,
    path: &str,
    value: JsonValue,
    mapping_path: &str,
) -> Result<(), TransformError> {
    let tokens = parse_path_tokens(
        path,
        TransformErrorKind::InvalidTarget,
        format!("{}.target", mapping_path),
    )?;
    if tokens.is_empty() {
        return Err(TransformError::new(
            TransformErrorKind::InvalidTarget,
            "target path is invalid",
        )
        .with_path(format!("{}.target", mapping_path)));
    }

    let mut current = root;
    for (index, token) in tokens.iter().enumerate() {
        let is_last = index == tokens.len() - 1;
        let key = match token {
            PathToken::Key(key) => key,
            PathToken::Index(_) => {
                return Err(TransformError::new(
                    TransformErrorKind::InvalidTarget,
                    "target path must not include indexes",
                )
                .with_path(format!("{}.target", mapping_path)));
            }
        };

        match current {
            JsonValue::Object(map) => {
                if is_last {
                    map.insert(key.to_string(), value);
                    return Ok(());
                }

                let entry = map
                    .entry(key.to_string())
                    .or_insert_with(|| JsonValue::Object(Map::new()));
                if !entry.is_object() {
                    return Err(TransformError::new(
                        TransformErrorKind::InvalidTarget,
                        "target path conflicts with non-object value",
                    )
                    .with_path(format!("{}.target", mapping_path)));
                }
                current = entry;
            }
            _ => {
                return Err(TransformError::new(
                    TransformErrorKind::InvalidTarget,
                    "target root must be an object",
                )
                .with_path(format!("{}.target", mapping_path)));
            }
        }
    }

    Ok(())
}
