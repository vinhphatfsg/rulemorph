use super::*;

pub(in crate::transform) fn merge_object(
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

pub(in crate::transform) fn set_path_object_only(
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

pub(in crate::transform) fn set_path_with_indexes(
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

pub(in crate::transform) fn remove_path(root: &mut JsonValue, tokens: &[PathToken]) {
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

pub(in crate::transform) fn set_path(
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
