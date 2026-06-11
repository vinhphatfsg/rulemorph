use super::*;

pub(super) fn scoped_path_type(base: Option<&FieldType>, path: &str) -> FieldType {
    let Some(base) = base else {
        return FieldType::JsonValue;
    };
    if path.is_empty() {
        return base.clone();
    }
    field_type_at_path(base, path).unwrap_or(FieldType::JsonValue)
}

pub(super) fn key_path(path: &str) -> Option<Vec<String>> {
    let tokens = parse_path_bounded(path)?;
    let mut keys = Vec::with_capacity(tokens.len());
    for token in tokens {
        match token {
            PathToken::Key(key) => keys.push(key),
            PathToken::Index(_) => return None,
        }
    }
    Some(keys)
}

pub(super) fn parse_path_bounded(path: &str) -> Option<Vec<PathToken>> {
    if path.len() > DTO_INFER_MAX_PATH_BYTES {
        return None;
    }
    let tokens = parse_path(path).ok()?;
    if tokens.len() > DTO_INFER_MAX_PATH_TOKENS {
        return None;
    }
    Some(tokens)
}

pub(super) fn field_type_at_path(field_type: &FieldType, path: &str) -> Option<FieldType> {
    let tokens = parse_path_bounded(path)?;
    field_type_at_tokens(field_type, &tokens)
}

pub(super) fn field_type_at_keys(field_type: &FieldType, keys: &[String]) -> Option<FieldType> {
    if keys.is_empty() {
        return Some(field_type.clone());
    }
    match field_type {
        FieldType::Object(node) => node
            .fields
            .iter()
            .find(|field| field.key == keys[0])
            .and_then(|field| field_type_at_keys(&field.field_type, &keys[1..])),
        FieldType::Map(inner) => field_type_at_keys(inner, &keys[1..]),
        FieldType::Nullable(inner) => field_type_at_keys(inner, keys),
        _ => None,
    }
}

pub(super) fn field_type_at_tokens(
    field_type: &FieldType,
    tokens: &[PathToken],
) -> Option<FieldType> {
    if tokens.is_empty() {
        return Some(field_type.clone());
    }
    match (&tokens[0], field_type) {
        (PathToken::Key(key), FieldType::Object(node)) => node
            .fields
            .iter()
            .find(|field| field.key == *key)
            .and_then(|field| field_type_at_tokens(&field.field_type, &tokens[1..])),
        (PathToken::Key(_), FieldType::Map(inner)) => field_type_at_tokens(inner, &tokens[1..]),
        (PathToken::Index(_), FieldType::Array(inner)) => field_type_at_tokens(inner, &tokens[1..]),
        (_, FieldType::Nullable(inner)) => field_type_at_tokens(inner, tokens),
        _ => None,
    }
}
