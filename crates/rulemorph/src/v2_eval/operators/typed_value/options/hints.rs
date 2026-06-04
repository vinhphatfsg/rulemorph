use serde_json::{Map as JsonMap, Value as JsonValue};
use std::collections::BTreeSet;

use super::super::{
    MAX_TYPED_VALUE_CODEC_NAME_BYTES, MAX_TYPED_VALUE_CODECS, MAX_TYPED_VALUE_HINT_PATH_BYTES,
    MAX_TYPED_VALUE_HINT_PATH_TOKENS, expr_error,
};
use super::types::{Hint, HintPath, HintType, OnMissing, PathPart, Profile};
use crate::error::TransformError;

pub(super) fn validate_hint_options(hints: &[Hint], path: &str) -> Result<(), TransformError> {
    for hint in hints {
        match hint.on_missing {
            OnMissing::Error | OnMissing::Propagate | OnMissing::Ignore => {}
        }
        if let Some(input) = hint.input.as_deref()
            && !matches!(input, "rfc3339")
        {
            return Err(expr_error(
                format!("unsupported field type input: {}", input),
                path,
            ));
        }
        if let Some(format) = hint.format.as_deref()
            && !matches!(format, "dynamodb_decimal")
        {
            return Err(expr_error(
                format!("unsupported field type format: {}", format),
                path,
            ));
        }
        if let Some(precision) = hint.output_precision.as_deref() {
            return Err(expr_error(
                format!("unsupported field type output_precision: {}", precision),
                path,
            ));
        }
    }
    Ok(())
}

pub(super) fn validate_profile_hint_type(
    profile: Profile,
    ty: HintType,
    label: &str,
    path: &str,
) -> Result<(), TransformError> {
    let allowed = match profile {
        Profile::DynamoDbAttributeValue | Profile::DynamoDbItem => matches!(
            ty,
            HintType::StringSet
                | HintType::NumberSet
                | HintType::BinarySetBase64
                | HintType::NumberString
                | HintType::NumberStringSet
                | HintType::BinaryBase64
        ),
        Profile::FirestoreValue | Profile::FirestoreFields | Profile::FirestoreDocument => {
            matches!(
                ty,
                HintType::Integer
                    | HintType::Timestamp
                    | HintType::BytesBase64
                    | HintType::BinaryBase64
                    | HintType::Reference
                    | HintType::GeoPoint
            )
        }
        Profile::MongoExtendedJson => matches!(
            ty,
            HintType::ObjectId
                | HintType::Date
                | HintType::Timestamp
                | HintType::BinaryBase64
                | HintType::Decimal128
                | HintType::Int32
                | HintType::Int64
                | HintType::Double
        ),
    };
    if allowed {
        Ok(())
    } else {
        Err(expr_error(
            format!(
                "{} is not supported by {} profile: {}",
                label,
                profile.name(),
                ty.name()
            ),
            path,
        ))
    }
}

pub(super) fn get_string(
    map: &JsonMap<String, JsonValue>,
    key: &str,
    path: &str,
) -> Result<Option<String>, TransformError> {
    match map.get(key) {
        Some(JsonValue::String(value)) => Ok(Some(value.clone())),
        Some(_) => Err(expr_error(format!("{} must be a string", key), path)),
        None => Ok(None),
    }
}

pub(super) fn get_nested_string(
    root: &JsonMap<String, JsonValue>,
    nested: Option<&JsonMap<String, JsonValue>>,
    key: &str,
    path: &str,
) -> Result<Option<String>, TransformError> {
    if let Some(value) = get_string(root, key, path)? {
        return Ok(Some(value));
    }
    if let Some(map) = nested {
        return get_string(map, key, path);
    }
    Ok(None)
}

pub(super) fn get_bool(
    map: &JsonMap<String, JsonValue>,
    key: &str,
    path: &str,
) -> Result<bool, TransformError> {
    match map.get(key) {
        Some(JsonValue::Bool(value)) => Ok(*value),
        Some(_) => Err(expr_error(format!("{} must be boolean", key), path)),
        None => Ok(false),
    }
}

pub(super) fn parse_on_missing(
    value: Option<&str>,
    path: &str,
) -> Result<OnMissing, TransformError> {
    match value {
        None | Some("error") => Ok(OnMissing::Error),
        Some("propagate") => Ok(OnMissing::Propagate),
        Some("ignore") => Ok(OnMissing::Ignore),
        Some(other) => Err(expr_error(
            format!("unsupported on_missing: {}", other),
            path,
        )),
    }
}

pub(super) fn parse_field_types(
    value: &JsonValue,
    hints: &mut Vec<Hint>,
    path: &str,
) -> Result<(), TransformError> {
    let map = value
        .as_object()
        .ok_or_else(|| expr_error("field_types must be an object", path))?;
    for (raw_path, spec) in map {
        let hint = parse_hint_spec(raw_path, spec, path)?;
        hints.push(hint);
    }
    Ok(())
}

pub(super) fn parse_hints(
    value: &JsonValue,
    hints: &mut Vec<Hint>,
    path: &str,
) -> Result<(), TransformError> {
    let items = value
        .as_array()
        .ok_or_else(|| expr_error("hints must be an array", path))?;
    for item in items {
        let map = item
            .as_object()
            .ok_or_else(|| expr_error("hint entry must be an object", path))?;
        let raw_path = get_string(map, "path", path)?
            .ok_or_else(|| expr_error("hint entry requires path", path))?;
        let hint = parse_hint_spec(&raw_path, item, path)?;
        hints.push(hint);
    }
    Ok(())
}

pub(super) fn parse_hint_spec(
    raw_path: &str,
    spec: &JsonValue,
    path: &str,
) -> Result<Hint, TransformError> {
    let mut nullable = false;
    let mut on_missing = OnMissing::Error;
    let mut format = None;
    let mut input = None;
    let mut output_precision = None;
    let mut subtype = None;
    let ty = match spec {
        JsonValue::String(name) => parse_hint_type(name, path)?,
        JsonValue::Object(map) => {
            for key in map.keys() {
                if !matches!(
                    key.as_str(),
                    "path"
                        | "type"
                        | "nullable"
                        | "on_missing"
                        | "format"
                        | "input"
                        | "output_precision"
                        | "subtype"
                ) {
                    return Err(expr_error(
                        format!("unknown field type option: {}", key),
                        path,
                    ));
                }
            }
            nullable = get_bool(map, "nullable", path)?;
            on_missing = parse_on_missing(get_string(map, "on_missing", path)?.as_deref(), path)?;
            format = get_string(map, "format", path)?;
            input = get_string(map, "input", path)?;
            output_precision = get_string(map, "output_precision", path)?;
            subtype = get_string(map, "subtype", path)?;
            let type_value = map
                .get("type")
                .ok_or_else(|| expr_error("field type object requires type", path))?;
            parse_hint_type_value(type_value, path)?
        }
        _ => return Err(expr_error("field type must be a string or object", path)),
    };
    Ok(Hint {
        path: parse_hint_path(raw_path, path)?,
        ty,
        nullable,
        on_missing,
        format,
        input,
        output_precision,
        subtype,
    })
}

pub(super) fn parse_hint_type_value(
    value: &JsonValue,
    path: &str,
) -> Result<HintType, TransformError> {
    let name = value
        .as_str()
        .ok_or_else(|| expr_error("field type must be a string", path))?;
    parse_hint_type(name, path)
}

pub(super) fn parse_hint_type(name: &str, path: &str) -> Result<HintType, TransformError> {
    match name {
        "string_set" => Ok(HintType::StringSet),
        "number_set" => Ok(HintType::NumberSet),
        "binary_set_base64" | "binary_set" => Ok(HintType::BinarySetBase64),
        "number_string" => Ok(HintType::NumberString),
        "number_string_set" => Ok(HintType::NumberStringSet),
        "binary_base64" => Ok(HintType::BinaryBase64),
        "integer" => Ok(HintType::Integer),
        "timestamp" => Ok(HintType::Timestamp),
        "bytes_base64" => Ok(HintType::BytesBase64),
        "reference" => Ok(HintType::Reference),
        "geo_point" => Ok(HintType::GeoPoint),
        "object_id" => Ok(HintType::ObjectId),
        "date" => Ok(HintType::Date),
        "decimal128" => Ok(HintType::Decimal128),
        "int32" => Ok(HintType::Int32),
        "int64" => Ok(HintType::Int64),
        "double" => Ok(HintType::Double),
        other => Err(expr_error(
            format!("unsupported field type: {}", other),
            path,
        )),
    }
}

pub(super) fn parse_dynamodb_sugar(
    map: &JsonMap<String, JsonValue>,
    hints: &mut Vec<Hint>,
    path: &str,
) -> Result<(), TransformError> {
    if let Some(sets) = map.get("sets") {
        let sets = sets
            .as_object()
            .ok_or_else(|| expr_error("sets must be an object", path))?;
        for (kind, paths) in sets {
            let ty = match kind.as_str() {
                "string" => HintType::StringSet,
                "number" => HintType::NumberSet,
                "binary" => HintType::BinarySetBase64,
                _ => return Err(expr_error(format!("unsupported set kind: {}", kind), path)),
            };
            for raw_path in parse_path_array(paths, "sets", path)? {
                hints.push(simple_hint(raw_path, ty, path)?);
            }
        }
    }
    if let Some(paths) = map.get("number_strings") {
        for raw_path in parse_path_array(paths, "number_strings", path)? {
            let mut hint = simple_hint(raw_path, HintType::NumberString, path)?;
            hint.format = Some("dynamodb_decimal".to_string());
            hints.push(hint);
        }
    }
    if let Some(paths) = map.get("binary_base64") {
        for raw_path in parse_path_array(paths, "binary_base64", path)? {
            hints.push(simple_hint(raw_path, HintType::BinaryBase64, path)?);
        }
    }
    Ok(())
}

pub(super) fn parse_path_array(
    value: &JsonValue,
    name: &str,
    path: &str,
) -> Result<Vec<String>, TransformError> {
    let items = value
        .as_array()
        .ok_or_else(|| expr_error(format!("{} must be an array", name), path))?;
    items
        .iter()
        .map(|item| {
            item.as_str()
                .map(ToOwned::to_owned)
                .ok_or_else(|| expr_error(format!("{} entries must be strings", name), path))
        })
        .collect()
}

pub(super) fn simple_hint(
    raw_path: String,
    ty: HintType,
    path: &str,
) -> Result<Hint, TransformError> {
    Ok(Hint {
        path: parse_hint_path(&raw_path, path)?,
        ty,
        nullable: false,
        on_missing: OnMissing::Error,
        format: None,
        input: None,
        output_precision: None,
        subtype: None,
    })
}

pub(super) fn parse_hint_path(raw: &str, path: &str) -> Result<HintPath, TransformError> {
    if raw.len() > MAX_TYPED_VALUE_HINT_PATH_BYTES {
        return Err(expr_error(
            "typed value hint path bytes exceed configured limit",
            path,
        ));
    }
    if raw == "." {
        return Ok(HintPath(Vec::new()));
    }
    if raw.is_empty() {
        return Err(expr_error("hint path must not be empty", path));
    }
    let chars: Vec<char> = raw.chars().collect();
    let mut parts = Vec::new();
    let mut i = 0usize;
    while i < chars.len() {
        if chars[i] == '.' {
            if i == 0 || i + 1 == chars.len() || chars[i + 1] == '.' {
                return Err(expr_error("hint path contains an empty segment", path));
            }
            i += 1;
            continue;
        }
        if chars[i] == '[' {
            if i + 2 < chars.len() && chars[i + 1] == '*' && chars[i + 2] == ']' {
                parts.push(PathPart::AnyIndex);
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
                                return Err(expr_error(
                                    "hint path contains an empty segment",
                                    path,
                                ));
                            }
                            parts.push(PathPart::Key(key));
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
                    return Err(expr_error("invalid quoted hint path", path));
                }
                continue;
            }
            return Err(expr_error("invalid hint path bracket syntax", path));
        }
        let start = i;
        while i < chars.len() && chars[i] != '.' && chars[i] != '[' {
            i += 1;
        }
        let key: String = chars[start..i].iter().collect();
        if key.is_empty() {
            return Err(expr_error("hint path contains an empty segment", path));
        }
        parts.push(PathPart::Key(key));
    }
    if parts.len() > MAX_TYPED_VALUE_HINT_PATH_TOKENS {
        return Err(expr_error(
            "typed value hint path token count exceeds configured limit",
            path,
        ));
    }
    Ok(HintPath(parts))
}

pub(super) fn validate_codec_metadata(
    count: usize,
    name: &str,
    path: &str,
) -> Result<(), TransformError> {
    if count > MAX_TYPED_VALUE_CODECS {
        return Err(expr_error(
            "typed value codec count exceeds configured limit",
            path,
        ));
    }
    if name.len() > MAX_TYPED_VALUE_CODEC_NAME_BYTES {
        return Err(expr_error(
            "typed value codec name bytes exceed configured limit",
            path,
        ));
    }
    Ok(())
}

pub(super) fn validate_duplicate_hints(hints: &[Hint], path: &str) -> Result<(), TransformError> {
    let mut seen = BTreeSet::new();
    for hint in hints {
        if !seen.insert(hint.path.clone()) {
            return Err(expr_error("duplicate field type path", path));
        }
    }
    Ok(())
}
