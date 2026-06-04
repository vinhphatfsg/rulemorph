use serde_json::{Map as JsonMap, Value as JsonValue};

use super::common::*;
use super::options::*;
use crate::error::TransformError;
use std::collections::BTreeSet;

pub(super) fn encode_dynamodb_value(
    value: &JsonValue,
    options: &CodecOptions,
    path_elems: &[PathElem<'_>],
    explicit_type: Option<HintType>,
    path: &str,
    guard: &mut ResourceGuard,
) -> Result<JsonValue, TransformError> {
    guard.visit_node(path, path_elems.len())?;
    validate_json_value_input_string_bytes(value, path)?;
    let hint = explicit_type
        .map(|ty| Hint {
            path: HintPath(Vec::new()),
            ty,
            nullable: false,
            on_missing: OnMissing::Error,
            format: None,
            input: None,
            output_precision: None,
            subtype: None,
        })
        .or_else(|| best_hint(options, path_elems).cloned());
    if value.is_null() && hint.as_ref().is_some_and(|hint| hint.nullable) {
        return single_key("NULL", JsonValue::Bool(true));
    }
    match hint.as_ref().map(|hint| hint.ty) {
        Some(HintType::StringSet) => encode_string_set(value, "SS", path),
        Some(HintType::NumberSet) | Some(HintType::NumberStringSet) => {
            encode_number_set(value, path)
        }
        Some(HintType::BinarySetBase64) => encode_binary_set(value, path),
        Some(HintType::NumberString) => {
            let s = value
                .as_str()
                .ok_or_else(|| expr_error("number_string field requires string", path))?;
            validate_dynamodb_number(s, path)?;
            single_key("N", JsonValue::String(s.to_string()))
        }
        Some(HintType::BinaryBase64) => {
            let s = value
                .as_str()
                .ok_or_else(|| expr_error("binary_base64 field requires string", path))?;
            validate_base64(s, path)?;
            single_key("B", JsonValue::String(s.to_string()))
        }
        Some(other) => Err(expr_error(
            format!(
                "field type {:?} is not supported by DynamoDB profile",
                other
            ),
            path,
        )),
        None => match value {
            JsonValue::String(s) => single_key("S", JsonValue::String(s.clone())),
            JsonValue::Number(n) => {
                let s = n.to_string();
                validate_dynamodb_number(&s, path)?;
                single_key("N", JsonValue::String(s))
            }
            JsonValue::Bool(b) => single_key("BOOL", JsonValue::Bool(*b)),
            JsonValue::Null => single_key("NULL", JsonValue::Bool(true)),
            JsonValue::Array(items) => {
                let mut out = Vec::with_capacity(items.len());
                for item in items {
                    let mut child = path_elems.to_vec();
                    child.push(PathElem::Index);
                    out.push(encode_dynamodb_value(
                        item, options, &child, None, path, guard,
                    )?);
                }
                single_key("L", JsonValue::Array(out))
            }
            JsonValue::Object(map) => {
                let mut out = JsonMap::new();
                for (key, item) in map {
                    validate_dynamodb_key(key, path)?;
                    guard.visit_field(path)?;
                    let mut child = path_elems.to_vec();
                    child.push(PathElem::Key(key));
                    out.insert(
                        key.clone(),
                        encode_dynamodb_value(item, options, &child, None, path, guard)?,
                    );
                }
                single_key("M", JsonValue::Object(out))
            }
        },
    }
}

pub(super) fn encode_string_set(
    value: &JsonValue,
    tag: &str,
    path: &str,
) -> Result<JsonValue, TransformError> {
    let items = array_of_strings(value, "set field requires array of strings", path)?;
    if items.is_empty() {
        return Err(expr_error("DynamoDB set must not be empty", path));
    }
    let mut seen = BTreeSet::new();
    for item in &items {
        if !seen.insert(item.clone()) {
            return Err(expr_error("DynamoDB set contains duplicate value", path));
        }
    }
    single_key(
        tag,
        JsonValue::Array(items.into_iter().map(JsonValue::String).collect()),
    )
}

pub(super) fn encode_number_set(
    value: &JsonValue,
    path: &str,
) -> Result<JsonValue, TransformError> {
    let values = value
        .as_array()
        .ok_or_else(|| expr_error("number set field requires array", path))?;
    if values.is_empty() {
        return Err(expr_error("DynamoDB set must not be empty", path));
    }
    let mut out = Vec::new();
    let mut seen = BTreeSet::new();
    for value in values {
        let s = match value {
            JsonValue::Number(n) => n.to_string(),
            JsonValue::String(s) => s.clone(),
            _ => {
                return Err(expr_error(
                    "number set entries must be numbers or strings",
                    path,
                ));
            }
        };
        validate_dynamodb_number(&s, path)?;
        let canonical = canonical_decimal(&s, path)?;
        if !seen.insert(canonical) {
            return Err(expr_error(
                "DynamoDB number set contains duplicate value",
                path,
            ));
        }
        out.push(JsonValue::String(s));
    }
    single_key("NS", JsonValue::Array(out))
}

pub(super) fn encode_binary_set(
    value: &JsonValue,
    path: &str,
) -> Result<JsonValue, TransformError> {
    let items = array_of_strings(value, "binary set field requires array of strings", path)?;
    if items.is_empty() {
        return Err(expr_error("DynamoDB set must not be empty", path));
    }
    let mut seen = BTreeSet::new();
    for item in &items {
        let bytes = decode_base64(item, path)?;
        if !seen.insert(bytes) {
            return Err(expr_error(
                "DynamoDB binary set contains duplicate value",
                path,
            ));
        }
    }
    single_key(
        "BS",
        JsonValue::Array(items.into_iter().map(JsonValue::String).collect()),
    )
}

pub(super) fn validate_string_set_values(
    values: &[String],
    path: &str,
) -> Result<(), TransformError> {
    if values.is_empty() {
        return Err(expr_error("DynamoDB set must not be empty", path));
    }
    let mut seen = BTreeSet::new();
    for value in values {
        if !seen.insert(value.clone()) {
            return Err(expr_error("DynamoDB set contains duplicate value", path));
        }
    }
    Ok(())
}

pub(super) fn validate_number_set_values(
    values: &[String],
    path: &str,
) -> Result<(), TransformError> {
    if values.is_empty() {
        return Err(expr_error("DynamoDB set must not be empty", path));
    }
    let mut seen = BTreeSet::new();
    for value in values {
        validate_dynamodb_number(value, path)?;
        let canonical = canonical_decimal(value, path)?;
        if !seen.insert(canonical) {
            return Err(expr_error(
                "DynamoDB number set contains duplicate value",
                path,
            ));
        }
    }
    Ok(())
}

pub(super) fn validate_binary_set_values(
    values: &[String],
    path: &str,
) -> Result<(), TransformError> {
    if values.is_empty() {
        return Err(expr_error("DynamoDB set must not be empty", path));
    }
    let mut seen = BTreeSet::new();
    for value in values {
        let bytes = decode_base64(value, path)?;
        if !seen.insert(bytes) {
            return Err(expr_error(
                "DynamoDB binary set contains duplicate value",
                path,
            ));
        }
    }
    Ok(())
}

fn validate_dynamodb_tag_matches_hint(
    tag: &str,
    hint_ty: Option<HintType>,
    path: &str,
) -> Result<(), TransformError> {
    let Some(hint_ty) = hint_ty else {
        return Ok(());
    };
    let expected = match hint_ty {
        HintType::StringSet => Some("SS"),
        HintType::NumberSet | HintType::NumberStringSet => Some("NS"),
        HintType::BinarySetBase64 => Some("BS"),
        HintType::NumberString => Some("N"),
        HintType::BinaryBase64 => Some("B"),
        _ => None,
    };
    if let Some(expected) = expected
        && tag != expected
    {
        return Err(expr_error(
            format!(
                "DynamoDB AttributeValue tag {} does not match field type {}",
                tag,
                hint_ty.name()
            ),
            path,
        ));
    }
    Ok(())
}

pub(super) fn decode_dynamodb_attribute(
    input: &JsonValue,
    options: &CodecOptions,
    path_elems: &[PathElem<'_>],
    path: &str,
    guard: &mut ResourceGuard,
) -> Result<JsonValue, TransformError> {
    guard.visit_node(path, path_elems.len())?;
    let map = input
        .as_object()
        .ok_or_else(|| expr_error("DynamoDB AttributeValue must be an object", path))?;
    if map.len() != 1 {
        return Err(expr_error(
            "DynamoDB AttributeValue must contain exactly one tag",
            path,
        ));
    }
    let (tag, value) = map.iter().next().unwrap();
    let hint_ty = decode_hint_type(options, path_elems);
    validate_dynamodb_tag_matches_hint(tag, hint_ty, path)?;
    match tag.as_str() {
        "S" => Ok(JsonValue::String(expect_string(
            value,
            "S value must be string",
            path,
        )?)),
        "N" => decode_provider_number(
            expect_string(value, "N value must be string", path)?,
            options,
            hint_ty,
            path,
        ),
        "B" => {
            let s = expect_string(value, "B value must be string", path)?;
            validate_base64(&s, path)?;
            Ok(JsonValue::String(s))
        }
        "BOOL" => {
            Ok(JsonValue::Bool(value.as_bool().ok_or_else(|| {
                expr_error("BOOL value must be boolean", path)
            })?))
        }
        "NULL" => {
            if value.as_bool() == Some(true) {
                Ok(JsonValue::Null)
            } else {
                Err(expr_error("NULL value must be true", path))
            }
        }
        "L" => {
            let arr = value
                .as_array()
                .ok_or_else(|| expr_error("L value must be array", path))?;
            let mut out = Vec::with_capacity(arr.len());
            for item in arr {
                let mut child = path_elems.to_vec();
                child.push(PathElem::Index);
                out.push(decode_dynamodb_attribute(
                    item, options, &child, path, guard,
                )?);
            }
            Ok(JsonValue::Array(out))
        }
        "M" => {
            let obj = value
                .as_object()
                .ok_or_else(|| expr_error("M value must be object", path))?;
            let mut out = JsonMap::new();
            for (key, item) in obj {
                guard.visit_field(path)?;
                validate_dynamodb_key(key, path)?;
                let mut child = path_elems.to_vec();
                child.push(PathElem::Key(key));
                out.insert(
                    key.clone(),
                    decode_dynamodb_attribute(item, options, &child, path, guard)?,
                );
            }
            Ok(JsonValue::Object(out))
        }
        "SS" => {
            let values = array_of_strings(value, "SS value must be array of strings", path)?;
            validate_string_set_values(&values, path)?;
            Ok(JsonValue::Array(
                values.into_iter().map(JsonValue::String).collect(),
            ))
        }
        "NS" => {
            let values = array_of_strings(value, "NS value must be array of strings", path)?;
            validate_number_set_values(&values, path)?;
            let mut out = Vec::new();
            for value in values {
                out.push(decode_provider_number(value, options, hint_ty, path)?);
            }
            Ok(JsonValue::Array(out))
        }
        "BS" => {
            let values = array_of_strings(value, "BS value must be array of strings", path)?;
            validate_binary_set_values(&values, path)?;
            Ok(JsonValue::Array(
                values.into_iter().map(JsonValue::String).collect(),
            ))
        }
        _ => Err(expr_error(
            format!("unknown DynamoDB AttributeValue tag: {}", tag),
            path,
        )),
    }
}

pub(super) fn decode_provider_number(
    value: String,
    options: &CodecOptions,
    hint_ty: Option<HintType>,
    path: &str,
) -> Result<JsonValue, TransformError> {
    validate_dynamodb_number(&value, path)?;
    if options.number_policy == NumberPolicy::ParseJsonNumberIfSafe
        || (options.decode_mode == DecodeMode::JsonShapeRoundtrip
            && hint_ty.is_some_and(|ty| {
                matches!(
                    ty,
                    HintType::NumberString
                        | HintType::NumberStringSet
                        | HintType::NumberSet
                        | HintType::Integer
                        | HintType::Int32
                        | HintType::Int64
                        | HintType::Double
                )
            }))
    {
        return parse_json_number_if_safe(&value, path).map(JsonValue::Number);
    }
    Ok(JsonValue::String(value))
}
