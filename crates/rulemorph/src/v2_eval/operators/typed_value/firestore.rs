use serde_json::{Map as JsonMap, Value as JsonValue};

use super::FIRESTORE_VALUE_MAX_BYTES;
use super::common::*;
use super::dynamodb::decode_provider_number;
use super::options::*;
use crate::error::TransformError;

pub(super) fn encode_firestore_fields(
    input: &JsonValue,
    options: &CodecOptions,
    path_elems: &[PathElem<'_>],
    guard: &mut ResourceGuard,
    path: &str,
) -> Result<JsonValue, TransformError> {
    let obj = input
        .as_object()
        .ok_or_else(|| expr_error("firestore fields profile requires root object", path))?;
    let mut out = JsonMap::new();
    for (key, value) in obj {
        validate_firestore_field_name(key, path)?;
        guard.visit_field(path)?;
        let mut child = path_elems.to_vec();
        child.push(PathElem::Key(key));
        out.insert(
            key.clone(),
            encode_firestore_value(value, options, &child, None, None, guard, path)?,
        );
    }
    Ok(JsonValue::Object(out))
}

pub(super) fn encode_firestore_value(
    value: &JsonValue,
    options: &CodecOptions,
    path_elems: &[PathElem<'_>],
    explicit_type: Option<HintType>,
    parent_array: Option<bool>,
    guard: &mut ResourceGuard,
    path: &str,
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
        return single_key("nullValue", JsonValue::Null);
    }
    match hint.as_ref().map(|hint| hint.ty) {
        Some(HintType::Integer) => {
            let s = firestore_integer_string(value, path)?;
            single_key("integerValue", JsonValue::String(s))
        }
        Some(HintType::Timestamp) => {
            let s = value
                .as_str()
                .ok_or_else(|| expr_error("timestamp field requires string", path))?;
            let formatted = format_rfc3339_micros(s, path)?;
            single_key("timestampValue", JsonValue::String(formatted))
        }
        Some(HintType::BytesBase64) | Some(HintType::BinaryBase64) => {
            let s = value
                .as_str()
                .ok_or_else(|| expr_error("bytes field requires string", path))?;
            let bytes = decode_base64(s, path)?;
            if bytes.len() > FIRESTORE_VALUE_MAX_BYTES {
                return Err(expr_error("Firestore bytesValue exceeds size limit", path));
            }
            single_key("bytesValue", JsonValue::String(s.to_string()))
        }
        Some(HintType::Reference) => {
            let s = value
                .as_str()
                .ok_or_else(|| expr_error("reference field requires string", path))?;
            validate_firestore_reference(s, path)?;
            single_key("referenceValue", JsonValue::String(s.to_string()))
        }
        Some(HintType::GeoPoint) => {
            validate_firestore_geo_point(value, path)?;
            single_key("geoPointValue", value.clone())
        }
        Some(other) => Err(expr_error(
            format!(
                "field type {:?} is not supported by Firestore profile",
                other
            ),
            path,
        )),
        None => match value {
            JsonValue::Null => single_key("nullValue", JsonValue::Null),
            JsonValue::Bool(b) => single_key("booleanValue", JsonValue::Bool(*b)),
            JsonValue::Number(n) => single_key("doubleValue", JsonValue::Number(n.clone())),
            JsonValue::String(s) => {
                if s.len() > FIRESTORE_VALUE_MAX_BYTES {
                    return Err(expr_error("Firestore stringValue exceeds size limit", path));
                }
                single_key("stringValue", JsonValue::String(s.clone()))
            }
            JsonValue::Array(items) => {
                if parent_array == Some(true) {
                    return Err(expr_error(
                        "Firestore direct nested array is not allowed",
                        path,
                    ));
                }
                let mut values = Vec::with_capacity(items.len());
                for item in items {
                    let mut child = path_elems.to_vec();
                    child.push(PathElem::Index);
                    values.push(encode_firestore_value(
                        item,
                        options,
                        &child,
                        None,
                        Some(true),
                        guard,
                        path,
                    )?);
                }
                let mut inner = JsonMap::new();
                inner.insert("values".to_string(), JsonValue::Array(values));
                single_key("arrayValue", JsonValue::Object(inner))
            }
            JsonValue::Object(map) => {
                let mut fields = JsonMap::new();
                for (key, item) in map {
                    validate_firestore_field_name(key, path)?;
                    guard.visit_field(path)?;
                    let mut child = path_elems.to_vec();
                    child.push(PathElem::Key(key));
                    fields.insert(
                        key.clone(),
                        encode_firestore_value(
                            item,
                            options,
                            &child,
                            None,
                            Some(false),
                            guard,
                            path,
                        )?,
                    );
                }
                let mut inner = JsonMap::new();
                inner.insert("fields".to_string(), JsonValue::Object(fields));
                single_key("mapValue", JsonValue::Object(inner))
            }
        },
    }
}

pub(super) fn decode_firestore_fields(
    input: &JsonValue,
    options: &CodecOptions,
    path_elems: &[PathElem<'_>],
    guard: &mut ResourceGuard,
    path: &str,
) -> Result<JsonValue, TransformError> {
    let map = input
        .as_object()
        .ok_or_else(|| expr_error("Firestore fields must be an object", path))?;
    let mut out = JsonMap::new();
    for (key, value) in map {
        validate_firestore_field_name(key, path)?;
        guard.visit_field(path)?;
        let mut child = path_elems.to_vec();
        child.push(PathElem::Key(key));
        out.insert(
            key.clone(),
            decode_firestore_value(value, options, &child, false, guard, path)?,
        );
    }
    Ok(JsonValue::Object(out))
}

fn validate_firestore_value_tag_matches_hint(
    tag: &str,
    hint_contract: Option<(HintType, bool)>,
    path: &str,
) -> Result<(), TransformError> {
    let Some((hint_ty, nullable)) = hint_contract else {
        return Ok(());
    };
    if tag == "nullValue" {
        if nullable {
            return Ok(());
        }
        return Err(expr_error(
            format!(
                "Firestore Value field {} does not match field type {}",
                tag,
                hint_ty.name()
            ),
            path,
        ));
    }
    let expected = match hint_ty {
        HintType::Integer => Some("integerValue"),
        HintType::Timestamp => Some("timestampValue"),
        HintType::BytesBase64 | HintType::BinaryBase64 => Some("bytesValue"),
        HintType::Reference => Some("referenceValue"),
        HintType::GeoPoint => Some("geoPointValue"),
        _ => None,
    };
    if let Some(expected) = expected
        && tag != expected
    {
        return Err(expr_error(
            format!(
                "Firestore Value field {} does not match field type {}",
                tag,
                hint_ty.name()
            ),
            path,
        ));
    }
    Ok(())
}

pub(super) fn decode_firestore_value(
    input: &JsonValue,
    options: &CodecOptions,
    path_elems: &[PathElem<'_>],
    parent_array: bool,
    guard: &mut ResourceGuard,
    path: &str,
) -> Result<JsonValue, TransformError> {
    guard.visit_node(path, path_elems.len())?;
    let map = input
        .as_object()
        .ok_or_else(|| expr_error("Firestore Value must be object", path))?;
    if map.len() != 1 {
        return Err(expr_error(
            "Firestore Value must contain exactly one value field",
            path,
        ));
    }
    let (key, value) = map.iter().next().unwrap();
    validate_firestore_value_tag_matches_hint(
        key,
        decode_hint_contract(options, path_elems),
        path,
    )?;
    match key.as_str() {
        "nullValue" => {
            if value.is_null() {
                Ok(JsonValue::Null)
            } else {
                Err(expr_error("Firestore nullValue must be null", path))
            }
        }
        "booleanValue" => {
            Ok(JsonValue::Bool(value.as_bool().ok_or_else(|| {
                expr_error("booleanValue must be boolean", path)
            })?))
        }
        "integerValue" => {
            let s = expect_string(value, "integerValue must be string", path)?;
            validate_int64_string(&s, "integerValue", path)?;
            decode_provider_number(s, options, decode_hint_type(options, path_elems), path)
        }
        "doubleValue" => match value {
            JsonValue::Number(n) => Ok(JsonValue::Number(n.clone())),
            JsonValue::String(s) if is_special_double_string(s) => {
                validate_firestore_double_string(s, path)?;
                Ok(JsonValue::String(s.clone()))
            }
            JsonValue::String(s)
                if options.number_policy == NumberPolicy::ParseJsonNumberIfSafe =>
            {
                parse_json_number_if_safe(s, path).map(JsonValue::Number)
            }
            JsonValue::String(s) => {
                validate_firestore_double_string(s, path)?;
                Ok(JsonValue::String(s.clone()))
            }
            _ => Err(expr_error("doubleValue must be number or string", path)),
        },
        "bytesValue" => {
            let s = expect_string(value, "Firestore bytesValue must be string", path)?;
            let bytes = decode_base64(&s, path)?;
            if bytes.len() > FIRESTORE_VALUE_MAX_BYTES {
                return Err(expr_error("Firestore bytesValue exceeds size limit", path));
            }
            Ok(JsonValue::String(s))
        }
        "timestampValue" => {
            let s = expect_string(value, "Firestore timestampValue must be string", path)?;
            format_rfc3339_micros(&s, path).map(JsonValue::String)
        }
        "stringValue" => {
            let s = expect_string(value, "Firestore string field must be string", path)?;
            if s.len() > FIRESTORE_VALUE_MAX_BYTES {
                return Err(expr_error("Firestore stringValue exceeds size limit", path));
            }
            Ok(JsonValue::String(s))
        }
        "referenceValue" => {
            let s = expect_string(value, "Firestore string field must be string", path)?;
            validate_firestore_reference(&s, path)?;
            Ok(JsonValue::String(s))
        }
        "geoPointValue" => {
            validate_firestore_geo_point(value, path)?;
            Ok(value.clone())
        }
        "arrayValue" => {
            if parent_array {
                return Err(expr_error(
                    "Firestore direct nested array is not allowed",
                    path,
                ));
            }
            let obj = value
                .as_object()
                .ok_or_else(|| expr_error("arrayValue must be object", path))?;
            if obj.keys().any(|key| key != "values") {
                return Err(expr_error("arrayValue contains unknown field", path));
            }
            let values = match obj.get("values") {
                Some(JsonValue::Array(values)) => Some(values),
                Some(_) => return Err(expr_error("arrayValue.values must be array", path)),
                None => None,
            };
            let mut out = Vec::new();
            if let Some(values) = values {
                for item in values {
                    let mut child = path_elems.to_vec();
                    child.push(PathElem::Index);
                    out.push(decode_firestore_value(
                        item, options, &child, true, guard, path,
                    )?);
                }
            }
            Ok(JsonValue::Array(out))
        }
        "mapValue" => {
            let obj = value
                .as_object()
                .ok_or_else(|| expr_error("mapValue must be object", path))?;
            if obj.keys().any(|key| key != "fields") {
                return Err(expr_error("mapValue contains unknown field", path));
            }
            if let Some(fields) = obj.get("fields") {
                decode_firestore_fields(fields, options, path_elems, guard, path)
            } else {
                Ok(JsonValue::Object(JsonMap::new()))
            }
        }
        other => Err(expr_error(
            format!("unknown Firestore Value field: {}", other),
            path,
        )),
    }
}
