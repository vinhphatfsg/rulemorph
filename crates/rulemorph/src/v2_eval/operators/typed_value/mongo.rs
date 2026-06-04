use serde_json::{Map as JsonMap, Value as JsonValue};

use super::common::*;
use super::options::*;
use crate::error::TransformError;

pub(super) fn encode_mongo_value(
    value: &JsonValue,
    options: &CodecOptions,
    path_elems: &[PathElem<'_>],
    explicit_type: Option<HintType>,
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
        return Ok(JsonValue::Null);
    }
    match hint.as_ref().map(|hint| hint.ty) {
        Some(HintType::ObjectId) => {
            let s = value
                .as_str()
                .ok_or_else(|| expr_error("object_id field requires string", path))?;
            validate_object_id(s, path)?;
            wrapper("$oid", JsonValue::String(s.to_string()))
        }
        Some(HintType::Date) => {
            let s = value
                .as_str()
                .ok_or_else(|| expr_error("date field requires string", path))?;
            if options.mongo_mode == MongoMode::Canonical {
                let millis = rfc3339_epoch_millis(s, path)?;
                mongo_canonical_date_wrapper(millis)
            } else {
                let millis = rfc3339_epoch_millis(s, path)?;
                if millis < 0 {
                    return mongo_canonical_date_wrapper(millis);
                }
                let date = format_rfc3339_millis(s, path)?;
                wrapper("$date", JsonValue::String(date))
            }
        }
        Some(HintType::BinaryBase64) => {
            let s = value
                .as_str()
                .ok_or_else(|| expr_error("binary field requires string", path))?;
            validate_base64(s, path)?;
            let subtype = hint
                .and_then(|hint| hint.subtype)
                .unwrap_or_else(|| "00".to_string());
            validate_binary_subtype(&subtype, path)?;
            let mut inner = JsonMap::new();
            inner.insert("base64".to_string(), JsonValue::String(s.to_string()));
            inner.insert("subType".to_string(), JsonValue::String(subtype));
            wrapper("$binary", JsonValue::Object(inner))
        }
        Some(HintType::Decimal128) => {
            let s = value
                .as_str()
                .ok_or_else(|| expr_error("decimal128 field requires string", path))?;
            validate_mongo_decimal128(s, path)?;
            wrapper("$numberDecimal", JsonValue::String(s.to_string()))
        }
        Some(HintType::Int32) => mongo_number_wrapper(value, "$numberInt", path),
        Some(HintType::Int64) => mongo_number_wrapper(value, "$numberLong", path),
        Some(HintType::Double) => mongo_number_wrapper(value, "$numberDouble", path),
        Some(other) => Err(expr_error(
            format!("field type {:?} is not supported by MongoDB profile", other),
            path,
        )),
        None => match value {
            JsonValue::Object(map) => {
                if let Some((key, inner)) = exactly_one_known_mongo_wrapper(map) {
                    if options.allow_extended_json_passthrough {
                        validate_mongo_passthrough_wrapper(key, inner, options, path)?;
                        return Ok(value.clone());
                    }
                    if options.extended_json_wrapper_objects == WrapperObjectPolicy::RejectUnhinted
                    {
                        return Err(expr_error(
                            "unhinted MongoDB Extended JSON wrapper-shaped object is not allowed",
                            path,
                        ));
                    }
                }
                if map.keys().any(|key| is_known_mongo_wrapper_key(key)) {
                    return Err(expr_error(
                        "malformed MongoDB Extended JSON wrapper-shaped object is not allowed",
                        path,
                    ));
                }
                let mut out = JsonMap::new();
                for (key, item) in map {
                    validate_input_string_bytes(key, path)?;
                    validate_mongo_field_name(key, path)?;
                    if key.starts_with('$') && !options.allow_dollar_prefixed_fields {
                        return Err(expr_error(
                            "MongoDB dollar-prefixed field requires opt-in",
                            path,
                        ));
                    }
                    guard.visit_field(path)?;
                    let mut child = path_elems.to_vec();
                    child.push(PathElem::Key(key));
                    out.insert(
                        key.clone(),
                        encode_mongo_value(item, options, &child, None, guard, path)?,
                    );
                }
                Ok(JsonValue::Object(out))
            }
            JsonValue::Number(_) if options.mongo_mode == MongoMode::Canonical => Err(expr_error(
                "canonical MongoDB Extended JSON requires numeric field type",
                path,
            )),
            JsonValue::Array(items) => {
                let mut out = Vec::with_capacity(items.len());
                for item in items {
                    let mut child = path_elems.to_vec();
                    child.push(PathElem::Index);
                    out.push(encode_mongo_value(
                        item, options, &child, None, guard, path,
                    )?);
                }
                Ok(JsonValue::Array(out))
            }
            _ => Ok(value.clone()),
        },
    }
}

pub(super) fn decode_mongo_value(
    value: &JsonValue,
    options: &CodecOptions,
    path_elems: &[PathElem<'_>],
    guard: &mut ResourceGuard,
    path: &str,
    depth: usize,
) -> Result<JsonValue, TransformError> {
    guard.visit_node(path, depth)?;
    let hint_contract = decode_hint_contract(options, path_elems);
    if let Some((hint_ty, nullable)) = hint_contract {
        match value {
            JsonValue::Null if nullable => return Ok(JsonValue::Null),
            JsonValue::Object(map) if exactly_one_known_mongo_wrapper(map).is_some() => {}
            _ => {
                return Err(expr_error(
                    format!("MongoDB value does not match field type {}", hint_ty.name()),
                    path,
                ));
            }
        }
    }
    match value {
        JsonValue::Array(items) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                let mut child = path_elems.to_vec();
                child.push(PathElem::Index);
                out.push(decode_mongo_value(
                    item,
                    options,
                    &child,
                    guard,
                    path,
                    depth + 1,
                )?);
            }
            Ok(JsonValue::Array(out))
        }
        JsonValue::Object(map) => {
            if let Some((key, inner)) = exactly_one_known_mongo_wrapper(map) {
                let hint = best_hint(options, path_elems);
                return decode_mongo_wrapper(
                    key,
                    inner,
                    options,
                    hint_contract,
                    hint.and_then(|hint| hint.subtype.as_deref()),
                    path,
                );
            }
            if let Some(key) = map.keys().find(|key| is_known_mongo_wrapper_key(key)) {
                return Err(expr_error(
                    format!("malformed MongoDB Extended JSON wrapper: {}", key),
                    path,
                ));
            }
            let mut out = JsonMap::new();
            for (key, item) in map {
                validate_input_string_bytes(key, path)?;
                validate_mongo_field_name(key, path)?;
                if key.starts_with('$') && !options.allow_dollar_prefixed_fields {
                    return Err(expr_error("unknown MongoDB dollar-prefixed field", path));
                }
                guard.visit_field(path)?;
                let mut child = path_elems.to_vec();
                child.push(PathElem::Key(key));
                out.insert(
                    key.clone(),
                    decode_mongo_value(item, options, &child, guard, path, depth + 1)?,
                );
            }
            Ok(JsonValue::Object(out))
        }
        JsonValue::String(value) => {
            validate_input_string_bytes(value, path)?;
            Ok(JsonValue::String(value.clone()))
        }
        JsonValue::Null => Ok(JsonValue::Null),
        _ => Ok(value.clone()),
    }
}

fn validate_mongo_wrapper_matches_hint(
    key: &str,
    hint_contract: Option<(HintType, bool)>,
    path: &str,
) -> Result<(), TransformError> {
    let Some((hint_ty, _)) = hint_contract else {
        return Ok(());
    };
    let expected = match hint_ty {
        HintType::ObjectId => Some("$oid"),
        HintType::Date => Some("$date"),
        HintType::BinaryBase64 => Some("$binary"),
        HintType::Decimal128 => Some("$numberDecimal"),
        HintType::Int32 => Some("$numberInt"),
        HintType::Int64 => Some("$numberLong"),
        HintType::Double => Some("$numberDouble"),
        _ => None,
    };
    if let Some(expected) = expected
        && key != expected
    {
        return Err(expr_error(
            format!(
                "MongoDB Extended JSON wrapper {} does not match field type {}",
                key,
                hint_ty.name()
            ),
            path,
        ));
    }
    Ok(())
}

pub(super) fn decode_mongo_wrapper(
    key: &str,
    value: &JsonValue,
    options: &CodecOptions,
    hint_contract: Option<(HintType, bool)>,
    hint_subtype: Option<&str>,
    path: &str,
) -> Result<JsonValue, TransformError> {
    validate_mongo_wrapper_matches_hint(key, hint_contract, path)?;
    let hint_ty = hint_contract.map(|(ty, _)| ty);
    match key {
        "$oid" => {
            let s = expect_string(value, "$oid must be string", path)?;
            validate_object_id(&s, path)?;
            Ok(JsonValue::String(s))
        }
        "$date" => match value {
            JsonValue::String(s) => format_rfc3339_millis(s, path).map(JsonValue::String),
            JsonValue::Object(map) => {
                if map.len() == 1 && map.contains_key("$numberLong") {
                    let millis = expect_string(
                        map.get("$numberLong").unwrap(),
                        "$date.$numberLong must be string",
                        path,
                    )?;
                    validate_mongo_numeric_wrapper("$numberLong", &millis, path)?;
                    format_epoch_millis_as_rfc3339_millis(&millis, path).map(JsonValue::String)
                } else {
                    Err(expr_error("malformed MongoDB $date wrapper", path))
                }
            }
            _ => Err(expr_error("malformed MongoDB $date wrapper", path)),
        },
        "$numberInt" | "$numberLong" | "$numberDouble" => {
            let s = expect_string(value, "MongoDB numeric wrapper must be string", path)?;
            validate_mongo_numeric_wrapper(key, &s, path)?;
            if key == "$numberDouble" && is_special_double_string(&s) {
                return Ok(JsonValue::String(s));
            }
            if options.number_policy == NumberPolicy::ParseJsonNumberIfSafe
                || (options.decode_mode == DecodeMode::JsonShapeRoundtrip
                    && hint_ty.is_some_and(|ty| {
                        matches!(ty, HintType::Int32 | HintType::Int64 | HintType::Double)
                    }))
            {
                parse_json_number_if_safe(&s, path).map(JsonValue::Number)
            } else {
                Ok(JsonValue::String(s))
            }
        }
        "$numberDecimal" => {
            let s = expect_string(value, "$numberDecimal must be string", path)?;
            validate_mongo_decimal128(&s, path)?;
            Ok(JsonValue::String(s))
        }
        "$binary" => {
            let map = value
                .as_object()
                .ok_or_else(|| expr_error("$binary must be object", path))?;
            if map.len() != 2 || !map.contains_key("base64") || !map.contains_key("subType") {
                return Err(expr_error("malformed MongoDB $binary wrapper", path));
            }
            let base64 = expect_string(
                map.get("base64").unwrap(),
                "$binary.base64 must be string",
                path,
            )?;
            let subtype = expect_string(
                map.get("subType").unwrap(),
                "$binary.subType must be string",
                path,
            )?;
            validate_base64(&base64, path)?;
            validate_binary_subtype(&subtype, path)?;
            if let Some(expected) = hint_subtype {
                validate_binary_subtype(expected, path)?;
                if subtype != expected {
                    return Err(expr_error(
                        "MongoDB binary subtype does not match field type subtype",
                        path,
                    ));
                }
            }
            Ok(JsonValue::String(base64))
        }
        "$regularExpression" | "$timestamp" | "$minKey" | "$maxKey" | "$uuid" => Err(expr_error(
            format!("unsupported MongoDB wrapper: {}", key),
            path,
        )),
        _ => Err(expr_error(
            format!("unknown MongoDB wrapper: {}", key),
            path,
        )),
    }
}

fn validate_mongo_passthrough_wrapper(
    key: &str,
    value: &JsonValue,
    options: &CodecOptions,
    path: &str,
) -> Result<(), TransformError> {
    match key {
        "$oid" | "$date" | "$numberInt" | "$numberLong" | "$numberDouble" | "$numberDecimal"
        | "$binary" => {
            let mut shape_options = options.clone();
            shape_options.decode_mode = DecodeMode::SafeJson;
            shape_options.number_policy = NumberPolicy::String;
            decode_mongo_wrapper(key, value, &shape_options, None, None, path).map(|_| ())
        }
        "$uuid" => validate_mongo_uuid_wrapper(value, path),
        "$timestamp" => validate_mongo_timestamp_wrapper(value, path),
        "$minKey" | "$maxKey" => validate_mongo_key_marker_wrapper(key, value, path),
        "$regularExpression" => validate_mongo_regular_expression_wrapper(value, path),
        _ => Err(expr_error(
            format!("unknown MongoDB wrapper: {}", key),
            path,
        )),
    }
}

fn validate_mongo_uuid_wrapper(value: &JsonValue, path: &str) -> Result<(), TransformError> {
    let s = expect_string(value, "$uuid must be string", path)?;
    validate_input_string_bytes(&s, path)?;
    let bytes = s.as_bytes();
    let valid = bytes.len() == 36
        && [8usize, 13, 18, 23]
            .iter()
            .all(|index| bytes[*index] == b'-')
        && bytes
            .iter()
            .enumerate()
            .all(|(index, byte)| [8usize, 13, 18, 23].contains(&index) || byte.is_ascii_hexdigit());
    if valid {
        Ok(())
    } else {
        Err(expr_error("malformed MongoDB $uuid wrapper", path))
    }
}

fn validate_mongo_timestamp_wrapper(value: &JsonValue, path: &str) -> Result<(), TransformError> {
    let map = value
        .as_object()
        .ok_or_else(|| expr_error("$timestamp must be object", path))?;
    if map.len() != 2 || !map.contains_key("t") || !map.contains_key("i") {
        return Err(expr_error("malformed MongoDB $timestamp wrapper", path));
    }
    expect_u32_json_number(map.get("t").unwrap(), "$timestamp.t must be uint32", path)?;
    expect_u32_json_number(map.get("i").unwrap(), "$timestamp.i must be uint32", path)?;
    Ok(())
}

fn validate_mongo_key_marker_wrapper(
    key: &str,
    value: &JsonValue,
    path: &str,
) -> Result<(), TransformError> {
    if value.as_i64() == Some(1) {
        Ok(())
    } else {
        Err(expr_error(format!("{} must be 1", key), path))
    }
}

fn validate_mongo_regular_expression_wrapper(
    value: &JsonValue,
    path: &str,
) -> Result<(), TransformError> {
    let map = value
        .as_object()
        .ok_or_else(|| expr_error("$regularExpression must be object", path))?;
    if map.len() != 2 || !map.contains_key("pattern") || !map.contains_key("options") {
        return Err(expr_error(
            "malformed MongoDB $regularExpression wrapper",
            path,
        ));
    }
    let pattern = expect_string(
        map.get("pattern").unwrap(),
        "$regularExpression.pattern must be string",
        path,
    )?;
    let options = expect_string(
        map.get("options").unwrap(),
        "$regularExpression.options must be string",
        path,
    )?;
    validate_input_string_bytes(&pattern, path)?;
    validate_input_string_bytes(&options, path)?;
    let mut seen = [false; 256];
    for byte in options.bytes() {
        if !matches!(byte, b'i' | b'm' | b's' | b'x') || seen[byte as usize] {
            return Err(expr_error(
                "$regularExpression.options contains unsupported flags",
                path,
            ));
        }
        seen[byte as usize] = true;
    }
    Ok(())
}

fn expect_u32_json_number(
    value: &JsonValue,
    message: &str,
    path: &str,
) -> Result<u32, TransformError> {
    let n = value.as_u64().ok_or_else(|| expr_error(message, path))?;
    u32::try_from(n).map_err(|_| expr_error(message, path))
}
