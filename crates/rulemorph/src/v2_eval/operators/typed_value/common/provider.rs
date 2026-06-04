use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{Map as JsonMap, Value as JsonValue};

use super::numeric::validate_mongo_numeric_wrapper;
use super::{expr_error, wrapper};
use crate::error::TransformError;

use super::super::FIRESTORE_FIELD_NAME_MAX_BYTES;

pub(in crate::v2_eval::operators::typed_value) fn firestore_integer_string(
    value: &JsonValue,
    path: &str,
) -> Result<String, TransformError> {
    match value {
        JsonValue::Number(number) => {
            if let Some(i) = number.as_i64() {
                Ok(i.to_string())
            } else {
                Err(expr_error(
                    "integer field requires int64-compatible value",
                    path,
                ))
            }
        }
        JsonValue::String(s) => {
            s.parse::<i64>()
                .map_err(|_| expr_error("integer field requires int64 string", path))?;
            Ok(s.clone())
        }
        _ => Err(expr_error("integer field requires number or string", path)),
    }
}

pub(in crate::v2_eval::operators::typed_value) fn validate_firestore_field_name(
    key: &str,
    path: &str,
) -> Result<(), TransformError> {
    if key.is_empty() || key.len() > FIRESTORE_FIELD_NAME_MAX_BYTES {
        return Err(expr_error(
            "Firestore field name violates length constraint",
            path,
        ));
    }
    if key.starts_with("__") && key.ends_with("__") {
        return Err(expr_error(
            "Firestore reserved field name is not allowed",
            path,
        ));
    }
    Ok(())
}

pub(in crate::v2_eval::operators::typed_value) fn format_rfc3339_micros(
    raw: &str,
    path: &str,
) -> Result<String, TransformError> {
    let dt = DateTime::parse_from_rfc3339(raw)
        .map_err(|_| expr_error("timestamp must be RFC3339", path))?
        .with_timezone(&Utc);
    Ok(dt.to_rfc3339_opts(SecondsFormat::Micros, true))
}

pub(in crate::v2_eval::operators::typed_value) fn format_rfc3339_millis(
    raw: &str,
    path: &str,
) -> Result<String, TransformError> {
    let dt = DateTime::parse_from_rfc3339(raw)
        .map_err(|_| expr_error("date must be RFC3339", path))?
        .with_timezone(&Utc);
    Ok(dt.to_rfc3339_opts(SecondsFormat::Millis, true))
}

pub(in crate::v2_eval::operators::typed_value) fn rfc3339_epoch_millis(
    raw: &str,
    path: &str,
) -> Result<i64, TransformError> {
    let dt = DateTime::parse_from_rfc3339(raw)
        .map_err(|_| expr_error("date must be RFC3339", path))?
        .with_timezone(&Utc);
    Ok(dt.timestamp_millis())
}

pub(in crate::v2_eval::operators::typed_value) fn format_epoch_millis_as_rfc3339_millis(
    raw: &str,
    path: &str,
) -> Result<String, TransformError> {
    let millis = raw
        .parse::<i64>()
        .map_err(|_| expr_error("$date.$numberLong must be int64 string", path))?;
    let dt = DateTime::<Utc>::from_timestamp_millis(millis)
        .ok_or_else(|| expr_error("$date.$numberLong is out of range", path))?;
    Ok(dt.to_rfc3339_opts(SecondsFormat::Millis, true))
}

pub(in crate::v2_eval::operators::typed_value) fn mongo_canonical_date_wrapper(
    millis: i64,
) -> Result<JsonValue, TransformError> {
    let mut inner = JsonMap::new();
    inner.insert(
        "$numberLong".to_string(),
        JsonValue::String(millis.to_string()),
    );
    wrapper("$date", JsonValue::Object(inner))
}

pub(in crate::v2_eval::operators::typed_value) fn mongo_number_wrapper(
    value: &JsonValue,
    key: &str,
    path: &str,
) -> Result<JsonValue, TransformError> {
    let s = match value {
        JsonValue::Number(n) => n.to_string(),
        JsonValue::String(s) => s.clone(),
        _ => {
            return Err(expr_error(
                "MongoDB numeric field requires number or string",
                path,
            ));
        }
    };
    validate_mongo_numeric_wrapper(key, &s, path)?;
    wrapper(key, JsonValue::String(s))
}

pub(in crate::v2_eval::operators::typed_value) fn validate_object_id(
    value: &str,
    path: &str,
) -> Result<(), TransformError> {
    if value.len() == 24 && value.bytes().all(|b| b.is_ascii_hexdigit()) {
        Ok(())
    } else {
        Err(expr_error("ObjectId must be 24 hex characters", path))
    }
}

pub(in crate::v2_eval::operators::typed_value) fn validate_binary_subtype(
    value: &str,
    path: &str,
) -> Result<(), TransformError> {
    if (value.len() == 1 || value.len() == 2) && value.bytes().all(|b| b.is_ascii_hexdigit()) {
        Ok(())
    } else {
        Err(expr_error(
            "binary subtype must be one or two hex characters",
            path,
        ))
    }
}

pub(in crate::v2_eval::operators::typed_value) fn validate_firestore_geo_point(
    value: &JsonValue,
    path: &str,
) -> Result<(), TransformError> {
    let map = value
        .as_object()
        .ok_or_else(|| expr_error("geoPointValue must be object", path))?;
    if map.len() != 2 || !map.contains_key("latitude") || !map.contains_key("longitude") {
        return Err(expr_error("malformed Firestore geoPointValue", path));
    }
    let latitude = map
        .get("latitude")
        .and_then(JsonValue::as_f64)
        .ok_or_else(|| expr_error("geoPointValue.latitude must be number", path))?;
    let longitude = map
        .get("longitude")
        .and_then(JsonValue::as_f64)
        .ok_or_else(|| expr_error("geoPointValue.longitude must be number", path))?;
    if !latitude.is_finite() || !(-90.0..=90.0).contains(&latitude) {
        return Err(expr_error("geoPointValue.latitude is out of range", path));
    }
    if !longitude.is_finite() || !(-180.0..=180.0).contains(&longitude) {
        return Err(expr_error("geoPointValue.longitude is out of range", path));
    }
    Ok(())
}

pub(in crate::v2_eval::operators::typed_value) fn validate_firestore_reference(
    value: &str,
    path: &str,
) -> Result<(), TransformError> {
    let parts: Vec<&str> = value.split('/').collect();
    let document_path_len = parts.len().saturating_sub(5);
    if parts.len() < 7
        || parts[0] != "projects"
        || parts[1].is_empty()
        || parts[2] != "databases"
        || parts[3].is_empty()
        || parts[4] != "documents"
        || document_path_len % 2 != 0
        || parts[5..].iter().any(|part| part.is_empty())
    {
        return Err(expr_error("malformed Firestore referenceValue", path));
    }
    Ok(())
}

pub(in crate::v2_eval::operators::typed_value) fn validate_base64(
    value: &str,
    path: &str,
) -> Result<(), TransformError> {
    decode_base64(value, path).map(|_| ())
}

pub(in crate::v2_eval::operators::typed_value) fn decode_base64(
    value: &str,
    path: &str,
) -> Result<Vec<u8>, TransformError> {
    let bytes = value.as_bytes();
    if bytes.len() % 4 != 0 {
        return Err(expr_error(
            "base64 value must be padded to a multiple of 4",
            path,
        ));
    }
    let mut out = Vec::new();
    let mut chunk = [0u8; 4];
    let group_count = bytes.len() / 4;
    for (group_index, group) in bytes.chunks(4).enumerate() {
        for (i, b) in group.iter().enumerate() {
            chunk[i] = match *b {
                b'A'..=b'Z' => *b - b'A',
                b'a'..=b'z' => *b - b'a' + 26,
                b'0'..=b'9' => *b - b'0' + 52,
                b'+' => 62,
                b'/' => 63,
                b'=' => 64,
                _ => return Err(expr_error("invalid base64 character", path)),
            };
        }
        let is_last_group = group_index + 1 == group_count;
        if !is_last_group && (chunk[2] == 64 || chunk[3] == 64) {
            return Err(expr_error("invalid base64 padding", path));
        }
        if chunk[0] == 64 || chunk[1] == 64 || (chunk[2] == 64 && chunk[3] != 64) {
            return Err(expr_error("invalid base64 padding", path));
        }
        if chunk[2] == 64 {
            if chunk[1] & 0b0000_1111 != 0 {
                return Err(expr_error("non-canonical base64 padding bits", path));
            }
        } else if chunk[3] == 64 && chunk[2] & 0b0000_0011 != 0 {
            return Err(expr_error("non-canonical base64 padding bits", path));
        }
        out.push((chunk[0] << 2) | (chunk[1] >> 4));
        if chunk[2] != 64 {
            out.push((chunk[1] << 4) | (chunk[2] >> 2));
        }
        if chunk[3] != 64 {
            out.push((chunk[2] << 6) | chunk[3]);
        }
    }
    Ok(out)
}

pub(in crate::v2_eval::operators::typed_value) fn is_known_mongo_wrapper_object(
    map: &JsonMap<String, JsonValue>,
) -> bool {
    map.len() == 1
        && map
            .keys()
            .next()
            .is_some_and(|key| is_known_mongo_wrapper_key(key))
}

pub(in crate::v2_eval::operators::typed_value) fn exactly_one_known_mongo_wrapper<'a>(
    map: &'a JsonMap<String, JsonValue>,
) -> Option<(&'a str, &'a JsonValue)> {
    if map.len() == 1 {
        let (key, value) = map.iter().next().unwrap();
        if is_known_mongo_wrapper_key(key) {
            return Some((key.as_str(), value));
        }
    }
    None
}

pub(in crate::v2_eval::operators::typed_value) fn is_known_mongo_wrapper_key(key: &str) -> bool {
    matches!(
        key,
        "$oid"
            | "$date"
            | "$numberInt"
            | "$numberLong"
            | "$numberDouble"
            | "$numberDecimal"
            | "$binary"
            | "$regularExpression"
            | "$timestamp"
            | "$minKey"
            | "$maxKey"
            | "$uuid"
    )
}
