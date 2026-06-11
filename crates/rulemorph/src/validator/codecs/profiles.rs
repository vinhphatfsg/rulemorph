use super::*;

pub(super) fn validate_profile_specific_options(
    map: &JsonMap<String, JsonValue>,
    base_path: &str,
    ctx: &mut ValidationCtx<'_>,
) {
    let Some(profile) = map.get("profile").and_then(JsonValue::as_str) else {
        return;
    };
    if profile != "mongo_extended_json" {
        for key in [
            "mode",
            "extended_json_wrapper_objects",
            "allow_dollar_prefixed_fields",
            "allow_extended_json_passthrough",
        ] {
            if map.contains_key(key) {
                ctx.push(
                    ErrorCode::InvalidExprShape,
                    "MongoDB options require mongo_extended_json profile",
                    base_path,
                );
            }
        }
    }
}

pub(super) fn validate_profile_hint_types(
    map: &JsonMap<String, JsonValue>,
    base_path: &str,
    ctx: &mut ValidationCtx<'_>,
) {
    for ty in collect_hint_types(map) {
        validate_profile_hint_type(map, &JsonValue::String(ty), "field type", base_path, ctx);
    }
}

fn collect_hint_types(map: &JsonMap<String, JsonValue>) -> Vec<String> {
    let mut types = Vec::new();
    if let Some(field_types) = map.get("field_types").and_then(JsonValue::as_object) {
        for spec in field_types.values() {
            collect_hint_type_from_spec(spec, &mut types);
        }
    }
    if let Some(hints) = map.get("hints").and_then(JsonValue::as_array) {
        for spec in hints {
            collect_hint_type_from_spec(spec, &mut types);
        }
    }
    if let Some(sets) = map.get("sets").and_then(JsonValue::as_object) {
        for kind in sets.keys() {
            match kind.as_str() {
                "string" => types.push("string_set".to_string()),
                "number" => types.push("number_set".to_string()),
                "binary" => types.push("binary_set_base64".to_string()),
                _ => {}
            }
        }
    }
    if map.contains_key("number_strings") {
        types.push("number_string".to_string());
    }
    if map.contains_key("binary_base64") {
        types.push("binary_base64".to_string());
    }
    types
}

fn collect_hint_type_from_spec(spec: &JsonValue, types: &mut Vec<String>) {
    match spec {
        JsonValue::String(raw) => types.push(raw.clone()),
        JsonValue::Object(map) => {
            if let Some(raw) = map.get("type").and_then(JsonValue::as_str) {
                types.push(raw.to_string());
            }
        }
        _ => {}
    }
}

pub(super) fn validate_profile_hint_type(
    map: &JsonMap<String, JsonValue>,
    value: &JsonValue,
    label: &str,
    base_path: &str,
    ctx: &mut ValidationCtx<'_>,
) {
    let Some(profile) = map.get("profile").and_then(JsonValue::as_str) else {
        return;
    };
    let Some(raw) = value.as_str() else {
        return;
    };
    if label == "type" && !profile_supports_root_type(profile) {
        ctx.push(
            ErrorCode::InvalidExprShape,
            &format!("type is not supported by {} profile", profile),
            base_path,
        );
        return;
    }
    let allowed = match profile {
        "dynamodb_attribute_value" | "dynamodb_item" => matches!(
            raw,
            "string_set"
                | "number_set"
                | "binary_set_base64"
                | "binary_set"
                | "number_string"
                | "number_string_set"
                | "binary_base64"
        ),
        "firestore_value" | "firestore_fields" | "firestore_document" => matches!(
            raw,
            "integer" | "timestamp" | "bytes_base64" | "binary_base64" | "reference" | "geo_point"
        ),
        "mongo_extended_json" => matches!(
            raw,
            "object_id" | "date" | "binary_base64" | "decimal128" | "int32" | "int64" | "double"
        ),
        _ => return,
    };
    if !allowed {
        ctx.push(
            ErrorCode::InvalidExprShape,
            &format!("{} is not supported by {} profile: {}", label, profile, raw),
            base_path,
        );
    }
}

pub(super) fn profile_supports_root_type(profile: &str) -> bool {
    matches!(
        profile,
        "dynamodb_attribute_value" | "firestore_value" | "mongo_extended_json"
    )
}
