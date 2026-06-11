use super::hints::{
    validate_combined_hint_count, validate_dynamodb_sugar_values, validate_field_types_value,
    validate_hint_type_value, validate_hints_value,
};
use super::path::{
    validate_duplicate_hint_paths, validate_root_hint_paths, validate_root_type_hint_conflicts,
};
use super::profiles::{
    validate_profile_hint_type, validate_profile_hint_types, validate_profile_specific_options,
};
use super::*;

pub(super) fn validate_codec_binding(
    value: &JsonValue,
    base_path: &str,
    ctx: &mut ValidationCtx<'_>,
) {
    let Some(map) = value.as_object() else {
        ctx.push(
            ErrorCode::InvalidExprShape,
            "codec binding must be an object",
            base_path,
        );
        return;
    };
    if map.contains_key("codec") {
        ctx.push(
            ErrorCode::InvalidExprShape,
            "codec binding cannot reference another codec",
            base_path,
        );
    }
    validate_codec_options(value, base_path, ctx);
}

pub(super) fn validate_codec_options(
    value: &JsonValue,
    base_path: &str,
    ctx: &mut ValidationCtx<'_>,
) {
    let map = match value {
        JsonValue::String(raw) => {
            validate_profile_name(raw, base_path, ctx);
            return;
        }
        JsonValue::Object(map) => map,
        _ => {
            ctx.push(
                ErrorCode::InvalidExprShape,
                "typed value options must be a profile string or object",
                base_path,
            );
            return;
        }
    };
    validate_option_keys(map, base_path, ctx);
    if let Some(JsonValue::Object(decode)) = map.get("decode") {
        validate_decode_option_keys(decode, base_path, ctx);
        validate_decode_option_values(decode, base_path, ctx);
    } else if map.contains_key("decode") {
        ctx.push(
            ErrorCode::InvalidExprShape,
            "decode must be an object",
            base_path,
        );
    }
    if map.contains_key("style") || map.contains_key("types") {
        ctx.push(
            ErrorCode::InvalidExprShape,
            "inline typed value codecs are experimental and not enabled",
            base_path,
        );
    }
    if map.contains_key("codec") && map.contains_key("profile") {
        ctx.push(
            ErrorCode::InvalidExprShape,
            "codec and profile cannot be used together",
            base_path,
        );
    }
    if let Some(codec) = map.get("codec") {
        if !codec.is_string() {
            ctx.push(
                ErrorCode::InvalidExprShape,
                "codec must be a string",
                base_path,
            );
        }
    } else if !map.contains_key("profile") {
        ctx.push(
            ErrorCode::InvalidExprShape,
            "typed value options require profile or codec",
            base_path,
        );
    }
    validate_codec_option_values(map, base_path, ctx);
}

pub(super) fn validate_resolved_codec_options(
    value: &JsonValue,
    base_path: &str,
    ctx: &mut ValidationCtx<'_>,
) {
    let Some(map) = value.as_object() else {
        return;
    };
    let Some(codec) = map.get("codec").and_then(JsonValue::as_str) else {
        return;
    };
    let Some(codec_value) = ctx.codec_binding(codec).cloned() else {
        ctx.push(
            ErrorCode::InvalidExprShape,
            &format!("unknown codec binding: {}", codec),
            base_path,
        );
        return;
    };
    let Some(codec_map) = codec_value.as_object() else {
        return;
    };

    let mut merged = JsonMap::new();
    for (key, value) in codec_map {
        if key != "codec" {
            merged.insert(key.clone(), value.clone());
        }
    }
    for (key, value) in map {
        if key != "codec" {
            merged.insert(key.clone(), value.clone());
        }
    }

    validate_profile_specific_options(&merged, base_path, ctx);
    if let Some(value) = merged.get("type") {
        validate_profile_hint_type(&merged, value, "type", base_path, ctx);
    }
    validate_combined_hint_count(&merged, base_path, ctx);
    validate_duplicate_hint_paths(&merged, base_path, ctx);
    validate_root_type_hint_conflicts(&merged, base_path, ctx);
    validate_root_hint_paths(&merged, base_path, ctx);
    validate_profile_hint_types(&merged, base_path, ctx);
}

fn validate_profile_name(raw: &str, base_path: &str, ctx: &mut ValidationCtx<'_>) {
    if !matches!(
        raw,
        "dynamodb_attribute_value"
            | "dynamodb_item"
            | "firestore_value"
            | "firestore_fields"
            | "firestore_document"
            | "mongo_extended_json"
    ) {
        ctx.push(
            ErrorCode::InvalidExprShape,
            &format!("unknown typed value profile: {}", raw),
            base_path,
        );
    }
}

fn validate_option_keys(
    map: &JsonMap<String, JsonValue>,
    base_path: &str,
    ctx: &mut ValidationCtx<'_>,
) {
    for key in map.keys() {
        if !matches!(
            key.as_str(),
            "profile"
                | "codec"
                | "on_missing"
                | "type"
                | "field_types"
                | "hints"
                | "decode"
                | "decode_strictness"
                | "number_policy"
                | "binary_policy"
                | "timestamp_policy"
                | "object_id_policy"
                | "set_policy"
                | "mode"
                | "extended_json_wrapper_objects"
                | "allow_dollar_prefixed_fields"
                | "allow_extended_json_passthrough"
                | "style"
                | "types"
                | "sets"
                | "number_strings"
                | "binary_base64"
        ) {
            ctx.push(
                ErrorCode::InvalidExprShape,
                &format!("unknown typed value option: {}", key),
                base_path,
            );
        }
    }
}

fn validate_decode_option_keys(
    map: &JsonMap<String, JsonValue>,
    base_path: &str,
    ctx: &mut ValidationCtx<'_>,
) {
    for key in map.keys() {
        if !matches!(
            key.as_str(),
            "mode"
                | "number_policy"
                | "binary_policy"
                | "timestamp_policy"
                | "object_id_policy"
                | "set_policy"
        ) {
            ctx.push(
                ErrorCode::InvalidExprShape,
                &format!("unknown typed value decode option: {}", key),
                base_path,
            );
        }
    }
}

fn validate_codec_option_values(
    map: &JsonMap<String, JsonValue>,
    base_path: &str,
    ctx: &mut ValidationCtx<'_>,
) {
    validate_optional_string_choice(
        map,
        "profile",
        &[
            "dynamodb_attribute_value",
            "dynamodb_item",
            "firestore_value",
            "firestore_fields",
            "firestore_document",
            "mongo_extended_json",
        ],
        "unknown typed value profile",
        base_path,
        ctx,
    );
    validate_optional_string_choice(
        map,
        "on_missing",
        &["error", "propagate", "ignore"],
        "unsupported on_missing",
        base_path,
        ctx,
    );
    validate_optional_string_choice(
        map,
        "decode_strictness",
        &["provider_strict"],
        "unsupported decode_strictness",
        base_path,
        ctx,
    );
    validate_optional_string_choice(
        map,
        "number_policy",
        &["string", "parse_json_number_if_safe"],
        "unsupported typed value number_policy",
        base_path,
        ctx,
    );
    validate_optional_string_choice(
        map,
        "binary_policy",
        &["base64"],
        "unsupported binary_policy",
        base_path,
        ctx,
    );
    validate_optional_string_choice(
        map,
        "timestamp_policy",
        &["string"],
        "unsupported timestamp_policy",
        base_path,
        ctx,
    );
    validate_optional_string_choice(
        map,
        "object_id_policy",
        &["string"],
        "unsupported object_id_policy",
        base_path,
        ctx,
    );
    validate_optional_string_choice(
        map,
        "set_policy",
        &["array"],
        "unsupported set_policy",
        base_path,
        ctx,
    );
    validate_optional_string_choice(
        map,
        "mode",
        &["relaxed", "canonical"],
        "unsupported typed value mode",
        base_path,
        ctx,
    );
    validate_optional_string_choice(
        map,
        "extended_json_wrapper_objects",
        &["reject_unhinted"],
        "unsupported extended_json_wrapper_objects",
        base_path,
        ctx,
    );
    validate_optional_bool(map, "allow_dollar_prefixed_fields", base_path, ctx);
    validate_optional_bool(map, "allow_extended_json_passthrough", base_path, ctx);
    validate_profile_specific_options(map, base_path, ctx);
    if let Some(value) = map.get("type") {
        validate_hint_type_value(value, "type", base_path, ctx);
        validate_profile_hint_type(map, value, "type", base_path, ctx);
    }
    if let Some(value) = map.get("field_types") {
        validate_field_types_value(value, base_path, ctx);
    }
    if let Some(value) = map.get("hints") {
        validate_hints_value(value, base_path, ctx);
    }
    validate_dynamodb_sugar_values(map, base_path, ctx);
    validate_combined_hint_count(map, base_path, ctx);
    validate_duplicate_hint_paths(map, base_path, ctx);
    validate_root_type_hint_conflicts(map, base_path, ctx);
    validate_root_hint_paths(map, base_path, ctx);
    validate_profile_hint_types(map, base_path, ctx);
}

fn validate_decode_option_values(
    map: &JsonMap<String, JsonValue>,
    base_path: &str,
    ctx: &mut ValidationCtx<'_>,
) {
    validate_optional_string_choice(
        map,
        "mode",
        &["safe_json", "json_shape_roundtrip"],
        "unsupported decode mode",
        base_path,
        ctx,
    );
    validate_optional_string_choice(
        map,
        "number_policy",
        &["string", "parse_json_number_if_safe"],
        "unsupported typed value number_policy",
        base_path,
        ctx,
    );
    validate_optional_string_choice(
        map,
        "binary_policy",
        &["base64"],
        "unsupported binary_policy",
        base_path,
        ctx,
    );
    validate_optional_string_choice(
        map,
        "timestamp_policy",
        &["string"],
        "unsupported timestamp_policy",
        base_path,
        ctx,
    );
    validate_optional_string_choice(
        map,
        "object_id_policy",
        &["string"],
        "unsupported object_id_policy",
        base_path,
        ctx,
    );
    validate_optional_string_choice(
        map,
        "set_policy",
        &["array"],
        "unsupported set_policy",
        base_path,
        ctx,
    );
}

pub(super) fn validate_optional_string_choice(
    map: &JsonMap<String, JsonValue>,
    key: &str,
    allowed: &[&str],
    label: &str,
    base_path: &str,
    ctx: &mut ValidationCtx<'_>,
) {
    let Some(value) = map.get(key) else {
        return;
    };
    let Some(raw) = value.as_str() else {
        ctx.push(
            ErrorCode::InvalidExprShape,
            &format!("{} must be a string", key),
            base_path,
        );
        return;
    };
    if !allowed.contains(&raw) {
        ctx.push(
            ErrorCode::InvalidExprShape,
            &format!("{}: {}", label, raw),
            base_path,
        );
    }
}

pub(super) fn validate_optional_bool(
    map: &JsonMap<String, JsonValue>,
    key: &str,
    base_path: &str,
    ctx: &mut ValidationCtx<'_>,
) {
    if map.get(key).is_some_and(|value| !value.is_boolean()) {
        ctx.push(
            ErrorCode::InvalidExprShape,
            &format!("{} must be boolean", key),
            base_path,
        );
    }
}
