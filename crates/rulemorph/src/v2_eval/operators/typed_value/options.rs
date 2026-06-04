use serde_json::{Map as JsonMap, Value as JsonValue};

mod hints;
mod types;

pub(super) use types::{
    CodecOptions, DecodeMode, Hint, HintPath, HintType, MongoMode, NumberPolicy, OnMissing,
    PathPart, Profile, WrapperObjectPolicy,
};

use super::super::super::{EvalValue, V2EvalContext, eval_v2_expr};
use super::{MAX_TYPED_VALUE_HINTS, expr_error};
use crate::error::TransformError;
use crate::v2_model::V2OpStep;
use hints::*;

pub(super) fn parse_options<'a>(
    op_step: &V2OpStep,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
) -> Result<CodecOptions, TransformError> {
    let mut raw = match op_step.args.len() {
        0 => JsonValue::Object(JsonMap::new()),
        1 => match eval_v2_expr(
            &op_step.args[0],
            record,
            context,
            out,
            &format!("{}.args[0]", path),
            ctx,
        )? {
            EvalValue::Missing => return Err(expr_error("typed value options are missing", path)),
            EvalValue::Value(value) => value,
        },
        _ => {
            return Err(expr_error(
                "typed value operation accepts at most one argument",
                path,
            ));
        }
    };

    if let JsonValue::String(profile) = raw {
        let mut map = JsonMap::new();
        map.insert("profile".to_string(), JsonValue::String(profile));
        raw = JsonValue::Object(map);
    }

    let map = raw.as_object().ok_or_else(|| {
        expr_error(
            "typed value options must be a profile string or object",
            path,
        )
    })?;
    validate_option_keys(map, path)?;
    if map.contains_key("style") || map.contains_key("types") {
        return Err(expr_error(
            "inline typed value codecs are experimental and not enabled",
            path,
        ));
    }
    if map.contains_key("codec") && map.contains_key("profile") {
        return Err(expr_error(
            "codec and profile cannot be used together",
            path,
        ));
    }

    let mut merged = JsonMap::new();
    if let Some(codec_name) = map.get("codec").and_then(JsonValue::as_str) {
        let rule = ctx
            .rule()
            .ok_or_else(|| expr_error("codec binding requires rule context", path))?;
        validate_codec_metadata(rule.codecs.len(), codec_name, path)?;
        let codec_value = rule
            .codecs
            .get(codec_name)
            .ok_or_else(|| expr_error(format!("unknown codec binding: {}", codec_name), path))?;
        let codec_map = codec_value
            .as_object()
            .ok_or_else(|| expr_error("codec binding must be an object", path))?;
        validate_option_keys(codec_map, path)?;
        for key in codec_map.keys() {
            if key == "codec" {
                return Err(expr_error(
                    "codec binding cannot reference another codec",
                    path,
                ));
            }
        }
        for (key, value) in codec_map {
            merged.insert(key.clone(), value.clone());
        }
    }
    for (key, value) in map {
        if key != "codec" {
            merged.insert(key.clone(), value.clone());
        }
    }
    if merged.contains_key("style") || merged.contains_key("types") {
        return Err(expr_error(
            "inline typed value codecs are experimental and not enabled",
            path,
        ));
    }

    let profile_name = get_string(&merged, "profile", path)?
        .ok_or_else(|| expr_error("typed value profile is required", path))?;
    let profile = Profile::parse(&profile_name, path)?;
    if !profile.is_mongo()
        && [
            "mode",
            "extended_json_wrapper_objects",
            "allow_dollar_prefixed_fields",
            "allow_extended_json_passthrough",
        ]
        .iter()
        .any(|key| merged.contains_key(*key))
    {
        return Err(expr_error(
            "MongoDB options require mongo_extended_json profile",
            path,
        ));
    }
    if get_string(&merged, "decode_strictness", path)?
        .as_deref()
        .is_some_and(|value| value != "provider_strict")
    {
        return Err(expr_error("unsupported decode_strictness", path));
    }

    let decode_map = match merged.get("decode") {
        Some(JsonValue::Object(map)) => {
            validate_decode_option_keys(map, path)?;
            Some(map)
        }
        Some(_) => return Err(expr_error("decode must be an object", path)),
        None => None,
    };
    let decode_mode = match decode_map
        .map(|map| get_string(map, "mode", path))
        .transpose()?
        .flatten()
        .as_deref()
    {
        None | Some("safe_json") => DecodeMode::SafeJson,
        Some("json_shape_roundtrip") => DecodeMode::JsonShapeRoundtrip,
        Some(other) => {
            return Err(expr_error(
                format!("unsupported decode mode: {}", other),
                path,
            ));
        }
    };
    let number_policy =
        match get_nested_string(&merged, decode_map, "number_policy", path)?.as_deref() {
            None | Some("string") => NumberPolicy::String,
            Some("parse_json_number_if_safe") => NumberPolicy::ParseJsonNumberIfSafe,
            Some(other) => {
                return Err(expr_error(
                    format!("unsupported typed value number_policy: {}", other),
                    path,
                ));
            }
        };
    for key in [
        "binary_policy",
        "timestamp_policy",
        "object_id_policy",
        "set_policy",
    ] {
        if let Some(value) = get_nested_string(&merged, decode_map, key, path)? {
            let supported = match key {
                "binary_policy" => value == "base64",
                "timestamp_policy" => value == "string",
                "object_id_policy" => value == "string",
                "set_policy" => value == "array",
                _ => false,
            };
            if !supported {
                return Err(expr_error(format!("unsupported {}: {}", key, value), path));
            }
        }
    }

    let on_missing = parse_on_missing(get_string(&merged, "on_missing", path)?.as_deref(), path)?;
    let mongo_mode = match get_string(&merged, "mode", path)?.as_deref() {
        None | Some("relaxed") => MongoMode::Relaxed,
        Some("canonical") => MongoMode::Canonical,
        Some(other) => {
            return Err(expr_error(
                format!("unsupported typed value mode: {}", other),
                path,
            ));
        }
    };
    let extended_json_wrapper_objects =
        match get_string(&merged, "extended_json_wrapper_objects", path)?.as_deref() {
            None | Some("reject_unhinted") => WrapperObjectPolicy::RejectUnhinted,
            Some(other) => {
                return Err(expr_error(
                    format!("unsupported extended_json_wrapper_objects: {}", other),
                    path,
                ));
            }
        };

    let mut hints = Vec::new();
    if let Some(field_types) = merged.get("field_types") {
        parse_field_types(field_types, &mut hints, path)?;
    }
    if let Some(hints_value) = merged.get("hints") {
        parse_hints(hints_value, &mut hints, path)?;
    }
    parse_dynamodb_sugar(&merged, &mut hints, path)?;
    if hints.len() > MAX_TYPED_VALUE_HINTS {
        return Err(expr_error("typed value hint count exceeds limit", path));
    }
    validate_duplicate_hints(&hints, path)?;
    validate_hint_options(&hints, path)?;

    let root_type = match merged.get("type") {
        Some(value) => Some(parse_hint_type_value(value, path)?),
        None => None,
    };
    if let Some(root_type) = root_type {
        if !profile.supports_root_type() {
            return Err(expr_error(
                format!("type is not supported by {} profile", profile.name()),
                path,
            ));
        }
        validate_profile_hint_type(profile, root_type, "type", path)?;
    }
    if !profile.supports_root_type() {
        for hint in &hints {
            if hint.path.0.is_empty() {
                return Err(expr_error(
                    format!(
                        "root field type path is not supported by {} profile",
                        profile.name()
                    ),
                    path,
                ));
            }
        }
    }
    for hint in &hints {
        validate_profile_hint_type(profile, hint.ty, "field type", path)?;
    }

    Ok(CodecOptions {
        profile,
        root_type,
        hints,
        on_missing,
        decode_mode,
        number_policy,
        mongo_mode,
        extended_json_wrapper_objects,
        allow_dollar_prefixed_fields: get_bool(&merged, "allow_dollar_prefixed_fields", path)?,
        allow_extended_json_passthrough: get_bool(
            &merged,
            "allow_extended_json_passthrough",
            path,
        )?,
    })
}

fn validate_option_keys(
    map: &JsonMap<String, JsonValue>,
    path: &str,
) -> Result<(), TransformError> {
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
            return Err(expr_error(
                format!("unknown typed value option: {}", key),
                path,
            ));
        }
    }
    Ok(())
}

fn validate_decode_option_keys(
    map: &JsonMap<String, JsonValue>,
    path: &str,
) -> Result<(), TransformError> {
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
            return Err(expr_error(
                format!("unknown typed value decode option: {}", key),
                path,
            ));
        }
    }
    Ok(())
}
