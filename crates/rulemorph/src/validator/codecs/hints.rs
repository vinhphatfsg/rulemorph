use super::options::{validate_optional_bool, validate_optional_string_choice};
use super::path::validate_hint_path;
use super::*;

pub(super) fn validate_field_types_value(
    value: &JsonValue,
    base_path: &str,
    ctx: &mut ValidationCtx<'_>,
) {
    let Some(map) = value.as_object() else {
        ctx.push(
            ErrorCode::InvalidExprShape,
            "field_types must be an object",
            base_path,
        );
        return;
    };
    for (raw_path, spec) in map {
        validate_hint_path(raw_path, base_path, ctx);
        validate_hint_spec(spec, base_path, ctx);
    }
}

pub(super) fn validate_hints_value(
    value: &JsonValue,
    base_path: &str,
    ctx: &mut ValidationCtx<'_>,
) {
    let Some(items) = value.as_array() else {
        ctx.push(
            ErrorCode::InvalidExprShape,
            "hints must be an array",
            base_path,
        );
        return;
    };
    for item in items {
        let Some(map) = item.as_object() else {
            ctx.push(
                ErrorCode::InvalidExprShape,
                "hint entry must be an object",
                base_path,
            );
            continue;
        };
        match map.get("path").and_then(JsonValue::as_str) {
            Some(raw_path) => validate_hint_path(raw_path, base_path, ctx),
            None => ctx.push(
                ErrorCode::InvalidExprShape,
                "hint entry requires path",
                base_path,
            ),
        }
        validate_hint_spec(item, base_path, ctx);
    }
}

pub(super) fn validate_combined_hint_count(
    map: &JsonMap<String, JsonValue>,
    base_path: &str,
    ctx: &mut ValidationCtx<'_>,
) {
    let count = map
        .get("field_types")
        .and_then(JsonValue::as_object)
        .map_or(0, JsonMap::len)
        + map
            .get("hints")
            .and_then(JsonValue::as_array)
            .map_or(0, Vec::len)
        + count_dynamodb_sugar_hints(map);
    if count > MAX_TYPED_VALUE_HINTS {
        ctx.push(
            ErrorCode::InvalidExprShape,
            "typed value hint count exceeds limit",
            base_path,
        );
    }
}

fn count_dynamodb_sugar_hints(map: &JsonMap<String, JsonValue>) -> usize {
    let set_count = map
        .get("sets")
        .and_then(JsonValue::as_object)
        .map_or(0, |sets| {
            sets.values()
                .map(|paths| paths.as_array().map_or(0, Vec::len))
                .sum()
        });
    set_count
        + map
            .get("number_strings")
            .and_then(JsonValue::as_array)
            .map_or(0, Vec::len)
        + map
            .get("binary_base64")
            .and_then(JsonValue::as_array)
            .map_or(0, Vec::len)
}

fn validate_hint_spec(spec: &JsonValue, base_path: &str, ctx: &mut ValidationCtx<'_>) {
    match spec {
        JsonValue::String(_) => validate_hint_type_value(spec, "field type", base_path, ctx),
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
                    ctx.push(
                        ErrorCode::InvalidExprShape,
                        &format!("unknown field type option: {}", key),
                        base_path,
                    );
                }
            }
            validate_optional_bool(map, "nullable", base_path, ctx);
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
                "format",
                &["dynamodb_decimal"],
                "unsupported field type format",
                base_path,
                ctx,
            );
            if map.get("subtype").is_some_and(|value| !value.is_string()) {
                ctx.push(
                    ErrorCode::InvalidExprShape,
                    "subtype must be a string",
                    base_path,
                );
            }
            validate_optional_string_choice(
                map,
                "input",
                &["rfc3339"],
                "unsupported field type input",
                base_path,
                ctx,
            );
            validate_optional_string_choice(
                map,
                "output_precision",
                &[],
                "unsupported field type output_precision",
                base_path,
                ctx,
            );
            match map.get("type") {
                Some(value) => validate_hint_type_value(value, "field type", base_path, ctx),
                None => ctx.push(
                    ErrorCode::InvalidExprShape,
                    "field type object requires type",
                    base_path,
                ),
            }
        }
        _ => ctx.push(
            ErrorCode::InvalidExprShape,
            "field type must be a string or object",
            base_path,
        ),
    }
}

pub(super) fn validate_hint_type_value(
    value: &JsonValue,
    label: &str,
    base_path: &str,
    ctx: &mut ValidationCtx<'_>,
) {
    let Some(raw) = value.as_str() else {
        ctx.push(
            ErrorCode::InvalidExprShape,
            &format!("{} must be a string", label),
            base_path,
        );
        return;
    };
    if !matches!(
        raw,
        "string_set"
            | "number_set"
            | "binary_set_base64"
            | "binary_set"
            | "number_string"
            | "number_string_set"
            | "binary_base64"
            | "integer"
            | "timestamp"
            | "bytes_base64"
            | "reference"
            | "geo_point"
            | "object_id"
            | "date"
            | "decimal128"
            | "int32"
            | "int64"
            | "double"
    ) {
        ctx.push(
            ErrorCode::InvalidExprShape,
            &format!("unsupported field type: {}", raw),
            base_path,
        );
    }
}

pub(super) fn validate_dynamodb_sugar_values(
    map: &JsonMap<String, JsonValue>,
    base_path: &str,
    ctx: &mut ValidationCtx<'_>,
) {
    if let Some(sets) = map.get("sets") {
        let Some(sets) = sets.as_object() else {
            ctx.push(
                ErrorCode::InvalidExprShape,
                "sets must be an object",
                base_path,
            );
            return;
        };
        for (kind, paths) in sets {
            if !matches!(kind.as_str(), "string" | "number" | "binary") {
                ctx.push(
                    ErrorCode::InvalidExprShape,
                    &format!("unsupported set kind: {}", kind),
                    base_path,
                );
            }
            validate_path_array(paths, "sets", base_path, ctx);
        }
    }
    if let Some(paths) = map.get("number_strings") {
        validate_path_array(paths, "number_strings", base_path, ctx);
    }
    if let Some(paths) = map.get("binary_base64") {
        validate_path_array(paths, "binary_base64", base_path, ctx);
    }
}

fn validate_path_array(
    value: &JsonValue,
    name: &str,
    base_path: &str,
    ctx: &mut ValidationCtx<'_>,
) {
    let Some(items) = value.as_array() else {
        ctx.push(
            ErrorCode::InvalidExprShape,
            &format!("{} must be an array", name),
            base_path,
        );
        return;
    };
    for item in items {
        match item.as_str() {
            Some(raw_path) => validate_hint_path(raw_path, base_path, ctx),
            None => ctx.push(
                ErrorCode::InvalidExprShape,
                &format!("{} entries must be strings", name),
                base_path,
            ),
        }
    }
}
