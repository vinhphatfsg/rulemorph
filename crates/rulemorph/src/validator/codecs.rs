use serde_json::{Map as JsonMap, Value as JsonValue};
use std::collections::BTreeSet;

use crate::error::ErrorCode;
use crate::model::{Expr, Mapping, RuleFile, V2RuleStep};
use crate::v2_model::{V2CallArg, V2Condition, V2Expr, V2OpStep, V2Pipe, V2Start, V2Step};
use crate::v2_parser::{parse_v2_condition, parse_v2_expr};

use super::ValidationCtx;
use super::v2_expr::expr_to_json_value;

const MAX_TYPED_VALUE_HINTS: usize = 1024;
const MAX_TYPED_VALUE_HINT_PATH_BYTES: usize = 4096;
const MAX_TYPED_VALUE_HINT_PATH_TOKENS: usize = 256;
const MAX_TYPED_VALUE_CODECS: usize = 1024;
const MAX_TYPED_VALUE_CODEC_NAME_BYTES: usize = 256;

pub(super) fn validate_codecs(rule: &RuleFile, ctx: &mut ValidationCtx<'_>) {
    if !rule.codecs.is_empty() && rule.version != 2 {
        ctx.push(
            ErrorCode::InvalidStep,
            "codecs is only supported in version 2",
            "codecs",
        );
        return;
    }

    if rule.codecs.len() > MAX_TYPED_VALUE_CODECS {
        ctx.push(
            ErrorCode::InvalidExprShape,
            "typed value codec count exceeds configured limit",
            "codecs",
        );
    }
    for (name, codec) in &rule.codecs {
        if name.len() > MAX_TYPED_VALUE_CODEC_NAME_BYTES {
            ctx.push(
                ErrorCode::InvalidExprShape,
                "typed value codec name bytes exceed configured limit",
                &format!("codecs.{}", name),
            );
        }
        validate_codec_binding(codec, &format!("codecs.{}", name), ctx);
    }
    for (name, def) in &rule.defs {
        let base = format!("defs.{}", name);
        if let Some(expr) = &def.expr {
            validate_expr_codec_refs(expr, &format!("{}.expr", base), rule.version, ctx);
        }
        if let Some(mappings) = &def.mappings {
            validate_mapping_codec_refs(mappings, &format!("{}.mappings", base), rule.version, ctx);
        }
    }
    validate_mapping_codec_refs(&rule.mappings, "mappings", rule.version, ctx);
    if let Some(steps) = &rule.steps {
        validate_step_codec_refs(steps, "steps", rule.version, ctx);
    }
    if let Some(expr) = &rule.record_when {
        validate_condition_expr_codec_refs(expr, "record_when", rule.version, ctx);
    }
    if let Some(finalize) = &rule.finalize
        && let Some(filter) = &finalize.filter
    {
        validate_condition_expr_codec_refs(filter, "finalize.filter", rule.version, ctx);
    }
    if let Some(finalize) = &rule.finalize
        && let Some(wrap) = &finalize.wrap
    {
        validate_finalize_wrap_codec_refs(wrap, "finalize.wrap", rule.version, ctx);
    }
}

fn validate_step_codec_refs(
    steps: &[V2RuleStep],
    base_path: &str,
    rule_version: u8,
    ctx: &mut ValidationCtx<'_>,
) {
    for (index, step) in steps.iter().enumerate() {
        let base = format!("{}[{}]", base_path, index);
        if let Some(mappings) = &step.mappings {
            validate_mapping_codec_refs(mappings, &format!("{}.mappings", base), rule_version, ctx);
        }
        if let Some(expr) = &step.record_when {
            validate_condition_expr_codec_refs(
                expr,
                &format!("{}.record_when", base),
                rule_version,
                ctx,
            );
        }
        if let Some(asserts) = &step.asserts {
            for (assert_index, assert) in asserts.iter().enumerate() {
                validate_condition_expr_codec_refs(
                    &assert.when,
                    &format!("{}.asserts[{}].when", base, assert_index),
                    rule_version,
                    ctx,
                );
            }
        }
        if let Some(branch) = &step.branch {
            validate_condition_expr_codec_refs(
                &branch.when,
                &format!("{}.branch.when", base),
                rule_version,
                ctx,
            );
        }
    }
}

fn validate_mapping_codec_refs(
    mappings: &[Mapping],
    base_path: &str,
    rule_version: u8,
    ctx: &mut ValidationCtx<'_>,
) {
    for (index, mapping) in mappings.iter().enumerate() {
        let base = format!("{}[{}]", base_path, index);
        if let Some(expr) = &mapping.expr {
            validate_expr_codec_refs(expr, &format!("{}.expr", base), rule_version, ctx);
        }
        if let Some(when) = &mapping.when {
            validate_condition_expr_codec_refs(when, &format!("{}.when", base), rule_version, ctx);
        }
    }
}

fn validate_expr_codec_refs(
    expr: &Expr,
    base_path: &str,
    rule_version: u8,
    ctx: &mut ValidationCtx<'_>,
) {
    if rule_version != 2 {
        return;
    }
    let Some(raw) = expr_to_json_value(expr) else {
        return;
    };
    let Ok(v2_expr) = parse_v2_expr(&raw) else {
        return;
    };
    validate_v2_expr_codec_refs(&v2_expr, base_path, ctx);
}

fn validate_condition_expr_codec_refs(
    expr: &Expr,
    base_path: &str,
    rule_version: u8,
    ctx: &mut ValidationCtx<'_>,
) {
    if rule_version != 2 {
        return;
    }
    let Some(raw) = expr_to_json_value(expr) else {
        return;
    };
    if let Ok(condition) = parse_v2_condition(&raw) {
        validate_condition_codec_refs(&condition, base_path, ctx);
    }
}

fn validate_finalize_wrap_codec_refs(
    value: &JsonValue,
    base_path: &str,
    rule_version: u8,
    ctx: &mut ValidationCtx<'_>,
) {
    if rule_version != 2 {
        return;
    }
    match value {
        JsonValue::Object(map) => {
            for (key, value) in map {
                validate_finalize_wrap_codec_refs(
                    value,
                    &format!("{}.{}", base_path, key),
                    rule_version,
                    ctx,
                );
            }
        }
        _ => {
            let Ok(v2_expr) = parse_v2_expr(value) else {
                return;
            };
            validate_v2_expr_codec_refs(&v2_expr, base_path, ctx);
        }
    }
}

fn validate_v2_expr_codec_refs(expr: &V2Expr, base_path: &str, ctx: &mut ValidationCtx<'_>) {
    match expr {
        V2Expr::Pipe(pipe) => validate_pipe_codec_refs(pipe, base_path, ctx),
        V2Expr::V1Fallback(_) => {}
    }
}

fn validate_pipe_codec_refs(pipe: &V2Pipe, base_path: &str, ctx: &mut ValidationCtx<'_>) {
    if let V2Start::V1Expr(expr) = &pipe.start {
        validate_expr_codec_refs(expr, &format!("{}[0]", base_path), 2, ctx);
    }
    for (index, step) in pipe.steps.iter().enumerate() {
        let step_path = format!("{}[{}]", base_path, index + 1);
        match step {
            V2Step::Op(op) => validate_op_codec_refs(op, &step_path, ctx),
            V2Step::If(if_step) => {
                validate_condition_codec_refs(&if_step.cond, &format!("{}.cond", step_path), ctx);
                validate_pipe_codec_refs(&if_step.then_branch, &format!("{}.then", step_path), ctx);
                if let Some(else_branch) = &if_step.else_branch {
                    validate_pipe_codec_refs(else_branch, &format!("{}.else", step_path), ctx);
                }
            }
            V2Step::Map(map_step) => {
                for (map_index, map_step) in map_step.steps.iter().enumerate() {
                    validate_v2_step_codec_refs(
                        map_step,
                        &format!("{}.steps[{}]", step_path, map_index),
                        ctx,
                    );
                }
            }
            V2Step::Let(let_step) => {
                for (name, expr) in &let_step.bindings {
                    validate_v2_expr_codec_refs(expr, &format!("{}.{}", step_path, name), ctx);
                }
            }
            V2Step::CustomCall(call) => {
                if let Some(with) = &call.with {
                    for (name, arg) in with {
                        if let V2CallArg::Expr(expr) = arg {
                            validate_v2_expr_codec_refs(
                                expr,
                                &format!("{}.with.{}", step_path, name),
                                ctx,
                            );
                        }
                    }
                }
            }
            V2Step::Ref(_) => {}
        }
    }
}

fn validate_v2_step_codec_refs(step: &V2Step, base_path: &str, ctx: &mut ValidationCtx<'_>) {
    match step {
        V2Step::Op(op) => validate_op_codec_refs(op, base_path, ctx),
        V2Step::If(if_step) => {
            validate_condition_codec_refs(&if_step.cond, &format!("{}.cond", base_path), ctx);
            validate_pipe_codec_refs(&if_step.then_branch, &format!("{}.then", base_path), ctx);
            if let Some(else_branch) = &if_step.else_branch {
                validate_pipe_codec_refs(else_branch, &format!("{}.else", base_path), ctx);
            }
        }
        V2Step::Map(map_step) => {
            for (index, child) in map_step.steps.iter().enumerate() {
                validate_v2_step_codec_refs(child, &format!("{}.steps[{}]", base_path, index), ctx);
            }
        }
        V2Step::Let(let_step) => {
            for (name, expr) in &let_step.bindings {
                validate_v2_expr_codec_refs(expr, &format!("{}.{}", base_path, name), ctx);
            }
        }
        V2Step::CustomCall(call) => {
            if let Some(with) = &call.with {
                for (name, arg) in with {
                    if let V2CallArg::Expr(expr) = arg {
                        validate_v2_expr_codec_refs(
                            expr,
                            &format!("{}.with.{}", base_path, name),
                            ctx,
                        );
                    }
                }
            }
        }
        V2Step::Ref(_) => {}
    }
}

fn validate_condition_codec_refs(
    condition: &V2Condition,
    base_path: &str,
    ctx: &mut ValidationCtx<'_>,
) {
    match condition {
        V2Condition::All(items) | V2Condition::Any(items) => {
            for (index, item) in items.iter().enumerate() {
                validate_condition_codec_refs(item, &format!("{}[{}]", base_path, index), ctx);
            }
        }
        V2Condition::Comparison(comparison) => {
            for (index, arg) in comparison.args.iter().enumerate() {
                validate_v2_expr_codec_refs(arg, &format!("{}.args[{}]", base_path, index), ctx);
            }
        }
        V2Condition::Expr(expr) => validate_v2_expr_codec_refs(expr, base_path, ctx),
    }
}

fn validate_op_codec_refs(op: &V2OpStep, base_path: &str, ctx: &mut ValidationCtx<'_>) {
    if op.op != "to_typed_value" && op.op != "from_typed_value" {
        for (index, arg) in op.args.iter().enumerate() {
            validate_v2_expr_codec_refs(arg, &format!("{}.args[{}]", base_path, index), ctx);
        }
        return;
    }
    let Some(arg) = op.args.first() else {
        return;
    };
    let Some(options) = literal_option_arg(arg) else {
        for (index, arg) in op.args.iter().enumerate() {
            validate_v2_expr_codec_refs(arg, &format!("{}.args[{}]", base_path, index), ctx);
        }
        return;
    };
    validate_codec_options(options, base_path, ctx);
    validate_resolved_codec_options(options, base_path, ctx);
}

fn literal_option_arg(expr: &V2Expr) -> Option<&JsonValue> {
    match expr {
        V2Expr::Pipe(pipe) if pipe.steps.is_empty() => match &pipe.start {
            V2Start::Literal(value) => Some(value),
            _ => None,
        },
        _ => None,
    }
}

fn validate_codec_binding(value: &JsonValue, base_path: &str, ctx: &mut ValidationCtx<'_>) {
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

fn validate_codec_options(value: &JsonValue, base_path: &str, ctx: &mut ValidationCtx<'_>) {
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

fn validate_resolved_codec_options(
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

fn validate_optional_string_choice(
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

fn validate_optional_bool(
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

fn validate_field_types_value(value: &JsonValue, base_path: &str, ctx: &mut ValidationCtx<'_>) {
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

fn validate_hints_value(value: &JsonValue, base_path: &str, ctx: &mut ValidationCtx<'_>) {
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

fn validate_combined_hint_count(
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

fn validate_hint_type_value(
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

fn validate_dynamodb_sugar_values(
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

fn validate_hint_path(raw: &str, base_path: &str, ctx: &mut ValidationCtx<'_>) {
    if let Err(message) = parse_hint_path(raw) {
        ctx.push(ErrorCode::InvalidExprShape, message, base_path);
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
struct ParsedHintPath(Vec<ParsedPathPart>);

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
enum ParsedPathPart {
    Key(String),
    AnyIndex,
}

fn parse_hint_path(raw: &str) -> Result<ParsedHintPath, &'static str> {
    if raw.len() > MAX_TYPED_VALUE_HINT_PATH_BYTES {
        return Err("typed value hint path bytes exceed configured limit");
    }
    if raw == "." {
        return Ok(ParsedHintPath(Vec::new()));
    }
    if raw.is_empty() {
        return Err("hint path must not be empty");
    }
    let chars: Vec<char> = raw.chars().collect();
    let mut parts = Vec::new();
    let mut i = 0usize;
    while i < chars.len() {
        if chars[i] == '.' {
            if i == 0 || i + 1 == chars.len() || chars[i + 1] == '.' {
                return Err("hint path contains an empty segment");
            }
            i += 1;
            continue;
        }
        if chars[i] == '[' {
            if i + 2 < chars.len() && chars[i + 1] == '*' && chars[i + 2] == ']' {
                parts.push(ParsedPathPart::AnyIndex);
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
                                return Err("hint path contains an empty segment");
                            }
                            parts.push(ParsedPathPart::Key(key));
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
                    return Err("invalid quoted hint path");
                }
                continue;
            }
            return Err("invalid hint path bracket syntax");
        }
        let start = i;
        while i < chars.len() && chars[i] != '.' && chars[i] != '[' {
            i += 1;
        }
        let key: String = chars[start..i].iter().collect();
        if key.is_empty() {
            return Err("hint path contains an empty segment");
        }
        parts.push(ParsedPathPart::Key(key));
    }
    if parts.len() > MAX_TYPED_VALUE_HINT_PATH_TOKENS {
        return Err("typed value hint path token count exceeds configured limit");
    }
    Ok(ParsedHintPath(parts))
}

fn validate_profile_specific_options(
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

fn validate_duplicate_hint_paths(
    map: &JsonMap<String, JsonValue>,
    base_path: &str,
    ctx: &mut ValidationCtx<'_>,
) {
    let mut seen = BTreeSet::new();
    for raw_path in collect_hint_paths(map) {
        let Ok(path) = parse_hint_path(&raw_path) else {
            continue;
        };
        if !seen.insert(path) {
            ctx.push(
                ErrorCode::InvalidExprShape,
                "duplicate field type path",
                base_path,
            );
        }
    }
}

fn collect_hint_paths(map: &JsonMap<String, JsonValue>) -> Vec<String> {
    let mut paths = Vec::new();
    if let Some(field_types) = map.get("field_types").and_then(JsonValue::as_object) {
        paths.extend(field_types.keys().cloned());
    }
    if let Some(hints) = map.get("hints").and_then(JsonValue::as_array) {
        for hint in hints {
            if let Some(path) = hint
                .as_object()
                .and_then(|hint| hint.get("path"))
                .and_then(JsonValue::as_str)
            {
                paths.push(path.to_string());
            }
        }
    }
    if let Some(sets) = map.get("sets").and_then(JsonValue::as_object) {
        for set_paths in sets.values() {
            if let Some(items) = set_paths.as_array() {
                paths.extend(
                    items
                        .iter()
                        .filter_map(JsonValue::as_str)
                        .map(ToOwned::to_owned),
                );
            }
        }
    }
    for key in ["number_strings", "binary_base64"] {
        if let Some(items) = map.get(key).and_then(JsonValue::as_array) {
            paths.extend(
                items
                    .iter()
                    .filter_map(JsonValue::as_str)
                    .map(ToOwned::to_owned),
            );
        }
    }
    paths
}

fn validate_profile_hint_types(
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

fn validate_profile_hint_type(
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
            "object_id"
                | "date"
                | "timestamp"
                | "binary_base64"
                | "decimal128"
                | "int32"
                | "int64"
                | "double"
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

fn profile_supports_root_type(profile: &str) -> bool {
    matches!(
        profile,
        "dynamodb_attribute_value" | "firestore_value" | "mongo_extended_json"
    )
}
