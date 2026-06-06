use std::collections::BTreeMap;

use serde_json::{Map, Value as JsonValue};

use super::{
    EvalLimits, Namespace, eval_mapping_traced, eval_mapping_traced_with_source_redaction_hint,
    eval_mapping_with_v2_context, eval_v2_expr_traced, eval_v2_pipe_traced,
    eval_when_expr_traced_with_v2_context, eval_when_expr_with_v2_context,
    expr_to_json_for_v2_pipe, parse_source, set_path,
};
use crate::custom_ops::{self, ContractMode};
use crate::error::{TransformError, TransformErrorKind};
use crate::model::{CustomOpDef, Expr, Mapping, RuleFile, RuleType, RuleTypeField, RuleTypeKind};
use crate::path::{PathToken, parse_path};
use crate::trace::{
    TraceCollector, TraceEventKind, TracePhase, canonical_acc_path, canonical_context_path,
    canonical_input_path, canonical_item_path, canonical_out_path, canonical_output_path,
};
use crate::v2_eval::{EvalValue as V2EvalValue, V2EvalContext, eval_v2_expr, eval_v2_pipe};
use crate::v2_model::{
    V2CallArg, V2Condition, V2CustomCallStep, V2Expr, V2ObjectFieldValue, V2OpStep, V2Pipe, V2Ref,
    V2Start, V2Step,
};
use crate::v2_parser::{
    custom_call_step_candidate, parse_custom_call_step, parse_v2_pipe_from_value,
};

#[allow(clippy::too_many_arguments)]
pub(crate) fn eval_custom_op_step<'a>(
    op_step: &V2OpStep,
    pipe_value: V2EvalValue,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
) -> Result<Option<V2EvalValue>, TransformError> {
    let Some(rule) = ctx.rule() else {
        return Ok(None);
    };
    if !rule.defs.contains_key(&op_step.op) {
        return Ok(None);
    }
    if custom_ops::is_reserved_or_builtin_custom_op_name(&op_step.op) {
        return Err(shadowed_custom_op_error(&op_step.op, path));
    }
    if !op_step.args.is_empty() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "custom op arguments must use with call options",
        )
        .with_path(path));
    }
    eval_custom_op(
        rule,
        &op_step.op,
        pipe_value,
        None,
        record,
        context,
        out,
        path,
        ctx,
    )
    .map(Some)
}

pub(crate) fn parse_known_custom_call_literal_start(
    start: &V2Start,
    ctx: &V2EvalContext<'_>,
    path: &str,
) -> Result<Option<V2CustomCallStep>, TransformError> {
    let V2Start::Literal(value) = start else {
        return Ok(None);
    };
    let Some((op_name, args_val)) = custom_call_step_candidate(value) else {
        return Ok(None);
    };
    if !ctx
        .rule()
        .is_some_and(|rule| rule.defs.contains_key(op_name))
    {
        return Ok(None);
    }
    match parse_custom_call_step(op_name, args_val) {
        Ok(Some(call)) => Ok(Some(call)),
        Ok(None) => Err(TransformError::new(
            TransformErrorKind::ExprError,
            "invalid custom op call: custom op call must use with call options",
        )
        .with_path(path)),
        Err(err) => Err(TransformError::new(
            TransformErrorKind::ExprError,
            format!("invalid custom op call: {}", err),
        )
        .with_path(path)),
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn eval_custom_call_step<'a>(
    call: &V2CustomCallStep,
    pipe_value: V2EvalValue,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
) -> Result<V2EvalValue, TransformError> {
    let Some(rule) = ctx.rule() else {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            format!("unknown custom op: {}", call.op),
        )
        .with_path(path));
    };
    eval_custom_op(
        rule,
        &call.op,
        pipe_value,
        call.with.as_deref(),
        record,
        context,
        out,
        path,
        ctx,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn eval_custom_op_step_traced<'a>(
    op_step: &V2OpStep,
    pipe_value: V2EvalValue,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
    collector: &mut TraceCollector,
) -> Result<Option<V2EvalValue>, TransformError> {
    let Some(rule) = ctx.rule() else {
        return Ok(None);
    };
    if !rule.defs.contains_key(&op_step.op) {
        return Ok(None);
    }
    if custom_ops::is_reserved_or_builtin_custom_op_name(&op_step.op) {
        return Err(shadowed_custom_op_error(&op_step.op, path));
    }
    if !op_step.args.is_empty() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "custom op arguments must use with call options",
        )
        .with_path(path));
    }
    eval_custom_op_traced(
        rule,
        &op_step.op,
        pipe_value,
        None,
        record,
        context,
        out,
        path,
        ctx,
        collector,
    )
    .map(Some)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn eval_custom_call_step_traced<'a>(
    call: &V2CustomCallStep,
    pipe_value: V2EvalValue,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
    collector: &mut TraceCollector,
) -> Result<V2EvalValue, TransformError> {
    let Some(rule) = ctx.rule() else {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            format!("unknown custom op: {}", call.op),
        )
        .with_path(path));
    };
    eval_custom_op_traced(
        rule,
        &call.op,
        pipe_value,
        call.with.as_deref(),
        record,
        context,
        out,
        path,
        ctx,
        collector,
    )
}

#[allow(clippy::too_many_arguments)]
fn eval_custom_op<'a>(
    rule: &'a RuleFile,
    name: &str,
    pipe_value: V2EvalValue,
    with: Option<&[(String, V2CallArg)]>,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
) -> Result<V2EvalValue, TransformError> {
    eval_custom_op_inner(
        rule, name, pipe_value, with, record, context, out, path, ctx, None,
    )
}

#[allow(clippy::too_many_arguments)]
fn eval_custom_op_traced<'a>(
    rule: &'a RuleFile,
    name: &str,
    pipe_value: V2EvalValue,
    with: Option<&[(String, V2CallArg)]>,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
    collector: &mut TraceCollector,
) -> Result<V2EvalValue, TransformError> {
    eval_custom_op_inner(
        rule,
        name,
        pipe_value,
        with,
        record,
        context,
        out,
        path,
        ctx,
        Some(collector),
    )
}

#[allow(clippy::too_many_arguments)]
fn eval_custom_op_inner<'a>(
    rule: &'a RuleFile,
    name: &str,
    pipe_value: V2EvalValue,
    with: Option<&[(String, V2CallArg)]>,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
    mut collector: Option<&mut TraceCollector>,
) -> Result<V2EvalValue, TransformError> {
    if custom_ops::is_reserved_or_builtin_custom_op_name(name) {
        return Err(shadowed_custom_op_error(name, path));
    }
    let def = rule.defs.get(name).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            format!("unknown custom op: {}", name),
        )
        .with_path(path)
    })?;
    let limits = ctx.limits();
    if ctx.custom_op_depth() >= limits.max_custom_op_call_depth {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "custom op call depth exceeds configured limit",
        )
        .with_path(path));
    }
    if ctx.increment_custom_op_calls() > limits.max_custom_op_calls_per_record {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "custom op calls per record exceed configured limit",
        )
        .with_path(path));
    }

    let (input, mode, input_redaction_hints) = match with {
        Some(with) => {
            let with_ctx = ctx
                .clone()
                .with_custom_op_depth(ctx.custom_op_depth().saturating_add(1));
            if let Some(collector) = collector.as_deref_mut() {
                let custom_body_input_scope = ctx.custom_op_depth() > 0;
                let traced_input = eval_with_object_traced(
                    with,
                    &def.input,
                    record,
                    context,
                    out,
                    path,
                    custom_body_input_scope,
                    &with_ctx,
                    collector,
                )?;
                (
                    traced_input.value,
                    ContractMode::AdapterExact,
                    traced_input.redaction_hints,
                )
            } else {
                (
                    eval_with_object(with, &def.input, record, context, out, path, &with_ctx)?,
                    ContractMode::AdapterExact,
                    CustomInputRedactionHints::default(),
                )
            }
        }
        None => match pipe_value {
            V2EvalValue::Value(value) => (
                value,
                ContractMode::InputWidth,
                CustomInputRedactionHints::default(),
            ),
            V2EvalValue::Missing => {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "custom op input contract mismatch: input is missing",
                )
                .with_path(path));
            }
        },
    };

    custom_ops::check_contract(&input, &def.input, mode, path).map_err(|err| {
        TransformError::new(
            err.kind,
            format!("custom op input contract mismatch: {}", err.message),
        )
        .with_path(path)
    })?;

    let output = if let Some(collector) = collector.as_deref_mut() {
        eval_custom_op_body_traced(
            rule,
            name,
            def,
            &input,
            &input_redaction_hints,
            path,
            limits,
            ctx,
            collector,
        )?
    } else {
        eval_custom_op_body(rule, name, def, &input, path, limits, ctx)?
    };
    let returns = def
        .returns
        .as_ref()
        .cloned()
        .unwrap_or_else(|| synthesize_mappings_return(def.mappings.as_deref().unwrap_or(&[])));
    custom_ops::check_contract(&output, &returns, ContractMode::OutputExact, path).map_err(
        |err| {
            TransformError::new(
                err.kind,
                format!("custom op output contract mismatch: {}", err.message),
            )
            .with_path(path)
        },
    )?;
    Ok(V2EvalValue::Value(output))
}

fn eval_with_object<'a>(
    with: &[(String, V2CallArg)],
    input_type: &RuleType,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
) -> Result<JsonValue, TransformError> {
    let mut fields = Vec::new();
    for (name, arg) in with {
        let value = match arg {
            V2CallArg::Value(value) => value.clone(),
            V2CallArg::Expr(expr) => match eval_v2_expr(
                expr,
                record,
                context,
                out,
                &format!("{}.with.{}", path, name),
                ctx,
            )? {
                V2EvalValue::Value(value) => value,
                V2EvalValue::Missing if is_optional_input_field(input_type, name) => continue,
                V2EvalValue::Missing => {
                    return Err(TransformError::new(
                        TransformErrorKind::ExprError,
                        "custom op input contract mismatch: with value is missing",
                    )
                    .with_path(format!("{}.with.{}", path, name)));
                }
            },
        };
        fields.push((name.clone(), value));
    }
    Ok(custom_ops::build_with_object(fields))
}

struct TracedCustomInput {
    value: JsonValue,
    redaction_hints: CustomInputRedactionHints,
}

#[derive(Default)]
struct CustomInputRedactionHints {
    fields: BTreeMap<String, Option<String>>,
}

enum RedactionHintOverride {
    Hint(String),
    Unknown,
}

impl CustomInputRedactionHints {
    fn redaction_hint_override(&self, path: &str) -> Option<RedactionHintOverride> {
        let (field, suffix) = local_input_field_ref(path)?;
        match self.fields.get(&field)? {
            Some(path_hint) => Some(RedactionHintOverride::Hint(append_path_suffix(
                path_hint, suffix,
            ))),
            None => Some(RedactionHintOverride::Unknown),
        }
    }

    fn body_redaction_hint_override(&self, path: &str) -> Option<RedactionHintOverride> {
        let Some((field, suffix)) = local_input_field_ref(path) else {
            return valid_local_input_path(path).then_some(RedactionHintOverride::Unknown);
        };
        match self.fields.get(&field) {
            Some(Some(path_hint)) => Some(RedactionHintOverride::Hint(append_path_suffix(
                path_hint, suffix,
            ))),
            Some(None) | None => Some(RedactionHintOverride::Unknown),
        }
    }
}

fn local_input_field_ref(path: &str) -> Option<(String, String)> {
    let path = local_input_path(path)?;

    if path.starts_with('[') {
        let tokens = parse_path(path).ok()?;
        let Some(PathToken::Key(field)) = tokens.first() else {
            return None;
        };
        return Some((field.clone(), path_token_suffix(&tokens[1..])));
    }

    let boundary = path
        .find(|ch: char| ch == '.' || ch == '[')
        .unwrap_or(path.len());
    if boundary == 0 {
        return None;
    }
    Some((path[..boundary].to_string(), path[boundary..].to_string()))
}

fn valid_local_input_path(path: &str) -> bool {
    local_input_path(path).is_some_and(|path| parse_path(path).is_ok())
}

fn local_input_path(path: &str) -> Option<&str> {
    let path = if let Some(path) = path.strip_prefix("$.") {
        path
    } else if path.starts_with("$[") {
        &path[1..]
    } else if let Some(path) = path.strip_prefix("@input.") {
        path
    } else if path.starts_with("@input[") {
        &path["@input".len()..]
    } else if path == "$" || path == "@input" || path.starts_with('@') {
        return None;
    } else {
        path
    };
    Some(path)
}

fn path_token_suffix(tokens: &[PathToken]) -> String {
    let mut suffix = String::new();
    for token in tokens {
        match token {
            PathToken::Key(key) => {
                suffix.push_str("[\"");
                for ch in key.chars() {
                    if ch == '\\' || ch == '"' {
                        suffix.push('\\');
                    }
                    suffix.push(ch);
                }
                suffix.push_str("\"]");
            }
            PathToken::Index(index) => {
                suffix.push('[');
                suffix.push_str(&index.to_string());
                suffix.push(']');
            }
        }
    }
    suffix
}

fn append_path_suffix(path_hint: &str, suffix: String) -> String {
    if suffix.is_empty() {
        path_hint.to_string()
    } else {
        format!("{}{}", path_hint, suffix)
    }
}

fn custom_input_field_redaction_hint(
    arg: &V2CallArg,
    custom_body_input_scope: bool,
) -> Option<String> {
    match arg {
        V2CallArg::Expr(expr) => v2_expr_redaction_hint(expr, custom_body_input_scope),
        V2CallArg::Value(_) => None,
    }
}

fn v2_expr_redaction_hint(expr: &V2Expr, custom_body_input_scope: bool) -> Option<String> {
    let mut hint = RedactionHint {
        text: String::new(),
        unknown_provenance: false,
        custom_body_input_scope,
    };
    collect_v2_expr_redaction_hints(expr, &mut hint, &CustomInputRedactionHints::default());
    if hint.unknown_provenance {
        return None;
    }
    let text = hint.text.trim();
    (!text.is_empty()).then(|| text.to_string())
}

fn eval_with_object_traced<'a>(
    with: &[(String, V2CallArg)],
    input_type: &RuleType,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    custom_body_input_scope: bool,
    ctx: &V2EvalContext<'a>,
    collector: &mut TraceCollector,
) -> Result<TracedCustomInput, TransformError> {
    let mut fields = Vec::new();
    let mut redaction_hints = CustomInputRedactionHints::default();
    for (name, arg) in with {
        let arg_path = format!("{}.with.{}", path, name);
        let redaction_hint = custom_input_field_redaction_hint(arg, custom_body_input_scope);
        let value = match arg {
            V2CallArg::Value(value) => {
                collector
                    .emit(TraceEventKind::LiteralEval, TracePhase::Instant)
                    .rule_path(&arg_path)
                    .finish_with_output(collector, value, None);
                value.clone()
            }
            V2CallArg::Expr(expr) => {
                match eval_v2_expr_traced(expr, record, context, out, &arg_path, ctx, collector)? {
                    V2EvalValue::Value(value) => value,
                    V2EvalValue::Missing if is_optional_input_field(input_type, name) => continue,
                    V2EvalValue::Missing => {
                        return Err(TransformError::new(
                            TransformErrorKind::ExprError,
                            "custom op input contract mismatch: with value is missing",
                        )
                        .with_path(arg_path));
                    }
                }
            }
        };
        redaction_hints.fields.insert(name.clone(), redaction_hint);
        fields.push((name.clone(), value));
    }
    Ok(TracedCustomInput {
        value: custom_ops::build_with_object(fields),
        redaction_hints,
    })
}

fn is_optional_input_field(input_type: &RuleType, name: &str) -> bool {
    match &input_type.kind {
        RuleTypeKind::Object(fields) => fields.get(name).is_some_and(|field| field.optional),
        _ => false,
    }
}

fn eval_custom_op_body(
    rule: &RuleFile,
    name: &str,
    def: &CustomOpDef,
    input: &JsonValue,
    path: &str,
    limits: EvalLimits,
    caller_ctx: &V2EvalContext<'_>,
) -> Result<JsonValue, TransformError> {
    if let Some(expr) = &def.expr {
        let expr_path = format!("defs.{}.expr", name);
        let value = expr_to_json_for_v2_pipe(expr).ok_or_else(|| {
            TransformError::new(
                TransformErrorKind::ExprError,
                "custom op expr must be a v2 pipe",
            )
            .with_path(&expr_path)
        })?;
        let pipe = parse_v2_pipe_from_value(&value).map_err(|err| {
            TransformError::new(TransformErrorKind::ExprError, err.to_string())
                .with_path(&expr_path)
        })?;
        let empty_out = JsonValue::Object(Map::new());
        let body_ctx = V2EvalContext::new()
            .with_limits(limits)
            .with_rule(rule)
            .with_pipe_value(V2EvalValue::Value(input.clone()))
            .with_custom_op_depth(caller_ctx.custom_op_depth() + 1)
            .with_custom_op_counter_from(caller_ctx);
        return match eval_v2_pipe(&pipe, input, None, &empty_out, &expr_path, &body_ctx)? {
            V2EvalValue::Value(value) => Ok(value),
            V2EvalValue::Missing => Err(TransformError::new(
                TransformErrorKind::ExprError,
                "custom op output contract mismatch: output is missing",
            )
            .with_path(path)),
        };
    }

    if let Some(mappings) = &def.mappings {
        let mut output = JsonValue::Object(Map::new());
        let body_ctx = V2EvalContext::new()
            .with_limits(limits)
            .with_rule(rule)
            .with_pipe_value(V2EvalValue::Value(input.clone()))
            .with_custom_op_depth(caller_ctx.custom_op_depth() + 1)
            .with_custom_op_counter_from(caller_ctx);
        for (index, mapping) in mappings.iter().enumerate() {
            let mapping_path = format!("defs.{}.mappings[{}]", name, index);
            if let Some(when) = &mapping.when {
                let keep = eval_when_expr_with_v2_context(
                    when,
                    input,
                    None,
                    &output,
                    &mapping_path,
                    2,
                    limits,
                    &body_ctx,
                )?;
                if !keep {
                    continue;
                }
            }
            let value = eval_mapping_with_v2_context(
                rule,
                mapping,
                input,
                None,
                &output,
                &mapping_path,
                2,
                limits,
                &body_ctx,
                None,
            )?;
            if let Some(value) = value {
                set_path(&mut output, &mapping.target, value, &mapping_path)?;
            }
        }
        return Ok(output);
    }

    Err(TransformError::new(
        TransformErrorKind::ExprError,
        "custom op must define expr or mappings",
    )
    .with_path(path))
}

fn eval_custom_op_body_traced(
    rule: &RuleFile,
    name: &str,
    def: &CustomOpDef,
    input: &JsonValue,
    input_redaction_hints: &CustomInputRedactionHints,
    path: &str,
    limits: EvalLimits,
    caller_ctx: &V2EvalContext<'_>,
    collector: &mut TraceCollector,
) -> Result<JsonValue, TransformError> {
    if let Some(expr) = &def.expr {
        let expr_path = format!("defs.{}.expr", name);
        let value = expr_to_json_for_v2_pipe(expr).ok_or_else(|| {
            TransformError::new(
                TransformErrorKind::ExprError,
                "custom op expr must be a v2 pipe",
            )
            .with_path(&expr_path)
        })?;
        let pipe = parse_v2_pipe_from_value(&value).map_err(|err| {
            TransformError::new(TransformErrorKind::ExprError, err.to_string())
                .with_path(&expr_path)
        })?;
        let empty_out = JsonValue::Object(Map::new());
        let body_ctx = V2EvalContext::new()
            .with_limits(limits)
            .with_rule(rule)
            .with_pipe_value(V2EvalValue::Value(input.clone()))
            .with_custom_op_depth(caller_ctx.custom_op_depth() + 1)
            .with_custom_op_counter_from(caller_ctx);
        return match eval_v2_pipe_traced(
            &pipe, input, None, &empty_out, &expr_path, &body_ctx, collector,
        )? {
            V2EvalValue::Value(value) => Ok(value),
            V2EvalValue::Missing => Err(TransformError::new(
                TransformErrorKind::ExprError,
                "custom op output contract mismatch: output is missing",
            )
            .with_path(path)),
        };
    }

    if let Some(mappings) = &def.mappings {
        let mut output = JsonValue::Object(Map::new());
        let body_ctx = V2EvalContext::new()
            .with_limits(limits)
            .with_rule(rule)
            .with_pipe_value(V2EvalValue::Value(input.clone()))
            .with_custom_op_depth(caller_ctx.custom_op_depth() + 1)
            .with_custom_op_counter_from(caller_ctx);
        for (index, mapping) in mappings.iter().enumerate() {
            let mapping_path = format!("defs.{}.mappings[{}]", name, index);
            collector
                .start_span(TraceEventKind::MappingStart, TracePhase::Start)
                .rule_path(&mapping_path)
                .attr_index("mapping_index", index)
                .finish(collector);

            let applied = if mapping.when.is_some() {
                let when_path = format!("{}.when", mapping_path);
                collector
                    .start_span(TraceEventKind::MappingWhenStart, TracePhase::Start)
                    .rule_path(&when_path)
                    .finish(collector);
                let keep = match eval_when_expr_traced_with_v2_context(
                    mapping.when.as_ref().expect("checked above"),
                    input,
                    None,
                    &output,
                    &when_path,
                    2,
                    limits,
                    &body_ctx,
                    collector,
                ) {
                    Ok(keep) => keep,
                    Err(mut error) => {
                        collector
                            .error_span(TraceEventKind::Error, "MAPPING_ERROR", "mapping failed")
                            .rule_path(&when_path)
                            .finish(collector);
                        collector
                            .error_span(TraceEventKind::Error, "MAPPING_ERROR", "mapping failed")
                            .rule_path(&mapping_path)
                            .finish(collector);
                        error.path = normalize_custom_body_when_error_path(
                            error.path,
                            &mapping_path,
                            &when_path,
                        );
                        return Err(error);
                    }
                };
                collector
                    .end_span(TraceEventKind::MappingWhenEnd, TracePhase::End)
                    .rule_path(&when_path)
                    .finish_with_output(collector, &JsonValue::Bool(keep), None);
                keep
            } else {
                true
            };

            collector
                .emit(TraceEventKind::MappingDecision, TracePhase::Instant)
                .rule_path(&mapping_path)
                .attr_bool("applied", applied)
                .attr_enum("skip_reason", if applied { "none" } else { "when_false" })
                .finish(collector);

            if !applied {
                collector
                    .end_span(TraceEventKind::MappingEnd, TracePhase::End)
                    .rule_path(&mapping_path)
                    .finish(collector);
                continue;
            }

            let value = match eval_custom_mapping_traced(
                rule,
                mapping,
                input,
                &output,
                &mapping_path,
                limits,
                &body_ctx,
                input_redaction_hints,
                collector,
            ) {
                Ok(value) => value,
                Err(error) => {
                    collector
                        .error_span(TraceEventKind::Error, "MAPPING_ERROR", "mapping failed")
                        .rule_path(&mapping_path)
                        .finish(collector);
                    return Err(error);
                }
            };
            if let Some(value) = value {
                if let Err(error) =
                    set_path(&mut output, &mapping.target, value.clone(), &mapping_path)
                {
                    collector
                        .error_span(TraceEventKind::Error, "MAPPING_ERROR", "mapping failed")
                        .rule_path(&mapping_path)
                        .finish(collector);
                    return Err(error);
                }
                let output_redaction_hint =
                    mapping_output_redaction_hint(mapping, input_redaction_hints);
                collector
                    .emit(TraceEventKind::OutputWrite, TracePhase::Instant)
                    .rule_path(format!("{}.target", mapping_path))
                    .output_path(canonical_output_path(&mapping.target))
                    .attr_path("target_path", canonical_output_path(&mapping.target))
                    .finish_with_output(collector, &value, output_redaction_hint.as_deref());
            }

            collector
                .end_span(TraceEventKind::MappingEnd, TracePhase::End)
                .rule_path(&mapping_path)
                .finish(collector);
        }
        return Ok(output);
    }

    Err(TransformError::new(
        TransformErrorKind::ExprError,
        "custom op must define expr or mappings",
    )
    .with_path(path))
}

#[allow(clippy::too_many_arguments)]
fn eval_custom_mapping_traced(
    rule: &RuleFile,
    mapping: &Mapping,
    input: &JsonValue,
    output: &JsonValue,
    mapping_path: &str,
    limits: EvalLimits,
    body_ctx: &V2EvalContext<'_>,
    input_redaction_hints: &CustomInputRedactionHints,
    collector: &mut TraceCollector,
) -> Result<Option<JsonValue>, TransformError> {
    let Some(source) = &mapping.source else {
        return eval_mapping_traced(
            rule,
            mapping,
            input,
            None,
            output,
            mapping_path,
            2,
            limits,
            body_ctx,
            collector,
        );
    };

    match custom_body_path_redaction_hint_override(source, input_redaction_hints) {
        Some(RedactionHintOverride::Hint(path_hint)) => {
            eval_mapping_traced_with_source_redaction_hint(
                rule,
                mapping,
                input,
                None,
                output,
                mapping_path,
                2,
                limits,
                body_ctx,
                collector,
                Some(&path_hint),
            )
        }
        Some(RedactionHintOverride::Unknown) => eval_mapping_traced_with_source_redaction_hint(
            rule,
            mapping,
            input,
            None,
            output,
            mapping_path,
            2,
            limits,
            body_ctx,
            collector,
            None,
        ),
        None => eval_mapping_traced(
            rule,
            mapping,
            input,
            None,
            output,
            mapping_path,
            2,
            limits,
            body_ctx,
            collector,
        ),
    }
}

fn synthesize_mappings_return(mappings: &[Mapping]) -> RuleType {
    let mut fields = BTreeMap::new();
    for mapping in mappings {
        let Ok(tokens) = parse_path(&mapping.target) else {
            continue;
        };
        insert_return_path(&mut fields, &tokens, mapping_may_be_absent(mapping));
    }
    RuleType {
        kind: RuleTypeKind::Object(fields),
        nullable: false,
    }
}

fn mapping_output_redaction_hint(
    mapping: &Mapping,
    input_redaction_hints: &CustomInputRedactionHints,
) -> Option<String> {
    let mut hint = RedactionHint {
        text: mapping.target.clone(),
        unknown_provenance: false,
        custom_body_input_scope: true,
    };
    if mapping.value.is_some() || mapping.default.is_some() {
        hint.unknown_provenance = true;
    }
    if let Some(source) = &mapping.source {
        collect_path_redaction_hint(source, &mut hint, input_redaction_hints);
    }
    if let Some(expr) = &mapping.expr {
        collect_expr_redaction_hints(expr, &mut hint, input_redaction_hints);
    }
    if hint.unknown_provenance {
        None
    } else {
        Some(hint.text)
    }
}

struct RedactionHint {
    text: String,
    unknown_provenance: bool,
    custom_body_input_scope: bool,
}

fn collect_expr_redaction_hints(
    expr: &Expr,
    hint: &mut RedactionHint,
    input_redaction_hints: &CustomInputRedactionHints,
) {
    match expr {
        Expr::Ref(expr_ref) => {
            collect_path_redaction_hint(&expr_ref.ref_path, hint, input_redaction_hints);
        }
        Expr::Op(expr_op) => {
            for arg in &expr_op.args {
                collect_expr_redaction_hints(arg, hint, input_redaction_hints);
            }
        }
        Expr::Chain(expr_chain) => {
            for part in &expr_chain.chain {
                collect_expr_redaction_hints(part, hint, input_redaction_hints);
            }
        }
        Expr::Literal(value) => collect_json_redaction_hints(value, hint, input_redaction_hints),
    }
}

fn collect_json_redaction_hints(
    _value: &JsonValue,
    hint: &mut RedactionHint,
    _input_redaction_hints: &CustomInputRedactionHints,
) {
    hint.unknown_provenance = true;
}

fn collect_path_redaction_hint(
    path: &str,
    hint: &mut RedactionHint,
    input_redaction_hints: &CustomInputRedactionHints,
) {
    if path == "$" || path == "@input" || (!hint.custom_body_input_scope && path.starts_with("$."))
    {
        hint.unknown_provenance = true;
        return;
    }
    let override_hint = if hint.custom_body_input_scope {
        custom_body_path_redaction_hint_override(path, input_redaction_hints)
    } else {
        input_redaction_hints.redaction_hint_override(path)
    };
    match override_hint {
        Some(RedactionHintOverride::Hint(path_hint)) => {
            hint.text.push(' ');
            hint.text.push_str(&path_hint);
        }
        Some(RedactionHintOverride::Unknown) => {
            hint.unknown_provenance = true;
        }
        None => {}
    }
    hint.text.push(' ');
    hint.text.push_str(path);
}

fn custom_body_path_redaction_hint_override(
    path: &str,
    input_redaction_hints: &CustomInputRedactionHints,
) -> Option<RedactionHintOverride> {
    if path == "$" || path == "@input" {
        return Some(RedactionHintOverride::Unknown);
    }
    if path.starts_with("$.")
        || path.starts_with("$[")
        || path.starts_with("@input.")
        || path.starts_with("@input[")
    {
        return input_redaction_hints.body_redaction_hint_override(path);
    }
    if is_body_local_output_ref(path) || is_body_local_unknown_ref(path) {
        return Some(RedactionHintOverride::Unknown);
    }
    match parse_source(path) {
        Ok((Namespace::Input, input_path)) => {
            input_redaction_hints.body_redaction_hint_override(input_path)
        }
        Ok((Namespace::Context, context_path)) => Some(RedactionHintOverride::Hint(
            canonical_context_path(context_path),
        )),
        Ok((Namespace::Out, _))
        | Ok((Namespace::Item | Namespace::Acc | Namespace::Pipe | Namespace::Local, _)) => {
            Some(RedactionHintOverride::Unknown)
        }
        Err(_) => None,
    }
}

fn is_body_local_output_ref(path: &str) -> bool {
    matches!(path, "@out" | "out")
        || path.starts_with("@out.")
        || path.starts_with("@out[")
        || path.starts_with("out.")
        || path.starts_with("out[")
}

fn is_body_local_unknown_ref(path: &str) -> bool {
    matches!(path, "@local" | "local" | "@acc" | "acc" | "@item" | "item")
        || path.starts_with("@local.")
        || path.starts_with("@local[")
        || path.starts_with("local.")
        || path.starts_with("local[")
        || path.starts_with("@acc.")
        || path.starts_with("@acc[")
        || path.starts_with("acc.")
        || path.starts_with("acc[")
        || path.starts_with("@item.")
        || path.starts_with("@item[")
        || path.starts_with("item.")
        || path.starts_with("item[")
}

fn collect_v2_expr_redaction_hints(
    expr: &V2Expr,
    hint: &mut RedactionHint,
    input_redaction_hints: &CustomInputRedactionHints,
) {
    match expr {
        V2Expr::Pipe(pipe) => {
            collect_v2_pipe_redaction_hints(pipe, hint, input_redaction_hints);
        }
        V2Expr::V1Fallback(expr) => {
            collect_expr_redaction_hints(expr, hint, input_redaction_hints);
        }
    }
}

fn collect_v2_pipe_redaction_hints(
    pipe: &V2Pipe,
    hint: &mut RedactionHint,
    input_redaction_hints: &CustomInputRedactionHints,
) {
    collect_v2_start_redaction_hints(&pipe.start, hint, input_redaction_hints);
    for step in &pipe.steps {
        collect_v2_step_redaction_hints(step, hint, input_redaction_hints);
    }
}

fn collect_v2_start_redaction_hints(
    start: &V2Start,
    hint: &mut RedactionHint,
    input_redaction_hints: &CustomInputRedactionHints,
) {
    match start {
        V2Start::Ref(v2_ref) => collect_v2_ref_redaction_hint(v2_ref, hint, input_redaction_hints),
        V2Start::PipeValue | V2Start::ImplicitPipeValue => {
            hint.unknown_provenance = true;
        }
        V2Start::Literal(value) => collect_json_redaction_hints(value, hint, input_redaction_hints),
        V2Start::V1Expr(expr) => collect_expr_redaction_hints(expr, hint, input_redaction_hints),
    }
}

fn collect_v2_step_redaction_hints(
    step: &V2Step,
    hint: &mut RedactionHint,
    input_redaction_hints: &CustomInputRedactionHints,
) {
    match step {
        V2Step::Op(op) => {
            if op.op.starts_with('@') {
                collect_path_redaction_hint(&op.op, hint, input_redaction_hints);
            } else {
                hint.unknown_provenance = true;
            }
            for arg in &op.args {
                collect_v2_expr_redaction_hints(arg, hint, input_redaction_hints);
            }
        }
        V2Step::Object(object) => {
            hint.unknown_provenance = true;
            for field in &object.fields {
                match &field.value {
                    V2ObjectFieldValue::Expr(expr) => {
                        collect_v2_expr_redaction_hints(expr, hint, input_redaction_hints);
                    }
                    V2ObjectFieldValue::Value(value) => {
                        collect_json_redaction_hints(value, hint, input_redaction_hints);
                    }
                }
            }
        }
        V2Step::CustomCall(call) => {
            hint.unknown_provenance = true;
            if let Some(with) = &call.with {
                for (_, arg) in with {
                    match arg {
                        V2CallArg::Expr(expr) => {
                            collect_v2_expr_redaction_hints(expr, hint, input_redaction_hints);
                        }
                        V2CallArg::Value(value) => {
                            collect_json_redaction_hints(value, hint, input_redaction_hints);
                        }
                    }
                }
            }
        }
        V2Step::Let(let_step) => {
            for (_, expr) in &let_step.bindings {
                collect_v2_expr_redaction_hints(expr, hint, input_redaction_hints);
            }
        }
        V2Step::If(if_step) => {
            hint.unknown_provenance = true;
            collect_v2_condition_redaction_hints(&if_step.cond, hint, input_redaction_hints);
            collect_v2_pipe_redaction_hints(&if_step.then_branch, hint, input_redaction_hints);
            if let Some(else_branch) = &if_step.else_branch {
                collect_v2_pipe_redaction_hints(else_branch, hint, input_redaction_hints);
            }
        }
        V2Step::Map(map_step) => {
            hint.unknown_provenance = true;
            for step in &map_step.steps {
                collect_v2_step_redaction_hints(step, hint, input_redaction_hints);
            }
        }
        V2Step::Ref(v2_ref) => collect_v2_ref_redaction_hint(v2_ref, hint, input_redaction_hints),
    }
}

fn collect_v2_condition_redaction_hints(
    condition: &V2Condition,
    hint: &mut RedactionHint,
    input_redaction_hints: &CustomInputRedactionHints,
) {
    match condition {
        V2Condition::All(conditions) | V2Condition::Any(conditions) => {
            for condition in conditions {
                collect_v2_condition_redaction_hints(condition, hint, input_redaction_hints);
            }
        }
        V2Condition::Comparison(comparison) => {
            for arg in &comparison.args {
                collect_v2_expr_redaction_hints(arg, hint, input_redaction_hints);
            }
        }
        V2Condition::Expr(expr) => {
            collect_v2_expr_redaction_hints(expr, hint, input_redaction_hints);
        }
    }
}

fn collect_v2_ref_redaction_hint(
    v2_ref: &V2Ref,
    hint: &mut RedactionHint,
    input_redaction_hints: &CustomInputRedactionHints,
) {
    match v2_ref {
        V2Ref::Input(path) => {
            collect_path_redaction_hint(&canonical_input_path(path), hint, input_redaction_hints);
        }
        V2Ref::Context(path) => {
            collect_path_redaction_hint(&canonical_context_path(path), hint, input_redaction_hints);
        }
        V2Ref::Out(path) => {
            collect_path_redaction_hint(&canonical_out_path(path), hint, input_redaction_hints);
        }
        V2Ref::Pipe(_) => {
            hint.unknown_provenance = true;
        }
        V2Ref::Item(path) => {
            collect_path_redaction_hint(&canonical_item_path(path), hint, input_redaction_hints);
        }
        V2Ref::Acc(path) => {
            collect_path_redaction_hint(&canonical_acc_path(path), hint, input_redaction_hints);
        }
        V2Ref::Local(_) => {
            hint.unknown_provenance = true;
        }
    }
}

fn insert_return_path(
    fields: &mut BTreeMap<String, RuleTypeField>,
    tokens: &[PathToken],
    optional: bool,
) -> bool {
    let Some(PathToken::Key(key)) = tokens.first() else {
        return !optional;
    };
    if tokens.len() == 1 {
        let field = fields.entry(key.clone()).or_insert_with(|| RuleTypeField {
            ty: json_rule_type(),
            optional,
        });
        field.ty = json_rule_type();
        field.optional &= optional;
        return !optional;
    }

    let field = fields.entry(key.clone()).or_insert_with(|| RuleTypeField {
        ty: RuleType {
            kind: RuleTypeKind::Object(BTreeMap::new()),
            nullable: false,
        },
        optional,
    });
    let child_required = match &mut field.ty.kind {
        RuleTypeKind::Object(child_fields) => {
            insert_return_path(child_fields, &tokens[1..], optional)
        }
        _ => {
            field.ty = json_rule_type();
            !optional
        }
    };
    field.optional &= !child_required;
    child_required
}

fn shadowed_custom_op_error(name: &str, path: &str) -> TransformError {
    TransformError::new(
        TransformErrorKind::ExprError,
        format!(
            "custom op `{}` must not shadow a built-in or reserved op",
            name
        ),
    )
    .with_path(path)
}

fn normalize_custom_body_when_error_path(
    path: Option<String>,
    mapping_path: &str,
    when_path: &str,
) -> Option<String> {
    let Some(path) = path else {
        return None;
    };
    if path == when_path {
        return Some(mapping_path.to_string());
    }
    if let Some(suffix) = path.strip_prefix(when_path) {
        if suffix.starts_with('.') || suffix.starts_with('[') {
            return Some(format!("{mapping_path}{suffix}"));
        }
    }
    Some(path)
}

fn mapping_may_be_absent(mapping: &Mapping) -> bool {
    let conditional = match &mapping.when {
        None => false,
        Some(crate::model::Expr::Literal(JsonValue::Bool(true))) => false,
        _ => true,
    };
    conditional || !(mapping.required || mapping.value.is_some() || mapping.default.is_some())
}

fn json_rule_type() -> RuleType {
    RuleType {
        kind: RuleTypeKind::Json,
        nullable: true,
    }
}
