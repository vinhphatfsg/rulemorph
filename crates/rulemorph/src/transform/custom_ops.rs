use std::collections::BTreeMap;

use serde_json::{Map, Value as JsonValue};

use super::{
    EvalLimits, eval_mapping_with_v2_context, eval_when_expr_with_v2_context,
    expr_to_json_for_v2_pipe, set_path,
};
use crate::custom_ops::{self, ContractMode};
use crate::error::{TransformError, TransformErrorKind};
use crate::model::{CustomOpDef, Mapping, RuleFile, RuleType, RuleTypeField, RuleTypeKind};
use crate::path::{PathToken, parse_path};
use crate::v2_eval::{EvalValue as V2EvalValue, V2EvalContext, eval_v2_expr, eval_v2_pipe};
use crate::v2_model::{V2CallArg, V2CustomCallStep, V2OpStep, V2Start};
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

    let (input, mode) = match with {
        Some(with) => {
            let with_ctx = ctx
                .clone()
                .with_custom_op_depth(ctx.custom_op_depth().saturating_add(1));
            (
                eval_with_object(with, &def.input, record, context, out, path, &with_ctx)?,
                ContractMode::AdapterExact,
            )
        }
        None => match pipe_value {
            V2EvalValue::Value(value) => (value, ContractMode::InputWidth),
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

    let output = eval_custom_op_body(rule, name, def, &input, path, limits, ctx)?;
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
