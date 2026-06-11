use super::options::{validate_codec_options, validate_resolved_codec_options};
use super::*;

pub(super) fn validate_step_codec_refs(
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

pub(super) fn validate_mapping_codec_refs(
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

pub(super) fn validate_expr_codec_refs(
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

pub(super) fn validate_condition_expr_codec_refs(
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

pub(super) fn validate_finalize_wrap_codec_refs(
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
            V2Step::Object(object) => {
                for field in &object.fields {
                    if let V2ObjectFieldValue::Expr(expr) = &field.value {
                        validate_v2_expr_codec_refs(
                            expr,
                            &object_field_rule_path(&step_path, &field.key),
                            ctx,
                        );
                    }
                }
            }
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
        V2Step::Object(object) => {
            for field in &object.fields {
                if let V2ObjectFieldValue::Expr(expr) = &field.value {
                    validate_v2_expr_codec_refs(
                        expr,
                        &object_field_rule_path(base_path, &field.key),
                        ctx,
                    );
                }
            }
        }
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
