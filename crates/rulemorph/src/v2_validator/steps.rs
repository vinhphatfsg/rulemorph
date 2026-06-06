use crate::v2_model::{
    V2Expr, V2IfStep, V2LetStep, V2MapStep, V2ObjectFieldValue, V2ObjectStep, V2Pipe, V2Start,
    V2Step, object_field_rule_path,
};
use crate::v2_parser::{custom_call_step_candidate, parse_custom_call_step};

use super::conditions::validate_v2_condition;
use super::context::{V2Scope, V2ValidationCtx};
use super::operators;
use super::references::validate_v2_ref;

/// Validate a v2 expression
pub fn validate_v2_expr(
    expr: &V2Expr,
    base_path: &str,
    scope: &V2Scope,
    ctx: &mut V2ValidationCtx<'_>,
) {
    match expr {
        V2Expr::Pipe(pipe) => validate_v2_pipe(pipe, base_path, scope, ctx),
        V2Expr::V1Fallback(_) => {
            // V1 expressions are validated by the existing v1 validator
        }
    }
}

/// Validate a v2 pipe
pub fn validate_v2_pipe(
    pipe: &V2Pipe,
    base_path: &str,
    scope: &V2Scope,
    ctx: &mut V2ValidationCtx<'_>,
) {
    let mut handled_start = false;
    if let Some(call) = parse_known_custom_call_literal_start(&pipe.start, ctx, base_path) {
        let step_path = format!("{}[0]", base_path);
        match call {
            Ok(call) => {
                validate_custom_call_args(&call, &step_path, scope, ctx);
                handled_start = true;
            }
            Err(err) => ctx.push_error(
                crate::error::ErrorCode::InvalidExprShape,
                format!("invalid custom op call: {}", err),
                &step_path,
            ),
        }
    }

    if !handled_start {
        // Validate start value (at index 0 in the array)
        validate_v2_start(&pipe.start, &format!("{}[0]", base_path), scope, ctx);
    }
    if !handled_start && matches!(pipe.start, V2Start::PipeValue) && !scope.allows_pipe() {
        ctx.push_error(
            crate::error::ErrorCode::InvalidRefNamespace,
            "$ refs are only valid inside pipe steps or custom op bodies",
            &format!("{}[0]", base_path),
        );
    }

    // Validate each step with proper scope management.
    // Steps start at index 1 in the pipe array (after start value).
    let has_pipe_value = !matches!(pipe.start, V2Start::ImplicitPipeValue) || scope.allows_pipe();
    let mut current_scope = if has_pipe_value {
        scope.clone().with_pipe()
    } else {
        scope.clone()
    };
    for (i, step) in pipe.steps.iter().enumerate() {
        let step_path = format!("{}[{}]", base_path, i + 1);
        validate_v2_step(step, &step_path, &mut current_scope, ctx);
    }
}

fn parse_known_custom_call_literal_start(
    start: &V2Start,
    ctx: &V2ValidationCtx<'_>,
    _base_path: &str,
) -> Option<Result<crate::v2_model::V2CustomCallStep, crate::v2_parser::V2ParseError>> {
    let V2Start::Literal(value) = start else {
        return None;
    };
    let (op_name, args_val) = custom_call_step_candidate(value)?;
    if !ctx.is_custom_op(op_name) {
        return None;
    }
    match parse_custom_call_step(op_name, args_val) {
        Ok(Some(call)) => Some(Ok(call)),
        Ok(None) => Some(Err(crate::v2_parser::V2ParseError::InvalidStep(
            "custom op call must use with call options".to_string(),
        ))),
        Err(err) => Some(Err(err)),
    }
}

/// Validate a v2 start value
fn validate_v2_start(
    start: &V2Start,
    base_path: &str,
    scope: &V2Scope,
    ctx: &mut V2ValidationCtx<'_>,
) {
    match start {
        V2Start::Ref(v2_ref) => validate_v2_ref(v2_ref, base_path, scope, ctx),
        V2Start::PipeValue | V2Start::ImplicitPipeValue => {} // Missing pipe input is valid for single-step ops.
        V2Start::Literal(_) => {}                             // Literals are always valid
        V2Start::V1Expr(_) => {} // V1 expressions are validated elsewhere
    }
}

/// Validate a v2 step
fn validate_v2_step(
    step: &V2Step,
    base_path: &str,
    scope: &mut V2Scope,
    ctx: &mut V2ValidationCtx<'_>,
) {
    match step {
        V2Step::Op(op_step) => operators::validate_v2_op_step(op_step, base_path, scope, ctx),
        V2Step::Object(object_step) => validate_v2_object_step(object_step, base_path, scope, ctx),
        V2Step::CustomCall(call_step) => {
            let call_scope = scope.clone().with_pipe();
            validate_custom_call_args(call_step, base_path, &call_scope, ctx);
        }
        V2Step::Let(let_step) => validate_v2_let_step(let_step, base_path, scope, ctx),
        V2Step::If(if_step) => validate_v2_if_step(if_step, base_path, scope, ctx),
        V2Step::Map(map_step) => validate_v2_map_step(map_step, base_path, scope, ctx),
        V2Step::Ref(v2_ref) => validate_v2_ref(v2_ref, base_path, scope, ctx),
    }
}

fn validate_v2_object_step(
    object_step: &V2ObjectStep,
    base_path: &str,
    scope: &V2Scope,
    ctx: &mut V2ValidationCtx<'_>,
) {
    for field in &object_step.fields {
        let field_path = object_field_rule_path(base_path, &field.key);
        if field.key.is_empty() {
            ctx.push_error(
                crate::error::ErrorCode::InvalidExprShape,
                "object field key must not be empty",
                &field_path,
            );
        }
        if let V2ObjectFieldValue::Expr(expr) = &field.value {
            validate_v2_expr(expr, &field_path, scope, ctx);
        }
    }
}

fn validate_custom_call_args(
    call_step: &crate::v2_model::V2CustomCallStep,
    base_path: &str,
    scope: &V2Scope,
    ctx: &mut V2ValidationCtx<'_>,
) {
    if let Some(with) = &call_step.with {
        for (name, arg) in with {
            if let crate::v2_model::V2CallArg::Expr(expr) = arg {
                validate_v2_expr(expr, &format!("{}.with.{}", base_path, name), scope, ctx);
            }
        }
    }
}

/// Validate a v2 let step (adds bindings to scope)
fn validate_v2_let_step(
    let_step: &V2LetStep,
    base_path: &str,
    scope: &mut V2Scope,
    ctx: &mut V2ValidationCtx<'_>,
) {
    for (name, expr) in &let_step.bindings {
        let binding_path = format!("{}.let.{}", base_path, name);

        // Validate the binding expression with current scope
        validate_v2_expr(expr, &binding_path, scope, ctx);

        // Add binding to scope for subsequent steps
        scope.add_binding(name.clone());
    }
}

/// Validate a v2 if step
fn validate_v2_if_step(
    if_step: &V2IfStep,
    base_path: &str,
    scope: &V2Scope,
    ctx: &mut V2ValidationCtx<'_>,
) {
    // Validate condition
    let cond_path = format!("{}.if.cond", base_path);
    validate_v2_condition(&if_step.cond, &cond_path, scope, ctx);

    // Validate then branch (creates new child scope)
    let then_path = format!("{}.if.then", base_path);
    let then_scope = V2Scope::with_parent(scope);
    validate_v2_pipe(&if_step.then_branch, &then_path, &then_scope, ctx);

    // Validate else branch if present
    if let Some(ref else_branch) = if_step.else_branch {
        let else_path = format!("{}.if.else", base_path);
        let else_scope = V2Scope::with_parent(scope);
        validate_v2_pipe(else_branch, &else_path, &else_scope, ctx);
    }
}

/// Validate a v2 map step
fn validate_v2_map_step(
    map_step: &V2MapStep,
    base_path: &str,
    scope: &V2Scope,
    ctx: &mut V2ValidationCtx<'_>,
) {
    // Create new scope with @item available
    let mut map_scope = V2Scope::with_parent(scope).with_item();

    for (i, step) in map_step.steps.iter().enumerate() {
        let step_path = format!("{}.map[{}]", base_path, i);
        validate_v2_step(step, &step_path, &mut map_scope, ctx);
    }
}
