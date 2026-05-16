//! v2 Expression Static Validator
//!
//! This module provides compile-time validation for v2 expressions,
//! catching errors that previously only occurred at runtime.

use crate::error::ErrorCode;
#[cfg(test)]
use crate::v2_model::V2Ref;
use crate::v2_model::{
    V2Comparison, V2Condition, V2Expr, V2IfStep, V2LetStep, V2MapStep, V2OpStep, V2Pipe, V2Start,
    V2Step,
};
use crate::v2_operator::{
    V2OperatorArgScope, is_valid_operator, operator_arg_range, operator_arg_scope,
};

mod context;
mod dependencies;
mod references;
mod types;

pub use self::context::{V2Scope, V2ValidationCtx};
pub use self::dependencies::{collect_out_references, validate_no_cyclic_dependencies};
pub use self::references::validate_v2_ref;
pub use self::types::{V2Type, infer_v2_expr_type};

// =============================================================================
// Step Validation
// =============================================================================

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
    // Validate start value (at index 0 in the array)
    validate_v2_start(&pipe.start, &format!("{}[0]", base_path), scope, ctx);

    // Validate each step with proper scope management
    // Steps start at index 1 in the pipe array (after start value)
    let mut current_scope = scope.clone();
    for (i, step) in pipe.steps.iter().enumerate() {
        let step_path = format!("{}[{}]", base_path, i + 1);
        validate_v2_step(step, &step_path, &mut current_scope, ctx);
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
        V2Start::PipeValue => {}  // $ is always valid
        V2Start::Literal(_) => {} // Literals are always valid
        V2Start::V1Expr(_) => {}  // V1 expressions are validated elsewhere
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
        V2Step::Op(op_step) => validate_v2_op_step(op_step, base_path, scope, ctx),
        V2Step::Let(let_step) => validate_v2_let_step(let_step, base_path, scope, ctx),
        V2Step::If(if_step) => validate_v2_if_step(if_step, base_path, scope, ctx),
        V2Step::Map(map_step) => validate_v2_map_step(map_step, base_path, scope, ctx),
        V2Step::Ref(v2_ref) => validate_v2_ref(v2_ref, base_path, scope, ctx),
    }
}

/// Validate a v2 op step
fn validate_v2_op_step(
    op_step: &V2OpStep,
    base_path: &str,
    scope: &V2Scope,
    ctx: &mut V2ValidationCtx<'_>,
) {
    // Check if op is known
    if !is_valid_op(&op_step.op) {
        ctx.push_error(
            ErrorCode::UnknownOp,
            format!("unknown operation: {}", op_step.op),
            base_path,
        );
    }

    // Validate argument count
    validate_op_args_count(&op_step.op, op_step.args.len(), base_path, ctx);

    // Validate each argument expression
    for (i, arg) in op_step.args.iter().enumerate() {
        let arg_path = format!("{}.args[{}]", base_path, i);
        let arg_scope = get_arg_scope_for_op(&op_step.op, i, op_step.args.len(), scope);
        validate_v2_expr(arg, &arg_path, &arg_scope, ctx);
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

// =============================================================================
// Condition Validation
// =============================================================================

/// Validate a v2 condition
pub fn validate_v2_condition(
    cond: &V2Condition,
    base_path: &str,
    scope: &V2Scope,
    ctx: &mut V2ValidationCtx<'_>,
) {
    match cond {
        V2Condition::All(conditions) => {
            for (i, c) in conditions.iter().enumerate() {
                let path = format!("{}.all[{}]", base_path, i);
                validate_v2_condition(c, &path, scope, ctx);
            }
        }
        V2Condition::Any(conditions) => {
            for (i, c) in conditions.iter().enumerate() {
                let path = format!("{}.any[{}]", base_path, i);
                validate_v2_condition(c, &path, scope, ctx);
            }
        }
        V2Condition::Comparison(comp) => {
            validate_v2_comparison(comp, base_path, scope, ctx);
        }
        V2Condition::Expr(expr) => {
            validate_v2_expr(expr, base_path, scope, ctx);
            // Type check: must be bool or unknown
            let typ = infer_v2_expr_type(expr);
            if typ.is_definitely_not_bool() {
                ctx.push_error(
                    ErrorCode::InvalidWhenType,
                    "condition must evaluate to boolean",
                    base_path,
                );
            }
        }
    }
}

/// Validate a v2 comparison
fn validate_v2_comparison(
    comp: &V2Comparison,
    base_path: &str,
    scope: &V2Scope,
    ctx: &mut V2ValidationCtx<'_>,
) {
    // Comparisons need exactly 2 arguments
    if comp.args.len() != 2 {
        ctx.push_error(
            ErrorCode::InvalidArgs,
            format!(
                "comparison requires exactly 2 arguments, got {}",
                comp.args.len()
            ),
            base_path,
        );
    }

    // Validate each argument
    for (i, arg) in comp.args.iter().enumerate() {
        let arg_path = format!("{}.args[{}]", base_path, i);
        validate_v2_expr(arg, &arg_path, scope, ctx);
    }
}

// =============================================================================
// Operation Validation
// =============================================================================

pub(crate) fn is_valid_op(op: &str) -> bool {
    is_valid_operator(op)
}

/// Get the appropriate scope for an operation argument
fn get_arg_scope_for_op(
    op: &str,
    arg_index: usize,
    arg_count: usize,
    parent_scope: &V2Scope,
) -> V2Scope {
    match operator_arg_scope(op, arg_index, arg_count) {
        Some(V2OperatorArgScope::Item) => V2Scope::with_parent(parent_scope).with_item(),
        Some(V2OperatorArgScope::ItemAndAcc) => {
            V2Scope::with_parent(parent_scope).with_item().with_acc()
        }
        _ => parent_scope.clone(),
    }
}

/// Validate operation argument count
fn validate_op_args_count(op: &str, count: usize, base_path: &str, ctx: &mut V2ValidationCtx<'_>) {
    let (min, max) = get_op_arg_range(op);

    if count < min {
        ctx.push_error(
            ErrorCode::InvalidArgs,
            format!(
                "{} requires at least {} argument(s), got {}",
                op, min, count
            ),
            base_path,
        );
    } else if let Some(max_val) = max {
        if count > max_val {
            ctx.push_error(
                ErrorCode::InvalidArgs,
                format!(
                    "{} accepts at most {} argument(s), got {}",
                    op, max_val, count
                ),
                base_path,
            );
        }
    }
}

/// Get the valid argument count range for an operation
/// Returns (min, max) where max is None for unlimited
fn get_op_arg_range(op: &str) -> (usize, Option<usize>) {
    operator_arg_range(op)
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // Scope tests
    #[test]
    fn test_scope_new() {
        let scope = V2Scope::new();
        assert!(!scope.allows_item());
        assert!(!scope.allows_acc());
        assert!(!scope.has_binding("x"));
    }

    #[test]
    fn test_scope_with_item() {
        let scope = V2Scope::new().with_item();
        assert!(scope.allows_item());
        assert!(!scope.allows_acc());
    }

    #[test]
    fn test_scope_with_acc() {
        let scope = V2Scope::new().with_acc();
        assert!(!scope.allows_item());
        assert!(scope.allows_acc());
    }

    #[test]
    fn test_scope_let_binding() {
        let mut scope = V2Scope::new();
        assert!(!scope.has_binding("x"));
        scope.add_binding("x".to_string());
        assert!(scope.has_binding("x"));
        assert!(!scope.has_binding("y"));
    }

    #[test]
    fn test_scope_lexical_parent() {
        let mut parent = V2Scope::new();
        parent.add_binding("x".to_string());

        let child = V2Scope::with_parent(&parent);
        assert!(child.has_binding("x")); // Inherited from parent
        assert!(!child.has_binding("y"));
    }

    #[test]
    fn test_scope_child_binding_not_in_parent() {
        let parent = V2Scope::new();
        let mut child = V2Scope::with_parent(&parent);
        child.add_binding("y".to_string());

        assert!(child.has_binding("y"));
        assert!(!parent.has_binding("y")); // Child binding not in parent
    }

    // Op validation tests
    #[test]
    fn test_is_valid_op() {
        for metadata in crate::v2_operator::V2_OPERATORS {
            assert!(
                is_valid_op(metadata.name),
                "{} must be valid",
                metadata.name
            );
        }
        assert!(!is_valid_op("nonexistent_op"));
    }

    #[test]
    fn test_v2_operator_metadata_covers_validation_and_trace_inventory() {
        let names = crate::v2_operator::V2_OPERATORS
            .iter()
            .map(|metadata| metadata.name)
            .collect::<std::collections::BTreeSet<_>>();

        assert_eq!(
            names.len(),
            crate::v2_operator::V2_OPERATORS.len(),
            "v2 operator metadata must not contain duplicate names"
        );
        assert!(
            crate::v2_operator::V2_OPERATORS
                .iter()
                .all(|metadata| metadata.validates),
            "all current v2 operators should remain validation-visible"
        );
        assert!(
            crate::v2_operator::operator("nonexistent_op").is_none(),
            "unknown operators must stay absent from shared metadata"
        );
        assert!(
            crate::v2_operator::operator_has_eager_args("nonexistent_op"),
            "unknown operator trace fallback must stay eager for behavior compatibility"
        );
        assert!(crate::v2_operator::operator_has_eager_args("concat"));
        assert!(crate::v2_operator::operator_has_lazy_arg_trace("coalesce"));
        assert!(crate::v2_operator::operator_has_item_level_trace("map"));
        assert!(!crate::v2_operator::operator_has_eager_args("lookup_first"));
        assert_eq!(
            crate::v2_operator::operator_arg_scope("zip_with", 2, 3),
            Some(crate::v2_operator::V2OperatorArgScope::Item)
        );
        assert_eq!(
            crate::v2_operator::operator_arg_scope("reduce", 0, 1),
            Some(crate::v2_operator::V2OperatorArgScope::ItemAndAcc)
        );
    }

    #[test]
    fn test_op_arg_range() {
        assert_eq!(get_op_arg_range("trim"), (0, Some(0)));
        assert_eq!(get_op_arg_range("multiply"), (1, None));
        assert_eq!(get_op_arg_range("subtract"), (1, None));
        assert_eq!(get_op_arg_range("divide"), (1, None));
        assert_eq!(get_op_arg_range("concat"), (1, None));
        assert_eq!(get_op_arg_range("lookup_first"), (2, Some(4)));
        assert_eq!(get_op_arg_range("split"), (1, Some(1)));
        assert_eq!(get_op_arg_range("pad_start"), (1, Some(2)));
        assert_eq!(get_op_arg_range("round"), (0, Some(1)));
        assert_eq!(get_op_arg_range("zip"), (1, None));
        assert_eq!(get_op_arg_range("gt"), (1, Some(1)));
        assert_eq!(get_op_arg_range("gte"), (1, Some(1)));
        assert_eq!(get_op_arg_range("lt"), (1, Some(1)));
        assert_eq!(get_op_arg_range("lte"), (1, Some(1)));
        assert_eq!(get_op_arg_range("eq"), (1, Some(1)));
        assert_eq!(get_op_arg_range("ne"), (1, Some(1)));
        assert_eq!(get_op_arg_range("match"), (1, Some(1)));
        assert_eq!(get_op_arg_range("zip_with"), (2, None));
        assert_eq!(get_op_arg_range("reduce"), (1, Some(1)));
        assert_eq!(get_op_arg_range("fold"), (2, Some(2)));
        assert_eq!(get_op_arg_range("to_unixtime"), (0, Some(2)));
    }

    #[test]
    fn test_validate_sort_by_order_arg_allowed() {
        let expr = V2Expr::Pipe(V2Pipe {
            start: V2Start::Ref(V2Ref::Input("items".to_string())),
            steps: vec![V2Step::Op(V2OpStep {
                op: "sort_by".to_string(),
                args: vec![
                    V2Expr::Pipe(V2Pipe {
                        start: V2Start::Ref(V2Ref::Item("value".to_string())),
                        steps: vec![],
                    }),
                    V2Expr::Pipe(V2Pipe {
                        start: V2Start::Literal(json!("desc")),
                        steps: vec![],
                    }),
                ],
            })],
        });
        let scope = V2Scope::new();
        let mut ctx = V2ValidationCtx::new(None);

        validate_v2_expr(&expr, "test", &scope, &mut ctx);

        assert!(
            ctx.errors().is_empty(),
            "expected no errors, got: {:?}",
            ctx.errors()
        );
    }

    #[test]
    fn test_validate_zip_with_item_scope_allowed() {
        let expr = V2Expr::Pipe(V2Pipe {
            start: V2Start::Ref(V2Ref::Input("left".to_string())),
            steps: vec![V2Step::Op(V2OpStep {
                op: "zip_with".to_string(),
                args: vec![
                    V2Expr::Pipe(V2Pipe {
                        start: V2Start::Ref(V2Ref::Input("right".to_string())),
                        steps: vec![],
                    }),
                    V2Expr::Pipe(V2Pipe {
                        start: V2Start::Ref(V2Ref::Item(String::new())),
                        steps: vec![],
                    }),
                ],
            })],
        });
        let scope = V2Scope::new();
        let mut ctx = V2ValidationCtx::new(None);

        validate_v2_expr(&expr, "test", &scope, &mut ctx);

        assert!(
            ctx.errors().is_empty(),
            "expected no errors, got: {:?}",
            ctx.errors()
        );
    }

    #[test]
    fn test_validate_v2_expr_rejects_unimplemented_op() {
        let expr = V2Expr::Pipe(V2Pipe {
            start: V2Start::Literal(json!("hello")),
            steps: vec![V2Step::Op(V2OpStep {
                op: "nonexistent_op".to_string(),
                args: vec![
                    V2Expr::Pipe(V2Pipe {
                        start: V2Start::Literal(json!("a")),
                        steps: vec![],
                    }),
                    V2Expr::Pipe(V2Pipe {
                        start: V2Start::Literal(json!("b")),
                        steps: vec![],
                    }),
                ],
            })],
        });
        let scope = V2Scope::new();
        let mut ctx = V2ValidationCtx::new(None);

        validate_v2_expr(&expr, "test", &scope, &mut ctx);

        assert!(
            ctx.errors()
                .iter()
                .any(|err| err.code == ErrorCode::UnknownOp)
        );
    }
}
