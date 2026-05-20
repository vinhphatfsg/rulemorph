//! v2 Expression Static Validator
//!
//! This module provides compile-time validation for v2 expressions,
//! catching errors that previously only occurred at runtime.

#[cfg(test)]
use crate::error::ErrorCode;
#[cfg(test)]
use crate::v2_model::V2OpStep;
#[cfg(test)]
use crate::v2_model::V2Ref;
#[cfg(test)]
use crate::v2_model::{V2Expr, V2Pipe, V2Start, V2Step};

mod conditions;
mod context;
mod dependencies;
mod operators;
mod references;
mod steps;
mod types;

pub use self::conditions::validate_v2_condition;
pub use self::context::{V2Scope, V2ValidationCtx};
pub use self::dependencies::{collect_out_references, validate_no_cyclic_dependencies};
pub(crate) use self::operators::is_valid_op;
pub use self::references::validate_v2_ref;
pub use self::steps::{validate_v2_expr, validate_v2_pipe};
pub use self::types::{V2Type, infer_v2_expr_type};

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
        for metadata in crate::v2_operator::operators() {
            assert!(
                operators::is_valid_op(metadata.name),
                "{} must be valid",
                metadata.name
            );
        }
        assert!(!operators::is_valid_op("nonexistent_op"));
    }

    #[test]
    fn test_v2_operator_metadata_covers_validation_and_trace_inventory() {
        let inventory = crate::v2_operator::operators().collect::<Vec<_>>();
        let names = inventory
            .iter()
            .map(|metadata| metadata.name)
            .collect::<std::collections::BTreeSet<_>>();

        assert_eq!(
            names.len(),
            inventory.len(),
            "v2 operator metadata must not contain duplicate names"
        );
        assert!(
            inventory.iter().all(|metadata| metadata.validates),
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
        assert_eq!(operators::get_op_arg_range("trim"), (0, Some(0)));
        assert_eq!(operators::get_op_arg_range("multiply"), (1, None));
        assert_eq!(operators::get_op_arg_range("subtract"), (1, None));
        assert_eq!(operators::get_op_arg_range("divide"), (1, None));
        assert_eq!(operators::get_op_arg_range("concat"), (1, None));
        assert_eq!(operators::get_op_arg_range("lookup_first"), (2, Some(4)));
        assert_eq!(operators::get_op_arg_range("split"), (1, Some(1)));
        assert_eq!(operators::get_op_arg_range("pad_start"), (1, Some(2)));
        assert_eq!(operators::get_op_arg_range("round"), (0, Some(1)));
        assert_eq!(operators::get_op_arg_range("zip"), (1, None));
        assert_eq!(operators::get_op_arg_range("gt"), (1, Some(1)));
        assert_eq!(operators::get_op_arg_range("gte"), (1, Some(1)));
        assert_eq!(operators::get_op_arg_range("lt"), (1, Some(1)));
        assert_eq!(operators::get_op_arg_range("lte"), (1, Some(1)));
        assert_eq!(operators::get_op_arg_range("eq"), (1, Some(1)));
        assert_eq!(operators::get_op_arg_range("ne"), (1, Some(1)));
        assert_eq!(operators::get_op_arg_range("match"), (1, Some(1)));
        assert_eq!(operators::get_op_arg_range("zip_with"), (2, None));
        assert_eq!(operators::get_op_arg_range("reduce"), (1, Some(1)));
        assert_eq!(operators::get_op_arg_range("fold"), (2, Some(2)));
        assert_eq!(operators::get_op_arg_range("to_unixtime"), (0, Some(2)));
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
