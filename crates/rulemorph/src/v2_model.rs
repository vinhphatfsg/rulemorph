//! v2 Expression Types for rulemorph v2.0
//!
//! This module defines the data structures for the v2 expression syntax,
//! which uses `@input.*`, `@context.*`, `@out.*` references and pipe-based
//! transformations.

use serde_json::Value as JsonValue;
use crate::model::Expr;

// =============================================================================
// v2 Expression Types
// =============================================================================

/// v2 expression - either a Pipe (new v2 syntax) or V1Fallback (legacy syntax)
#[derive(Debug, Clone, PartialEq)]
pub enum V2Expr {
    Pipe(V2Pipe),
    V1Fallback(Expr),
}

/// v2 Pipe - a start value followed by transformation steps
#[derive(Debug, Clone, PartialEq)]
pub struct V2Pipe {
    pub start: V2Start,
    pub steps: Vec<V2Step>,
}

/// v2 Start - the starting value of a pipe
#[derive(Debug, Clone, PartialEq)]
pub enum V2Start {
    Ref(V2Ref),
    PipeValue,
    Literal(JsonValue),
    V1Expr(Box<Expr>),
}

/// v2 Reference - namespace-qualified references with @ prefix
/// Examples: @input.name, @context.users[0].id, @out.user_id, @item.value, @acc.total, @myVar
#[derive(Debug, Clone, PartialEq)]
pub enum V2Ref {
    Input(String),   // @input.path
    Context(String), // @context.path
    Out(String),     // @out.path
    Item(String),    // @item.path (in map)
    Acc(String),     // @acc.path (in reduce)
    Local(String),   // @varName (let-bound)
}

/// v2 Step - a transformation step in a pipe
#[derive(Debug, Clone, PartialEq)]
pub enum V2Step {
    Op(V2OpStep),
    Let(V2LetStep),
    If(V2IfStep),
    Map(V2MapStep),
}

/// v2 Op Step - a named operation with arguments
#[derive(Debug, Clone, PartialEq)]
pub struct V2OpStep {
    pub op: String,
    pub args: Vec<V2Expr>,
}

/// v2 Let Step - variable bindings
#[derive(Debug, Clone, PartialEq)]
pub struct V2LetStep {
    pub bindings: Vec<(String, V2Expr)>,
}

/// v2 If Step - conditional branching
#[derive(Debug, Clone, PartialEq)]
pub struct V2IfStep {
    pub cond: V2Condition,
    pub then_branch: V2Pipe,
    pub else_branch: Option<V2Pipe>,
}

/// v2 Map Step - array iteration
#[derive(Debug, Clone, PartialEq)]
pub struct V2MapStep {
    pub steps: Vec<V2Step>,
}

/// v2 Condition - logical conditions for if/when
#[derive(Debug, Clone, PartialEq)]
pub enum V2Condition {
    All(Vec<V2Condition>),
    Any(Vec<V2Condition>),
    Comparison(V2Comparison),
    Expr(V2Expr),
}

/// v2 Comparison - comparison operations
#[derive(Debug, Clone, PartialEq)]
pub struct V2Comparison {
    pub op: V2ComparisonOp,
    pub args: Vec<V2Expr>,
}

/// v2 Comparison Operator
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum V2ComparisonOp {
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
    Match,
}

// =============================================================================
// v2 Model Tests
// =============================================================================

#[cfg(test)]
mod v2_model_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_v2_ref_input_creation() {
        let v2_ref = V2Ref::Input("name".to_string());
        assert_eq!(v2_ref, V2Ref::Input("name".to_string()));
    }

    #[test]
    fn test_v2_ref_context_creation() {
        let v2_ref = V2Ref::Context("users[0].id".to_string());
        assert_eq!(v2_ref, V2Ref::Context("users[0].id".to_string()));
    }

    #[test]
    fn test_v2_ref_out_creation() {
        let v2_ref = V2Ref::Out("user_id".to_string());
        assert_eq!(v2_ref, V2Ref::Out("user_id".to_string()));
    }

    #[test]
    fn test_v2_ref_item_creation() {
        let v2_ref = V2Ref::Item("value".to_string());
        assert_eq!(v2_ref, V2Ref::Item("value".to_string()));
    }

    #[test]
    fn test_v2_ref_acc_creation() {
        let v2_ref = V2Ref::Acc("total".to_string());
        assert_eq!(v2_ref, V2Ref::Acc("total".to_string()));
    }

    #[test]
    fn test_v2_ref_local_creation() {
        let v2_ref = V2Ref::Local("price".to_string());
        assert_eq!(v2_ref, V2Ref::Local("price".to_string()));
    }

    #[test]
    fn test_v2_pipe_creation() {
        let pipe = V2Pipe {
            start: V2Start::PipeValue,
            steps: vec![],
        };
        assert_eq!(pipe.start, V2Start::PipeValue);
        assert!(pipe.steps.is_empty());
    }

    #[test]
    fn test_v2_pipe_with_ref_start() {
        let pipe = V2Pipe {
            start: V2Start::Ref(V2Ref::Input("name".to_string())),
            steps: vec![],
        };
        assert_eq!(pipe.start, V2Start::Ref(V2Ref::Input("name".to_string())));
    }

    #[test]
    fn test_v2_pipe_with_literal_start() {
        let pipe = V2Pipe {
            start: V2Start::Literal(json!("hello")),
            steps: vec![],
        };
        assert_eq!(pipe.start, V2Start::Literal(json!("hello")));
    }

    #[test]
    fn test_v2_step_op_creation() {
        let step = V2Step::Op(V2OpStep {
            op: "trim".to_string(),
            args: vec![],
        });
        if let V2Step::Op(op) = step {
            assert_eq!(op.op, "trim");
        } else {
            panic!("Expected Op step");
        }
    }

    #[test]
    fn test_v2_step_let_creation() {
        let step = V2Step::Let(V2LetStep {
            bindings: vec![("x".to_string(), V2Expr::Pipe(V2Pipe {
                start: V2Start::PipeValue,
                steps: vec![],
            }))],
        });
        if let V2Step::Let(let_step) = step {
            assert_eq!(let_step.bindings.len(), 1);
            assert_eq!(let_step.bindings[0].0, "x");
        } else {
            panic!("Expected Let step");
        }
    }

    #[test]
    fn test_v2_step_if_creation() {
        let step = V2Step::If(V2IfStep {
            cond: V2Condition::Expr(V2Expr::Pipe(V2Pipe {
                start: V2Start::PipeValue,
                steps: vec![],
            })),
            then_branch: V2Pipe {
                start: V2Start::PipeValue,
                steps: vec![],
            },
            else_branch: None,
        });
        assert!(matches!(step, V2Step::If(_)));
    }

    #[test]
    fn test_v2_step_map_creation() {
        let step = V2Step::Map(V2MapStep {
            steps: vec![],
        });
        if let V2Step::Map(map_step) = step {
            assert!(map_step.steps.is_empty());
        } else {
            panic!("Expected Map step");
        }
    }

    #[test]
    fn test_v2_condition_all_creation() {
        let cond = V2Condition::All(vec![]);
        if let V2Condition::All(conditions) = cond {
            assert!(conditions.is_empty());
        } else {
            panic!("Expected All condition");
        }
    }

    #[test]
    fn test_v2_condition_any_creation() {
        let cond = V2Condition::Any(vec![]);
        if let V2Condition::Any(conditions) = cond {
            assert!(conditions.is_empty());
        } else {
            panic!("Expected Any condition");
        }
    }

    #[test]
    fn test_v2_condition_comparison_creation() {
        let cond = V2Condition::Comparison(V2Comparison {
            op: V2ComparisonOp::Eq,
            args: vec![],
        });
        if let V2Condition::Comparison(c) = cond {
            assert_eq!(c.op, V2ComparisonOp::Eq);
        } else {
            panic!("Expected Comparison condition");
        }
    }

    #[test]
    fn test_v2_comparison_ops() {
        let ops = [
            V2ComparisonOp::Eq,
            V2ComparisonOp::Ne,
            V2ComparisonOp::Gt,
            V2ComparisonOp::Gte,
            V2ComparisonOp::Lt,
            V2ComparisonOp::Lte,
            V2ComparisonOp::Match,
        ];
        assert_eq!(ops.len(), 7);
        let eq = V2ComparisonOp::Eq;
        let eq_copy = eq;
        assert_eq!(eq, eq_copy);
    }
}
