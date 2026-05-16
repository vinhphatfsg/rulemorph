//! v2 Expression Parser for rulemorph v2.0
//!
//! This module parses v2 expression syntax including:
//! - `@input.*`, `@context.*`, `@out.*` namespace references
//! - `@item.*`, `@acc.*` iteration references
//! - `@localVar` local variable references
//! - `$` pipe value
//! - `lit:` escape prefix for literals
//! - Pipe arrays: `[start_value, step1, step2, ...]`

#[cfg(test)]
use crate::v2_model::V2Ref;
#[cfg(test)]
use crate::v2_model::V2Step;
#[cfg(test)]
use crate::v2_model::{V2ComparisonOp, V2Condition};
// Note: V2Step::Ref variant is used for reference steps like "@doubled"

mod condition;
mod error;
mod pipe;
mod ref_parse;
mod step;

pub use condition::parse_v2_condition;
pub use error::V2ParseError;
use pipe::parse_v2_expr_args;
pub use pipe::{
    is_v2_expr, parse_v2_expr, parse_v2_pipe, parse_v2_pipe_from_value, parse_v2_start,
};
pub use ref_parse::{extract_literal, is_literal_escape, is_pipe_value, is_v2_ref, parse_v2_ref};
pub use step::parse_v2_step;

// =============================================================================
// v2 Parser Tests
// =============================================================================

#[cfg(test)]
mod v2_ref_parser_tests {
    use super::*;

    #[test]
    fn test_parse_input_ref() {
        assert_eq!(
            parse_v2_ref("@input.name"),
            Some(V2Ref::Input("name".to_string()))
        );
        assert_eq!(
            parse_v2_ref("@input.user.profile.name"),
            Some(V2Ref::Input("user.profile.name".to_string()))
        );
        assert_eq!(
            parse_v2_ref("@input.items[0].id"),
            Some(V2Ref::Input("items[0].id".to_string()))
        );
        assert_eq!(parse_v2_ref("@input"), Some(V2Ref::Input(String::new())));
    }

    #[test]
    fn test_parse_context_ref() {
        assert_eq!(
            parse_v2_ref("@context.config"),
            Some(V2Ref::Context("config".to_string()))
        );
        assert_eq!(
            parse_v2_ref("@context.users[0].id"),
            Some(V2Ref::Context("users[0].id".to_string()))
        );
        assert_eq!(
            parse_v2_ref("@context"),
            Some(V2Ref::Context(String::new()))
        );
    }

    #[test]
    fn test_parse_out_ref() {
        assert_eq!(
            parse_v2_ref("@out.user_id"),
            Some(V2Ref::Out("user_id".to_string()))
        );
        assert_eq!(
            parse_v2_ref("@out.computed_field"),
            Some(V2Ref::Out("computed_field".to_string()))
        );
        assert_eq!(parse_v2_ref("@out"), Some(V2Ref::Out(String::new())));
    }

    #[test]
    fn test_parse_item_ref() {
        assert_eq!(
            parse_v2_ref("@item.value"),
            Some(V2Ref::Item("value".to_string()))
        );
        assert_eq!(parse_v2_ref("@item"), Some(V2Ref::Item(String::new())));
    }

    #[test]
    fn test_parse_acc_ref() {
        assert_eq!(
            parse_v2_ref("@acc.total"),
            Some(V2Ref::Acc("total".to_string()))
        );
        assert_eq!(parse_v2_ref("@acc"), Some(V2Ref::Acc(String::new())));
    }

    #[test]
    fn test_parse_local_ref() {
        assert_eq!(
            parse_v2_ref("@myVar"),
            Some(V2Ref::Local("myVar".to_string()))
        );
        assert_eq!(
            parse_v2_ref("@price"),
            Some(V2Ref::Local("price".to_string()))
        );
        assert_eq!(
            parse_v2_ref("@_temp"),
            Some(V2Ref::Local("_temp".to_string()))
        );
        assert_eq!(
            parse_v2_ref("@var123"),
            Some(V2Ref::Local("var123".to_string()))
        );
    }

    #[test]
    fn test_invalid_refs() {
        // No @ prefix
        assert_eq!(parse_v2_ref("input.name"), None);
        // Empty after @
        assert_eq!(parse_v2_ref("@"), None);
        // Trailing dot for root namespaces should be invalid
        assert_eq!(parse_v2_ref("@input."), None);
        assert_eq!(parse_v2_ref("@context."), None);
        assert_eq!(parse_v2_ref("@out."), None);
        assert_eq!(parse_v2_ref("@item."), None);
        assert_eq!(parse_v2_ref("@acc."), None);
        // Invalid identifier
        assert_eq!(parse_v2_ref("@123invalid"), None);
    }

    #[test]
    fn test_is_pipe_value() {
        assert!(is_pipe_value("$"));
        assert!(!is_pipe_value("$$"));
        assert!(!is_pipe_value("@input.name"));
        assert!(!is_pipe_value(""));
    }

    #[test]
    fn test_is_literal_escape() {
        assert!(is_literal_escape("lit:@input.name"));
        assert!(is_literal_escape("lit:$"));
        assert!(is_literal_escape("lit:"));
        assert!(!is_literal_escape("@input.name"));
        assert!(!is_literal_escape("literal:"));
    }

    #[test]
    fn test_extract_literal() {
        assert_eq!(extract_literal("lit:@input.name"), Some("@input.name"));
        assert_eq!(extract_literal("lit:$"), Some("$"));
        assert_eq!(extract_literal("lit:"), Some(""));
        assert_eq!(extract_literal("@input.name"), None);
    }

    #[test]
    fn test_is_v2_ref() {
        assert!(is_v2_ref("@input.name"));
        assert!(is_v2_ref("@myVar"));
        assert!(!is_v2_ref("input.name"));
        assert!(!is_v2_ref("$"));
        assert!(!is_v2_ref("lit:@input"));
    }
}

// =============================================================================
// v2 Condition Parser Tests (T05)
// =============================================================================

#[cfg(test)]
mod v2_condition_parser_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_parse_condition_all() {
        let value = json!({
            "all": [
                { "eq": ["@input.status", "active"] },
                { "gt": ["@input.age", 18] }
            ]
        });

        let cond = parse_v2_condition(&value).unwrap();
        if let V2Condition::All(conditions) = cond {
            assert_eq!(conditions.len(), 2);
        } else {
            panic!("Expected All condition");
        }
    }

    #[test]
    fn test_parse_condition_any() {
        let value = json!({
            "any": [
                { "eq": ["@input.role", "admin"] },
                { "eq": ["@input.role", "moderator"] }
            ]
        });

        let cond = parse_v2_condition(&value).unwrap();
        if let V2Condition::Any(conditions) = cond {
            assert_eq!(conditions.len(), 2);
        } else {
            panic!("Expected Any condition");
        }
    }

    #[test]
    fn test_parse_condition_eq() {
        let value = json!({ "eq": ["@input.name", "John"] });
        let cond = parse_v2_condition(&value).unwrap();

        if let V2Condition::Comparison(comp) = cond {
            assert_eq!(comp.op, V2ComparisonOp::Eq);
            assert_eq!(comp.args.len(), 2);
        } else {
            panic!("Expected Comparison condition");
        }
    }

    #[test]
    fn test_parse_condition_ne() {
        let value = json!({ "ne": ["@input.status", "deleted"] });
        let cond = parse_v2_condition(&value).unwrap();

        if let V2Condition::Comparison(comp) = cond {
            assert_eq!(comp.op, V2ComparisonOp::Ne);
        } else {
            panic!("Expected Comparison condition");
        }
    }

    #[test]
    fn test_parse_condition_gt() {
        let value = json!({ "gt": ["@input.age", 18] });
        let cond = parse_v2_condition(&value).unwrap();

        if let V2Condition::Comparison(comp) = cond {
            assert_eq!(comp.op, V2ComparisonOp::Gt);
        } else {
            panic!("Expected Comparison condition");
        }
    }

    #[test]
    fn test_parse_condition_gte() {
        let value = json!({ "gte": ["@input.score", 60] });
        let cond = parse_v2_condition(&value).unwrap();

        if let V2Condition::Comparison(comp) = cond {
            assert_eq!(comp.op, V2ComparisonOp::Gte);
        } else {
            panic!("Expected Comparison condition");
        }
    }

    #[test]
    fn test_parse_condition_lt() {
        let value = json!({ "lt": ["@input.count", 100] });
        let cond = parse_v2_condition(&value).unwrap();

        if let V2Condition::Comparison(comp) = cond {
            assert_eq!(comp.op, V2ComparisonOp::Lt);
        } else {
            panic!("Expected Comparison condition");
        }
    }

    #[test]
    fn test_parse_condition_lte() {
        let value = json!({ "lte": ["@input.retries", 3] });
        let cond = parse_v2_condition(&value).unwrap();

        if let V2Condition::Comparison(comp) = cond {
            assert_eq!(comp.op, V2ComparisonOp::Lte);
        } else {
            panic!("Expected Comparison condition");
        }
    }

    #[test]
    fn test_parse_condition_match() {
        let value = json!({ "match": ["@input.email", "^[a-z]+@"] });
        let cond = parse_v2_condition(&value).unwrap();

        if let V2Condition::Comparison(comp) = cond {
            assert_eq!(comp.op, V2ComparisonOp::Match);
        } else {
            panic!("Expected Comparison condition");
        }
    }

    #[test]
    fn test_parse_nested_conditions() {
        let value = json!({
            "all": [
                { "any": [
                    { "eq": ["@input.type", "A"] },
                    { "eq": ["@input.type", "B"] }
                ]},
                { "gt": ["@input.value", 0] }
            ]
        });

        let cond = parse_v2_condition(&value).unwrap();
        if let V2Condition::All(conditions) = cond {
            assert_eq!(conditions.len(), 2);
            assert!(matches!(conditions[0], V2Condition::Any(_)));
        } else {
            panic!("Expected All condition");
        }
    }
}

// =============================================================================
// v2 Step Parser Tests (T06)
// =============================================================================

#[cfg(test)]
mod v2_step_parser_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_parse_let_step() {
        let value = json!({
            "let": {
                "x": "@input.value",
                "y": 10
            }
        });

        let step = parse_v2_step(&value).unwrap();
        if let V2Step::Let(let_step) = step {
            assert_eq!(let_step.bindings.len(), 2);
        } else {
            panic!("Expected Let step");
        }
    }

    #[test]
    fn test_parse_if_step() {
        let value = json!({
            "if": { "gt": ["@input.age", 18] },
            "then": ["adult"],
            "else": ["minor"]
        });

        let step = parse_v2_step(&value).unwrap();
        if let V2Step::If(if_step) = step {
            assert!(matches!(if_step.cond, V2Condition::Comparison(_)));
            assert!(if_step.else_branch.is_some());
        } else {
            panic!("Expected If step");
        }
    }

    #[test]
    fn test_parse_if_step_without_else() {
        let value = json!({
            "if": { "eq": ["@input.enabled", true] },
            "then": ["process"]
        });

        let step = parse_v2_step(&value).unwrap();
        if let V2Step::If(if_step) = step {
            assert!(if_step.else_branch.is_none());
        } else {
            panic!("Expected If step");
        }
    }

    #[test]
    fn test_parse_map_step() {
        let value = json!({
            "map": [
                { "op": "multiply", "args": [2] }
            ]
        });

        let step = parse_v2_step(&value).unwrap();
        if let V2Step::Map(map_step) = step {
            assert_eq!(map_step.steps.len(), 1);
        } else {
            panic!("Expected Map step");
        }
    }

    #[test]
    fn test_parse_op_step_shorthand() {
        // String shorthand for simple operations
        let step = parse_v2_step(&json!("trim")).unwrap();
        if let V2Step::Op(op) = step {
            assert_eq!(op.op, "trim");
            assert!(op.args.is_empty());
        } else {
            panic!("Expected Op step");
        }
    }

    #[test]
    fn test_parse_op_step_with_args() {
        let value = json!({
            "op": "concat",
            "args": ["@input.first", " ", "@input.last"]
        });

        let step = parse_v2_step(&value).unwrap();
        if let V2Step::Op(op) = step {
            assert_eq!(op.op, "concat");
            assert_eq!(op.args.len(), 3);
        } else {
            panic!("Expected Op step");
        }
    }

    #[test]
    fn test_parse_complex_pipe_with_steps() {
        // Complex pipe with multiple step types
        let arr = vec![
            json!("@input.items"),
            json!({ "let": { "threshold": 100 } }),
            json!({ "map": [
                { "if": { "gt": ["@item.value", "@threshold"] },
                  "then": ["@item.value"],
                  "else": [0]
                }
            ]}),
        ];

        let pipe = parse_v2_pipe(&arr).unwrap();
        assert_eq!(pipe.steps.len(), 2);
        assert!(matches!(pipe.steps[0], V2Step::Let(_)));
        assert!(matches!(pipe.steps[1], V2Step::Map(_)));
    }
}
