use serde_json::json;

use super::*;
use crate::v2_model::{V2Condition, V2Ref, V2Step};

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

mod v2_step_parser_tests {
    use super::*;

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
