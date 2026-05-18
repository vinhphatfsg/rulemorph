use super::*;
use crate::v2_model::{V2ComparisonOp, V2Condition, V2Ref};
use serde_json::{Value as JsonValue, json};

mod v2_pipe_parser_tests {
    use super::*;

    #[test]
    fn test_parse_simple_pipe() {
        // ["@input.name", "trim"]
        let arr = vec![json!("@input.name"), json!("trim")];
        let pipe = parse_v2_pipe(&arr).unwrap();

        assert_eq!(pipe.start, V2Start::Ref(V2Ref::Input("name".to_string())));
        assert_eq!(pipe.steps.len(), 1);
        if let V2Step::Op(op) = &pipe.steps[0] {
            assert_eq!(op.op, "trim");
            assert!(op.args.is_empty());
        } else {
            panic!("Expected Op step");
        }
    }

    #[test]
    fn test_parse_pipe_with_multiple_steps() {
        // ["@input.name", "trim", "uppercase"]
        let arr = vec![json!("@input.name"), json!("trim"), json!("uppercase")];
        let pipe = parse_v2_pipe(&arr).unwrap();

        assert_eq!(pipe.steps.len(), 2);
    }

    #[test]
    fn test_parse_pipe_with_op_object() {
        // ["@input.value", { "op": "add", "args": [10] }]
        let arr = vec![json!("@input.value"), json!({ "op": "add", "args": [10] })];
        let pipe = parse_v2_pipe(&arr).unwrap();

        if let V2Step::Op(op) = &pipe.steps[0] {
            assert_eq!(op.op, "add");
            assert_eq!(op.args.len(), 1);
        } else {
            panic!("Expected Op step");
        }
    }

    #[test]
    fn test_parse_pipe_with_pipe_value_start() {
        // ["$", "trim"]
        let arr = vec![json!("$"), json!("trim")];
        let pipe = parse_v2_pipe(&arr).unwrap();

        assert_eq!(pipe.start, V2Start::PipeValue);
    }

    #[test]
    fn test_parse_pipe_with_literal_start() {
        // [42, { "op": "multiply", "args": [2] }]
        let arr = vec![json!(42), json!({ "op": "multiply", "args": [2] })];
        let pipe = parse_v2_pipe(&arr).unwrap();

        assert_eq!(pipe.start, V2Start::Literal(json!(42)));
    }

    #[test]
    fn test_parse_pipe_with_literal_escape() {
        // ["lit:@input.name", "trim"]
        let arr = vec![json!("lit:@input.name"), json!("trim")];
        let pipe = parse_v2_pipe(&arr).unwrap();

        assert_eq!(pipe.start, V2Start::Literal(json!("@input.name")));
    }

    #[test]
    fn test_parse_empty_pipe_error() {
        let arr: Vec<JsonValue> = vec![];
        let result = parse_v2_pipe(&arr);
        assert_eq!(result, Err(V2ParseError::EmptyPipe));
    }

    #[test]
    fn test_parse_v2_start_ref() {
        let result = parse_v2_start(&json!("@input.name")).unwrap();
        assert_eq!(result, V2Start::Ref(V2Ref::Input("name".to_string())));
    }

    #[test]
    fn test_parse_v2_start_pipe_value() {
        let result = parse_v2_start(&json!("$")).unwrap();
        assert_eq!(result, V2Start::PipeValue);
    }

    #[test]
    fn test_parse_v2_start_literal() {
        let result = parse_v2_start(&json!(123)).unwrap();
        assert_eq!(result, V2Start::Literal(json!(123)));

        let result = parse_v2_start(&json!(true)).unwrap();
        assert_eq!(result, V2Start::Literal(json!(true)));

        let result = parse_v2_start(&json!(null)).unwrap();
        assert_eq!(result, V2Start::Literal(json!(null)));
    }

    #[test]
    fn test_parse_v2_start_invalid_at_ref_error() {
        let invalid_refs = [json!("@"), json!("@foo-bar"), json!("@123invalid")];
        for value in invalid_refs {
            let err = parse_v2_start(&value).unwrap_err();
            assert!(matches!(err, V2ParseError::InvalidStart(_)));
        }
    }
}

mod v2_rulefile_parser_tests {
    use super::*;

    #[test]
    fn test_parse_v2_expr_from_yaml_array() {
        // Test that parse_v2_expr can handle YAML-parsed array representing v2 expr
        let value = json!(["@input.name", "trim", "uppercase"]);
        let expr = parse_v2_expr(&value).unwrap();

        if let V2Expr::Pipe(pipe) = expr {
            assert_eq!(pipe.start, V2Start::Ref(V2Ref::Input("name".to_string())));
            assert_eq!(pipe.steps.len(), 2);
        } else {
            panic!("Expected Pipe expression");
        }
    }

    #[test]
    fn test_parse_v2_expr_with_op_args() {
        // v2 expr with op that has arguments
        let value = json!(["@input.price", { "op": "multiply", "args": [0.9] }]);
        let expr = parse_v2_expr(&value).unwrap();

        if let V2Expr::Pipe(pipe) = expr {
            assert_eq!(pipe.steps.len(), 1);
            if let V2Step::Op(op) = &pipe.steps[0] {
                assert_eq!(op.op, "multiply");
                assert_eq!(op.args.len(), 1);
            } else {
                panic!("Expected Op step");
            }
        } else {
            panic!("Expected Pipe expression");
        }
    }

    #[test]
    fn test_parse_v2_expr_literal_object_start_pipe() {
        // Pipe start can be a literal object followed by steps
        let value = json!([{"foo": 1}, "keys"]);
        let expr = parse_v2_expr(&value).unwrap();

        if let V2Expr::Pipe(pipe) = expr {
            assert_eq!(pipe.start, V2Start::Literal(json!({"foo": 1})));
            assert_eq!(pipe.steps.len(), 1);
            if let V2Step::Op(op) = &pipe.steps[0] {
                assert_eq!(op.op, "keys");
            } else {
                panic!("Expected Op step");
            }
        } else {
            panic!("Expected Pipe expression");
        }
    }

    #[test]
    fn test_parse_v2_expr_literal_object_with_op_key_start_pipe() {
        // Literal object starts should not be coerced into implicit steps.
        let value = json!([{"op": "x"}, "keys"]);
        let expr = parse_v2_expr(&value).unwrap();

        if let V2Expr::Pipe(pipe) = expr {
            assert_eq!(pipe.start, V2Start::Literal(json!({"op": "x"})));
            assert_eq!(pipe.steps.len(), 1);
            if let V2Step::Op(op) = &pipe.steps[0] {
                assert_eq!(op.op, "keys");
            } else {
                panic!("Expected Op step");
            }
        } else {
            panic!("Expected Pipe expression");
        }
    }

    #[test]
    fn test_parse_v2_condition_from_record_when() {
        // Test parsing conditions as used in record_when
        let value = json!({
            "all": [
                { "gt": ["@input.score", 0] },
                { "eq": ["@input.active", true] }
            ]
        });

        let cond = crate::v2_parser::parse_v2_condition(&value).unwrap();
        if let V2Condition::All(conditions) = cond {
            assert_eq!(conditions.len(), 2);
        } else {
            panic!("Expected All condition");
        }
    }

    #[test]
    fn test_parse_v2_expr_single_ref() {
        // Single reference without steps
        let value = json!("@input.name");
        let expr = parse_v2_expr(&value).unwrap();

        if let V2Expr::Pipe(pipe) = expr {
            assert_eq!(pipe.start, V2Start::Ref(V2Ref::Input("name".to_string())));
            assert!(pipe.steps.is_empty());
        } else {
            panic!("Expected Pipe expression");
        }
    }

    #[test]
    fn test_parse_v2_expr_invalid_at_ref_error() {
        let value = json!("@foo-bar");
        let err = parse_v2_expr(&value).unwrap_err();
        assert!(matches!(err, V2ParseError::InvalidStart(_)));
    }

    #[test]
    fn test_parse_v2_expr_v1_fallback_op() {
        // v1 style op within v2 pipe: { op: "uppercase", args: [] }
        let value = json!(["@input.name", { "op": "uppercase", "args": [] }]);
        let expr = parse_v2_expr(&value).unwrap();

        if let V2Expr::Pipe(pipe) = expr {
            if let V2Step::Op(op) = &pipe.steps[0] {
                assert_eq!(op.op, "uppercase");
            } else {
                panic!("Expected Op step");
            }
        } else {
            panic!("Expected Pipe expression");
        }
    }

    #[test]
    fn test_parse_v2_expr_single_step_comparison_alias() {
        // Single-step pipe should treat alias comparison op as a step.
        let value = json!([{ "gt": 80 }]);
        let expr = parse_v2_expr(&value).unwrap();

        if let V2Expr::Pipe(pipe) = expr {
            assert_eq!(pipe.start, V2Start::PipeValue);
            assert_eq!(pipe.steps.len(), 1);
            if let V2Step::Op(op) = &pipe.steps[0] {
                assert_eq!(op.op, "gt");
                assert_eq!(op.args.len(), 1);
            } else {
                panic!("Expected Op step");
            }
        } else {
            panic!("Expected Pipe expression");
        }
    }

    #[test]
    fn test_parse_v2_mapping_when_condition() {
        // mapping.when with v2 condition syntax
        let value = json!({ "eq": ["@input.role", "admin"] });
        let cond = crate::v2_parser::parse_v2_condition(&value).unwrap();

        if let V2Condition::Comparison(comp) = cond {
            assert_eq!(comp.op, V2ComparisonOp::Eq);
            assert_eq!(comp.args.len(), 2);
        } else {
            panic!("Expected Comparison condition");
        }
    }

    #[test]
    fn test_parse_v2_expr_with_let_step() {
        // v2 expr with let binding
        let value = json!([
            "@input.price",
            { "let": { "base": "$" } },
            { "op": "add", "args": [100] }
        ]);

        let expr = parse_v2_expr(&value).unwrap();
        if let V2Expr::Pipe(pipe) = expr {
            assert_eq!(pipe.steps.len(), 2);
            assert!(matches!(pipe.steps[0], V2Step::Let(_)));
            assert!(matches!(pipe.steps[1], V2Step::Op(_)));
        } else {
            panic!("Expected Pipe expression");
        }
    }

    #[test]
    fn test_parse_v2_expr_with_if_step() {
        // v2 expr with if step
        let value = json!([
            "@input.amount",
            {
                "if": { "gt": ["$", 10000] },
                "then": [{ "op": "multiply", "args": [0.9] }],
                "else": ["$"]
            }
        ]);

        let expr = parse_v2_expr(&value).unwrap();
        if let V2Expr::Pipe(pipe) = expr {
            assert_eq!(pipe.steps.len(), 1);
            assert!(matches!(pipe.steps[0], V2Step::If(_)));
        } else {
            panic!("Expected Pipe expression");
        }
    }

    #[test]
    fn test_parse_v2_expr_with_map_step() {
        // v2 expr with map step for array processing
        let value = json!([
            "@input.items",
            {
                "map": [
                    { "op": "get", "args": ["name"] }
                ]
            }
        ]);

        let expr = parse_v2_expr(&value).unwrap();
        if let V2Expr::Pipe(pipe) = expr {
            assert_eq!(pipe.steps.len(), 1);
            assert!(matches!(pipe.steps[0], V2Step::Map(_)));
        } else {
            panic!("Expected Pipe expression");
        }
    }

    #[test]
    fn test_is_v2_expr_pipe_array() {
        // Helper function to detect v2 syntax
        assert!(is_v2_expr(&json!(["@input.name", "trim"])));
        assert!(is_v2_expr(&json!([])));
        assert!(is_v2_expr(&json!(["hello", "trim"])));
        assert!(is_v2_expr(&json!([{"lookup_first": []}, "trim"])));
        assert!(is_v2_expr(&json!("@input.name")));
        assert!(is_v2_expr(&json!("lit:@input.name")));
        assert!(!is_v2_expr(&json!({ "ref": "input.name" })));
        assert!(!is_v2_expr(&json!({ "op": "uppercase", "args": [] })));
    }
}
