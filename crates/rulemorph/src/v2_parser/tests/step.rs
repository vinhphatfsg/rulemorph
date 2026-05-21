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
