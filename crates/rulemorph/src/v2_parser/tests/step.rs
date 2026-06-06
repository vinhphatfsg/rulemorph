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
    fn test_parse_object_step_shorthand() {
        let value = json!({
            "object": {
                "name": ["$.name", "uppercase"],
                "tags": { "value": ["new", "vip"] },
                "meta": { "source": "literal" }
            }
        });

        let step = parse_v2_step(&value).unwrap();
        if let V2Step::Object(object) = step {
            assert_eq!(object.fields.len(), 3);
            let name = object
                .fields
                .iter()
                .find(|field| field.key == "name")
                .expect("name field");
            assert!(matches!(name.value, V2ObjectFieldValue::Expr(_)));
            let tags = object
                .fields
                .iter()
                .find(|field| field.key == "tags")
                .expect("tags field");
            assert!(matches!(tags.value, V2ObjectFieldValue::Value(_)));
            let meta = object
                .fields
                .iter()
                .find(|field| field.key == "meta")
                .expect("meta field");
            assert!(matches!(meta.value, V2ObjectFieldValue::Expr(_)));
        } else {
            panic!("Expected Object step");
        }
    }

    #[test]
    fn test_parse_object_step_explicit_op_form() {
        let value = json!({
            "op": "object",
            "args": [
                {
                    "id": "@input.id"
                }
            ]
        });

        let step = parse_v2_step(&value).unwrap();
        if let V2Step::Object(object) = step {
            assert_eq!(object.fields.len(), 1);
            assert_eq!(object.fields[0].key, "id");
        } else {
            panic!("Expected Object step");
        }
    }

    #[test]
    fn test_parse_object_step_rejects_invalid_args() {
        let value = json!({
            "op": "object",
            "args": []
        });

        let err = parse_v2_step(&value).unwrap_err();
        assert!(err.to_string().contains("object step expects exactly one object argument"));
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
