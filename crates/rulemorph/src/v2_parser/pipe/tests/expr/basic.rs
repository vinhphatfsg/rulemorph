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
fn test_parse_v2_expr_literal_object_with_object_key_start_pipe() {
        // Multi-step pipe keeps the first object as a literal start even if it has an object key.
        let value = json!([{"object": {"name": "alice"}}, "keys"]);
        let expr = parse_v2_expr(&value).unwrap();

        if let V2Expr::Pipe(pipe) = expr {
            assert_eq!(pipe.start, V2Start::Literal(json!({"object": {"name": "alice"}})));
            assert_eq!(pipe.steps.len(), 1);
            assert!(matches!(&pipe.steps[0], V2Step::Op(op) if op.op == "keys"));
        } else {
            panic!("Expected Pipe expression");
        }
}

#[test]
fn test_parse_v2_expr_single_object_step_uses_implicit_pipe_start() {
        let value = json!([{"object": {"name": "$.name"}}]);
        let expr = parse_v2_expr(&value).unwrap();

        if let V2Expr::Pipe(pipe) = expr {
            assert_eq!(pipe.start, V2Start::ImplicitPipeValue);
            assert_eq!(pipe.steps.len(), 1);
            assert!(matches!(&pipe.steps[0], V2Step::Object(_)));
        } else {
            panic!("Expected Pipe expression");
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
