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
            assert_eq!(pipe.start, V2Start::ImplicitPipeValue);
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
