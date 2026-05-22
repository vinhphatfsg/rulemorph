#[test]
fn test_eval_pipe_simple_ref() {
    let pipe = V2Pipe {
        start: V2Start::Ref(V2Ref::Input("name".to_string())),
        steps: vec![],
    };
    let record = json!({"name": "Alice"});
    let out = json!({});
    let ctx = V2EvalContext::new();
    let result = eval_v2_pipe(&pipe, &record, None, &out, "test", &ctx);
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("Alice")));
}

#[test]
fn test_eval_pipe_literal_start() {
    let pipe = V2Pipe {
        start: V2Start::Literal(json!(42)),
        steps: vec![],
    };
    let record = json!({});
    let out = json!({});
    let ctx = V2EvalContext::new();
    let result = eval_v2_pipe(&pipe, &record, None, &out, "test", &ctx);
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(42)));
}

#[test]
fn test_eval_pipe_chain_ops() {
    // ["  hello  ", trim, uppercase]
    let pipe = V2Pipe {
        start: V2Start::Literal(json!("  hello  ")),
        steps: vec![
            V2Step::Op(V2OpStep {
                op: "trim".to_string(),
                args: vec![],
            }),
            V2Step::Op(V2OpStep {
                op: "uppercase".to_string(),
                args: vec![],
            }),
        ],
    };
    let record = json!({});
    let out = json!({});
    let ctx = V2EvalContext::new();
    let result = eval_v2_pipe(&pipe, &record, None, &out, "test", &ctx);
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("HELLO")));
}

#[test]
fn test_eval_expr_with_v2_pipe() {
    let expr = V2Expr::Pipe(V2Pipe {
        start: V2Start::Literal(json!("hello")),
        steps: vec![V2Step::Op(V2OpStep {
            op: "uppercase".to_string(),
            args: vec![],
        })],
    });
    let record = json!({});
    let out = json!({});
    let ctx = V2EvalContext::new();
    let result = eval_v2_expr(&expr, &record, None, &out, "test", &ctx);
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("HELLO")));
}
