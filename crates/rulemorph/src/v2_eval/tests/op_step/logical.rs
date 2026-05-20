#[test]
fn test_eval_op_and_or_short_circuit() {
    let or_op = V2OpStep {
        op: "or".to_string(),
        args: vec![V2Expr::Pipe(V2Pipe {
            start: V2Start::Literal(json!(1)),
            steps: vec![V2Step::Op(V2OpStep {
                op: "divide".to_string(),
                args: vec![V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(0)),
                    steps: vec![],
                })],
            })],
        })],
    };
    let and_op = V2OpStep {
        op: "and".to_string(),
        args: vec![V2Expr::Pipe(V2Pipe {
            start: V2Start::Literal(json!(1)),
            steps: vec![V2Step::Op(V2OpStep {
                op: "divide".to_string(),
                args: vec![V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(0)),
                    steps: vec![],
                })],
            })],
        })],
    };
    let ctx = V2EvalContext::new();

    let or_result = eval_v2_op_step(
        &or_op,
        EvalValue::Value(json!(true)),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(or_result, Ok(EvalValue::Value(v)) if v == json!(true)));

    let and_result = eval_v2_op_step(
        &and_op,
        EvalValue::Value(json!(false)),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(and_result, Ok(EvalValue::Value(v)) if v == json!(false)));
}

#[test]
fn test_eval_op_coalesce() {
    let op = V2OpStep {
        op: "coalesce".to_string(),
        args: vec![V2Expr::Pipe(V2Pipe {
            start: V2Start::Literal(json!("default")),
            steps: vec![],
        })],
    };
    let ctx = V2EvalContext::new();

    // When pipe value is present, use it
    let result = eval_v2_op_step(
        &op,
        EvalValue::Value(json!("value")),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("value")));

    // When pipe value is null, use first non-null arg
    let result = eval_v2_op_step(
        &op,
        EvalValue::Value(json!(null)),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("default")));

    // When pipe value is missing, use first non-null arg
    let result = eval_v2_op_step(
        &op,
        EvalValue::Missing,
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("default")));
}
