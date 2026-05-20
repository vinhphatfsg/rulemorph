#[test]
fn test_eval_op_add() {
    let op = V2OpStep {
        op: "add".to_string(),
        args: vec![V2Expr::Pipe(V2Pipe {
            start: V2Start::Literal(json!(10)),
            steps: vec![],
        })],
    };
    let ctx = V2EvalContext::new();
    let result = eval_v2_op_step(
        &op,
        EvalValue::Value(json!(5)),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(15.0)));
}

#[test]
fn test_eval_op_subtract() {
    let op = V2OpStep {
        op: "subtract".to_string(),
        args: vec![V2Expr::Pipe(V2Pipe {
            start: V2Start::Literal(json!(3)),
            steps: vec![],
        })],
    };
    let ctx = V2EvalContext::new();
    let result = eval_v2_op_step(
        &op,
        EvalValue::Value(json!(10)),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(7.0)));
}

#[test]
fn test_eval_op_multiply() {
    let op = V2OpStep {
        op: "multiply".to_string(),
        args: vec![V2Expr::Pipe(V2Pipe {
            start: V2Start::Literal(json!(0.9)),
            steps: vec![],
        })],
    };
    let ctx = V2EvalContext::new();
    let result = eval_v2_op_step(
        &op,
        EvalValue::Value(json!(100)),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(90.0)));
}

#[test]
fn test_eval_op_divide() {
    let op = V2OpStep {
        op: "divide".to_string(),
        args: vec![V2Expr::Pipe(V2Pipe {
            start: V2Start::Literal(json!(2)),
            steps: vec![],
        })],
    };
    let ctx = V2EvalContext::new();
    let result = eval_v2_op_step(
        &op,
        EvalValue::Value(json!(10)),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(5.0)));
}

#[test]
fn test_eval_op_divide_by_zero() {
    let op = V2OpStep {
        op: "divide".to_string(),
        args: vec![V2Expr::Pipe(V2Pipe {
            start: V2Start::Literal(json!(0)),
            steps: vec![],
        })],
    };
    let ctx = V2EvalContext::new();
    let result = eval_v2_op_step(
        &op,
        EvalValue::Value(json!(10)),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(result.is_err());
}
