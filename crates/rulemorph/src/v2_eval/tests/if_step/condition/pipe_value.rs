#[test]
fn test_eval_condition_with_pipe_value() {
    // Condition: { gt: ["$", 100] }
    let cond = V2Condition::Comparison(V2Comparison {
        op: V2ComparisonOp::Gt,
        args: vec![
            V2Expr::Pipe(V2Pipe {
                start: V2Start::PipeValue,
                steps: vec![],
            }),
            V2Expr::Pipe(V2Pipe {
                start: V2Start::Literal(json!(100)),
                steps: vec![],
            }),
        ],
    });
    let record = json!({});
    let out = json!({});
    let ctx = V2EvalContext::new().with_pipe_value(EvalValue::Value(json!(150)));
    let result = eval_v2_condition(&cond, &record, None, &out, "test", &ctx);
    assert!(matches!(result, Ok(true)));
}
