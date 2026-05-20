#[test]
fn test_eval_map_step_with_multiply() {
    // map: [multiply: 2] on [1, 2, 3] -> [2, 4, 6]
    let map_step = V2MapStep {
        steps: vec![V2Step::Op(V2OpStep {
            op: "multiply".to_string(),
            args: vec![V2Expr::Pipe(V2Pipe {
                start: V2Start::Literal(json!(2)),
                steps: vec![],
            })],
        })],
    };
    let record = json!({});
    let out = json!({});
    let ctx = V2EvalContext::new();
    let result = eval_v2_map_step(
        &map_step,
        EvalValue::Value(json!([1, 2, 3])),
        &record,
        None,
        &out,
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!([2.0, 4.0, 6.0])));
}
