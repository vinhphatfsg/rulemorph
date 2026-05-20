#[test]
fn test_eval_op_to_string() {
    let op = V2OpStep {
        op: "to_string".to_string(),
        args: vec![],
    };
    let ctx = V2EvalContext::new();

    // Number to string
    let result = eval_v2_op_step(
        &op,
        EvalValue::Value(json!(42)),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("42")));

    // Bool to string
    let result = eval_v2_op_step(
        &op,
        EvalValue::Value(json!(true)),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("true")));
}
