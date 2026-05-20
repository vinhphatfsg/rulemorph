#[test]
fn test_eval_op_json_merge() {
    let op = V2OpStep {
        op: "merge".to_string(),
        args: vec![lit(json!({"b": 2}))],
    };
    let ctx = V2EvalContext::new();
    let result = eval_v2_op_step(
        &op,
        EvalValue::Value(json!({"a": 1})),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!({"a": 1, "b": 2})));
}
