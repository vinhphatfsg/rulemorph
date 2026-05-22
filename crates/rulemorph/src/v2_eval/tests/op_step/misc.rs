#[test]
fn test_eval_op_unknown() {
    let op = V2OpStep {
        op: "unknown_op".to_string(),
        args: vec![],
    };
    let ctx = V2EvalContext::new();
    let result = eval_v2_op_step(
        &op,
        EvalValue::Value(json!("test")),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(result.is_err());
}
