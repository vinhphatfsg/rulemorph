#[test]
fn test_eval_start_pipe_value() {
    let ctx = V2EvalContext::new().with_pipe_value(EvalValue::Value(json!(42)));
    let result = eval_v2_start(
        &V2Start::PipeValue,
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(42)));
}

#[test]
fn test_eval_start_pipe_value_not_available() {
    // When pipe value is not set, it returns Missing (not error)
    // This allows ops like lookup_first that don't use pipe input to work
    let ctx = V2EvalContext::new();
    let result = eval_v2_start(
        &V2Start::PipeValue,
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), EvalValue::Missing);
}

#[test]
fn test_eval_start_pipe_value_missing() {
    let ctx = V2EvalContext::new().with_pipe_value(EvalValue::Missing);
    let result = eval_v2_start(
        &V2Start::PipeValue,
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Missing)));
}
