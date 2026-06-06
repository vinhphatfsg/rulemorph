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

#[test]
fn test_eval_op_rejects_explicit_args_outside_operator_metadata_range() {
    let op = V2OpStep {
        op: "trim".to_string(),
        args: vec![lit(json!("ignored"))],
    };
    let ctx = V2EvalContext::new();
    let err = eval_v2_op_step(
        &op,
        EvalValue::Value(json!(" test ")),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    )
    .expect_err("runtime should enforce the same explicit arg count as validation");

    assert_eq!(err.kind, TransformErrorKind::ExprError);
    assert_eq!(err.path.as_deref(), Some("test"));
    assert!(
        err.message
            .contains("trim accepts at most 0 argument(s), got 1"),
        "unexpected error message: {}",
        err.message
    );
}
