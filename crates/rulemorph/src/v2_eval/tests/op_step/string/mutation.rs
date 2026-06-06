#[test]
fn test_eval_op_replace() {
    let op = V2OpStep {
        op: "replace".to_string(),
        args: vec![lit(json!("world")), lit(json!("there"))],
    };
    let ctx = V2EvalContext::new();
    let result = eval_v2_op_step(
        &op,
        EvalValue::Value(json!("hello world")),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("hello there")));
}

#[test]
fn test_eval_op_concat_requires_explicit_operand() {
    let op = V2OpStep {
        op: "concat".to_string(),
        args: vec![],
    };
    let ctx = V2EvalContext::new();
    let result = eval_v2_op_step(
        &op,
        EvalValue::Value(json!("Ada")),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );

    assert!(matches!(result, Err(err) if err.to_string().contains("concat requires at least 1 argument(s), got 0")));
}

#[test]
fn test_eval_op_split_and_pad() {
    let split = V2OpStep {
        op: "split".to_string(),
        args: vec![lit(json!(","))],
    };
    let pad_start = V2OpStep {
        op: "pad_start".to_string(),
        args: vec![lit(json!(3)), lit(json!("0"))],
    };
    let pad_end = V2OpStep {
        op: "pad_end".to_string(),
        args: vec![lit(json!(3)), lit(json!("0"))],
    };
    let ctx = V2EvalContext::new();

    let split_result = eval_v2_op_step(
        &split,
        EvalValue::Value(json!("a,b,c")),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(
        split_result,
        Ok(EvalValue::Value(v)) if v == json!(["a", "b", "c"])
    ));

    let pad_start_result = eval_v2_op_step(
        &pad_start,
        EvalValue::Value(json!("7")),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(pad_start_result, Ok(EvalValue::Value(v)) if v == json!("007")));

    let pad_end_result = eval_v2_op_step(
        &pad_end,
        EvalValue::Value(json!("7")),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(pad_end_result, Ok(EvalValue::Value(v)) if v == json!("700")));
}
