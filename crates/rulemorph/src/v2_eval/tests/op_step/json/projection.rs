#[test]
fn test_eval_op_pick_multiple_paths() {
    let op = V2OpStep {
        op: "pick".to_string(),
        args: vec![lit(json!("name")), lit(json!("price"))],
    };
    let ctx = V2EvalContext::new();
    let result = eval_v2_op_step(
        &op,
        EvalValue::Value(json!({"name": "apple", "price": 100, "category": "fruit"})),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(
        result,
        Ok(EvalValue::Value(v)) if v == json!({"name": "apple", "price": 100})
    ));
}

#[test]
fn test_eval_op_omit_multiple_paths() {
    let op = V2OpStep {
        op: "omit".to_string(),
        args: vec![lit(json!("category")), lit(json!("price"))],
    };
    let ctx = V2EvalContext::new();
    let result = eval_v2_op_step(
        &op,
        EvalValue::Value(json!({"name": "apple", "price": 100, "category": "fruit"})),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(
        result,
        Ok(EvalValue::Value(v)) if v == json!({"name": "apple"})
    ));
}

#[test]
fn test_eval_op_pick_paths_array_arg() {
    let op = V2OpStep {
        op: "pick".to_string(),
        args: vec![lit(json!(["name", "price"]))],
    };
    let ctx = V2EvalContext::new();
    let result = eval_v2_op_step(
        &op,
        EvalValue::Value(json!({"name": "apple", "price": 100, "category": "fruit"})),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(
        result,
        Ok(EvalValue::Value(v)) if v == json!({"name": "apple", "price": 100})
    ));
}
