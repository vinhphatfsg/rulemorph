#[test]
fn test_eval_op_type_casts() {
    let op_int = V2OpStep {
        op: "int".to_string(),
        args: vec![],
    };
    let op_float = V2OpStep {
        op: "float".to_string(),
        args: vec![],
    };
    let op_bool = V2OpStep {
        op: "bool".to_string(),
        args: vec![],
    };
    let op_string = V2OpStep {
        op: "string".to_string(),
        args: vec![],
    };
    let ctx = V2EvalContext::new();

    let int_result = eval_v2_op_step(
        &op_int,
        EvalValue::Value(json!("42")),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(int_result, Ok(EvalValue::Value(v)) if v == json!(42)));

    let float_result = eval_v2_op_step(
        &op_float,
        EvalValue::Value(json!("2.5")),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    if let Ok(EvalValue::Value(v)) = float_result {
        let value = v.as_f64().unwrap();
        assert!((value - 2.5).abs() < 1e-9);
    } else {
        panic!("expected float cast");
    }

    let bool_result = eval_v2_op_step(
        &op_bool,
        EvalValue::Value(json!("true")),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(bool_result, Ok(EvalValue::Value(v)) if v == json!(true)));

    let string_result = eval_v2_op_step(
        &op_string,
        EvalValue::Value(json!(12)),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(string_result, Ok(EvalValue::Value(v)) if v == json!("12")));
}
