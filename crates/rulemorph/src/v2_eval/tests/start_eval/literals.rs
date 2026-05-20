#[test]
fn test_eval_start_literal_string() {
    let ctx = V2EvalContext::new();
    let result = eval_v2_start(
        &V2Start::Literal(json!("hello")),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("hello")));
}

#[test]
fn test_eval_start_literal_number() {
    let ctx = V2EvalContext::new();
    let result = eval_v2_start(
        &V2Start::Literal(json!(42)),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(42)));
}

#[test]
fn test_eval_start_literal_bool() {
    let ctx = V2EvalContext::new();
    let result = eval_v2_start(
        &V2Start::Literal(json!(true)),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(true)));
}

#[test]
fn test_eval_start_literal_null() {
    let ctx = V2EvalContext::new();
    let result = eval_v2_start(
        &V2Start::Literal(json!(null)),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(null)));
}

#[test]
fn test_eval_start_literal_array() {
    let ctx = V2EvalContext::new();
    let result = eval_v2_start(
        &V2Start::Literal(json!([1, 2, 3])),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!([1, 2, 3])));
}

#[test]
fn test_eval_start_literal_object() {
    let ctx = V2EvalContext::new();
    let result = eval_v2_start(
        &V2Start::Literal(json!({"key": "value"})),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!({"key": "value"})));
}
