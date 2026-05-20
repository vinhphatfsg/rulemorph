#[test]
fn test_eval_start_ref() {
    let ctx = V2EvalContext::new();
    let result = eval_v2_start(
        &V2Start::Ref(V2Ref::Input("name".to_string())),
        &json!({"name": "Bob"}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("Bob")));
}
