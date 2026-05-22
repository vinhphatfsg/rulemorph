#[test]
fn test_eval_input_ref() {
    let record = json!({"name": "Alice", "age": 30});
    let ctx = V2EvalContext::new();
    let result = eval_v2_ref(
        &V2Ref::Input("name".to_string()),
        &record,
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("Alice")));
}

#[test]
fn test_eval_input_ref_nested() {
    let record = json!({"user": {"profile": {"name": "Bob"}}});
    let ctx = V2EvalContext::new();
    let result = eval_v2_ref(
        &V2Ref::Input("user.profile.name".to_string()),
        &record,
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("Bob")));
}

#[test]
fn test_eval_input_ref_missing() {
    let record = json!({"name": "Alice"});
    let ctx = V2EvalContext::new();
    let result = eval_v2_ref(
        &V2Ref::Input("nonexistent".to_string()),
        &record,
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Missing)));
}

#[test]
fn test_eval_input_ref_empty_path() {
    let record = json!({"name": "Alice"});
    let ctx = V2EvalContext::new();
    let result = eval_v2_ref(
        &V2Ref::Input(String::new()),
        &record,
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!({"name": "Alice"})));
}

#[test]
fn test_eval_context_ref() {
    let record = json!({});
    let context = json!({"rate": 1.5, "config": {"enabled": true}});
    let ctx = V2EvalContext::new();
    let result = eval_v2_ref(
        &V2Ref::Context("rate".to_string()),
        &record,
        Some(&context),
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(1.5)));
}

#[test]
fn test_eval_context_ref_nested() {
    let record = json!({});
    let context = json!({"config": {"enabled": true}});
    let ctx = V2EvalContext::new();
    let result = eval_v2_ref(
        &V2Ref::Context("config.enabled".to_string()),
        &record,
        Some(&context),
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(true)));
}

#[test]
fn test_eval_context_ref_no_context_missing() {
    let record = json!({});
    let ctx = V2EvalContext::new();
    let result = eval_v2_ref(
        &V2Ref::Context("rate".to_string()),
        &record,
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Missing)));
}

#[test]
fn test_eval_out_ref() {
    let record = json!({});
    let out = json!({"computed": 42});
    let ctx = V2EvalContext::new();
    let result = eval_v2_ref(
        &V2Ref::Out("computed".to_string()),
        &record,
        None,
        &out,
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(42)));
}
