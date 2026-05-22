#[test]
fn test_eval_local_ref() {
    let ctx =
        V2EvalContext::new().with_let_binding("price".to_string(), EvalValue::Value(json!(100)));
    let result = eval_v2_ref(
        &V2Ref::Local("price".to_string()),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(100)));
}

#[test]
fn test_eval_local_ref_undefined_error() {
    let ctx = V2EvalContext::new();
    let result = eval_v2_ref(
        &V2Ref::Local("undefined".to_string()),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(result.is_err());
}

#[test]
fn test_eval_item_ref() {
    let item_value = json!({"name": "item1", "value": 10});
    let ctx = V2EvalContext::new().with_item(EvalItem {
        value: &item_value,
        index: 2,
    });
    let result = eval_v2_ref(
        &V2Ref::Item("name".to_string()),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("item1")));
}

#[test]
fn test_eval_item_ref_index() {
    let item_value = json!({"name": "item1"});
    let ctx = V2EvalContext::new().with_item(EvalItem {
        value: &item_value,
        index: 5,
    });
    let result = eval_v2_ref(
        &V2Ref::Item("index".to_string()),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(5)));
}

#[test]
fn test_eval_item_ref_no_scope_error() {
    let ctx = V2EvalContext::new();
    let result = eval_v2_ref(
        &V2Ref::Item("value".to_string()),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(result.is_err());
}

#[test]
fn test_eval_acc_ref() {
    let acc_value = json!(100);
    let ctx = V2EvalContext::new().with_acc(&acc_value);
    let result = eval_v2_ref(
        &V2Ref::Acc(String::new()),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(100)));
}

#[test]
fn test_eval_acc_ref_no_scope_error() {
    let ctx = V2EvalContext::new();
    let result = eval_v2_ref(
        &V2Ref::Acc("value".to_string()),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(result.is_err());
}
