#[test]
fn test_eval_map_step_empty_array() {
    let map_step = V2MapStep {
        steps: vec![V2Step::Op(V2OpStep {
            op: "uppercase".to_string(),
            args: vec![],
        })],
    };
    let record = json!({});
    let out = json!({});
    let ctx = V2EvalContext::new();
    let result = eval_v2_map_step(
        &map_step,
        EvalValue::Value(json!([])),
        &record,
        None,
        &out,
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!([])));
}

#[test]
fn test_eval_map_step_missing_returns_missing() {
    let map_step = V2MapStep {
        steps: vec![V2Step::Op(V2OpStep {
            op: "uppercase".to_string(),
            args: vec![],
        })],
    };
    let record = json!({});
    let out = json!({});
    let ctx = V2EvalContext::new();
    let result = eval_v2_map_step(
        &map_step,
        EvalValue::Missing,
        &record,
        None,
        &out,
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Missing)));
}

#[test]
fn test_eval_map_step_non_array_error() {
    let map_step = V2MapStep {
        steps: vec![V2Step::Op(V2OpStep {
            op: "uppercase".to_string(),
            args: vec![],
        })],
    };
    let record = json!({});
    let out = json!({});
    let ctx = V2EvalContext::new();
    let result = eval_v2_map_step(
        &map_step,
        EvalValue::Value(json!("not an array")),
        &record,
        None,
        &out,
        "test",
        &ctx,
    );
    assert!(result.is_err());
}
