#[test]
fn test_eval_map_step_simple() {
    // map: [uppercase] on ["a", "b", "c"] -> ["A", "B", "C"]
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
        EvalValue::Value(json!(["a", "b", "c"])),
        &record,
        None,
        &out,
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(["A", "B", "C"])));
}

#[test]
fn test_eval_map_step_with_item_ref() {
    // Access @item.name from each object
    let map_step = V2MapStep {
        steps: vec![V2Step::Op(V2OpStep {
            op: "concat".to_string(),
            args: vec![V2Expr::Pipe(V2Pipe {
                start: V2Start::Literal(json!("!")),
                steps: vec![],
            })],
        })],
    };
    let record = json!({});
    let out = json!({});
    let ctx = V2EvalContext::new();
    let result = eval_v2_map_step(
        &map_step,
        EvalValue::Value(json!(["hello", "world"])),
        &record,
        None,
        &out,
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(["hello!", "world!"])));
}

#[test]
fn test_eval_map_step_multiple_ops() {
    // map: [trim, uppercase] on ["  a  ", "  b  "] -> ["A", "B"]
    let map_step = V2MapStep {
        steps: vec![
            V2Step::Op(V2OpStep {
                op: "trim".to_string(),
                args: vec![],
            }),
            V2Step::Op(V2OpStep {
                op: "uppercase".to_string(),
                args: vec![],
            }),
        ],
    };
    let record = json!({});
    let out = json!({});
    let ctx = V2EvalContext::new();
    let result = eval_v2_map_step(
        &map_step,
        EvalValue::Value(json!(["  a  ", "  b  "])),
        &record,
        None,
        &out,
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(["A", "B"])));
}
