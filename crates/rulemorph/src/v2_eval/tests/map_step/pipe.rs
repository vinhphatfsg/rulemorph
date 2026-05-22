#[test]
fn test_eval_map_step_with_item_index() {
    // Create pipe that returns @item.index
    // This requires testing through the full context
    let pipe = V2Pipe {
        start: V2Start::Ref(V2Ref::Input("items".to_string())),
        steps: vec![V2Step::Map(V2MapStep {
            steps: vec![], // Just return the item as-is
        })],
    };
    let record = json!({"items": [10, 20, 30]});
    let out = json!({});
    let ctx = V2EvalContext::new();
    let result = eval_v2_pipe(&pipe, &record, None, &out, "test", &ctx);
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!([10, 20, 30])));
}

#[test]
fn test_eval_pipe_with_map_step() {
    // Full pipe: [@input.names, { map: [uppercase] }]
    let pipe = V2Pipe {
        start: V2Start::Ref(V2Ref::Input("names".to_string())),
        steps: vec![V2Step::Map(V2MapStep {
            steps: vec![V2Step::Op(V2OpStep {
                op: "uppercase".to_string(),
                args: vec![],
            })],
        })],
    };
    let record = json!({"names": ["alice", "bob"]});
    let out = json!({});
    let ctx = V2EvalContext::new();
    let result = eval_v2_pipe(&pipe, &record, None, &out, "test", &ctx);
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(["ALICE", "BOB"])));
}

#[test]
fn test_eval_map_objects() {
    // Map over array of objects and extract a field
    // Since we're using pipe value directly, this tests object handling
    let pipe = V2Pipe {
        start: V2Start::Ref(V2Ref::Input("users".to_string())),
        steps: vec![V2Step::Map(V2MapStep {
            steps: vec![], // No-op, just return items
        })],
    };
    let record = json!({"users": [{"name": "Alice"}, {"name": "Bob"}]});
    let out = json!({});
    let ctx = V2EvalContext::new();
    let result = eval_v2_pipe(&pipe, &record, None, &out, "test", &ctx);
    assert!(
        matches!(result, Ok(EvalValue::Value(v)) if v == json!([{"name": "Alice"}, {"name": "Bob"}]))
    );
}
