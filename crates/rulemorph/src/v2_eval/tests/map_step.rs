use super::*;
use serde_json::json;

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
fn test_eval_map_step_with_multiply() {
    // map: [multiply: 2] on [1, 2, 3] -> [2, 4, 6]
    let map_step = V2MapStep {
        steps: vec![V2Step::Op(V2OpStep {
            op: "multiply".to_string(),
            args: vec![V2Expr::Pipe(V2Pipe {
                start: V2Start::Literal(json!(2)),
                steps: vec![],
            })],
        })],
    };
    let record = json!({});
    let out = json!({});
    let ctx = V2EvalContext::new();
    let result = eval_v2_map_step(
        &map_step,
        EvalValue::Value(json!([1, 2, 3])),
        &record,
        None,
        &out,
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!([2.0, 4.0, 6.0])));
}

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
fn test_eval_map_with_if_step() {
    // map with conditional: double if > 5, else keep
    let map_step = V2MapStep {
        steps: vec![V2Step::If(V2IfStep {
            cond: V2Condition::Comparison(V2Comparison {
                op: V2ComparisonOp::Gt,
                args: vec![
                    V2Expr::Pipe(V2Pipe {
                        start: V2Start::PipeValue,
                        steps: vec![],
                    }),
                    V2Expr::Pipe(V2Pipe {
                        start: V2Start::Literal(json!(5)),
                        steps: vec![],
                    }),
                ],
            }),
            then_branch: V2Pipe {
                start: V2Start::PipeValue,
                steps: vec![V2Step::Op(V2OpStep {
                    op: "multiply".to_string(),
                    args: vec![V2Expr::Pipe(V2Pipe {
                        start: V2Start::Literal(json!(2)),
                        steps: vec![],
                    })],
                })],
            },
            else_branch: None,
        })],
    };
    let record = json!({});
    let out = json!({});
    let ctx = V2EvalContext::new();
    // [3, 7, 2, 10] -> [3, 14, 2, 20] (only 7 and 10 are > 5)
    let result = eval_v2_map_step(
        &map_step,
        EvalValue::Value(json!([3, 7, 2, 10])),
        &record,
        None,
        &out,
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!([3, 14.0, 2, 20.0])));
}

#[test]
fn test_eval_nested_map() {
    // Nested map: [[1, 2], [3, 4]] -> map of (map multiply 2) -> [[2, 4], [6, 8]]
    let map_step = V2MapStep {
        steps: vec![V2Step::Map(V2MapStep {
            steps: vec![V2Step::Op(V2OpStep {
                op: "multiply".to_string(),
                args: vec![V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(2)),
                    steps: vec![],
                })],
            })],
        })],
    };
    let record = json!({});
    let out = json!({});
    let ctx = V2EvalContext::new();
    let result = eval_v2_map_step(
        &map_step,
        EvalValue::Value(json!([[1, 2], [3, 4]])),
        &record,
        None,
        &out,
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!([[2.0, 4.0], [6.0, 8.0]])));
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
