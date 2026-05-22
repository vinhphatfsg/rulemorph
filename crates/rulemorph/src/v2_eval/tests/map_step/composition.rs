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
