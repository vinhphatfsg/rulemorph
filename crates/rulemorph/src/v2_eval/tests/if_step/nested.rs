#[test]
fn test_eval_nested_if() {
    // Nested if: if x > 100 then (if x > 500 then "gold" else "silver") else "bronze"
    let if_step = V2IfStep {
        cond: V2Condition::Comparison(V2Comparison {
            op: V2ComparisonOp::Gt,
            args: vec![
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::PipeValue,
                    steps: vec![],
                }),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(100)),
                    steps: vec![],
                }),
            ],
        }),
        then_branch: V2Pipe {
            start: V2Start::PipeValue,
            steps: vec![V2Step::If(V2IfStep {
                cond: V2Condition::Comparison(V2Comparison {
                    op: V2ComparisonOp::Gt,
                    args: vec![
                        V2Expr::Pipe(V2Pipe {
                            start: V2Start::PipeValue,
                            steps: vec![],
                        }),
                        V2Expr::Pipe(V2Pipe {
                            start: V2Start::Literal(json!(500)),
                            steps: vec![],
                        }),
                    ],
                }),
                then_branch: V2Pipe {
                    start: V2Start::Literal(json!("gold")),
                    steps: vec![],
                },
                else_branch: Some(V2Pipe {
                    start: V2Start::Literal(json!("silver")),
                    steps: vec![],
                }),
            })],
        },
        else_branch: Some(V2Pipe {
            start: V2Start::Literal(json!("bronze")),
            steps: vec![],
        }),
    };
    let record = json!({});
    let out = json!({});
    let ctx = V2EvalContext::new();

    // 50 -> bronze
    let result = eval_v2_if_step(
        &if_step,
        EvalValue::Value(json!(50)),
        &record,
        None,
        &out,
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("bronze")));

    // 200 -> silver
    let result = eval_v2_if_step(
        &if_step,
        EvalValue::Value(json!(200)),
        &record,
        None,
        &out,
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("silver")));

    // 600 -> gold
    let result = eval_v2_if_step(
        &if_step,
        EvalValue::Value(json!(600)),
        &record,
        None,
        &out,
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("gold")));
}
