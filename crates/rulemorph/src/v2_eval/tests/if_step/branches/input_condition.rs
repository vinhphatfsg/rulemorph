#[test]
fn test_eval_if_with_input_condition() {
    // if: { cond: { eq: ["@input.role", "admin"] }, then: [100], else: [50] }
    let if_step = V2IfStep {
        cond: V2Condition::Comparison(V2Comparison {
            op: V2ComparisonOp::Eq,
            args: vec![
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Ref(V2Ref::Input("role".to_string())),
                    steps: vec![],
                }),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!("admin")),
                    steps: vec![],
                }),
            ],
        }),
        then_branch: V2Pipe {
            start: V2Start::Literal(json!(100)),
            steps: vec![],
        },
        else_branch: Some(V2Pipe {
            start: V2Start::Literal(json!(50)),
            steps: vec![],
        }),
    };
    let record = json!({"role": "admin"});
    let out = json!({});
    let ctx = V2EvalContext::new();
    let result = eval_v2_if_step(
        &if_step,
        EvalValue::Value(json!(0)),
        &record,
        None,
        &out,
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(100)));

    // When not admin
    let record2 = json!({"role": "user"});
    let result2 = eval_v2_if_step(
        &if_step,
        EvalValue::Value(json!(0)),
        &record2,
        None,
        &out,
        "test",
        &ctx,
    );
    assert!(matches!(result2, Ok(EvalValue::Value(v)) if v == json!(50)));
}
