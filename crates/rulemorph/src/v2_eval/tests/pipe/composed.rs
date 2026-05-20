#[test]
fn test_eval_pipe_complex_chain() {
    // [@input.price, let: {original: $}, multiply: 0.9, let: {discounted: $},
    //  if: {cond: {gt: [$, 1000]}, then: [subtract: 100]}]
    let pipe = V2Pipe {
        start: V2Start::Ref(V2Ref::Input("price".to_string())),
        steps: vec![
            V2Step::Let(V2LetStep {
                bindings: vec![(
                    "original".to_string(),
                    V2Expr::Pipe(V2Pipe {
                        start: V2Start::PipeValue,
                        steps: vec![],
                    }),
                )],
            }),
            V2Step::Op(V2OpStep {
                op: "multiply".to_string(),
                args: vec![V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(0.9)),
                    steps: vec![],
                })],
            }),
            V2Step::If(V2IfStep {
                cond: V2Condition::Comparison(V2Comparison {
                    op: V2ComparisonOp::Gt,
                    args: vec![
                        V2Expr::Pipe(V2Pipe {
                            start: V2Start::PipeValue,
                            steps: vec![],
                        }),
                        V2Expr::Pipe(V2Pipe {
                            start: V2Start::Literal(json!(1000)),
                            steps: vec![],
                        }),
                    ],
                }),
                then_branch: V2Pipe {
                    start: V2Start::PipeValue,
                    steps: vec![V2Step::Op(V2OpStep {
                        op: "subtract".to_string(),
                        args: vec![V2Expr::Pipe(V2Pipe {
                            start: V2Start::Literal(json!(100)),
                            steps: vec![],
                        })],
                    })],
                },
                else_branch: None,
            }),
        ],
    };
    let record = json!({"price": 2000});
    let out = json!({});
    let ctx = V2EvalContext::new();
    let result = eval_v2_pipe(&pipe, &record, None, &out, "test", &ctx);
    // 2000 * 0.9 = 1800 > 1000, so 1800 - 100 = 1700
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(1700.0)));
}

#[test]
fn test_eval_pipe_all_step_types() {
    // Test combining let, op, if, map in one pipe
    let pipe = V2Pipe {
        start: V2Start::Ref(V2Ref::Input("items".to_string())),
        steps: vec![
            // map: multiply each by 2
            V2Step::Map(V2MapStep {
                steps: vec![V2Step::Op(V2OpStep {
                    op: "multiply".to_string(),
                    args: vec![V2Expr::Pipe(V2Pipe {
                        start: V2Start::Literal(json!(2)),
                        steps: vec![],
                    })],
                })],
            }),
        ],
    };
    let record = json!({"items": [1, 2, 3]});
    let out = json!({});
    let ctx = V2EvalContext::new();
    let result = eval_v2_pipe(&pipe, &record, None, &out, "test", &ctx);
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!([2.0, 4.0, 6.0])));
}

#[test]
fn test_eval_pipe_deep_nesting() {
    // Deeply nested: input -> map -> if -> op
    let pipe = V2Pipe {
        start: V2Start::Ref(V2Ref::Input("scores".to_string())),
        steps: vec![V2Step::Map(V2MapStep {
            steps: vec![V2Step::If(V2IfStep {
                cond: V2Condition::Comparison(V2Comparison {
                    op: V2ComparisonOp::Gte,
                    args: vec![
                        V2Expr::Pipe(V2Pipe {
                            start: V2Start::PipeValue,
                            steps: vec![],
                        }),
                        V2Expr::Pipe(V2Pipe {
                            start: V2Start::Literal(json!(60)),
                            steps: vec![],
                        }),
                    ],
                }),
                then_branch: V2Pipe {
                    start: V2Start::Literal(json!("pass")),
                    steps: vec![],
                },
                else_branch: Some(V2Pipe {
                    start: V2Start::Literal(json!("fail")),
                    steps: vec![],
                }),
            })],
        })],
    };
    let record = json!({"scores": [80, 55, 90, 45]});
    let out = json!({});
    let ctx = V2EvalContext::new();
    let result = eval_v2_pipe(&pipe, &record, None, &out, "test", &ctx);
    assert!(
        matches!(result, Ok(EvalValue::Value(v)) if v == json!(["pass", "fail", "pass", "fail"]))
    );
}
