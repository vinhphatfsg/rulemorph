use super::*;
use serde_json::json;

include!("if_step/condition.rs");

// ------ If step evaluation tests ------

#[test]
fn test_eval_if_step_then_branch() {
    // if: { cond: { gt: ["$", 10] }, then: [{ multiply: 2 }] }
    let if_step = V2IfStep {
        cond: V2Condition::Comparison(V2Comparison {
            op: V2ComparisonOp::Gt,
            args: vec![
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::PipeValue,
                    steps: vec![],
                }),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(10)),
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
    };
    let record = json!({});
    let out = json!({});
    let ctx = V2EvalContext::new();
    let result = eval_v2_if_step(
        &if_step,
        EvalValue::Value(json!(20)),
        &record,
        None,
        &out,
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(40.0)));
}

#[test]
fn test_eval_if_step_else_branch() {
    // if: { cond: { gt: ["$", 10] }, then: [{ multiply: 2 }], else: [{ multiply: 0.5 }] }
    let if_step = V2IfStep {
        cond: V2Condition::Comparison(V2Comparison {
            op: V2ComparisonOp::Gt,
            args: vec![
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::PipeValue,
                    steps: vec![],
                }),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(10)),
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
        else_branch: Some(V2Pipe {
            start: V2Start::PipeValue,
            steps: vec![V2Step::Op(V2OpStep {
                op: "multiply".to_string(),
                args: vec![V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(0.5)),
                    steps: vec![],
                })],
            })],
        }),
    };
    let record = json!({});
    let out = json!({});
    let ctx = V2EvalContext::new();
    // pipe value 5 is less than 10, so else branch is taken
    let result = eval_v2_if_step(
        &if_step,
        EvalValue::Value(json!(5)),
        &record,
        None,
        &out,
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(2.5)));
}

#[test]
fn test_eval_if_step_no_else_returns_pipe_value() {
    // if: { cond: { gt: ["$", 10] }, then: [{ multiply: 2 }] }
    let if_step = V2IfStep {
        cond: V2Condition::Comparison(V2Comparison {
            op: V2ComparisonOp::Gt,
            args: vec![
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::PipeValue,
                    steps: vec![],
                }),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(10)),
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
    };
    let record = json!({});
    let out = json!({});
    let ctx = V2EvalContext::new();
    // pipe value 5 is less than 10, no else branch, returns original pipe value
    let result = eval_v2_if_step(
        &if_step,
        EvalValue::Value(json!(5)),
        &record,
        None,
        &out,
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(5)));
}

#[test]
fn test_eval_pipe_with_if_step() {
    // [10000, { if: { cond: { gt: ["$", 5000] }, then: [{ multiply: 0.9 }] } }]
    let pipe = V2Pipe {
        start: V2Start::Literal(json!(10000)),
        steps: vec![V2Step::If(V2IfStep {
            cond: V2Condition::Comparison(V2Comparison {
                op: V2ComparisonOp::Gt,
                args: vec![
                    V2Expr::Pipe(V2Pipe {
                        start: V2Start::PipeValue,
                        steps: vec![],
                    }),
                    V2Expr::Pipe(V2Pipe {
                        start: V2Start::Literal(json!(5000)),
                        steps: vec![],
                    }),
                ],
            }),
            then_branch: V2Pipe {
                start: V2Start::PipeValue,
                steps: vec![V2Step::Op(V2OpStep {
                    op: "multiply".to_string(),
                    args: vec![V2Expr::Pipe(V2Pipe {
                        start: V2Start::Literal(json!(0.9)),
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
    let result = eval_v2_pipe(&pipe, &record, None, &out, "test", &ctx);
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(9000.0)));
}

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
