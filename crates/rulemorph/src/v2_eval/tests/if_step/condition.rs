// ------ Condition evaluation tests ------

include!("condition/comparison.rs");

#[test]
fn test_eval_condition_all_true() {
    let cond = V2Condition::All(vec![
        V2Condition::Comparison(V2Comparison {
            op: V2ComparisonOp::Gt,
            args: vec![
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(10)),
                    steps: vec![],
                }),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(5)),
                    steps: vec![],
                }),
            ],
        }),
        V2Condition::Comparison(V2Comparison {
            op: V2ComparisonOp::Lt,
            args: vec![
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(10)),
                    steps: vec![],
                }),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(20)),
                    steps: vec![],
                }),
            ],
        }),
    ]);
    let record = json!({});
    let out = json!({});
    let ctx = V2EvalContext::new();
    let result = eval_v2_condition(&cond, &record, None, &out, "test", &ctx);
    assert!(matches!(result, Ok(true)));
}

#[test]
fn test_eval_condition_all_false() {
    let cond = V2Condition::All(vec![
        V2Condition::Comparison(V2Comparison {
            op: V2ComparisonOp::Gt,
            args: vec![
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(10)),
                    steps: vec![],
                }),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(5)),
                    steps: vec![],
                }),
            ],
        }),
        V2Condition::Comparison(V2Comparison {
            op: V2ComparisonOp::Lt, // 10 < 5 is false
            args: vec![
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(10)),
                    steps: vec![],
                }),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(5)),
                    steps: vec![],
                }),
            ],
        }),
    ]);
    let record = json!({});
    let out = json!({});
    let ctx = V2EvalContext::new();
    let result = eval_v2_condition(&cond, &record, None, &out, "test", &ctx);
    assert!(matches!(result, Ok(false)));
}

#[test]
fn test_eval_condition_any_true() {
    let cond = V2Condition::Any(vec![
        V2Condition::Comparison(V2Comparison {
            op: V2ComparisonOp::Eq,
            args: vec![
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!("admin")),
                    steps: vec![],
                }),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!("user")),
                    steps: vec![],
                }),
            ],
        }),
        V2Condition::Comparison(V2Comparison {
            op: V2ComparisonOp::Gt,
            args: vec![
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(100)),
                    steps: vec![],
                }),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(50)),
                    steps: vec![],
                }),
            ],
        }),
    ]);
    let record = json!({});
    let out = json!({});
    let ctx = V2EvalContext::new();
    let result = eval_v2_condition(&cond, &record, None, &out, "test", &ctx);
    assert!(matches!(result, Ok(true)));
}

#[test]
fn test_eval_condition_any_false() {
    let cond = V2Condition::Any(vec![
        V2Condition::Comparison(V2Comparison {
            op: V2ComparisonOp::Eq,
            args: vec![
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(1)),
                    steps: vec![],
                }),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(2)),
                    steps: vec![],
                }),
            ],
        }),
        V2Condition::Comparison(V2Comparison {
            op: V2ComparisonOp::Eq,
            args: vec![
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(3)),
                    steps: vec![],
                }),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(4)),
                    steps: vec![],
                }),
            ],
        }),
    ]);
    let record = json!({});
    let out = json!({});
    let ctx = V2EvalContext::new();
    let result = eval_v2_condition(&cond, &record, None, &out, "test", &ctx);
    assert!(matches!(result, Ok(false)));
}

#[test]
fn test_eval_condition_expr_truthy() {
    let cond = V2Condition::Expr(V2Expr::Pipe(V2Pipe {
        start: V2Start::Literal(json!(true)),
        steps: vec![],
    }));
    let record = json!({});
    let out = json!({});
    let ctx = V2EvalContext::new();
    let result = eval_v2_condition(&cond, &record, None, &out, "test", &ctx);
    assert!(matches!(result, Ok(true)));
}

#[test]
fn test_eval_condition_expr_falsy() {
    let cond = V2Condition::Expr(V2Expr::Pipe(V2Pipe {
        start: V2Start::Literal(json!(false)),
        steps: vec![],
    }));
    let record = json!({});
    let out = json!({});
    let ctx = V2EvalContext::new();
    let result = eval_v2_condition(&cond, &record, None, &out, "test", &ctx);
    assert!(matches!(result, Ok(false)));
}

#[test]
fn test_eval_condition_expr_non_bool_errors() {
    let cond = V2Condition::Expr(V2Expr::Pipe(V2Pipe {
        start: V2Start::Literal(json!("active")),
        steps: vec![],
    }));
    let record = json!({});
    let out = json!({});
    let ctx = V2EvalContext::new();
    let result = eval_v2_condition(&cond, &record, None, &out, "test", &ctx);
    assert!(matches!(result, Err(err)
        if err.kind == TransformErrorKind::ExprError
            && err.message == "when/record_when must evaluate to boolean"
            && err.path.as_deref() == Some("test.expr")
    ));
}

#[test]
fn test_eval_condition_expr_missing_is_false() {
    let cond = V2Condition::Expr(V2Expr::Pipe(V2Pipe {
        start: V2Start::Ref(V2Ref::Input("active".to_string())),
        steps: vec![],
    }));
    let record = json!({});
    let out = json!({});
    let ctx = V2EvalContext::new();
    let result = eval_v2_condition(&cond, &record, None, &out, "test", &ctx);
    assert!(matches!(result, Ok(false)));
}

#[test]
fn test_eval_condition_with_pipe_value() {
    // Condition: { gt: ["$", 100] }
    let cond = V2Condition::Comparison(V2Comparison {
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
    });
    let record = json!({});
    let out = json!({});
    let ctx = V2EvalContext::new().with_pipe_value(EvalValue::Value(json!(150)));
    let result = eval_v2_condition(&cond, &record, None, &out, "test", &ctx);
    assert!(matches!(result, Ok(true)));
}
