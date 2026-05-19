use super::*;
use serde_json::json;

// ------ Condition evaluation tests ------

#[test]
fn test_eval_condition_eq_true() {
    let cond = V2Condition::Comparison(V2Comparison {
        op: V2ComparisonOp::Eq,
        args: vec![
            V2Expr::Pipe(V2Pipe {
                start: V2Start::Literal(json!(10)),
                steps: vec![],
            }),
            V2Expr::Pipe(V2Pipe {
                start: V2Start::Literal(json!(10)),
                steps: vec![],
            }),
        ],
    });
    let record = json!({});
    let out = json!({});
    let ctx = V2EvalContext::new();
    let result = eval_v2_condition(&cond, &record, None, &out, "test", &ctx);
    assert!(matches!(result, Ok(true)));
}

#[test]
fn test_eval_condition_eq_false() {
    let cond = V2Condition::Comparison(V2Comparison {
        op: V2ComparisonOp::Eq,
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
    });
    let record = json!({});
    let out = json!({});
    let ctx = V2EvalContext::new();
    let result = eval_v2_condition(&cond, &record, None, &out, "test", &ctx);
    assert!(matches!(result, Ok(false)));
}

#[test]
fn test_eval_condition_eq_numeric_string_is_false() {
    let cond = V2Condition::Comparison(V2Comparison {
        op: V2ComparisonOp::Eq,
        args: vec![
            V2Expr::Pipe(V2Pipe {
                start: V2Start::Literal(json!("1")),
                steps: vec![],
            }),
            V2Expr::Pipe(V2Pipe {
                start: V2Start::Literal(json!(1)),
                steps: vec![],
            }),
        ],
    });
    let record = json!({});
    let out = json!({});
    let ctx = V2EvalContext::new();
    let result = eval_v2_condition(&cond, &record, None, &out, "test", &ctx);
    assert!(matches!(result, Ok(false)));
}

#[test]
fn test_eval_condition_eq_missing_as_null() {
    let cond = V2Condition::Comparison(V2Comparison {
        op: V2ComparisonOp::Eq,
        args: vec![
            V2Expr::Pipe(V2Pipe {
                start: V2Start::Ref(V2Ref::Input("optional".to_string())),
                steps: vec![],
            }),
            V2Expr::Pipe(V2Pipe {
                start: V2Start::Literal(json!(null)),
                steps: vec![],
            }),
        ],
    });
    let record = json!({});
    let out = json!({});
    let ctx = V2EvalContext::new();
    let result = eval_v2_condition(&cond, &record, None, &out, "test", &ctx);
    assert!(matches!(result, Ok(true)));
}

#[test]
fn test_eval_condition_ne() {
    let cond = V2Condition::Comparison(V2Comparison {
        op: V2ComparisonOp::Ne,
        args: vec![
            V2Expr::Pipe(V2Pipe {
                start: V2Start::Literal(json!("a")),
                steps: vec![],
            }),
            V2Expr::Pipe(V2Pipe {
                start: V2Start::Literal(json!("b")),
                steps: vec![],
            }),
        ],
    });
    let record = json!({});
    let out = json!({});
    let ctx = V2EvalContext::new();
    let result = eval_v2_condition(&cond, &record, None, &out, "test", &ctx);
    assert!(matches!(result, Ok(true)));
}

#[test]
fn test_eval_condition_gt() {
    let cond = V2Condition::Comparison(V2Comparison {
        op: V2ComparisonOp::Gt,
        args: vec![
            V2Expr::Pipe(V2Pipe {
                start: V2Start::Literal(json!(20)),
                steps: vec![],
            }),
            V2Expr::Pipe(V2Pipe {
                start: V2Start::Literal(json!(10)),
                steps: vec![],
            }),
        ],
    });
    let record = json!({});
    let out = json!({});
    let ctx = V2EvalContext::new();
    let result = eval_v2_condition(&cond, &record, None, &out, "test", &ctx);
    assert!(matches!(result, Ok(true)));
}

#[test]
fn test_eval_condition_gt_non_numeric_string_compares_lexicographically() {
    let cond = V2Condition::Comparison(V2Comparison {
        op: V2ComparisonOp::Gt,
        args: vec![
            V2Expr::Pipe(V2Pipe {
                start: V2Start::Literal(json!("B")),
                steps: vec![],
            }),
            V2Expr::Pipe(V2Pipe {
                start: V2Start::Literal(json!("A")),
                steps: vec![],
            }),
        ],
    });
    let record = json!({});
    let out = json!({});
    let ctx = V2EvalContext::new();
    let result = eval_v2_condition(&cond, &record, None, &out, "test", &ctx);
    assert!(matches!(result, Ok(true)));
}

#[test]
fn test_eval_condition_lt() {
    let cond = V2Condition::Comparison(V2Comparison {
        op: V2ComparisonOp::Lt,
        args: vec![
            V2Expr::Pipe(V2Pipe {
                start: V2Start::Literal(json!(5)),
                steps: vec![],
            }),
            V2Expr::Pipe(V2Pipe {
                start: V2Start::Literal(json!(10)),
                steps: vec![],
            }),
        ],
    });
    let record = json!({});
    let out = json!({});
    let ctx = V2EvalContext::new();
    let result = eval_v2_condition(&cond, &record, None, &out, "test", &ctx);
    assert!(matches!(result, Ok(true)));
}

#[test]
fn test_eval_condition_gte_equal() {
    let cond = V2Condition::Comparison(V2Comparison {
        op: V2ComparisonOp::Gte,
        args: vec![
            V2Expr::Pipe(V2Pipe {
                start: V2Start::Literal(json!(10)),
                steps: vec![],
            }),
            V2Expr::Pipe(V2Pipe {
                start: V2Start::Literal(json!(10)),
                steps: vec![],
            }),
        ],
    });
    let record = json!({});
    let out = json!({});
    let ctx = V2EvalContext::new();
    let result = eval_v2_condition(&cond, &record, None, &out, "test", &ctx);
    assert!(matches!(result, Ok(true)));
}

#[test]
fn test_eval_condition_lte_less() {
    let cond = V2Condition::Comparison(V2Comparison {
        op: V2ComparisonOp::Lte,
        args: vec![
            V2Expr::Pipe(V2Pipe {
                start: V2Start::Literal(json!(5)),
                steps: vec![],
            }),
            V2Expr::Pipe(V2Pipe {
                start: V2Start::Literal(json!(10)),
                steps: vec![],
            }),
        ],
    });
    let record = json!({});
    let out = json!({});
    let ctx = V2EvalContext::new();
    let result = eval_v2_condition(&cond, &record, None, &out, "test", &ctx);
    assert!(matches!(result, Ok(true)));
}

#[test]
fn test_eval_condition_match() {
    let cond = V2Condition::Comparison(V2Comparison {
        op: V2ComparisonOp::Match,
        args: vec![
            V2Expr::Pipe(V2Pipe {
                start: V2Start::Literal(json!("hello123")),
                steps: vec![],
            }),
            V2Expr::Pipe(V2Pipe {
                start: V2Start::Literal(json!("^hello\\d+")),
                steps: vec![],
            }),
        ],
    });
    let record = json!({});
    let out = json!({});
    let ctx = V2EvalContext::new();
    let result = eval_v2_condition(&cond, &record, None, &out, "test", &ctx);
    assert!(matches!(result, Ok(true)));
}

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
