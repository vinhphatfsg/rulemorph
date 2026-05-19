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
