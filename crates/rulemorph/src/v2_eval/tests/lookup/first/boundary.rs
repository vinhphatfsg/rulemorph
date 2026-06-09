#[test]
fn test_lookup_first_no_match() {
    let op = V2OpStep {
        op: "lookup_first".to_string(),
        args: vec![
            V2Expr::Pipe(V2Pipe {
                start: V2Start::Ref(V2Ref::Context("departments".to_string())),
                steps: vec![],
            }),
            V2Expr::Pipe(V2Pipe {
                start: V2Start::Literal(json!("id")),
                steps: vec![],
            }),
            V2Expr::Pipe(V2Pipe {
                start: V2Start::Literal(json!(999)), // Non-existent ID
                steps: vec![],
            }),
            V2Expr::Pipe(V2Pipe {
                start: V2Start::Literal(json!("name")),
                steps: vec![],
            }),
        ],
    };
    let record = json!({});
    let context = json!({"departments": make_departments()});
    let out = json!({});
    let ctx = V2EvalContext::new();
    let result = eval_v2_op_step(
        &op,
        EvalValue::Value(json!(null)),
        &record,
        Some(&context),
        &out,
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Missing)));
}

#[test]
fn test_lookup_first_missing_match_value_does_not_match_null() {
    let users = json!([
        {"id": null, "name": "MissingUser"},
        {"id": 1, "name": "Alice"}
    ]);
    let op = V2OpStep {
        op: "lookup_first".to_string(),
        args: vec![
            V2Expr::Pipe(V2Pipe {
                start: V2Start::Ref(V2Ref::Context("users".to_string())),
                steps: vec![],
            }),
            V2Expr::Pipe(V2Pipe {
                start: V2Start::Literal(json!("id")),
                steps: vec![],
            }),
            V2Expr::Pipe(V2Pipe {
                start: V2Start::Ref(V2Ref::Input("user_id".to_string())),
                steps: vec![],
            }),
            V2Expr::Pipe(V2Pipe {
                start: V2Start::Literal(json!("name")),
                steps: vec![],
            }),
        ],
    };
    let record = json!({});
    let context = json!({"users": users});
    let out = json!({});
    let ctx = V2EvalContext::new();
    let result = eval_v2_op_step(
        &op,
        EvalValue::Value(json!(null)),
        &record,
        Some(&context),
        &out,
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Missing)));
}

#[test]
fn test_lookup_first_missing_from() {
    let op = V2OpStep {
        op: "lookup_first".to_string(),
        args: vec![
            V2Expr::Pipe(V2Pipe {
                start: V2Start::Ref(V2Ref::Context("nonexistent".to_string())),
                steps: vec![],
            }),
            V2Expr::Pipe(V2Pipe {
                start: V2Start::Literal(json!("id")),
                steps: vec![],
            }),
            V2Expr::Pipe(V2Pipe {
                start: V2Start::Literal(json!(1)),
                steps: vec![],
            }),
        ],
    };
    let record = json!({});
    let context = json!({});
    let out = json!({});
    let ctx = V2EvalContext::new();
    let result = eval_v2_op_step(
        &op,
        EvalValue::Value(json!(null)),
        &record,
        Some(&context),
        &out,
        "test",
        &ctx,
    );
    // Missing 'from' returns Missing
    assert!(matches!(result, Ok(EvalValue::Missing)));
}

#[test]
fn test_lookup_first_insufficient_args() {
    let op = V2OpStep {
        op: "lookup_first".to_string(),
        args: vec![V2Expr::Pipe(V2Pipe {
            start: V2Start::Literal(json!([])),
            steps: vec![],
        })],
    };
    let record = json!({});
    let out = json!({});
    let ctx = V2EvalContext::new();
    let result = eval_v2_op_step(
        &op,
        EvalValue::Value(json!(null)),
        &record,
        None,
        &out,
        "test",
        &ctx,
    );
    assert!(result.is_err());
}

#[test]
fn test_lookup_first_rejects_extra_args() {
    let op = V2OpStep {
        op: "lookup_first".to_string(),
        args: vec![
            V2Expr::Pipe(V2Pipe {
                start: V2Start::Literal(json!([])),
                steps: vec![],
            }),
            V2Expr::Pipe(V2Pipe {
                start: V2Start::Literal(json!("id")),
                steps: vec![],
            }),
            V2Expr::Pipe(V2Pipe {
                start: V2Start::Literal(json!(1)),
                steps: vec![],
            }),
            V2Expr::Pipe(V2Pipe {
                start: V2Start::Literal(json!("name")),
                steps: vec![],
            }),
            V2Expr::Pipe(V2Pipe {
                start: V2Start::Literal(json!("extra")),
                steps: vec![],
            }),
        ],
    };
    let record = json!({});
    let out = json!({});
    let ctx = V2EvalContext::new();
    let err = eval_v2_op_step(
        &op,
        EvalValue::Value(json!(null)),
        &record,
        None,
        &out,
        "test",
        &ctx,
    )
    .expect_err("lookup_first should enforce metadata max args at runtime");

    assert_eq!(err.kind, TransformErrorKind::ExprError);
    assert!(
        err.message
            .contains("lookup_first accepts at most 4 argument(s), got 5"),
        "unexpected error message: {}",
        err.message
    );
}
