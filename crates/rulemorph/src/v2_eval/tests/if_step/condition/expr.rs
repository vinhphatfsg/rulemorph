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
