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
