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
