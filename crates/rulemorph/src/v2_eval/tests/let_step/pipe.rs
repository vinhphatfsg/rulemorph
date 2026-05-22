#[test]
fn test_eval_pipe_with_let() {
    // [100, { let: { x: $ } }, @x] -> 100
    let pipe = V2Pipe {
        start: V2Start::Literal(json!(100)),
        steps: vec![V2Step::Let(V2LetStep {
            bindings: vec![(
                "x".to_string(),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::PipeValue,
                    steps: vec![],
                }),
            )],
        })],
    };
    let record = json!({});
    let out = json!({});
    let ctx = V2EvalContext::new();
    let result = eval_v2_pipe(&pipe, &record, None, &out, "test", &ctx);
    // Let step doesn't change pipe value
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(100)));
}

#[test]
fn test_eval_pipe_let_then_op() {
    // [100, { let: { factor: 2 } }, { op: "multiply", args: [@factor] }] -> 200
    let pipe = V2Pipe {
        start: V2Start::Literal(json!(100)),
        steps: vec![
            V2Step::Let(V2LetStep {
                bindings: vec![(
                    "factor".to_string(),
                    V2Expr::Pipe(V2Pipe {
                        start: V2Start::Literal(json!(2)),
                        steps: vec![],
                    }),
                )],
            }),
            V2Step::Op(V2OpStep {
                op: "multiply".to_string(),
                args: vec![V2Expr::Pipe(V2Pipe {
                    start: V2Start::Ref(V2Ref::Local("factor".to_string())),
                    steps: vec![],
                })],
            }),
        ],
    };
    let record = json!({});
    let out = json!({});
    let ctx = V2EvalContext::new();
    let result = eval_v2_pipe(&pipe, &record, None, &out, "test", &ctx);
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(200.0)));
}
