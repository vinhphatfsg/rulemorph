#[test]
fn test_eval_pipe_with_context() {
    // [@context.multiplier, multiply: @input.value]
    let pipe = V2Pipe {
        start: V2Start::Ref(V2Ref::Context("multiplier".to_string())),
        steps: vec![V2Step::Op(V2OpStep {
            op: "multiply".to_string(),
            args: vec![V2Expr::Pipe(V2Pipe {
                start: V2Start::Ref(V2Ref::Input("value".to_string())),
                steps: vec![],
            })],
        })],
    };
    let record = json!({"value": 10});
    let context = json!({"multiplier": 5});
    let out = json!({});
    let ctx = V2EvalContext::new();
    let result = eval_v2_pipe(&pipe, &record, Some(&context), &out, "test", &ctx);
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(50.0)));
}

#[test]
fn test_eval_pipe_with_out_ref() {
    // [@out.previous, add: 1]
    let pipe = V2Pipe {
        start: V2Start::Ref(V2Ref::Out("previous".to_string())),
        steps: vec![V2Step::Op(V2OpStep {
            op: "add".to_string(),
            args: vec![V2Expr::Pipe(V2Pipe {
                start: V2Start::Literal(json!(1)),
                steps: vec![],
            })],
        })],
    };
    let record = json!({});
    let out = json!({"previous": 99});
    let ctx = V2EvalContext::new();
    let result = eval_v2_pipe(&pipe, &record, None, &out, "test", &ctx);
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(100.0)));
}

#[test]
fn test_eval_pipe_coalesce_chain() {
    // [@input.primary, coalesce: @input.secondary, coalesce: "default"]
    let pipe = V2Pipe {
        start: V2Start::Ref(V2Ref::Input("primary".to_string())),
        steps: vec![
            V2Step::Op(V2OpStep {
                op: "coalesce".to_string(),
                args: vec![V2Expr::Pipe(V2Pipe {
                    start: V2Start::Ref(V2Ref::Input("secondary".to_string())),
                    steps: vec![],
                })],
            }),
            V2Step::Op(V2OpStep {
                op: "coalesce".to_string(),
                args: vec![V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!("default")),
                    steps: vec![],
                })],
            }),
        ],
    };

    // Test with primary present
    let record = json!({"primary": "first"});
    let out = json!({});
    let ctx = V2EvalContext::new();
    let result = eval_v2_pipe(&pipe, &record, None, &out, "test", &ctx);
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("first")));

    // Test with primary null, secondary present
    let record = json!({"primary": null, "secondary": "second"});
    let result = eval_v2_pipe(&pipe, &record, None, &out, "test", &ctx);
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("second")));

    // Test with both null, use default
    let record = json!({"primary": null, "secondary": null});
    let result = eval_v2_pipe(&pipe, &record, None, &out, "test", &ctx);
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("default")));
}
