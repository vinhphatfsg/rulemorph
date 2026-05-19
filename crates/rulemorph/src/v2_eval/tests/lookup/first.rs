#[test]
fn test_lookup_first_basic() {
    // lookup_first: {from: @context.departments, match: [id, 2], get: name}
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
                start: V2Start::Literal(json!(2)),
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
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("Sales")));
}

#[test]
fn test_lookup_first_uses_pipe_value_from() {
    let op = V2OpStep {
        op: "lookup_first".to_string(),
        args: vec![
            V2Expr::Pipe(V2Pipe {
                start: V2Start::Literal(json!("id")),
                steps: vec![],
            }),
            V2Expr::Pipe(V2Pipe {
                start: V2Start::Literal(json!(2)),
                steps: vec![],
            }),
            V2Expr::Pipe(V2Pipe {
                start: V2Start::Literal(json!("budget")),
                steps: vec![],
            }),
        ],
    };
    let record = json!({});
    let out = json!({});
    let ctx = V2EvalContext::new();
    let result = eval_v2_op_step(
        &op,
        EvalValue::Value(make_departments()),
        &record,
        None,
        &out,
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(50000)));
}

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
fn test_lookup_first_return_whole_object() {
    // Without 'get', return the whole matched object
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
                start: V2Start::Literal(json!(1)),
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
    assert!(
        matches!(result, Ok(EvalValue::Value(v)) if v == json!({"id": 1, "name": "Engineering", "budget": 100000}))
    );
}

#[test]
fn test_lookup_first_with_input_match_value() {
    // Match using value from input
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
                start: V2Start::Ref(V2Ref::Input("dept_id".to_string())),
                steps: vec![],
            }),
            V2Expr::Pipe(V2Pipe {
                start: V2Start::Literal(json!("name")),
                steps: vec![],
            }),
        ],
    };
    let record = json!({"dept_id": 3});
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
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("HR")));
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
fn test_lookup_in_pipe() {
    // Full pipe: lookup then transform result
    // Simpler test: just lookup and verify
    let pipe = V2Pipe {
        start: V2Start::Literal(json!(null)),
        steps: vec![V2Step::Op(V2OpStep {
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
                    start: V2Start::Ref(V2Ref::Input("dept_id".to_string())),
                    steps: vec![],
                }),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!("budget")),
                    steps: vec![],
                }),
            ],
        })],
    };
    let record = json!({"dept_id": 2}); // Sales dept
    let context = json!({"departments": make_departments()});
    let out = json!({});
    let ctx = V2EvalContext::new();
    let result = eval_v2_pipe(&pipe, &record, Some(&context), &out, "test", &ctx);
    // Sales budget is 50000
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(50000)));
}

#[test]
fn test_lookup_then_multiply() {
    // Two-step pipe: lookup, then multiply
    let pipe = V2Pipe {
        start: V2Start::Ref(V2Ref::Context("departments".to_string())),
        steps: vec![],
    };
    let record = json!({"dept_id": 2});
    let context = json!({"departments": make_departments()});
    let out = json!({});
    let ctx = V2EvalContext::new();

    // First verify context is accessible
    let result = eval_v2_pipe(&pipe, &record, Some(&context), &out, "test", &ctx);
    assert!(result.is_ok());

    // Now test just the lookup op step directly
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
                start: V2Start::Literal(json!(2)),
                steps: vec![],
            }),
            V2Expr::Pipe(V2Pipe {
                start: V2Start::Literal(json!("budget")),
                steps: vec![],
            }),
        ],
    };
    let result = eval_v2_op_step(
        &op,
        EvalValue::Value(json!(null)),
        &record,
        Some(&context),
        &out,
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Value(ref v)) if *v == json!(50000)));

    // Now multiply it
    let multiply_op = V2OpStep {
        op: "multiply".to_string(),
        args: vec![V2Expr::Pipe(V2Pipe {
            start: V2Start::Literal(json!(1.1)),
            steps: vec![],
        })],
    };
    let budget = result.unwrap();
    let result2 = eval_v2_op_step(&multiply_op, budget, &record, None, &out, "test", &ctx);
    // multiply returns f64, check approximately 55000
    match result2 {
        Ok(EvalValue::Value(v)) => {
            let num = v.as_f64().expect("should be number");
            assert!(
                (num - 55000.0).abs() < 0.001,
                "expected 55000.0, got {}",
                num
            );
        }
        other => panic!("expected Ok(EvalValue::Value), got {:?}", other),
    }
}
