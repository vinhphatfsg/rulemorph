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
