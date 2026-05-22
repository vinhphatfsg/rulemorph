#[test]
fn test_eval_op_array_map_and_reduce() {
    let map_expr = V2Expr::Pipe(V2Pipe {
        start: V2Start::Ref(V2Ref::Item(String::new())),
        steps: vec![V2Step::Op(V2OpStep {
            op: "add".to_string(),
            args: vec![lit(json!(1))],
        })],
    });
    let map = V2OpStep {
        op: "map".to_string(),
        args: vec![map_expr],
    };
    let reduce_expr = V2Expr::Pipe(V2Pipe {
        start: V2Start::Ref(V2Ref::Acc(String::new())),
        steps: vec![V2Step::Op(V2OpStep {
            op: "add".to_string(),
            args: vec![V2Expr::Pipe(V2Pipe {
                start: V2Start::Ref(V2Ref::Item(String::new())),
                steps: vec![],
            })],
        })],
    });
    let reduce = V2OpStep {
        op: "reduce".to_string(),
        args: vec![reduce_expr],
    };
    let ctx = V2EvalContext::new();

    let map_result = eval_v2_op_step(
        &map,
        EvalValue::Value(json!([1, 2, 3])),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(map_result, Ok(EvalValue::Value(v)) if v == json!([2.0, 3.0, 4.0])));

    let reduce_result = eval_v2_op_step(
        &reduce,
        EvalValue::Value(json!([1, 2, 3])),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(reduce_result, Ok(EvalValue::Value(v)) if v == json!(6.0)));
}

#[test]
fn test_eval_op_first_last() {
    let first = V2OpStep {
        op: "first".to_string(),
        args: vec![],
    };
    let last = V2OpStep {
        op: "last".to_string(),
        args: vec![],
    };
    let ctx = V2EvalContext::new();

    let first_result = eval_v2_op_step(
        &first,
        EvalValue::Value(json!([1, 2])),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(first_result, Ok(EvalValue::Value(v)) if v == json!(1)));

    let last_result = eval_v2_op_step(
        &last,
        EvalValue::Value(json!([1, 2])),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(last_result, Ok(EvalValue::Value(v)) if v == json!(2)));
}
