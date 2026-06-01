fn eval_math_op(
    op: &str,
    pipe: JsonValue,
    args: Vec<JsonValue>,
) -> Result<EvalValue, crate::error::TransformError> {
    eval_math_op_with_range_limit(op, pipe, args, Some(10_000))
}

fn eval_range_op(
    args: Vec<JsonValue>,
    max_range_items: Option<usize>,
) -> Result<EvalValue, crate::error::TransformError> {
    let op = V2OpStep {
        op: "range".to_string(),
        args: args.into_iter().map(lit).collect(),
    };
    let ctx = V2EvalContext::new().with_limits(crate::transform::EvalLimits {
        max_range_items,
        ..Default::default()
    });
    eval_v2_op_step(
        &op,
        EvalValue::Missing,
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    )
}

fn pipe_value_expr() -> V2Expr {
    V2Expr::Pipe(V2Pipe {
        start: V2Start::PipeValue,
        steps: vec![],
    })
}

fn input_ref_expr(path: &str) -> V2Expr {
    V2Expr::Pipe(V2Pipe {
        start: V2Start::Ref(V2Ref::Input(path.to_string())),
        steps: vec![],
    })
}

fn item_ref_expr(path: &str) -> V2Expr {
    V2Expr::Pipe(V2Pipe {
        start: V2Start::Ref(V2Ref::Item(path.to_string())),
        steps: vec![],
    })
}

fn eval_math_op_with_range_limit(
    op: &str,
    pipe: JsonValue,
    args: Vec<JsonValue>,
    max_range_items: Option<usize>,
) -> Result<EvalValue, crate::error::TransformError> {
    let op = V2OpStep {
        op: op.to_string(),
        args: args.into_iter().map(lit).collect(),
    };
    let ctx = V2EvalContext::new().with_limits(crate::transform::EvalLimits {
        max_range_items,
        ..Default::default()
    });
    eval_v2_op_step(
        &op,
        EvalValue::Value(pipe),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    )
}

#[test]
fn test_eval_math_ops_success() {
    assert!(matches!(eval_math_op("abs", json!(-3.5), vec![]), Ok(EvalValue::Value(v)) if v == json!(3.5)));
    assert!(matches!(eval_math_op("floor", json!(3.9), vec![]), Ok(EvalValue::Value(v)) if v == json!(3)));
    assert!(matches!(eval_math_op("ceil", json!(3.1), vec![]), Ok(EvalValue::Value(v)) if v == json!(4)));
    assert!(matches!(eval_math_op("trunc", json!(-3.9), vec![]), Ok(EvalValue::Value(v)) if v == json!(-3)));
    assert!(matches!(eval_math_op("sqrt", json!(81), vec![]), Ok(EvalValue::Value(v)) if v == json!(9)));
    assert!(matches!(eval_math_op("pow", json!(2), vec![json!(8)]), Ok(EvalValue::Value(v)) if v == json!(256)));
    assert!(matches!(eval_math_op("mod", json!(-5), vec![json!(3)]), Ok(EvalValue::Value(v)) if v == json!(1)));
    assert!(matches!(eval_math_op("mod", json!(5), vec![json!(-3)]), Ok(EvalValue::Value(v)) if v == json!(2)));
    assert!(matches!(eval_math_op("clamp", json!(120), vec![json!(0), json!(100)]), Ok(EvalValue::Value(v)) if v == json!(100)));
    assert!(matches!(eval_math_op("sign", json!(-0.25), vec![]), Ok(EvalValue::Value(v)) if v == json!(-1)));
    assert!(matches!(eval_range_op(vec![json!(2), json!(8)], Some(10_000)), Ok(EvalValue::Value(v)) if v == json!([2, 3, 4, 5, 6, 7])));
    assert!(matches!(eval_range_op(vec![json!(8), json!(2)], Some(10_000)), Ok(EvalValue::Value(v)) if v == json!([8, 7, 6, 5, 4, 3])));
    assert!(matches!(eval_range_op(vec![json!(8), json!(2), json!(-2)], Some(10_000)), Ok(EvalValue::Value(v)) if v == json!([8, 6, 4])));
    assert!(matches!(eval_range_op(vec![json!(0), json!(10), json!(-1)], Some(10_000)), Ok(EvalValue::Value(v)) if v == json!([])));
    assert!(matches!(eval_range_op(vec![json!(10), json!(0), json!(1)], Some(10_000)), Ok(EvalValue::Value(v)) if v == json!([])));
    assert!(matches!(
        eval_range_op(vec![json!(i64::MAX - 2), json!(i64::MAX), json!(1)], Some(10_000)),
        Ok(EvalValue::Value(v)) if v == json!([i64::MAX - 2, i64::MAX - 1])
    ));
}

#[test]
fn test_eval_range_can_use_pipe_value_when_explicitly_referenced() {
    let op = V2OpStep {
        op: "range".to_string(),
        args: vec![lit(json!(2)), pipe_value_expr()],
    };
    let ctx = V2EvalContext::new().with_limits(crate::transform::EvalLimits {
        max_range_items: Some(10_000),
        ..Default::default()
    });
    let result = eval_v2_op_step(
        &op,
        EvalValue::Value(json!(5)),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!([2, 3, 4])));
}

#[test]
fn test_eval_v1_fallback_math_ops_skip_args_when_pipe_is_missing() {
    let op = V2OpStep {
        op: "pow".to_string(),
        args: vec![item_ref_expr("invalid")],
    };
    let result = eval_v2_op_step(
        &op,
        EvalValue::Missing,
        &json!({}),
        None,
        &json!({}),
        "test",
        &V2EvalContext::new(),
    );
    assert!(matches!(result, Ok(EvalValue::Missing)));
}

#[test]
fn test_eval_v1_fallback_math_ops_validate_arg_count_before_missing_pipe() {
    let op = V2OpStep {
        op: "pow".to_string(),
        args: vec![],
    };
    let result = eval_v2_op_step(
        &op,
        EvalValue::Missing,
        &json!({}),
        None,
        &json!({}),
        "test",
        &V2EvalContext::new(),
    );
    assert!(result.is_err());
}

#[test]
fn test_eval_range_stops_after_missing_arg_without_evaluating_later_args() {
    let op = V2OpStep {
        op: "range".to_string(),
        args: vec![input_ref_expr("missing"), item_ref_expr("invalid")],
    };
    let result = eval_v2_op_step(
        &op,
        EvalValue::Missing,
        &json!({}),
        None,
        &json!({}),
        "test",
        &V2EvalContext::new(),
    );
    assert!(matches!(result, Ok(EvalValue::Missing)));
}

#[test]
fn test_eval_math_ops_reject_invalid_inputs() {
    assert!(eval_math_op("mod", json!(5), vec![json!(0)]).is_err());
    assert!(eval_math_op("sqrt", json!(-1), vec![]).is_err());
    assert!(eval_math_op("sqrt", json!("NaN"), vec![]).is_err());
    assert!(eval_math_op("sqrt", json!("inf"), vec![]).is_err());
    assert!(eval_math_op("pow", json!(0), vec![json!(-1)]).is_err());
    assert!(eval_math_op("pow", json!(-8), vec![json!(0.5)]).is_err());
    assert!(eval_math_op("pow", json!(1e308), vec![json!(2)]).is_err());
    assert!(eval_math_op("clamp", json!(5), vec![json!(10), json!(1)]).is_err());
    assert!(eval_math_op("clamp", json!(5), vec![json!("NaN"), json!(10)]).is_err());
    assert!(eval_range_op(vec![json!(0.5), json!(10)], Some(10_000)).is_err());
    assert!(eval_range_op(vec![json!(0), json!(10), json!(0)], Some(10_000)).is_err());
    assert!(eval_range_op(vec![json!(0), json!(10_001)], Some(10_000)).is_err());
    assert!(eval_range_op(vec![json!(0), json!(100_000), json!(1)], Some(10_000)).is_err());
    let pipe_first_err = eval_math_op("range", json!(2), vec![json!(8)]).unwrap_err();
    assert!(
        pipe_first_err
            .to_string()
            .contains("[{ range: [start, end, step?] }]")
    );
}

#[test]
fn test_eval_range_limit_can_be_disabled_by_trusted_options() {
    let result = eval_range_op(vec![json!(0), json!(10_001)], None);
    assert!(
        matches!(result, Ok(EvalValue::Value(v)) if v.as_array().is_some_and(|items| items.len() == 10_001))
    );
}
