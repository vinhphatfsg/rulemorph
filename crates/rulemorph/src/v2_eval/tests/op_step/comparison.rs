#[test]
fn test_eval_op_comparison_aliases() {
    let ctx = V2EvalContext::new();
    let cases = [
        ("eq", json!(1), json!("1"), true),
        ("ne", json!(1), json!(2), true),
        ("lt", json!(5), json!(10), true),
        ("lte", json!(10), json!(10), true),
        ("gt", json!(10), json!(5), true),
        ("gte", json!(10), json!(10), true),
        ("match", json!("apple"), json!("^a.*"), true),
    ];

    for (op, left, right, expected) in cases {
        let op_step = V2OpStep {
            op: op.to_string(),
            args: vec![lit(right)],
        };
        let result = eval_v2_op_step(
            &op_step,
            EvalValue::Value(left),
            &json!({}),
            None,
            &json!({}),
            "test",
            &ctx,
        );
        assert!(
            matches!(result, Ok(EvalValue::Value(v)) if v == json!(expected)),
            "op {}",
            op
        );
    }
}

#[test]
fn test_eval_op_comparison_symbols() {
    let ctx = V2EvalContext::new();
    let cases = [
        ("==", json!(1), json!("1"), true),
        ("!=", json!(1), json!(2), true),
        ("<", json!(5), json!(10), true),
        ("<=", json!(10), json!(10), true),
        (">", json!(10), json!(5), true),
        (">=", json!(10), json!(10), true),
        ("~=", json!("apple"), json!("^a.*"), true),
    ];

    for (op, left, right, expected) in cases {
        let op_step = V2OpStep {
            op: op.to_string(),
            args: vec![lit(right)],
        };
        let result = eval_v2_op_step(
            &op_step,
            EvalValue::Value(left),
            &json!({}),
            None,
            &json!({}),
            "test",
            &ctx,
        );
        assert!(
            matches!(result, Ok(EvalValue::Value(v)) if v == json!(expected)),
            "op {}",
            op
        );
    }
}

#[test]
fn test_eval_op_comparison_missing_null_semantics() {
    let ctx = V2EvalContext::new();
    let missing_eq_null = V2OpStep {
        op: "eq".to_string(),
        args: vec![lit(json!(null))],
    };
    let result = eval_v2_op_step(
        &missing_eq_null,
        EvalValue::Missing,
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(true)));

    let missing_ne_value = V2OpStep {
        op: "ne".to_string(),
        args: vec![lit(json!("value"))],
    };
    let result = eval_v2_op_step(
        &missing_ne_value,
        EvalValue::Missing,
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(true)));
}

#[test]
fn test_eval_op_comparison_error_paths() {
    let ctx = V2EvalContext::new();
    let non_numeric_compare = V2OpStep {
        op: "<".to_string(),
        args: vec![lit(json!(1))],
    };
    let err = eval_v2_op_step(
        &non_numeric_compare,
        EvalValue::Value(json!("not-a-number")),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    )
    .expect_err("non-numeric comparison should error");
    assert_eq!(err.kind, TransformErrorKind::ExprError);
    assert_eq!(err.message, "comparison operand must be a number");
    assert_eq!(err.path.as_deref(), Some("test"));

    let invalid_regex = V2OpStep {
        op: "~=".to_string(),
        args: vec![lit(json!("["))],
    };
    let err = eval_v2_op_step(
        &invalid_regex,
        EvalValue::Value(json!("apple")),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    )
    .expect_err("invalid regex should error");
    assert_eq!(err.kind, TransformErrorKind::ExprError);
    assert!(err.message.starts_with("invalid regex pattern:"));
    assert_eq!(err.path.as_deref(), Some("test.args[0]"));
}
