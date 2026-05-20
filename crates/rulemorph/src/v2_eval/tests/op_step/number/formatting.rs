#[test]
fn test_eval_op_round_and_to_base() {
    let round = V2OpStep {
        op: "round".to_string(),
        args: vec![lit(json!(2))],
    };
    let to_base = V2OpStep {
        op: "to_base".to_string(),
        args: vec![lit(json!(2))],
    };
    let ctx = V2EvalContext::new();

    let rounded = eval_v2_op_step(
        &round,
        EvalValue::Value(json!(1.2345)),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    )
    .unwrap();
    if let EvalValue::Value(v) = rounded {
        let value = v.as_f64().unwrap();
        assert!((value - 1.23).abs() < 1e-9);
    } else {
        panic!("expected rounded value");
    }

    let base = eval_v2_op_step(
        &to_base,
        EvalValue::Value(json!(10)),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(base, Ok(EvalValue::Value(v)) if v == json!("1010")));
}
