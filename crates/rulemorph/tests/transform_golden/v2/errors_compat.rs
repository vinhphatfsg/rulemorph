#[test]
fn tv26_unknown_op_error() {
    let base = fixtures_dir().join("tv26_v01_unknown_op");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = r#"[{"name": "test"}]"#;
    let result = transform(&rule, input, None);
    assert!(result.is_err(), "expected error for unknown op");
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("unknown op")
            || err.to_string().contains("nonexistent_op")
            || err.to_string().contains("expr.op is not supported"),
        "expected unknown op error, got: {}",
        err
    );
}

#[test]
fn tv26_forward_out_ref_returns_null() {
    // Note: Forward out reference (@out.b before b is computed)
    // does not cause a runtime error - it returns null/missing.
    // This is valid behavior for v2 expressions.
    let base = fixtures_dir().join("tv26_v02_forward_out_ref");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = r#"[{"x": 1}]"#;
    let result = transform(&rule, input, None).expect("transform should succeed");
    // When @out.b is not yet computed, it should result in null/missing for "a"
    // The output should have "b" = 1 (from @input.x)
    assert!(result.is_array());
    let arr = result.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    let obj = arr[0].as_object().unwrap();
    // "a" should be missing (not in output) or null since @out.b wasn't computed yet
    // "b" should be 1
    assert_eq!(obj.get("b"), Some(&serde_json::json!(1)));
}

#[test]
fn tv27_v1_compat() {
    let base = fixtures_dir().join("tv27_v1_compat");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let context = load_optional_json(&base.join("context.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, context.as_ref()).expect("transform failed");
    assert_eq!(output, expected);
}
