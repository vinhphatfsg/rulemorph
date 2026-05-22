#[test]
fn t16_array_ops() {
    assert_json_fixture("t16_array_ops");
}

#[test]
fn t17_json_ops_merge() {
    assert_json_fixture("t17_json_ops_merge");
}

#[test]
fn t18_json_ops_deep_merge() {
    assert_json_fixture("t18_json_ops_deep_merge");
}

#[test]
fn t19_json_ops_pick() {
    assert_json_fixture("t19_json_ops_pick");
}

#[test]
fn t20_json_ops_omit() {
    assert_json_fixture("t20_json_ops_omit");
}

#[test]
fn t21_json_ops_keys_values_entries() {
    assert_json_fixture("t21_json_ops_keys_values_entries");
}

#[test]
fn t22_json_ops_object_flatten() {
    assert_json_fixture("t22_json_ops_object_flatten");
}

#[test]
fn t23_json_ops_object_unflatten() {
    assert_json_fixture("t23_json_ops_object_unflatten");
}

#[test]
fn t24_json_ops_missing() {
    assert_json_fixture("t24_json_ops_missing");
}

#[test]
fn t25_json_ops_get_chain() {
    let base = fixtures_dir().join("t25_json_ops_get_chain");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let context = load_optional_json(&base.join("context.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, context.as_ref()).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn t26_chain_all_ops() {
    assert_json_fixture("t26_chain_all_ops");
}

#[test]
fn t27_json_ops_from_entries() {
    assert_json_fixture("t27_json_ops_from_entries");
}

#[test]
fn t28_expr_chain_nested() {
    assert_json_fixture("t28_expr_chain_nested");
}

#[test]
fn t29_json_ops_len() {
    assert_json_fixture("t29_json_ops_len");
}
