#[test]
fn preflight_input_with_warnings_matches_plain_preflight_error_contract() {
    let base = fixtures_dir().join("p02_preflight_missing_required");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));

    let plain = preflight_validate(&rule, &input, None).expect_err("plain preflight error");
    let with_options = preflight_validate_input_with_warnings_with_base_dir_and_options(
        &rule,
        InputData::Text(&input),
        None,
        &base,
        &NormalizationOptions::default(),
    )
    .expect_err("input preflight error");

    assert_eq!(with_options.kind, plain.kind);
    assert_eq!(with_options.message, plain.message);
    assert_eq!(with_options.path, plain.path);
}
