#[test]
fn p02_preflight_missing_required() {
    let base = fixtures_dir().join("p02_preflight_missing_required");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_expected_error(&base.join("expected_error.json"));

    let err = preflight_validate(&rule, &input, None).expect_err("expected preflight error");
    assert_eq!(transform_kind_to_str(&err.kind), expected.kind);
    assert_eq!(err.path, expected.path);
}

#[test]
fn p03_preflight_type_cast_failed() {
    let base = fixtures_dir().join("p03_preflight_type_cast_failed");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_expected_error(&base.join("expected_error.json"));

    let err = preflight_validate(&rule, &input, None).expect_err("expected preflight error");
    assert_eq!(transform_kind_to_str(&err.kind), expected.kind);
    assert_eq!(err.path, expected.path);
}

#[test]
fn p05_preflight_finalize_sort_missing() {
    let base = fixtures_dir().join("p05_preflight_finalize_sort_missing");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_expected_error(&base.join("expected_error.json"));

    let err = preflight_validate(&rule, &input, None).expect_err("expected preflight error");
    assert_eq!(transform_kind_to_str(&err.kind), expected.kind);
    assert_eq!(err.path, expected.path);
}
