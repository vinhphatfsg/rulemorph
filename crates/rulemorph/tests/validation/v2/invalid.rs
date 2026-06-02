#[test]
fn v2_invalid_rules_should_fail_validation() {
    let cases = [
        "tv26_v01_unknown_op",
        "tv26_v03_literal_start_unknown_op",
        "tv26_v04_empty_pipe",
        "tv26_v05_branch_when_v1_non_bool",
        "tv43_finalize_wrap_invalid_expr",
    ];

    for case in cases {
        let rule = load_rule(case);
        let expected = load_expected_errors(case);
        let errors = validate_rule_file(&rule).unwrap_err();
        let actual = normalize_errors(errors);
        assert_eq!(actual, expected, "error mismatch for {}", case);
    }
}

#[test]
fn v2_forward_out_ref_should_fail_validation() {
    // tv26_v02_forward_out_ref should fail with ForwardOutReference error
    let rule = load_rule("tv26_v02_forward_out_ref");
    let expected = load_expected_errors("tv26_v02_forward_out_ref");
    let errors = validate_rule_file(&rule).unwrap_err();
    let actual = normalize_errors(errors);
    assert_eq!(
        actual, expected,
        "error mismatch for tv26_v02_forward_out_ref"
    );
}

#[test]
fn v2_chain_starting_with_pipe_ref_is_validated_as_v2_pipe() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: value
    expr:
      chain:
        - "$.foo"
        - op: uppercase
"#,
    )
    .expect("rule parses");

    let errors = validate_rule_file(&rule).expect_err("top-level pipe ref is invalid");
    assert!(
        errors
            .iter()
            .any(|err| err.code == ErrorCode::InvalidRefNamespace)
    );
}
