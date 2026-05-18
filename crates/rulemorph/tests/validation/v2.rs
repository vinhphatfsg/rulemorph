#[test]
fn v2_valid_rules_should_pass_validation() {
    let cases = [
        "tv22_basic",
        "tv23_steps",
        "tv24_conditions",
        "tv25_lookup",
        "tv27_v1_compat",
        "tv28_map_let_binding",
        "tv29_v2_out_sibling_ok",
        "tv30_literal_escape",
        "tv36_branch_uses_out",
        "tv39_finalize_filter_index",
        "tv41_branch_return_out_update",
    ];

    for case in cases {
        let rule = load_rule(case);
        if let Err(errors) = validate_rule_file(&rule) {
            let codes: Vec<&'static str> = errors.iter().map(|e| e.code.as_str()).collect();
            panic!("expected valid rules for {}, got {:?}", case, codes);
        }
    }
}

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
fn v2_steps_cyclic_out_refs_should_fail_validation() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
steps:
  - mappings:
      - target: a
        expr:
          - "@out.b"
  - mappings:
      - target: b
        expr:
          - "@out.a"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let errors = validate_rule_file(&rule).expect_err("cyclic steps should fail");

    assert!(
        errors
            .iter()
            .any(|err| err.code == ErrorCode::CyclicDependency),
        "expected CyclicDependency, got {errors:?}"
    );
}

#[test]
fn v2_steps_are_exclusive_with_mappings_and_record_when() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
record_when:
  eq: ["@input.enabled", true]
mappings:
  - target: existing
    value: 1
steps:
  - mappings:
      - target: name
        source: input.name
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let errors = validate_rule_file(&rule).expect_err("steps exclusivity should fail");

    assert!(
        errors.iter().any(|err| {
            err.code == ErrorCode::StepsMappingExclusive && err.path.as_deref() == Some("steps")
        }),
        "expected StepsMappingExclusive at steps, got {errors:?}"
    );
}

#[test]
fn v2_step_branch_then_error_includes_source_location() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
steps:
  - branch:
      when: { eq: ["@input.kind", "a"] }
      then: ""
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let errors = validate_rule_file_with_source(&rule, yaml).expect_err("empty then should fail");
    let error = errors
        .iter()
        .find(|err| {
            err.code == ErrorCode::InvalidStep
                && err.path.as_deref() == Some("steps[0].branch.then")
        })
        .expect("expected InvalidStep for branch.then");
    let location = error.location.clone().expect("expected location");
    assert_eq!(location.line, 9);
}
