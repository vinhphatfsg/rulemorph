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
