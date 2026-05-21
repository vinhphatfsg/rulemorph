#[test]
fn branch_self_cycle_is_rejected() {
    let dir = unique_temp_dir("branch-self-cycle");
    let rule_path = dir.join("self.yaml");
    fs::write(
        &rule_path,
        r#"version: 2
input:
  format: json
  json: {}
steps:
  - branch:
      when: { eq: [1, 1] }
      then: self.yaml
"#,
    )
    .expect("write rule");
    let rule = parse_rule_file(&fs::read_to_string(&rule_path).expect("read rule")).expect("parse");
    let err = transform_with_base_dir(&rule, r#"{"id":1}"#, None, &dir)
        .expect_err("cycle should be rejected");
    assert!(err.to_string().contains("cycle"));
}

#[test]
fn branch_depth_limit_is_rejected_with_stable_error() {
    let dir = unique_temp_dir("branch-depth-limit");
    let depth = 66usize;
    for index in 0..depth {
        let path = dir.join(format!("rule{index}.yaml"));
        let next = format!("rule{}.yaml", index + 1);
        fs::write(
            &path,
            format!(
                r#"version: 2
input:
  format: json
  json: {{}}
steps:
  - branch:
      when: {{ eq: [1, 1] }}
      then: {next}
      return: true
"#
            ),
        )
        .expect("write rule");
    }
    fs::write(
        dir.join(format!("rule{depth}.yaml")),
        r#"version: 2
input:
  format: json
  json: {}
mappings:
  - target: ok
    value: true
"#,
    )
    .expect("write terminal rule");

    let rule =
        parse_rule_file(&fs::read_to_string(dir.join("rule0.yaml")).expect("read root rule"))
            .expect("parse");
    let err = transform_with_base_dir(&rule, r#"{"id":1}"#, None, &dir)
        .expect_err("branch depth limit should be rejected");

    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert_eq!(err.message, "branch rule depth limit exceeded");
    assert_eq!(err.path.as_deref(), Some("steps[0].branch.then"));
}
