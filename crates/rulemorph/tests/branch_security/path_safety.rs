#[test]
fn branch_path_must_stay_under_base_dir() {
    let dir = unique_temp_dir("branch-base");
    let outside = unique_temp_dir("branch-outside");
    let outside_rule = outside.join("outside.yaml");
    fs::write(
        &outside_rule,
        r#"version: 2
input:
  format: json
  json: {}
mappings: []
"#,
    )
    .expect("write outside rule");
    let rule = parse_rule_file(&format!(
        r#"version: 2
input:
  format: json
  json: {{}}
steps:
  - branch:
      when: {{ eq: [1, 1] }}
      then: {}
"#,
        outside_rule.display()
    ))
    .expect("parse");
    let err = transform_with_base_dir(&rule, r#"{"id":1}"#, None, &dir)
        .expect_err("outside branch should be rejected");
    assert!(err.to_string().contains("base directory"));
}

#[test]
fn nested_branch_can_reference_parent_within_base_dir() {
    let dir = unique_temp_dir("branch-nested-parent");
    let subdir = dir.join("sub");
    fs::create_dir_all(&subdir).expect("create subdir");
    let main_rule = dir.join("main.yaml");
    let child_rule = subdir.join("child.yaml");
    let common_rule = dir.join("common.yaml");
    fs::write(
        &main_rule,
        r#"version: 2
input:
  format: json
  json: {}
steps:
  - branch:
      when: { eq: [1, 1] }
      then: sub/child.yaml
      return: true
"#,
    )
    .expect("write main");
    fs::write(
        &child_rule,
        r#"version: 2
input:
  format: json
  json: {}
steps:
  - branch:
      when: { eq: [1, 1] }
      then: ../common.yaml
      return: true
"#,
    )
    .expect("write child");
    fs::write(
        &common_rule,
        r#"version: 2
input:
  format: json
  json: {}
mappings:
  - target: ok
    value: true
"#,
    )
    .expect("write common");
    let rule = parse_rule_file(&fs::read_to_string(&main_rule).expect("read main")).expect("parse");
    let output =
        transform_with_base_dir(&rule, r#"{"id":1}"#, None, &dir).expect("nested branch succeeds");
    assert_eq!(output, serde_json::json!([{ "ok": true }]));
}
