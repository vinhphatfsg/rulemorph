#[test]
fn branch_can_reference_json_rule_by_extension() {
    let dir = unique_temp_dir("branch-json-rule");
    let main_rule = dir.join("main.yaml");
    let then_rule = dir.join("then.json");
    fs::write(
        &main_rule,
        r#"version: 2
input:
  format: json
  json: {}
steps:
  - branch:
      when: { eq: [1, 1] }
      then: then.json
      return: true
"#,
    )
    .expect("write main");
    fs::write(
        &then_rule,
        r#"{
  "version": 2,
  "input": { "format": "json", "json": {} },
  "mappings": [{ "target": "ok", "value": true }]
}
"#,
    )
    .expect("write then");
    let rule = parse_rule_file(&fs::read_to_string(&main_rule).expect("read main")).expect("parse");
    let output =
        transform_with_base_dir(&rule, r#"{"id":1}"#, None, &dir).expect("json branch succeeds");
    assert_eq!(output, serde_json::json!([{ "ok": true }]));
}
