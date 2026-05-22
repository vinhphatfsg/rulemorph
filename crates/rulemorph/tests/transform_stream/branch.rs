#[test]
fn transform_stream_with_base_dir_resolves_branch_rules() {
    let dir = unique_temp_dir("base-dir-branch");
    fs::write(
        dir.join("child.yaml"),
        r#"version: 2
input:
  format: json
  json: {}
mappings:
  - target: child_name
    source: name
"#,
    )
    .expect("write child");
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
steps:
  - mappings:
      - target: name
        source: name
  - branch:
      when: { eq: ["@out.name", "alice"] }
      then: child.yaml
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let stream = transform_stream_with_base_dir(&rule, r#"[{"name":"alice"}]"#, None, &dir)
        .expect("stream transform");
    let items = stream.collect::<Result<Vec<_>, _>>().expect("stream items");

    assert_eq!(items.len(), 1);
    assert_eq!(
        items[0].output,
        Some(json!({ "name": "alice", "child_name": "alice" }))
    );
    assert!(items[0].warnings.is_empty());
}
