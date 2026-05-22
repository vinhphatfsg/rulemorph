#[test]
fn xml_rejects_invalid_records_path_at_runtime() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: xml
  xml:
    records_path: users[0].user
mappings:
  - target: "id"
    source: "id"
"#,
    )
    .expect("parse rule");
    let err = transform(&rule, "<users><user /></users>", None)
        .expect_err("invalid XML records_path should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidRecordsPath);
}

#[test]
fn xml_rejects_records_path_that_matches_no_elements() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: xml
  xml:
    records_path: users.usr
mappings:
  - target: "id"
    source: "id"
"#,
    )
    .expect("parse rule");
    let err = transform(&rule, "<users><user /></users>", None)
        .expect_err("missing XML records_path should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidRecordsPath);
    assert_eq!(err.path.as_deref(), Some("input.xml.records_path"));
}

#[test]
fn xml_records_path_accepts_non_ascii_element_names() {
    let yaml = r##"
version: 2
input:
  format: xml
  xml:
    records_path: 利用者.名前
mappings:
  - target: "name"
    source: "#text"
"##;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let input = r#"<利用者><名前>太郎</名前></利用者>"#;
    let output = transform(&rule, input, None).expect("transform");
    assert_eq!(output, serde_json::json!([{ "name": "太郎" }]));
}
