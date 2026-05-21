#[test]
fn xml_rejects_attr_text_key_collision() {
    let rule = parse_rule_file(
        r##"
version: 2
input:
  format: xml
  xml:
    records_path: users.user
    attr_prefix: "#"
    text_key: "#text"
mappings:
  - target: "id"
    source: "id"
"##,
    )
    .expect("parse rule");
    let err = transform(
        &rule,
        r#"<users><user text="attr">body</user></users>"#,
        None,
    )
    .expect_err("attr/text key collision should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}
