#[test]
fn xml_rejects_namespace_strip_collision() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: xml
  xml:
    records_path: users.user
    namespaces: strip
mappings:
  - target: "id"
    source: "id"
"#,
    )
    .expect("parse rule");
    let err = transform(
        &rule,
        r#"<users xmlns:a="urn:a" xmlns:b="urn:b"><user><a:name>Alice</a:name><b:name>Bob</b:name></user></users>"#,
        None,
    )
    .expect_err("namespace strip collision should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn xml_rejects_attribute_namespace_strip_collision() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: xml
  xml:
    records_path: users.user
    namespaces: strip
mappings:
  - target: "id"
    source: "id"
"#,
    )
    .expect("parse rule");
    let err = transform(
        &rule,
        r#"<users xmlns:a="urn:a" xmlns:b="urn:b"><user a:id="1" b:id="2" /></users>"#,
        None,
    )
    .expect_err("attribute namespace strip collision should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}
