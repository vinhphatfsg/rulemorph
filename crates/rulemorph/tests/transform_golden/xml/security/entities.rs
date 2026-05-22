#[test]
fn xml_dtd_is_rejected() {
    let yaml = r#"
version: 2
input:
  format: xml
  xml:
    records_path: users.user
mappings:
  - target: "id"
    source: "id"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let err = transform(&rule, r#"<!DOCTYPE users><users><user /></users>"#, None)
        .expect_err("DTD should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn xml_external_entity_dtd_is_rejected() {
    let yaml = r#"
version: 2
input:
  format: xml
  xml:
    records_path: users.user
mappings:
  - target: "id"
    source: "id"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let err = transform(
        &rule,
        r#"<!DOCTYPE users [<!ENTITY xxe SYSTEM "file:///etc/passwd">]><users><user>&xxe;</user></users>"#,
        None,
    )
    .expect_err("external entity DTD should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn xml_processing_instruction_is_rejected() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: xml
  xml:
    records_path: users.user
mappings:
  - target: "id"
    source: "id"
"#,
    )
    .expect("parse rule");
    let err = transform(
        &rule,
        r#"<?xml-stylesheet href="file:///tmp/x" type="text/xsl"?><users><user /></users>"#,
        None,
    )
    .expect_err("processing instruction should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn xml_undefined_entity_is_rejected() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: xml
  xml:
    records_path: users.user
mappings:
  - target: "name"
    source: "name"
"#,
    )
    .expect("parse rule");
    let err = transform(&rule, "<users><user>&xxe;</user></users>", None)
        .expect_err("undefined entity should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}
