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
