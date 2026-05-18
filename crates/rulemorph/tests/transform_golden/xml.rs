#[test]
fn xml_input_normalizes_attributes_text_and_repeated_children() {
    let yaml = r##"
version: 2
input:
  format: xml
  xml:
    records_path: users.user
    attr_prefix: "@"
    text_key: "#text"
    child_policy: array
mappings:
  - target: "id"
    source: 'input.["@id"]'
  - target: "name"
    source: 'input.name[0]["#text"]'
  - target: "first_role"
    source: 'input.role[0]["#text"]'
"##;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let input = r#"<users><user id="1"><name>Alice</name><role>admin</role><role>editor</role></user></users>"#;
    let output = transform(&rule, input, None).expect("transform");
    assert_eq!(
        output,
        serde_json::json!([{ "id": "1", "name": "Alice", "first_role": "admin" }])
    );
}

#[test]
fn xml_mixed_content_preserves_token_separators_before_normalization() {
    let yaml = r##"
version: 2
input:
  format: xml
  xml:
    records_path: root
    text_key: "#text"
mappings:
  - target: "text"
    source: 'input.["#text"]'
"##;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let input = r#"<root>hello <b>ignored</b> world</root>"#;
    let output = transform(&rule, input, None).expect("transform");
    assert_eq!(output, serde_json::json!([{ "text": "hello world" }]));
}

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
fn xml_rejects_node_limit_exceeded() {
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
    let options = NormalizationOptions {
        max_xml_nodes: 1,
        ..NormalizationOptions::default()
    };
    let err =
        normalize_records_with_options(&rule, InputData::Text("<users><user /></users>"), &options)
            .expect_err("node limit should fail");
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
fn xml_allows_scoped_namespace_prefix_shadowing() {
    let rule = parse_rule_file(
        r##"
version: 2
input:
  format: xml
  xml:
    records_path: users.user
mappings:
  - target: "outer_name"
    source: 'input.["a:name"][0]["#text"]'
  - target: "inner_name"
    source: 'input.group[0]["a:name"][0]["#text"]'
"##,
    )
    .expect("parse rule");
    let output = transform(
        &rule,
        r#"<users xmlns:a="urn:outer"><user><a:name>Outer</a:name><group xmlns:a="urn:inner"><a:name>Inner</a:name></group></user></users>"#,
        None,
    )
    .expect("scoped namespace shadowing should be valid");
    assert_eq!(
        output,
        serde_json::json!([{ "outer_name": "Outer", "inner_name": "Inner" }])
    );
}

#[test]
fn xml_allows_scoped_default_namespace_shadowing() {
    let rule = parse_rule_file(
        r##"
version: 2
input:
  format: xml
  xml:
    records_path: users.user
mappings:
  - target: "outer_name"
    source: 'input.name[0]["#text"]'
  - target: "inner_name"
    source: 'input.group[0].name[0]["#text"]'
"##,
    )
    .expect("parse rule");
    let output = transform(
        &rule,
        r#"<users xmlns="urn:outer"><user><name>Outer</name><group xmlns="urn:inner"><name>Inner</name></group></user></users>"#,
        None,
    )
    .expect("scoped default namespace shadowing should be valid");
    assert_eq!(
        output,
        serde_json::json!([{ "outer_name": "Outer", "inner_name": "Inner" }])
    );
}

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
fn xml_rejects_depth_limit_exceeded() {
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
    let options = NormalizationOptions {
        max_depth: 1,
        ..NormalizationOptions::default()
    };
    let err = normalize_records_with_options(
        &rule,
        InputData::Text("<users><user><name>Alice</name></user></users>"),
        &options,
    )
    .expect_err("depth limit should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn xml_rejects_self_closing_element_over_depth_limit() {
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
    let options = NormalizationOptions {
        max_depth: 2,
        ..NormalizationOptions::default()
    };
    let err = normalize_records_with_options(
        &rule,
        InputData::Text("<users><user><name /></user></users>"),
        &options,
    )
    .expect_err("self-closing element over depth limit should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn xml_rejects_text_limit_exceeded() {
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
    let options = NormalizationOptions {
        max_text_bytes: 3,
        ..NormalizationOptions::default()
    };
    let err = normalize_records_with_options(
        &rule,
        InputData::Text("<users><user>Alice</user></users>"),
        &options,
    )
    .expect_err("text limit should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn xml_rejects_array_limit_exceeded() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: xml
  xml:
    records_path: users.user
mappings:
  - target: "roles"
    source: "role"
"#,
    )
    .expect("parse rule");
    let options = NormalizationOptions {
        max_array_len: 1,
        ..NormalizationOptions::default()
    };
    let err = normalize_records_with_options(
        &rule,
        InputData::Text("<users><user><role>a</role><role>b</role></user></users>"),
        &options,
    )
    .expect_err("array limit should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn xml_rejects_records_limit_exceeded() {
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
    let options = NormalizationOptions {
        max_records: 1,
        ..NormalizationOptions::default()
    };
    let err = normalize_records_with_options(
        &rule,
        InputData::Text("<users><user /><user /></users>"),
        &options,
    )
    .expect_err("record limit should fail");
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
