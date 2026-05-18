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
