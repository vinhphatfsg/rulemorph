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
