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
