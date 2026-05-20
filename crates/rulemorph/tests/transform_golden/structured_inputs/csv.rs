#[test]
fn csv_normalization_rejects_too_many_records_while_iterating() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: csv
  csv:
    has_header: true
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
    let mut records =
        normalize_records_with_options(&rule, InputData::Text("id\n1\n2\n"), &options)
            .expect("CSV iterator should be created before the second record is read");
    let first = records
        .next()
        .expect("first record should exist")
        .expect("first record should parse");
    assert_eq!(first, serde_json::json!({ "id": "1" }));
    let err = records
        .next()
        .expect("second record should report the record limit")
        .expect_err("record limit should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn csv_rejects_non_byte_delimiter() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: csv
  csv:
    has_header: true
    delimiter: "，"
mappings:
  - target: "id"
    source: "id"
"#,
    )
    .expect("parse rule");
    let err = normalize_records_with_options(
        &rule,
        InputData::Text("id，name\n1，Alice\n"),
        &NormalizationOptions::default(),
    )
    .expect_err("non-byte delimiter should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}
