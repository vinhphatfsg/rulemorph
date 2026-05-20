#[test]
fn transform_input_rejects_invalid_utf8_bytes() {
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
    let err = transform_input(&rule, InputData::Bytes(&[0xff, 0xfe, b'\n']), None)
        .expect_err("invalid UTF-8 bytes should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("UTF-8"));
}

#[test]
fn normalization_rejects_byte_input_over_byte_limit() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: "id"
    source: "id"
"#,
    )
    .expect("parse rule");
    let options = NormalizationOptions {
        max_input_bytes: 4,
        ..NormalizationOptions::default()
    };
    let err = normalize_records_with_options(&rule, InputData::Bytes(br#"{ "id": 1 }"#), &options)
        .expect_err("byte input limit should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}
