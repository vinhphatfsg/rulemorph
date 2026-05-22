#[test]
fn json_input_duplicate_key_is_invalid() {
    let yaml = r#"
version: 2
input:
  format: json
  json:
    records_path: items
mappings:
  - target: "id"
    source: "id"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let err = transform(&rule, r#"{ "items": [{ "id": 1, "id": 2 }] }"#, None)
        .expect_err("duplicate key should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn normalization_rejects_input_over_byte_limit() {
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
    let err = normalize_records_with_options(&rule, InputData::Text(r#"{ "id": 1 }"#), &options)
        .expect_err("limit should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}
