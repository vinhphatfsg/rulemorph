#[test]
fn csv_trailing_missing_field_is_invalid_input() {
    let yaml = r#"
version: 2
input:
  format: csv
  csv:
    has_header: true
mappings:
  - target: "id"
    source: "id"
  - target: "name"
    source: "name"
    default: "missing-name"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let err = transform(&rule, "id,name\n1\n", None).expect_err("short csv row should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("CSV error"), "{}", err.message);
}

#[test]
fn csv_extra_field_is_invalid_input() {
    let yaml = r#"
version: 2
input:
  format: csv
  csv:
    has_header: true
mappings:
  - target: "id"
    source: "id"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let err = transform(&rule, "id\n1,extra\n", None).expect_err("wide csv row should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("CSV error"), "{}", err.message);
}

#[test]
fn csv_no_header_short_row_is_invalid_input() {
    let yaml = r#"
version: 2
input:
  format: csv
  csv:
    has_header: false
    columns:
      - name: id
      - name: name
mappings:
  - target: "id"
    source: "id"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let err = transform(&rule, "1\n", None).expect_err("short csv row should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(
        err.message.contains("expected 2")
            || err.message.contains("expected 2 fields")
            || err.message.contains("expected 2"),
        "{}",
        err.message
    );
}

#[test]
fn csv_no_header_extra_field_is_invalid_input() {
    let yaml = r#"
version: 2
input:
  format: csv
  csv:
    has_header: false
    columns:
      - name: id
      - name: name
mappings:
  - target: "id"
    source: "id"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let err = transform(&rule, "1,Ada,extra\n", None).expect_err("wide csv row should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("expected 2"), "{}", err.message);
}

#[test]
fn csv_duplicate_header_is_invalid_input() {
    let yaml = r#"
version: 2
input:
  format: csv
  csv:
    has_header: true
mappings:
  - target: "id"
    source: "id"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let err = transform(&rule, "id,id\n1,2\n", None).expect_err("duplicate header should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

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
