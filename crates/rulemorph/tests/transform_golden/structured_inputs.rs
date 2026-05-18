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
fn json_records_path_rejects_too_many_records_before_materializing() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: json
  json:
    records_path: users
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
        InputData::Text(r#"{ "users": [{ "id": 1 }, { "id": 2 }] }"#),
        &options,
    )
    .expect_err("record limit should fail before records are materialized");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn json_records_path_rejects_single_object_when_record_limit_is_zero() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: json
  json:
    records_path: user
mappings:
  - target: "id"
    source: "id"
"#,
    )
    .expect("parse rule");
    let options = NormalizationOptions {
        max_records: 0,
        ..NormalizationOptions::default()
    };
    let err = normalize_records_with_options(
        &rule,
        InputData::Text(r#"{ "user": { "id": 1 } }"#),
        &options,
    )
    .expect_err("single object should still honor max_records");
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

include!("structured_inputs/yaml.rs");
include!("structured_inputs/toml.rs");
