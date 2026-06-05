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
fn json_records_path_rejects_scalar_value() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: json
  json:
    records_path: user.id
mappings:
  - target: "id"
    source: "id"
"#,
    )
    .expect("parse rule");
    let err = normalize_records_with_options(
        &rule,
        InputData::Text(r#"{ "user": { "id": 1 } }"#),
        &NormalizationOptions::default(),
    )
    .expect_err("scalar records_path should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert_eq!(err.message, "records_path must point to an array or object");
}
