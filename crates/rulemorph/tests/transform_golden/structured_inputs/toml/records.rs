#[test]
fn toml_records_path_rejects_too_many_records_before_materializing() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: toml
  toml:
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
        InputData::Text("[[users]]\nid = 1\n[[users]]\nid = 2\n"),
        &options,
    )
    .expect_err("record limit should fail before TOML records are materialized");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}
