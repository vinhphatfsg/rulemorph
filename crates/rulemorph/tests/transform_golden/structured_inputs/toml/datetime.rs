#[test]
fn toml_datetime_is_string() {
    let toml_rule = r#"
version: 2
input:
  format: toml
  toml:
    records_path: users
mappings:
  - target: "created_at"
    source: "created_at"
"#;
    let rule = parse_rule_file(toml_rule).expect("parse rule");
    let input = "[[users]]\ncreated_at = 2026-05-08T12:00:00Z\n";
    let output = transform(&rule, input, None).expect("transform");
    assert_eq!(
        output,
        serde_json::json!([{ "created_at": "2026-05-08T12:00:00Z" }])
    );
}

#[test]
fn toml_quoted_private_datetime_key_stays_object() {
    let toml_rule = r#"
version: 2
input:
  format: toml
  toml:
    records_path: users
mappings:
  - target: "metadata"
    source: "metadata"
"#;
    let rule = parse_rule_file(toml_rule).expect("parse rule");
    let input = "[[users]]\n[users.metadata]\n'$__toml_private_datetime' = 2026-05-08T12:00:00Z\n";
    let records = normalize_records_with_options(
        &rule,
        InputData::Text(input),
        &NormalizationOptions::default(),
    )
    .expect("normalize toml")
    .collect::<Result<Vec<_>, _>>()
    .expect("normalized records");
    assert_eq!(
        records,
        vec![serde_json::json!({
            "metadata": { "$__toml_private_datetime": "2026-05-08T12:00:00Z" }
        })]
    );
}
