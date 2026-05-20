#[test]
fn yaml_alias_limit_ignores_asterisks_in_scalars() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: yaml
  yaml:
    records_path: users
mappings:
  - target: "name"
    source: "name"
  - target: "note"
    source: "note"
"#,
    )
    .expect("parse rule");
    let input = r#"
users:
  - name: Alice
    note: "**********"
    block: |
      **********
"#;
    let options = NormalizationOptions {
        max_yaml_aliases: 1,
        ..NormalizationOptions::default()
    };
    let records = normalize_records_with_options(&rule, InputData::Text(input), &options)
        .expect("asterisks in scalar values should not count as aliases")
        .collect::<Result<Vec<_>, _>>()
        .expect("normalized records");
    assert_eq!(
        records,
        vec![serde_json::json!({ "name": "Alice", "note": "**********", "block": "**********\n" })]
    );
}

#[test]
fn yaml_rejects_alias_expansion_limit_exceeded() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: yaml
  yaml:
    records_path: users
mappings:
  - target: "name"
    source: "name"
"#,
    )
    .expect("parse rule");
    let input = "base: &base { name: Alice }\nusers: [*base, *base]\n";
    let options = NormalizationOptions {
        max_yaml_aliases: 1,
        ..NormalizationOptions::default()
    };
    let err = normalize_records_with_options(&rule, InputData::Text(input), &options)
        .expect_err("alias limit should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}
