#[test]
fn excel_rejects_formula_without_cache() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: excel
  excel:
    sheet: Users
mappings:
  - target: "id"
    source: "id"
"#,
    )
    .expect("parse rule");
    let input = build_test_xlsx(XlsxFixtureOptions {
        formula_without_cache: true,
        ..XlsxFixtureOptions::default()
    });
    let err = transform_input(&rule, InputData::Bytes(&input), None)
        .expect_err("formula without cache should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn excel_formula_policy_returns_formula_without_cache() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: excel
  excel:
    sheet: Users
    formula: formula
mappings:
  - target: "id"
    source: "id"
"#,
    )
    .expect("parse rule");
    let input = build_test_xlsx(XlsxFixtureOptions {
        formula_without_cache: true,
        ..XlsxFixtureOptions::default()
    });
    let output = transform_input(&rule, InputData::Bytes(&input), None).expect("formula transform");
    assert_eq!(output, serde_json::json!([{ "id": "1+1" }]));
}

#[test]
fn excel_formula_error_policy_rejects_formula_without_cache() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: excel
  excel:
    sheet: Users
    formula: error
mappings:
  - target: "id"
    source: "id"
"#,
    )
    .expect("parse rule");
    let input = build_test_xlsx(XlsxFixtureOptions {
        formula_without_cache: true,
        ..XlsxFixtureOptions::default()
    });
    let err = transform_input(&rule, InputData::Bytes(&input), None)
        .expect_err("formula error policy should fail on formulas");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}
