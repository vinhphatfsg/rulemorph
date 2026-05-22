#[test]
fn excel_selects_sheet_with_escaped_name() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: excel
  excel:
    sheet: "Users & Billing"
mappings:
  - target: "id"
    source: "id"
  - target: "name"
    source: "name"
"#,
    )
    .expect("parse rule");
    let input = build_test_xlsx(XlsxFixtureOptions {
        escaped_sheet_name: true,
        ..XlsxFixtureOptions::default()
    });
    let output = transform_input(&rule, InputData::Bytes(&input), None)
        .expect("escaped sheet name should resolve");
    assert_eq!(output, serde_json::json!([{ "id": 1, "name": "Alice" }]));
}
