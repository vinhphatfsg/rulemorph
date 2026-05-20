#[test]
fn excel_rejects_sheet_limit_exceeded() {
    let base = fixtures_dir().join("t34_excel_input");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read(base.join("input.xlsx")).expect("read xlsx");
    let options = NormalizationOptions {
        max_excel_sheets: 0,
        ..NormalizationOptions::default()
    };
    let err = normalize_records_with_options(&rule, InputData::Bytes(&input), &options)
        .expect_err("sheet limit should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn excel_rejects_text_input() {
    let rule = load_rule(&fixtures_dir().join("t34_excel_input").join("rules.yaml"));
    let err = normalize_records_with_options(
        &rule,
        InputData::Text("not xlsx"),
        &NormalizationOptions::default(),
    )
    .expect_err("excel text input should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn excel_rejects_input_over_byte_limit() {
    let rule = load_rule(&fixtures_dir().join("t34_excel_input").join("rules.yaml"));
    let input =
        fs::read(fixtures_dir().join("t34_excel_input").join("input.xlsx")).expect("read xlsx");
    let options = NormalizationOptions {
        max_input_bytes: 1,
        ..NormalizationOptions::default()
    };
    let err = normalize_records_with_options(&rule, InputData::Bytes(&input), &options)
        .expect_err("input byte limit should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn excel_preflight_accepts_byte_input() {
    let rule = load_rule(&fixtures_dir().join("t34_excel_input").join("rules.yaml"));
    let input =
        fs::read(fixtures_dir().join("t34_excel_input").join("input.xlsx")).expect("read xlsx");
    preflight_validate_input(&rule, InputData::Bytes(&input), None)
        .expect("excel preflight should accept byte input");
}

#[test]
fn excel_applies_row_and_cell_limits_to_selected_sheet_only() {
    let rule = load_rule(&fixtures_dir().join("t34_excel_input").join("rules.yaml"));
    let input = build_test_xlsx(XlsxFixtureOptions {
        extra_sheet: true,
        ..XlsxFixtureOptions::default()
    });
    let options = NormalizationOptions {
        max_excel_rows: 2,
        max_excel_cells: 4,
        ..NormalizationOptions::default()
    };
    let records = normalize_records_with_options(&rule, InputData::Bytes(&input), &options)
        .expect("unselected sheet should not count toward row/cell limits")
        .collect::<Result<Vec<_>, _>>()
        .expect("records should normalize");
    assert_eq!(
        records,
        vec![serde_json::json!({ "id": 1, "name": "Alice" })]
    );
}

#[test]
fn excel_rejects_empty_selected_range_with_clear_error() {
    let rule = load_rule(&fixtures_dir().join("t34_excel_input").join("rules.yaml"));
    let input = build_test_xlsx(XlsxFixtureOptions {
        empty_sheet: true,
        ..XlsxFixtureOptions::default()
    });
    let err = transform_input(&rule, InputData::Bytes(&input), None)
        .expect_err("empty sheet should fail before header lookup");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("selected range has no columns"));
}

#[test]
fn excel_rejects_duplicate_header() {
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
        duplicate_header: true,
        ..XlsxFixtureOptions::default()
    });
    let err = transform_input(&rule, InputData::Bytes(&input), None)
        .expect_err("duplicate header should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}
