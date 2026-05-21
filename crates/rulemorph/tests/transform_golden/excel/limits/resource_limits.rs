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
