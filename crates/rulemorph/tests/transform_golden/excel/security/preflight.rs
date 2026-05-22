#[test]
fn excel_preflight_ignores_unqualified_sheet_id_attribute() {
    let rule = load_rule(&fixtures_dir().join("t34_excel_input").join("rules.yaml"));
    let input = build_test_xlsx(XlsxFixtureOptions {
        conflicting_sheet_relationship: true,
        ..XlsxFixtureOptions::default()
    });
    let err = normalize_records_with_options(
        &rule,
        InputData::Bytes(&input),
        &NormalizationOptions::default(),
    )
    .expect_err("preflight must inspect the r:id worksheet, not an unqualified id");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert_eq!(err.message, "input exceeds max_excel_rows");
}

#[test]
fn excel_preflight_rejects_case_variant_duplicate_sheet_part() {
    let rule = load_rule(&fixtures_dir().join("t34_excel_input").join("rules.yaml"));
    let input = build_test_xlsx(XlsxFixtureOptions {
        case_variant_duplicate_sheet: true,
        ..XlsxFixtureOptions::default()
    });
    let err = normalize_records_with_options(
        &rule,
        InputData::Bytes(&input),
        &NormalizationOptions::default(),
    )
    .expect_err("case-variant duplicate sheet parts should be rejected before parsing");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert_eq!(err.message, "Excel ZIP entry names must be unique");
}
