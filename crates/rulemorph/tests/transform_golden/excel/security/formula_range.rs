#[test]
fn excel_rejects_formula_extent_over_cell_limit() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: excel
  excel:
    sheet: Users
    has_header: false
    formula: formula
    columns:
      - name: "a"
        column: "A"
      - name: "b"
        column: "B"
mappings:
  - target: "a"
    source: "a"
"#,
    )
    .expect("parse rule");
    let input = build_test_xlsx(XlsxFixtureOptions {
        far_formula_without_cache: true,
        ..XlsxFixtureOptions::default()
    });
    let options = NormalizationOptions {
        max_excel_rows: 200,
        max_excel_cells: 250,
        ..NormalizationOptions::default()
    };
    let err = normalize_records_with_options(&rule, InputData::Bytes(&input), &options)
        .expect_err("formula extent should count toward effective cells");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert_eq!(err.message, "input exceeds max_excel_cells");
}

#[test]
fn excel_rejects_shared_formula_metadata() {
    let rule = load_rule(&fixtures_dir().join("t34_excel_input").join("rules.yaml"));
    let input = build_test_xlsx(XlsxFixtureOptions {
        shared_formula: true,
        ..XlsxFixtureOptions::default()
    });
    let err = normalize_records_with_options(
        &rule,
        InputData::Bytes(&input),
        &NormalizationOptions::default(),
    )
    .expect_err("shared formula should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn excel_rejects_sparse_far_cell_dense_range_limit() {
    let rule = load_rule(&fixtures_dir().join("t34_excel_input").join("rules.yaml"));
    let input = build_test_xlsx(XlsxFixtureOptions {
        sparse_far_cell: true,
        ..XlsxFixtureOptions::default()
    });
    let err = normalize_records_with_options(
        &rule,
        InputData::Bytes(&input),
        &NormalizationOptions::default(),
    )
    .expect_err("sparse far cell should fail before calamine range allocation");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}
