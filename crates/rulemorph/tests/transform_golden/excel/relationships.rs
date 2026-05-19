#[test]
fn excel_accepts_sheet_relationship_with_custom_namespace_prefix() {
    let rule = load_rule(&fixtures_dir().join("t34_excel_input").join("rules.yaml"));
    let input = build_test_xlsx(XlsxFixtureOptions {
        custom_relationship_prefix: true,
        ..XlsxFixtureOptions::default()
    });
    let output = transform_input(&rule, InputData::Bytes(&input), None)
        .expect("custom relationship namespace prefix should resolve");
    assert_eq!(output, serde_json::json!([{ "id": 1, "name": "Alice" }]));
}

#[test]
fn excel_rejects_unqualified_sheet_relationship_id() {
    let rule = load_rule(&fixtures_dir().join("t34_excel_input").join("rules.yaml"));
    let input = build_test_xlsx(XlsxFixtureOptions {
        unqualified_sheet_relationship_only: true,
        ..XlsxFixtureOptions::default()
    });
    let err = normalize_records_with_options(
        &rule,
        InputData::Bytes(&input),
        &NormalizationOptions::default(),
    )
    .expect_err("unqualified sheet id must not be accepted as a relationship");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert_eq!(err.message, "Excel workbook sheet is missing relationship");
}

#[test]
fn excel_rejects_relationship_id_bound_to_wrong_namespace() {
    let rule = load_rule(&fixtures_dir().join("t34_excel_input").join("rules.yaml"));
    let input = build_test_xlsx(XlsxFixtureOptions {
        wrong_relationship_namespace: true,
        ..XlsxFixtureOptions::default()
    });
    let err = normalize_records_with_options(
        &rule,
        InputData::Bytes(&input),
        &NormalizationOptions::default(),
    )
    .expect_err("relationship id with the wrong namespace must not be accepted");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert_eq!(err.message, "Excel workbook sheet is missing relationship");
}

#[test]
fn excel_rejects_multiple_qualified_sheet_relationship_ids() {
    let rule = load_rule(&fixtures_dir().join("t34_excel_input").join("rules.yaml"));
    let input = build_test_xlsx(XlsxFixtureOptions {
        duplicate_qualified_sheet_relationships: true,
        ..XlsxFixtureOptions::default()
    });
    let err = normalize_records_with_options(
        &rule,
        InputData::Bytes(&input),
        &NormalizationOptions::default(),
    )
    .expect_err("multiple qualified sheet relationship ids should be rejected");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert_eq!(
        err.message,
        "Excel workbook sheet has multiple relationships"
    );
}

#[test]
fn excel_rejects_literal_relationship_id_bound_to_wrong_namespace() {
    let rule = load_rule(&fixtures_dir().join("t34_excel_input").join("rules.yaml"));
    let input = build_test_xlsx(XlsxFixtureOptions {
        conflicting_wrong_literal_relationship: true,
        ..XlsxFixtureOptions::default()
    });
    let err = normalize_records_with_options(
        &rule,
        InputData::Bytes(&input),
        &NormalizationOptions::default(),
    )
    .expect_err("literal relationship id with wrong namespace should be rejected");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert_eq!(
        err.message,
        "Excel workbook sheet relationship uses an invalid namespace"
    );
}
