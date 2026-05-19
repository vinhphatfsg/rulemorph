#[test]
fn excel_input_with_header_normalizes_rows() {
    let base = fixtures_dir().join("t34_excel_input");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read(base.join("input.xlsx")).expect("read xlsx");
    let output =
        transform_input(&rule, InputData::Bytes(&input), None).expect("transform excel input");
    let expected = load_json(&base.join("expected.json"));
    assert_eq!(output, expected);
}

include!("excel/dynamodb.rs");

#[test]
fn excel_flat_spreadsheet_columns_transform_to_nested_json_document() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: excel
  excel:
    sheet: Orders
mappings:
  - target: "document"
    expr:
      - "@input"
      - object_unflatten: []
"#,
    )
    .expect("parse rule");
    let input = build_string_table_xlsx(
        "Orders",
        &[
            "order.id",
            "buyer.name",
            "buyer.email",
            "shipping.address.city",
            "items.primary.sku",
            "items.primary.qty",
            "items.secondary.sku",
            "items.secondary.qty",
        ],
        &[
            vec![
                "o001",
                "Alice",
                "alice@example.com",
                "Tokyo",
                "p001",
                "2",
                "p002",
                "1",
            ],
            vec![
                "o002",
                "Bob",
                "bob@example.com",
                "Osaka",
                "p003",
                "4",
                "p004",
                "3",
            ],
        ],
    );
    let output =
        transform_input(&rule, InputData::Bytes(&input), None).expect("transform excel input");
    assert_eq!(
        output,
        serde_json::json!([
            {
                "document": {
                    "order": { "id": "o001" },
                    "buyer": { "name": "Alice", "email": "alice@example.com" },
                    "shipping": { "address": { "city": "Tokyo" } },
                    "items": {
                        "primary": { "sku": "p001", "qty": "2" },
                        "secondary": { "sku": "p002", "qty": "1" }
                    }
                }
            },
            {
                "document": {
                    "order": { "id": "o002" },
                    "buyer": { "name": "Bob", "email": "bob@example.com" },
                    "shipping": { "address": { "city": "Osaka" } },
                    "items": {
                        "primary": { "sku": "p003", "qty": "4" },
                        "secondary": { "sku": "p004", "qty": "3" }
                    }
                }
            }
        ])
    );
}

#[test]
fn xlsform_survey_sheet_transforms_to_question_schema() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: excel
  excel:
    sheet: survey
mappings:
  - target: "name"
    source: "name"
  - target: "kind"
    source: "type"
  - target: "label"
    source: "label"
  - target: "required"
    source: "required"
    type: "bool"
  - target: "relevance"
    source: "relevant"
"#,
    )
    .expect("parse rule");
    let input = build_string_table_xlsx(
        "survey",
        &["type", "name", "label", "required", "relevant"],
        &[
            vec!["text", "respondent_name", "Respondent name", "true", ""],
            vec!["integer", "age", "Age", "false", "${respondent_name} != ''"],
        ],
    );
    let output =
        transform_input(&rule, InputData::Bytes(&input), None).expect("transform excel input");
    assert_eq!(
        output,
        serde_json::json!([
            {
                "name": "respondent_name",
                "kind": "text",
                "label": "Respondent name",
                "required": true,
                "relevance": ""
            },
            {
                "name": "age",
                "kind": "integer",
                "label": "Age",
                "required": false,
                "relevance": "${respondent_name} != ''"
            }
        ])
    );
}

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

include!("excel/relationships.rs");

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

include!("excel/security.rs");
