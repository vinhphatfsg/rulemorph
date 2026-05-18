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

#[test]
fn excel_rows_transform_to_dynamodb_attribute_values() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: excel
  excel:
    sheet: Users
mappings:
  - target: "Item.PK.S"
    expr:
      op: "concat"
      args: ["USER#", { ref: "input.user_id" }]
  - target: "Item.SK.S"
    value: "PROFILE"
  - target: "Item.email.S"
    source: "email"
    type: "string"
  - target: "Item.age.N"
    source: "age"
    type: "string"
  - target: "Item.active.BOOL"
    source: "active"
    type: "bool"
"#,
    )
    .expect("parse rule");
    let input = build_dynamodb_users_xlsx();
    let output =
        transform_input(&rule, InputData::Bytes(&input), None).expect("transform excel input");
    assert_eq!(
        output,
        serde_json::json!([
            {
                "Item": {
                    "PK": { "S": "USER#u001" },
                    "SK": { "S": "PROFILE" },
                    "email": { "S": "alice@example.com" },
                    "age": { "N": "31" },
                    "active": { "BOOL": true }
                }
            },
            {
                "Item": {
                    "PK": { "S": "USER#u002" },
                    "SK": { "S": "PROFILE" },
                    "email": { "S": "bob@example.com" },
                    "age": { "N": "28" },
                    "active": { "BOOL": false }
                }
            }
        ])
    );
}

#[test]
fn excel_rows_transform_to_dynamodb_batch_write_item_payload() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: excel
  excel:
    sheet: Users
mappings:
  - target: "PutRequest.Item.PK.S"
    expr:
      op: "concat"
      args: ["USER#", { ref: "input.user_id" }]
  - target: "PutRequest.Item.SK.S"
    value: "PROFILE"
  - target: "PutRequest.Item.email.S"
    source: "email"
    type: "string"
  - target: "PutRequest.Item.age.N"
    source: "age"
    type: "string"
  - target: "PutRequest.Item.active.BOOL"
    source: "active"
    type: "bool"
finalize:
  wrap:
    RequestItems:
      UsersTable: "@out"
"#,
    )
    .expect("parse rule");
    let input = build_dynamodb_users_xlsx();
    let output =
        transform_input(&rule, InputData::Bytes(&input), None).expect("transform excel input");
    assert_eq!(
        output,
        serde_json::json!({
            "RequestItems": {
                "UsersTable": [
                    {
                        "PutRequest": {
                            "Item": {
                                "PK": { "S": "USER#u001" },
                                "SK": { "S": "PROFILE" },
                                "email": { "S": "alice@example.com" },
                                "age": { "N": "31" },
                                "active": { "BOOL": true }
                            }
                        }
                    },
                    {
                        "PutRequest": {
                            "Item": {
                                "PK": { "S": "USER#u002" },
                                "SK": { "S": "PROFILE" },
                                "email": { "S": "bob@example.com" },
                                "age": { "N": "28" },
                                "active": { "BOOL": false }
                            }
                        }
                    }
                ]
            }
        })
    );
}

#[test]
fn excel_rows_transform_to_dynamodb_extended_attribute_values() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: excel
  excel:
    sheet: Products
mappings:
  - target: "Item.PK.S"
    expr:
      - "PRODUCT#"
      - concat: ["@input.sku"]
  - target: "Item.SK.S"
    value: "METADATA"
  - target: "Item.name.S"
    source: "name"
    type: "string"
  - target: "Item.price.N"
    source: "price"
    type: "string"
  - target: "Item.active.BOOL"
    source: "active"
    type: "bool"
  - target: "Item.tags.SS"
    expr:
      - "@input.tags"
      - split: [","]
  - target: "Item.dimensions.M.width.N"
    source: 'input.["dimensions.width"]'
    type: "string"
  - target: "Item.dimensions.M.height.N"
    source: 'input.["dimensions.height"]'
    type: "string"
  - target: "Item.archived.NULL"
    value: false
"#,
    )
    .expect("parse rule");
    let input = build_string_table_xlsx(
        "Products",
        &[
            "sku",
            "name",
            "price",
            "active",
            "tags",
            "dimensions.width",
            "dimensions.height",
        ],
        &[
            vec![
                "p001",
                "Notebook",
                "1299",
                "true",
                "stationery,paper",
                "148",
                "210",
            ],
            vec!["p002", "Pen", "199", "false", "stationery,ink", "10", "140"],
        ],
    );
    let output =
        transform_input(&rule, InputData::Bytes(&input), None).expect("transform excel input");
    assert_eq!(
        output,
        serde_json::json!([
            {
                "Item": {
                    "PK": { "S": "PRODUCT#p001" },
                    "SK": { "S": "METADATA" },
                    "name": { "S": "Notebook" },
                    "price": { "N": "1299" },
                    "active": { "BOOL": true },
                    "tags": { "SS": ["stationery", "paper"] },
                    "dimensions": { "M": { "width": { "N": "148" }, "height": { "N": "210" } } },
                    "archived": { "NULL": false }
                }
            },
            {
                "Item": {
                    "PK": { "S": "PRODUCT#p002" },
                    "SK": { "S": "METADATA" },
                    "name": { "S": "Pen" },
                    "price": { "N": "199" },
                    "active": { "BOOL": false },
                    "tags": { "SS": ["stationery", "ink"] },
                    "dimensions": { "M": { "width": { "N": "10" }, "height": { "N": "140" } } },
                    "archived": { "NULL": false }
                }
            }
        ])
    );
}

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

#[test]
fn excel_rejects_zip_entry_count_limit_exceeded() {
    let rule = load_rule(&fixtures_dir().join("t34_excel_input").join("rules.yaml"));
    let input =
        fs::read(fixtures_dir().join("t34_excel_input").join("input.xlsx")).expect("read xlsx");
    let options = NormalizationOptions {
        max_excel_zip_entries: 1,
        ..NormalizationOptions::default()
    };
    let err = normalize_records_with_options(&rule, InputData::Bytes(&input), &options)
        .expect_err("zip entry count limit should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn excel_rejects_zip_uncompressed_limit_exceeded() {
    let rule = load_rule(&fixtures_dir().join("t34_excel_input").join("rules.yaml"));
    let input =
        fs::read(fixtures_dir().join("t34_excel_input").join("input.xlsx")).expect("read xlsx");
    let options = NormalizationOptions {
        max_excel_uncompressed_bytes: 1,
        ..NormalizationOptions::default()
    };
    let err = normalize_records_with_options(&rule, InputData::Bytes(&input), &options)
        .expect_err("zip total uncompressed limit should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn excel_rejects_zip_entry_uncompressed_limit_exceeded() {
    let rule = load_rule(&fixtures_dir().join("t34_excel_input").join("rules.yaml"));
    let input =
        fs::read(fixtures_dir().join("t34_excel_input").join("input.xlsx")).expect("read xlsx");
    let options = NormalizationOptions {
        max_excel_entry_uncompressed_bytes: 1,
        ..NormalizationOptions::default()
    };
    let err = normalize_records_with_options(&rule, InputData::Bytes(&input), &options)
        .expect_err("zip entry uncompressed limit should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn excel_rejects_shared_strings_limit_exceeded() {
    let rule = load_rule(&fixtures_dir().join("t34_excel_input").join("rules.yaml"));
    let input =
        fs::read(fixtures_dir().join("t34_excel_input").join("input.xlsx")).expect("read xlsx");
    let options = NormalizationOptions {
        max_excel_shared_strings: 1,
        ..NormalizationOptions::default()
    };
    let err = normalize_records_with_options(&rule, InputData::Bytes(&input), &options)
        .expect_err("shared strings limit should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn excel_rejects_styles_limit_exceeded() {
    let rule = load_rule(&fixtures_dir().join("t34_excel_input").join("rules.yaml"));
    let input =
        fs::read(fixtures_dir().join("t34_excel_input").join("input.xlsx")).expect("read xlsx");
    let options = NormalizationOptions {
        max_excel_styles: 0,
        ..NormalizationOptions::default()
    };
    let err = normalize_records_with_options(&rule, InputData::Bytes(&input), &options)
        .expect_err("styles limit should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn excel_rejects_macro_enabled_workbook() {
    let rule = load_rule(&fixtures_dir().join("t34_excel_input").join("rules.yaml"));
    let input = build_test_xlsx(XlsxFixtureOptions {
        macro_enabled: true,
        ..XlsxFixtureOptions::default()
    });
    let err = normalize_records_with_options(
        &rule,
        InputData::Bytes(&input),
        &NormalizationOptions::default(),
    )
    .expect_err("macro workbook should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn excel_rejects_external_relationships() {
    let rule = load_rule(&fixtures_dir().join("t34_excel_input").join("rules.yaml"));
    let input = build_test_xlsx(XlsxFixtureOptions {
        external_relationship: true,
        ..XlsxFixtureOptions::default()
    });
    let err = normalize_records_with_options(
        &rule,
        InputData::Bytes(&input),
        &NormalizationOptions::default(),
    )
    .expect_err("external relationship should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}
