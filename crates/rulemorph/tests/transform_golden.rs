use std::fs;

use rulemorph::{
    InputData, NormalizationOptions, RuleFormat, TransformErrorKind,
    normalize_records_with_options, parse_rule_file, preflight_validate_input, transform,
    transform_input,
};
mod common;

use common::golden::{
    assert_json_fixture, assert_text_fixture, assert_transform_error_fixture, assert_xlsx_fixture,
    fixtures_dir, load_json, load_optional_json, load_rule, load_rule_with_format,
};
use common::xlsx::{
    XlsxFixtureOptions, build_dynamodb_users_xlsx, build_string_table_xlsx, build_test_xlsx,
};

#[test]
fn t01_csv_basic() {
    assert_text_fixture("t01_csv_basic", "input.csv");
}

#[test]
fn t02_csv_no_header() {
    assert_text_fixture("t02_csv_no_header", "input.csv");
}

include!("transform_golden/input_limits.rs");

#[test]
fn t30_json_rule_file_transform_golden() {
    let base = fixtures_dir().join("t30_json_rule_file");
    let rule = load_rule_with_format(&base.join("rules.json"), RuleFormat::Json);
    let input = fs::read_to_string(base.join("input.json")).expect("read input.json");
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn t31_yaml_input_transform_golden() {
    assert_text_fixture("t31_yaml_input", "input.yaml");
}

#[test]
fn t32_toml_input_transform_golden() {
    assert_text_fixture("t32_toml_input", "input.toml");
}

#[test]
fn t33_xml_input_transform_golden() {
    assert_text_fixture("t33_xml_input", "input.xml");
}

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
fn t35_html_input_transform_golden() {
    assert_text_fixture("t35_html_input", "input.html");
}

#[test]
fn t36_spreadsheets_plugin_products() {
    assert_xlsx_fixture("t36_spreadsheets_plugin_products");
}

#[test]
fn t37_spreadsheets_plugin_orders() {
    assert_xlsx_fixture("t37_spreadsheets_plugin_orders");
}

#[test]
fn t38_spreadsheets_plugin_survey() {
    assert_xlsx_fixture("t38_spreadsheets_plugin_survey");
}

#[test]
fn t39_pyproject_dependency_inventory() {
    assert_text_fixture("t39_pyproject_dependency_inventory", "input.toml");
}

#[test]
fn t40_cargo_dependency_feature_inventory() {
    assert_text_fixture("t40_cargo_dependency_feature_inventory", "input.toml");
}

#[test]
fn t41_github_actions_matrix() {
    assert_text_fixture("t41_github_actions_matrix", "input.yaml");
}

#[test]
fn t42_openapi_endpoint_catalog() {
    assert_text_fixture("t42_openapi_endpoint_catalog", "input.yaml");
}

#[test]
fn t43_mongodb_schema_summary() {
    assert_text_fixture("t43_mongodb_schema_summary", "input.json");
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

include!("transform_golden/xml.rs");

#[test]
fn html_input_extracts_selector_fields() {
    let yaml = r#"
version: 2
input:
  format: html
  html:
    records_selector: "table#users tbody tr"
    fields:
      id:
        selector: "td:nth-child(1)"
        value: text
      name:
        selector: "td:nth-child(2)"
        value: text
      profile_url:
        selector: "a.profile"
        value: attr
        attr: href
mappings:
  - target: "id"
    source: "id"
  - target: "name"
    source: "name"
  - target: "profile_url"
    source: "profile_url"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let input = r#"<table id="users"><tbody><tr><td>1</td><td>Alice</td><td><a class="profile" href="/users/1">Profile</a></td></tr></tbody></table>"#;
    let output = transform(&rule, input, None).expect("transform");
    assert_eq!(
        output,
        serde_json::json!([{ "id": "1", "name": "Alice", "profile_url": "/users/1" }])
    );
}

#[test]
fn html_multiple_no_match_returns_empty_array() {
    let yaml = r#"
version: 2
input:
  format: html
  html:
    records_selector: ".article"
    fields:
      tags:
        selector: ".tag"
        value: text
        multiple: true
mappings:
  - target: "tags"
    source: "tags"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let output =
        transform(&rule, r#"<article class="article"></article>"#, None).expect("transform");
    assert_eq!(output, serde_json::json!([{ "tags": [] }]));
}

#[test]
fn html_field_without_selector_uses_record_element() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: html
  html:
    records_selector: ".item"
    fields:
      name:
        value: text
mappings:
  - target: "name"
    source: "name"
"#,
    )
    .expect("parse rule");
    let output =
        transform(&rule, r#"<p class="item"> Alice <b>Smith</b> </p>"#, None).expect("transform");
    assert_eq!(output, serde_json::json!([{ "name": "Alice Smith" }]));
}

#[test]
fn html_inner_html_is_extracted_without_execution() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: html
  html:
    records_selector: ".article"
    fields:
      body:
        selector: ".body"
        value: html
mappings:
  - target: "body"
    source: "body"
"#,
    )
    .expect("parse rule");
    let output = transform(
        &rule,
        r#"<article class="article"><div class="body"><b>Alice</b><script>fetch("/x")</script></div></article>"#,
        None,
    )
    .expect("transform");
    assert_eq!(
        output,
        serde_json::json!([{ "body": "<b>Alice</b><script>fetch(\"/x\")</script>" }])
    );
}

#[test]
fn html_inner_html_preserves_raw_spacing() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: html
  html:
    records_selector: ".article"
    fields:
      body:
        selector: ".body"
        value: html
mappings:
  - target: "body"
    source: "body"
"#,
    )
    .expect("parse rule");
    let output = transform(
        &rule,
        r#"<article class="article"><div class="body"><span> Alice   Smith </span></div></article>"#,
        None,
    )
    .expect("transform");
    assert_eq!(
        output,
        serde_json::json!([{ "body": "<span> Alice   Smith </span>" }])
    );
}

#[test]
fn html_multiple_missing_attrs_are_excluded() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: html
  html:
    records_selector: ".article"
    fields:
      urls:
        selector: "a"
        value: attr
        attr: href
        multiple: true
mappings:
  - target: "urls"
    source: "urls"
"#,
    )
    .expect("parse rule");
    let output = transform(
        &rule,
        r#"<article class="article"><a>missing</a><a href="/ok">ok</a></article>"#,
        None,
    )
    .expect("transform");
    assert_eq!(output, serde_json::json!([{ "urls": ["/ok"] }]));
}

#[test]
fn html_rejects_array_limit_exceeded() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: html
  html:
    records_selector: ".article"
    fields:
      tags:
        selector: ".tag"
        value: text
        multiple: true
mappings:
  - target: "tags"
    source: "tags"
"#,
    )
    .expect("parse rule");
    let options = NormalizationOptions {
        max_array_len: 1,
        ..NormalizationOptions::default()
    };
    let err = normalize_records_with_options(
        &rule,
        InputData::Text(
            r#"<article class="article"><span class="tag">a</span><span class="tag">b</span></article>"#,
        ),
        &options,
    )
    .expect_err("array limit should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn html_rejects_node_limit_exceeded() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: html
  html:
    records_selector: ".item"
    fields:
      name:
        value: text
mappings:
  - target: "name"
    source: "name"
"#,
    )
    .expect("parse rule");
    let options = NormalizationOptions {
        max_html_nodes: 1,
        ..NormalizationOptions::default()
    };
    let err = normalize_records_with_options(
        &rule,
        InputData::Text(r#"<div><p class="item">Alice</p></div>"#),
        &options,
    )
    .expect_err("node limit should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn html_rejects_parser_created_node_limit_exceeded() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: html
  html:
    records_selector: ".item"
    fields:
      name:
        value: text
mappings:
  - target: "name"
    source: "name"
"#,
    )
    .expect("parse rule");
    let options = NormalizationOptions {
        max_html_nodes: 1,
        ..NormalizationOptions::default()
    };
    let err = normalize_records_with_options(
        &rule,
        InputData::Text(r#"<p class="item">Alice</p>"#),
        &options,
    )
    .expect_err("parsed DOM node limit should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn html_allows_literal_less_than_sequences_when_dom_nodes_within_limit() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: html
  html:
    records_selector: ".item"
    fields:
      name:
        selector: ".name"
        value: text
mappings:
  - target: "name"
    source: "name"
"#,
    )
    .expect("parse rule");
    let options = NormalizationOptions {
        max_html_nodes: 20,
        ..NormalizationOptions::default()
    };
    let literal_tags = "<article><aside><a><address><abbr><area><audio><bdi><bdo><base><button>";
    let input = format!(
        r#"<article class="item"><script>const sample = "{literal_tags}";</script><span class="name">Alice</span></article>"#
    );
    let output = normalize_records_with_options(&rule, InputData::Text(&input), &options)
        .expect("literal less-than sequences should not count as parsed DOM nodes")
        .collect::<Result<Vec<_>, _>>()
        .expect("records should normalize");
    assert_eq!(output, vec![serde_json::json!({ "name": "Alice" })]);
}

#[test]
fn html_rejects_invalid_selector() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: html
  html:
    records_selector: ".item["
    fields:
      name:
        value: text
mappings:
  - target: "name"
    source: "name"
"#,
    )
    .expect("parse rule");
    let err = transform(&rule, r#"<p class="item">Alice</p>"#, None)
        .expect_err("selector parse should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
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

#[test]
fn csv_normalization_rejects_too_many_records_while_iterating() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: csv
  csv:
    has_header: true
mappings:
  - target: "id"
    source: "id"
"#,
    )
    .expect("parse rule");
    let options = NormalizationOptions {
        max_records: 1,
        ..NormalizationOptions::default()
    };
    let mut records =
        normalize_records_with_options(&rule, InputData::Text("id\n1\n2\n"), &options)
            .expect("CSV iterator should be created before the second record is read");
    let first = records
        .next()
        .expect("first record should exist")
        .expect("first record should parse");
    assert_eq!(first, serde_json::json!({ "id": "1" }));
    let err = records
        .next()
        .expect("second record should report the record limit")
        .expect_err("record limit should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn json_records_path_rejects_too_many_records_before_materializing() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: json
  json:
    records_path: users
mappings:
  - target: "id"
    source: "id"
"#,
    )
    .expect("parse rule");
    let options = NormalizationOptions {
        max_records: 1,
        ..NormalizationOptions::default()
    };
    let err = normalize_records_with_options(
        &rule,
        InputData::Text(r#"{ "users": [{ "id": 1 }, { "id": 2 }] }"#),
        &options,
    )
    .expect_err("record limit should fail before records are materialized");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn json_records_path_rejects_single_object_when_record_limit_is_zero() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: json
  json:
    records_path: user
mappings:
  - target: "id"
    source: "id"
"#,
    )
    .expect("parse rule");
    let options = NormalizationOptions {
        max_records: 0,
        ..NormalizationOptions::default()
    };
    let err = normalize_records_with_options(
        &rule,
        InputData::Text(r#"{ "user": { "id": 1 } }"#),
        &options,
    )
    .expect_err("single object should still honor max_records");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn yaml_records_path_rejects_too_many_records_before_materializing() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: yaml
  yaml:
    records_path: users
mappings:
  - target: "id"
    source: "id"
"#,
    )
    .expect("parse rule");
    let options = NormalizationOptions {
        max_records: 1,
        ..NormalizationOptions::default()
    };
    let err = normalize_records_with_options(
        &rule,
        InputData::Text("users:\n  - id: 1\n  - id: 2\n"),
        &options,
    )
    .expect_err("record limit should fail before YAML records are materialized");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn toml_records_path_rejects_too_many_records_before_materializing() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: toml
  toml:
    records_path: users
mappings:
  - target: "id"
    source: "id"
"#,
    )
    .expect("parse rule");
    let options = NormalizationOptions {
        max_records: 1,
        ..NormalizationOptions::default()
    };
    let err = normalize_records_with_options(
        &rule,
        InputData::Text("[[users]]\nid = 1\n[[users]]\nid = 2\n"),
        &options,
    )
    .expect_err("record limit should fail before TOML records are materialized");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn csv_rejects_non_byte_delimiter() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: csv
  csv:
    has_header: true
    delimiter: "，"
mappings:
  - target: "id"
    source: "id"
"#,
    )
    .expect("parse rule");
    let err = normalize_records_with_options(
        &rule,
        InputData::Text("id，name\n1，Alice\n"),
        &NormalizationOptions::default(),
    )
    .expect_err("non-byte delimiter should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn yaml_input_uses_records_path() {
    let yaml = r#"
version: 2
input:
  format: yaml
  yaml:
    records_path: users
mappings:
  - target: "id"
    source: "id"
  - target: "name"
    source: "name"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let input = "users:\n  - id: 1\n    name: Alice\n";
    let output = transform(&rule, input, None).expect("transform");
    assert_eq!(output, serde_json::json!([{ "id": 1, "name": "Alice" }]));
}

#[test]
fn yaml_rejects_non_string_mapping_key() {
    let yaml = r#"
version: 2
input:
  format: yaml
  yaml: {}
mappings:
  - target: "value"
    source: "value"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let err = transform(&rule, "1: value\n", None).expect_err("non-string key should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn yaml_rejects_trailing_document() {
    let yaml = r#"
version: 2
input:
  format: yaml
  yaml:
    records_path: users
mappings:
  - target: "id"
    source: "id"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let err = transform(&rule, "users:\n  - id: 1\n---\nusers:\n  - id: 2\n", None)
        .expect_err("multi-document YAML should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

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

#[test]
fn toml_datetime_is_string() {
    let yaml = r#"
version: 2
input:
  format: toml
  toml:
    records_path: users
mappings:
  - target: "created_at"
    source: "created_at"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let input = "[[users]]\ncreated_at = 2026-05-08T12:00:00Z\n";
    let output = transform(&rule, input, None).expect("transform");
    assert_eq!(
        output,
        serde_json::json!([{ "created_at": "2026-05-08T12:00:00Z" }])
    );
}

#[test]
fn toml_quoted_private_datetime_key_stays_object() {
    let yaml = r#"
version: 2
input:
  format: toml
  toml:
    records_path: users
mappings:
  - target: "metadata"
    source: "metadata"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let input = "[[users]]\n[users.metadata]\n'$__toml_private_datetime' = 2026-05-08T12:00:00Z\n";
    let records = normalize_records_with_options(
        &rule,
        InputData::Text(input),
        &NormalizationOptions::default(),
    )
    .expect("normalize toml")
    .collect::<Result<Vec<_>, _>>()
    .expect("normalized records");
    assert_eq!(
        records,
        vec![serde_json::json!({
            "metadata": { "$__toml_private_datetime": "2026-05-08T12:00:00Z" }
        })]
    );
}

#[test]
fn toml_rejects_depth_limit_exceeded() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: toml
  toml: {}
mappings:
  - target: "value"
    source: "a.b.c.value"
"#,
    )
    .expect("parse rule");
    let options = NormalizationOptions {
        max_depth: 2,
        ..NormalizationOptions::default()
    };
    let err =
        normalize_records_with_options(&rule, InputData::Text("[a.b.c]\nvalue = 1\n"), &options)
            .expect_err("depth limit should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn toml_allows_nested_table_at_equivalent_depth_limit() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: toml
  toml: {}
mappings:
  - target: "value"
    source: "a.b.c.value"
"#,
    )
    .expect("parse rule");
    let options = NormalizationOptions {
        max_depth: 4,
        ..NormalizationOptions::default()
    };
    let mut records =
        normalize_records_with_options(&rule, InputData::Text("[a.b.c]\nvalue = 1\n"), &options)
            .expect("nested TOML table should fit within equivalent JSON depth");
    let record = records.next().expect("record").expect("record ok");
    assert_eq!(
        record,
        serde_json::json!({ "a": { "b": { "c": { "value": 1 } } } })
    );
}

#[test]
fn toml_allows_inline_table_at_equivalent_depth_limit() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: toml
  toml: {}
mappings:
  - target: "value"
    source: "record.value"
"#,
    )
    .expect("parse rule");
    let options = NormalizationOptions {
        max_depth: 2,
        ..NormalizationOptions::default()
    };
    let mut records = normalize_records_with_options(
        &rule,
        InputData::Text("record = { value = 1 }\n"),
        &options,
    )
    .expect("inline TOML table should fit within equivalent JSON depth");
    let record = records.next().expect("record").expect("record ok");
    assert_eq!(record, serde_json::json!({ "record": { "value": 1 } }));
}

#[test]
fn toml_rejects_nested_inline_table_over_depth_limit() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: toml
  toml: {}
mappings:
  - target: "value"
    source: "record.inner.value"
"#,
    )
    .expect("parse rule");
    let options = NormalizationOptions {
        max_depth: 2,
        ..NormalizationOptions::default()
    };
    let err = normalize_records_with_options(
        &rule,
        InputData::Text("record = { inner = { value = 1 } }\n"),
        &options,
    )
    .expect_err("nested inline table should exceed depth limit");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn toml_rejects_array_limit_exceeded() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: toml
  toml: {}
mappings:
  - target: "values"
    source: "values"
"#,
    )
    .expect("parse rule");
    let options = NormalizationOptions {
        max_array_len: 1,
        ..NormalizationOptions::default()
    };
    let err = normalize_records_with_options(&rule, InputData::Text("values = [1, 2]\n"), &options)
        .expect_err("array limit should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn toml_allows_array_at_limit() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: toml
  toml: {}
mappings:
  - target: "values"
    source: "values"
"#,
    )
    .expect("parse rule");
    let options = NormalizationOptions {
        max_array_len: 1,
        ..NormalizationOptions::default()
    };
    let mut records =
        normalize_records_with_options(&rule, InputData::Text("values = [1]\n"), &options)
            .expect("array at limit should pass");
    let record = records.next().expect("record").expect("record ok");
    assert_eq!(record, serde_json::json!({ "values": [1] }));
}

#[test]
fn yaml_rejects_text_limit_during_parse() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: yaml
  yaml: {}
mappings:
  - target: "name"
    source: "name"
"#,
    )
    .expect("parse rule");
    let options = NormalizationOptions {
        max_text_bytes: 3,
        ..NormalizationOptions::default()
    };
    let err = normalize_records_with_options(&rule, InputData::Text("name: Alice\n"), &options)
        .expect_err("text limit should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn yaml_allows_array_at_limit() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: yaml
  yaml: {}
mappings:
  - target: "values"
    source: "values"
"#,
    )
    .expect("parse rule");
    let options = NormalizationOptions {
        max_array_len: 1,
        ..NormalizationOptions::default()
    };
    let mut records =
        normalize_records_with_options(&rule, InputData::Text("values: [1]\n"), &options)
            .expect("array at limit should pass");
    let record = records.next().expect("record").expect("record ok");
    assert_eq!(record, serde_json::json!({ "values": [1] }));
}

#[test]
fn t03_json_out_context() {
    let base = fixtures_dir().join("t03_json_out_context");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let context = load_optional_json(&base.join("context.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, context.as_ref()).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn t04_json_root_coalesce_default() {
    assert_json_fixture("t04_json_root_coalesce_default");
}

#[test]
fn t05_expr_transforms() {
    assert_json_fixture("t05_expr_transforms");
}

#[test]
fn t06_lookup_context() {
    let base = fixtures_dir().join("t06_lookup_context");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let context = load_optional_json(&base.join("context.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, context.as_ref()).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn t07_array_index_paths() {
    let base = fixtures_dir().join("t07_array_index_paths");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let context = load_optional_json(&base.join("context.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, context.as_ref()).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn t08_escaped_keys() {
    assert_json_fixture("t08_escaped_keys");
}

#[test]
fn t09_when_mapping() {
    assert_json_fixture("t09_when_mapping");
}

#[test]
fn t10_when_compare() {
    assert_json_fixture("t10_when_compare");
}

#[test]
fn t11_when_logical_ops() {
    assert_json_fixture("t11_when_logical_ops");
}

#[test]
fn t13_expr_extended() {
    assert_json_fixture("t13_expr_extended");
}

#[test]
fn t14_expr_chain() {
    assert_json_fixture("t14_expr_chain");
}

#[test]
fn t15_record_when() {
    assert_json_fixture("t15_record_when");
}

#[test]
fn t16_array_ops() {
    assert_json_fixture("t16_array_ops");
}

#[test]
fn t17_json_ops_merge() {
    assert_json_fixture("t17_json_ops_merge");
}

#[test]
fn t18_json_ops_deep_merge() {
    assert_json_fixture("t18_json_ops_deep_merge");
}

#[test]
fn t19_json_ops_pick() {
    assert_json_fixture("t19_json_ops_pick");
}

#[test]
fn t20_json_ops_omit() {
    assert_json_fixture("t20_json_ops_omit");
}

#[test]
fn t21_json_ops_keys_values_entries() {
    assert_json_fixture("t21_json_ops_keys_values_entries");
}

#[test]
fn t22_json_ops_object_flatten() {
    assert_json_fixture("t22_json_ops_object_flatten");
}

#[test]
fn t23_json_ops_object_unflatten() {
    assert_json_fixture("t23_json_ops_object_unflatten");
}

#[test]
fn t24_json_ops_missing() {
    assert_json_fixture("t24_json_ops_missing");
}

#[test]
fn t25_json_ops_get_chain() {
    let base = fixtures_dir().join("t25_json_ops_get_chain");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let context = load_optional_json(&base.join("context.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, context.as_ref()).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn t26_chain_all_ops() {
    assert_json_fixture("t26_chain_all_ops");
}

#[test]
fn t27_json_ops_from_entries() {
    assert_json_fixture("t27_json_ops_from_entries");
}

#[test]
fn t28_expr_chain_nested() {
    assert_json_fixture("t28_expr_chain_nested");
}

#[test]
fn t29_json_ops_len() {
    assert_json_fixture("t29_json_ops_len");
}

#[test]
fn r01_float_non_finite() {
    assert_transform_error_fixture("r01_float_non_finite");
}

#[test]
fn r02_json_ops_invalid_path_pick() {
    assert_transform_error_fixture("r02_json_ops_invalid_path_pick");
}

#[test]
fn r03_json_ops_non_object() {
    assert_transform_error_fixture("r03_json_ops_non_object");
}

#[test]
fn r04_json_ops_null_arg() {
    assert_transform_error_fixture("r04_json_ops_null_arg");
}

#[test]
fn r05_json_ops_unflatten_array_index() {
    assert_transform_error_fixture("r05_json_ops_unflatten_array_index");
}

#[test]
fn r06_json_ops_flatten_brackets() {
    assert_transform_error_fixture("r06_json_ops_flatten_brackets");
}

#[test]
fn r07_json_ops_flatten_empty_key() {
    assert_transform_error_fixture("r07_json_ops_flatten_empty_key");
}

#[test]
fn r08_json_ops_from_entries_single_pair() {
    assert_transform_error_fixture("r08_json_ops_from_entries_single_pair");
}

#[test]
fn r09_asserts_failed() {
    assert_transform_error_fixture("r09_asserts_failed");
}

// =============================================================================
// v2 Golden Tests (T22-T27)
// =============================================================================

include!("transform_golden/v2.rs");
