use std::fs;

use rulemorph::{
    ErrorCode, RuleFormat, parse_rule_file, parse_rule_file_with_format, validate_rule_file,
    validate_rule_file_with_source,
};

mod common;

use common::validation::{fixtures_dir, load_expected_errors, load_rule, normalize_errors};

#[test]
fn valid_rules_should_pass_validation() {
    let cases = [
        "t01_csv_basic",
        "t02_csv_no_header",
        "t03_json_out_context",
        "t04_json_root_coalesce_default",
        "t05_expr_transforms",
        "t06_lookup_context",
        "t07_array_index_paths",
        "t08_escaped_keys",
        "t09_when_mapping",
        "t10_when_compare",
        "t11_when_logical_ops",
        "t13_expr_extended",
        "t14_expr_chain",
        "t15_record_when",
        "t16_array_ops",
        "t17_json_ops_merge",
        "t18_json_ops_deep_merge",
        "t19_json_ops_pick",
        "t20_json_ops_omit",
        "t21_json_ops_keys_values_entries",
        "t22_json_ops_object_flatten",
        "t23_json_ops_object_unflatten",
        "t24_json_ops_missing",
        "t25_json_ops_get_chain",
        "t26_chain_all_ops",
        "t27_json_ops_from_entries",
        "t28_expr_chain_nested",
        "t29_json_ops_len",
        "t31_yaml_input",
        "t32_toml_input",
        "t33_xml_input",
        "t34_excel_input",
        "t35_html_input",
    ];

    for case in cases {
        let rule = load_rule(case);
        if let Err(errors) = validate_rule_file(&rule) {
            let codes: Vec<&'static str> = errors.iter().map(|e| e.code.as_str()).collect();
            panic!("expected valid rules for {}, got {:?}", case, codes);
        }
    }
}

#[test]
fn invalid_rules_should_match_expected_errors() {
    let cases = [
        "v01_missing_mapping_value",
        "v02_duplicate_target",
        "v03_invalid_ref_namespace",
        "v04_forward_out_reference",
        "v05_unknown_op",
        "v06_invalid_delimiter_length",
        "v07_invalid_lookup_args",
        "v08_invalid_path",
        "v09_invalid_when_type",
        "v10_invalid_record_when_type",
        "v11_invalid_item_ref",
    ];

    for case in cases {
        let rule = load_rule(case);
        let expected = load_expected_errors(case);
        let errors = validate_rule_file(&rule).unwrap_err();
        let actual = normalize_errors(errors);
        assert_eq!(actual, expected, "error mismatch for fixture {}", case);
    }
}

#[test]
fn invalid_rules_report_error_codes() {
    let rule = load_rule("v01_missing_mapping_value");
    let errors = validate_rule_file(&rule).unwrap_err();
    let codes: Vec<ErrorCode> = errors.iter().map(|e| e.code.clone()).collect();
    assert!(codes.contains(&ErrorCode::MissingMappingValue));
}

#[test]
fn extended_input_sections_are_validated() {
    let yaml = r#"
version: 2
input:
  format: yaml
mappings:
  - target: "id"
    source: "id"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let errors = validate_rule_file(&rule).expect_err("missing yaml section should fail");
    assert!(
        errors
            .iter()
            .any(|err| err.code.as_str() == "MissingYamlSection")
    );
}

#[test]
fn html_attr_value_requires_attr_name() {
    let yaml = r#"
version: 2
input:
  format: html
  html:
    records_selector: ".item"
    fields:
      url:
        selector: "a"
        value: attr
mappings:
  - target: "url"
    source: "url"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let errors = validate_rule_file(&rule).expect_err("missing attr should fail");
    assert!(
        errors
            .iter()
            .any(|err| err.path.as_deref() == Some("input.html.fields.url.attr"))
    );
}

#[test]
fn excel_without_headers_requires_columns() {
    let yaml = r#"
version: 2
input:
  format: excel
  excel:
    has_header: false
mappings:
  - target: "id"
    source: "id"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let errors = validate_rule_file(&rule).expect_err("missing columns should fail");
    assert!(
        errors
            .iter()
            .any(|err| err.code.as_str() == "MissingExcelColumns"
                && err.path.as_deref() == Some("input.excel.columns"))
    );
}

#[test]
fn csv_columns_must_be_non_empty_and_unique() {
    let yaml = r#"
version: 2
input:
  format: csv
  csv:
    has_header: false
    columns:
      - name: ""
      - name: id
      - name: id
mappings:
  - target: "id"
    source: "id"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let errors = validate_rule_file(&rule).expect_err("invalid columns should fail");
    assert!(
        errors
            .iter()
            .any(|err| err.code.as_str() == "InvalidInputOption"
                && err.path.as_deref() == Some("input.csv.columns[0].name"))
    );
    assert!(
        errors
            .iter()
            .any(|err| err.code.as_str() == "DuplicateInputField"
                && err.path.as_deref() == Some("input.csv.columns[2].name"))
    );
}

#[test]
fn csv_without_header_rejects_empty_columns() {
    let yaml = r#"
version: 2
input:
  format: csv
  csv:
    has_header: false
    columns: []
mappings:
  - target: "id"
    source: "id"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let errors = validate_rule_file(&rule).expect_err("empty columns should fail");
    assert!(
        errors
            .iter()
            .any(|err| err.code.as_str() == "MissingCsvColumns"
                && err.path.as_deref() == Some("input.csv.columns"))
    );
}

#[test]
fn csv_rejects_multibyte_delimiter() {
    let yaml = r#"
version: 2
input:
  format: csv
  csv:
    delimiter: "，"
mappings:
  - target: "id"
    source: "id"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let errors = validate_rule_file(&rule).expect_err("multibyte delimiter should fail");
    assert!(
        errors
            .iter()
            .any(|err| err.code == ErrorCode::InvalidDelimiterLength
                && err.path.as_deref() == Some("input.csv.delimiter"))
    );
}

#[test]
fn unselected_input_sections_are_ignored_by_normal_validation() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
  html:
    records_selector: ""
    fields: {}
mappings:
  - target: "id"
    source: "id"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    validate_rule_file(&rule).expect("unselected html section should be ignored");
}

#[test]
fn xml_records_path_rejects_non_element_path_syntax() {
    let yaml = r#"
version: 2
input:
  format: xml
  xml:
    records_path: "users/user"
mappings:
  - target: "id"
    source: "id"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let errors = validate_rule_file(&rule).expect_err("invalid XML path should fail");
    assert!(errors.iter().any(|err| err.code.as_str() == "InvalidPath"
        && err.path.as_deref() == Some("input.xml.records_path")));
}

#[test]
fn xml_records_path_accepts_non_ascii_element_names() {
    let yaml = r##"
version: 2
input:
  format: xml
  xml:
    records_path: 利用者.名前
mappings:
  - target: "name"
    source: "#text"
"##;
    let rule = parse_rule_file(yaml).expect("parse rule");
    validate_rule_file(&rule).expect("valid Unicode XML names should pass validation");
}

include!("validation/rule_format.rs");

// =============================================================================
// v2 Validation Tests
// =============================================================================

include!("validation/v2.rs");
