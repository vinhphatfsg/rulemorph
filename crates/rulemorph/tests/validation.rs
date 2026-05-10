use std::fs;
use std::path::{Path, PathBuf};

use rulemorph::{
    ErrorCode, RuleError, RuleFormat, parse_rule_file, parse_rule_file_with_format,
    validate_rule_file, validate_rule_file_with_source,
};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct ExpectedError {
    code: String,
    path: Option<String>,
}

fn fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
}

fn load_rule(case: &str) -> rulemorph::RuleFile {
    let rules_path = fixtures_dir().join(case).join("rules.yaml");
    let yaml = fs::read_to_string(&rules_path)
        .unwrap_or_else(|_| panic!("failed to read {}", rules_path.display()));
    parse_rule_file(&yaml)
        .unwrap_or_else(|err| panic!("failed to parse YAML {}: {}", rules_path.display(), err))
}

fn load_expected_errors(case: &str) -> Vec<ExpectedError> {
    let errors_path = fixtures_dir().join(case).join("expected_errors.json");
    let json = fs::read_to_string(&errors_path)
        .unwrap_or_else(|_| panic!("failed to read {}", errors_path.display()));
    serde_json::from_str(&json).unwrap_or_else(|err| {
        panic!(
            "failed to parse expected errors {}: {}",
            errors_path.display(),
            err
        )
    })
}

fn normalize_errors(errors: Vec<RuleError>) -> Vec<(String, Option<String>)> {
    let mut normalized: Vec<(String, Option<String>)> = errors
        .into_iter()
        .map(|err| (err.code.as_str().to_string(), err.path))
        .collect();
    normalized.sort();
    normalized
}

fn normalize_expected(errors: Vec<ExpectedError>) -> Vec<(String, Option<String>)> {
    let mut normalized: Vec<(String, Option<String>)> =
        errors.into_iter().map(|err| (err.code, err.path)).collect();
    normalized.sort();
    normalized
}

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
        let expected = normalize_expected(load_expected_errors(case));
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

#[test]
fn parse_json_rule_file_with_explicit_format() {
    let source = r#"{
      "version": 2,
      "input": { "format": "json", "json": { "records_path": "items" } },
      "mappings": [{ "target": "id", "source": "id" }]
    }"#;
    let rule = parse_rule_file_with_format(source, RuleFormat::Json).expect("parse json rule");
    assert_eq!(rule.version, 2);
}

#[test]
fn json_rule_file_fixture_should_pass_validation() {
    let rules_path = fixtures_dir().join("t30_json_rule_file").join("rules.json");
    let source = fs::read_to_string(&rules_path)
        .unwrap_or_else(|_| panic!("failed to read {}", rules_path.display()));
    let rule = parse_rule_file_with_format(&source, RuleFormat::Json).expect("parse json rule");
    validate_rule_file(&rule).expect("json rule fixture should validate");
}

#[test]
fn json_rule_rejects_duplicate_key() {
    let source = r#"{
      "version": 2,
      "version": 1,
      "input": { "format": "json", "json": {} },
      "mappings": []
    }"#;
    let err = parse_rule_file_with_format(source, RuleFormat::Json)
        .expect_err("duplicate JSON keys must fail");
    assert!(err.message.contains("duplicate key"));
}

#[test]
fn json_rule_rejects_trailing_comma() {
    let source = r#"{ "version": 2, }"#;
    let err =
        parse_rule_file_with_format(source, RuleFormat::Json).expect_err("trailing comma fails");
    assert!(err.message.contains("trailing comma") || err.message.contains("expected"));
}

#[test]
fn json_rule_rejects_trailing_garbage() {
    let source = r#"{
      "version": 2,
      "input": { "format": "json", "json": {} },
      "mappings": []
    } trailing"#;
    let err =
        parse_rule_file_with_format(source, RuleFormat::Json).expect_err("trailing garbage fails");
    assert!(err.message.contains("trailing characters") || err.message.contains("expected"));
}

#[test]
fn yaml_rule_rejects_duplicate_key() {
    let source = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: id
    source: id
    source: other_id
"#;
    let err = parse_rule_file_with_format(source, RuleFormat::Yaml)
        .expect_err("duplicate YAML keys must fail");
    assert!(err.message.contains("duplicate key"));
    assert!(err.line_column().is_some());
}

#[test]
fn yaml_rule_parse_error_preserves_location() {
    let source = "version: 2\ninput: [\n";
    let err = parse_rule_file(source).expect_err("malformed YAML must fail");
    assert!(err.location().is_some());

    let err = parse_rule_file_with_format(source, RuleFormat::Yaml)
        .expect_err("malformed YAML must fail");
    assert!(err.line_column().is_some());
}

#[test]
fn yaml_rule_rejects_trailing_document() {
    let source = r#"
version: 2
input:
  format: csv
  csv:
    has_header: true
mappings:
  - target: "id"
    source: "id"
---
version: 2
"#;
    let err = parse_rule_file(source).expect_err("trailing YAML document must fail");
    assert!(err.to_string().contains("exactly one document"));
}

#[test]
fn validation_errors_include_location_with_source() {
    let rules_path = fixtures_dir()
        .join("v01_missing_mapping_value")
        .join("rules.yaml");
    let yaml = fs::read_to_string(&rules_path)
        .unwrap_or_else(|_| panic!("failed to read {}", rules_path.display()));
    let rule = parse_rule_file(&yaml).unwrap();
    let errors = validate_rule_file_with_source(&rule, &yaml).unwrap_err();
    let error = errors
        .iter()
        .find(|err| err.code == ErrorCode::MissingMappingValue)
        .expect("expected MissingMappingValue");
    let location = error.location.clone().expect("expected location");
    assert_eq!(location.line, 7);
}

// =============================================================================
// v2 Validation Tests
// =============================================================================

#[test]
fn v2_valid_rules_should_pass_validation() {
    let cases = [
        "tv22_basic",
        "tv23_steps",
        "tv24_conditions",
        "tv25_lookup",
        "tv27_v1_compat",
        "tv28_map_let_binding",
        "tv29_v2_out_sibling_ok",
        "tv30_literal_escape",
        "tv36_branch_uses_out",
        "tv39_finalize_filter_index",
        "tv41_branch_return_out_update",
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
fn v2_invalid_rules_should_fail_validation() {
    let cases = [
        "tv26_v01_unknown_op",
        "tv26_v03_literal_start_unknown_op",
        "tv26_v04_empty_pipe",
        "tv26_v05_branch_when_v1_non_bool",
        "tv43_finalize_wrap_invalid_expr",
    ];

    for case in cases {
        let rule = load_rule(case);
        let expected = normalize_expected(load_expected_errors(case));
        let errors = validate_rule_file(&rule).unwrap_err();
        let actual = normalize_errors(errors);
        assert_eq!(actual, expected, "error mismatch for {}", case);
    }
}

#[test]
fn v2_forward_out_ref_should_fail_validation() {
    // tv26_v02_forward_out_ref should fail with ForwardOutReference error
    let rule = load_rule("tv26_v02_forward_out_ref");
    let expected = normalize_expected(load_expected_errors("tv26_v02_forward_out_ref"));
    let errors = validate_rule_file(&rule).unwrap_err();
    let actual = normalize_errors(errors);
    assert_eq!(
        actual, expected,
        "error mismatch for tv26_v02_forward_out_ref"
    );
}
