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

include!("transform_golden/excel.rs");

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

include!("transform_golden/xml.rs");
include!("transform_golden/html.rs");

include!("transform_golden/structured_inputs.rs");

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
