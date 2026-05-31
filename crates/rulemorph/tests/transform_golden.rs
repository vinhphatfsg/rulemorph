use std::fs;

#[cfg(feature = "excel")]
use rulemorph::preflight_validate_input;
use rulemorph::{
    InputData, NormalizationOptions, RuleFormat, TransformErrorKind,
    normalize_records_with_options, parse_rule_file, transform, transform_input,
};
mod common;

#[cfg(feature = "excel")]
use common::golden::assert_xlsx_fixture;
use common::golden::{
    assert_json_fixture, assert_text_fixture, assert_transform_error_fixture, fixtures_dir,
    load_json, load_optional_json, load_rule, load_rule_with_format,
};
#[cfg(feature = "excel")]
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

include!("transform_golden/input_formats.rs");

#[cfg(feature = "excel")]
include!("transform_golden/excel.rs");

include!("transform_golden/xml.rs");
#[cfg(feature = "html")]
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

include!("transform_golden/json_ops.rs");

#[test]
fn r01_float_non_finite() {
    assert_transform_error_fixture("r01_float_non_finite");
}

#[test]
fn r09_asserts_failed() {
    assert_transform_error_fixture("r09_asserts_failed");
}

// =============================================================================
// v2 Golden Tests (T22-T27)
// =============================================================================

include!("transform_golden/v2.rs");
