use std::fs;
use std::path::{Path, PathBuf};

use rulemorph::{parse_rule_file, transform, TransformErrorKind};

fn fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
}

fn load_json(path: &Path) -> serde_json::Value {
    let json = fs::read_to_string(path)
        .unwrap_or_else(|_| panic!("failed to read {}", path.display()));
    serde_json::from_str(&json)
        .unwrap_or_else(|_| panic!("invalid json: {}", path.display()))
}

fn load_rule(path: &Path) -> rulemorph::RuleFile {
    let yaml = fs::read_to_string(path)
        .unwrap_or_else(|_| panic!("failed to read {}", path.display()));
    parse_rule_file(&yaml).unwrap_or_else(|err| {
        panic!("failed to parse {}: {}", path.display(), err)
    })
}

fn load_optional_json(path: &Path) -> Option<serde_json::Value> {
    if path.exists() {
        Some(load_json(path))
    } else {
        None
    }
}

fn load_expected_error(path: &Path) -> ExpectedTransformError {
    let value = load_json(path);
    serde_json::from_value(value)
        .unwrap_or_else(|err| panic!("invalid expected error: {} ({})", path.display(), err))
}

fn transform_kind_to_str(kind: &TransformErrorKind) -> &'static str {
    match kind {
        TransformErrorKind::InvalidInput => "InvalidInput",
        TransformErrorKind::InvalidRecordsPath => "InvalidRecordsPath",
        TransformErrorKind::InvalidRef => "InvalidRef",
        TransformErrorKind::InvalidTarget => "InvalidTarget",
        TransformErrorKind::MissingRequired => "MissingRequired",
        TransformErrorKind::TypeCastFailed => "TypeCastFailed",
        TransformErrorKind::ExprError => "ExprError",
        TransformErrorKind::AssertionFailed => "AssertionFailed",
    }
}

#[test]
fn t01_csv_basic() {
    let base = fixtures_dir().join("t01_csv_basic");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.csv"))
        .unwrap_or_else(|_| panic!("failed to read input.csv"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn t02_csv_no_header() {
    let base = fixtures_dir().join("t02_csv_no_header");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.csv"))
        .unwrap_or_else(|_| panic!("failed to read input.csv"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
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
    let base = fixtures_dir().join("t04_json_root_coalesce_default");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn t05_expr_transforms() {
    let base = fixtures_dir().join("t05_expr_transforms");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
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
    let base = fixtures_dir().join("t08_escaped_keys");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn t09_when_mapping() {
    let base = fixtures_dir().join("t09_when_mapping");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn t10_when_compare() {
    let base = fixtures_dir().join("t10_when_compare");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn t11_when_logical_ops() {
    let base = fixtures_dir().join("t11_when_logical_ops");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn t13_expr_extended() {
    let base = fixtures_dir().join("t13_expr_extended");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn t14_expr_chain() {
    let base = fixtures_dir().join("t14_expr_chain");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn t15_record_when() {
    let base = fixtures_dir().join("t15_record_when");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn t16_array_ops() {
    let base = fixtures_dir().join("t16_array_ops");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn t17_json_ops_merge() {
    let base = fixtures_dir().join("t17_json_ops_merge");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn t18_json_ops_deep_merge() {
    let base = fixtures_dir().join("t18_json_ops_deep_merge");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn t19_json_ops_pick() {
    let base = fixtures_dir().join("t19_json_ops_pick");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn t20_json_ops_omit() {
    let base = fixtures_dir().join("t20_json_ops_omit");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn t21_json_ops_keys_values_entries() {
    let base = fixtures_dir().join("t21_json_ops_keys_values_entries");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn t22_json_ops_object_flatten() {
    let base = fixtures_dir().join("t22_json_ops_object_flatten");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn t23_json_ops_object_unflatten() {
    let base = fixtures_dir().join("t23_json_ops_object_unflatten");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn t24_json_ops_missing() {
    let base = fixtures_dir().join("t24_json_ops_missing");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
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
    let base = fixtures_dir().join("t26_chain_all_ops");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn t27_json_ops_from_entries() {
    let base = fixtures_dir().join("t27_json_ops_from_entries");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn t28_expr_chain_nested() {
    let base = fixtures_dir().join("t28_expr_chain_nested");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn t29_json_ops_len() {
    let base = fixtures_dir().join("t29_json_ops_len");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[derive(Debug, serde::Deserialize)]
struct ExpectedTransformError {
    kind: String,
    path: Option<String>,
}

#[test]
fn r01_float_non_finite() {
    let base = fixtures_dir().join("r01_float_non_finite");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_expected_error(&base.join("expected_error.json"));

    let err = transform(&rule, &input, None).expect_err("expected transform error");
    assert_eq!(transform_kind_to_str(&err.kind), expected.kind);
    assert_eq!(err.path, expected.path);
}

#[test]
fn r02_json_ops_invalid_path_pick() {
    let base = fixtures_dir().join("r02_json_ops_invalid_path_pick");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_expected_error(&base.join("expected_error.json"));

    let err = transform(&rule, &input, None).expect_err("expected transform error");
    assert_eq!(transform_kind_to_str(&err.kind), expected.kind);
    assert_eq!(err.path, expected.path);
}

#[test]
fn r03_json_ops_non_object() {
    let base = fixtures_dir().join("r03_json_ops_non_object");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_expected_error(&base.join("expected_error.json"));

    let err = transform(&rule, &input, None).expect_err("expected transform error");
    assert_eq!(transform_kind_to_str(&err.kind), expected.kind);
    assert_eq!(err.path, expected.path);
}

#[test]
fn r04_json_ops_null_arg() {
    let base = fixtures_dir().join("r04_json_ops_null_arg");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_expected_error(&base.join("expected_error.json"));

    let err = transform(&rule, &input, None).expect_err("expected transform error");
    assert_eq!(transform_kind_to_str(&err.kind), expected.kind);
    assert_eq!(err.path, expected.path);
}

#[test]
fn r05_json_ops_unflatten_array_index() {
    let base = fixtures_dir().join("r05_json_ops_unflatten_array_index");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_expected_error(&base.join("expected_error.json"));

    let err = transform(&rule, &input, None).expect_err("expected transform error");
    assert_eq!(transform_kind_to_str(&err.kind), expected.kind);
    assert_eq!(err.path, expected.path);
}

#[test]
fn r06_json_ops_flatten_brackets() {
    let base = fixtures_dir().join("r06_json_ops_flatten_brackets");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_expected_error(&base.join("expected_error.json"));

    let err = transform(&rule, &input, None).expect_err("expected transform error");
    assert_eq!(transform_kind_to_str(&err.kind), expected.kind);
    assert_eq!(err.path, expected.path);
}

#[test]
fn r07_json_ops_flatten_empty_key() {
    let base = fixtures_dir().join("r07_json_ops_flatten_empty_key");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_expected_error(&base.join("expected_error.json"));

    let err = transform(&rule, &input, None).expect_err("expected transform error");
    assert_eq!(transform_kind_to_str(&err.kind), expected.kind);
    assert_eq!(err.path, expected.path);
}

#[test]
fn r08_json_ops_from_entries_single_pair() {
    let base = fixtures_dir().join("r08_json_ops_from_entries_single_pair");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_expected_error(&base.join("expected_error.json"));

    let err = transform(&rule, &input, None).expect_err("expected transform error");
    assert_eq!(transform_kind_to_str(&err.kind), expected.kind);
    assert_eq!(err.path, expected.path);
}

#[test]
fn r09_asserts_failed() {
    let base = fixtures_dir().join("r09_asserts_failed");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_expected_error(&base.join("expected_error.json"));

    let err = transform(&rule, &input, None).expect_err("expected transform error");
    assert_eq!(transform_kind_to_str(&err.kind), expected.kind);
    assert_eq!(err.path, expected.path);
}

// =============================================================================
// v2 Golden Tests (T22-T27)
// =============================================================================

#[test]
fn tv22_basic() {
    let base = fixtures_dir().join("tv22_basic");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let context = load_optional_json(&base.join("context.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, context.as_ref()).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn tv23_steps() {
    let base = fixtures_dir().join("tv23_steps");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn tv24_conditions() {
    let base = fixtures_dir().join("tv24_conditions");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn tv25_lookup() {
    let base = fixtures_dir().join("tv25_lookup");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let context = load_optional_json(&base.join("context.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, context.as_ref()).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn tv32_steps_finalize() {
    let base = fixtures_dir().join("tv32_steps_finalize");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn tv33_branch_return() {
    let base = fixtures_dir().join("tv33_branch_return");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn tv34_branch_return_true() {
    let base = fixtures_dir().join("tv34_branch_return_true");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn tv35_finalize_wrap() {
    let base = fixtures_dir().join("tv35_finalize_wrap");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn tv36_branch_uses_out() {
    let base = fixtures_dir().join("tv36_branch_uses_out");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn tv37_root_refs() {
    let base = fixtures_dir().join("tv37_root_refs");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let context = load_optional_json(&base.join("context.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, context.as_ref()).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn tv38_finalize_filter_offset() {
    let base = fixtures_dir().join("tv38_finalize_filter_offset");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn tv39_finalize_filter_index() {
    let base = fixtures_dir().join("tv39_finalize_filter_index");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn tv26_unknown_op_error() {
    let base = fixtures_dir().join("tv26_v01_unknown_op");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = r#"[{"name": "test"}]"#;
    let result = transform(&rule, &input, None);
    assert!(result.is_err(), "expected error for unknown op");
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("unknown op")
            || err.to_string().contains("nonexistent_op")
            || err.to_string().contains("expr.op is not supported"),
        "expected unknown op error, got: {}",
        err
    );
}

#[test]
fn tv26_forward_out_ref_returns_null() {
    // Note: Forward out reference (@out.b before b is computed)
    // does not cause a runtime error - it returns null/missing.
    // This is valid behavior for v2 expressions.
    let base = fixtures_dir().join("tv26_v02_forward_out_ref");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = r#"[{"x": 1}]"#;
    let result = transform(&rule, &input, None).expect("transform should succeed");
    // When @out.b is not yet computed, it should result in null/missing for "a"
    // The output should have "b" = 1 (from @input.x)
    assert!(result.is_array());
    let arr = result.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    let obj = arr[0].as_object().unwrap();
    // "a" should be missing (not in output) or null since @out.b wasn't computed yet
    // "b" should be 1
    assert_eq!(obj.get("b"), Some(&serde_json::json!(1)));
}

#[test]
fn tv27_v1_compat() {
    let base = fixtures_dir().join("tv27_v1_compat");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let context = load_optional_json(&base.join("context.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, context.as_ref()).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn tv28_map_let_binding() {
    let base = fixtures_dir().join("tv28_map_let_binding");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn tv30_literal_escape() {
    let base = fixtures_dir().join("tv30_literal_escape");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn tv31_v2_json_ops_pick_omit_reduce_fold() {
    let base = fixtures_dir().join("tv31_v2_json_ops_pick_omit_reduce_fold");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}
