use std::fs;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use rulemorph::{
    parse_rule_file, validate_rule_file, validate_rule_file_with_source, ErrorCode, RuleError,
};

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
    parse_rule_file(&yaml).unwrap_or_else(|err| {
        panic!("failed to parse YAML {}: {}", rules_path.display(), err)
    })
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
    let mut normalized: Vec<(String, Option<String>)> = errors
        .into_iter()
        .map(|err| (err.code, err.path))
        .collect();
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
        assert_eq!(
            actual, expected,
            "error mismatch for fixture {}",
            case
        );
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
    let location = error
        .location
        .clone()
        .expect("expected location");
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
    ];

    for case in cases {
        let rule = load_rule(case);
        let expected = normalize_expected(load_expected_errors(case));
        let errors = validate_rule_file(&rule).unwrap_err();
        let actual = normalize_errors(errors);
        assert_eq!(
            actual, expected,
            "error mismatch for {}",
            case
        );
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
