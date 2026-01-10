use std::fs;
use std::path::{Path, PathBuf};

use transform_rules::{parse_rule_file, preflight_validate, TransformErrorKind};

fn fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
}

fn load_rule(path: &Path) -> transform_rules::RuleFile {
    let yaml = fs::read_to_string(path)
        .unwrap_or_else(|_| panic!("failed to read {}", path.display()));
    parse_rule_file(&yaml).unwrap_or_else(|err| {
        panic!("failed to parse {}: {}", path.display(), err)
    })
}

fn load_expected_error(path: &Path) -> ExpectedTransformError {
    let json = fs::read_to_string(path)
        .unwrap_or_else(|_| panic!("failed to read {}", path.display()));
    let value: serde_json::Value = serde_json::from_str(&json)
        .unwrap_or_else(|_| panic!("invalid json: {}", path.display()));
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
    }
}

#[derive(Debug, serde::Deserialize)]
struct ExpectedTransformError {
    kind: String,
    path: Option<String>,
}

#[test]
fn p01_preflight_ok() {
    let base = fixtures_dir().join("p01_preflight_ok");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));

    preflight_validate(&rule, &input, None).expect("preflight failed");
}

#[test]
fn p02_preflight_missing_required() {
    let base = fixtures_dir().join("p02_preflight_missing_required");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_expected_error(&base.join("expected_error.json"));

    let err = preflight_validate(&rule, &input, None).expect_err("expected preflight error");
    assert_eq!(transform_kind_to_str(&err.kind), expected.kind);
    assert_eq!(err.path, expected.path);
}

#[test]
fn p03_preflight_type_cast_failed() {
    let base = fixtures_dir().join("p03_preflight_type_cast_failed");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_expected_error(&base.join("expected_error.json"));

    let err = preflight_validate(&rule, &input, None).expect_err("expected preflight error");
    assert_eq!(transform_kind_to_str(&err.kind), expected.kind);
    assert_eq!(err.path, expected.path);
}
