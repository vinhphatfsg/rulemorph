#![allow(dead_code)]

use std::fs;
use std::path::{Path, PathBuf};

use rulemorph::{
    InputData, RuleFormat, TransformErrorKind, parse_rule_file, parse_rule_file_with_format,
    transform, transform_input,
};

pub(crate) fn fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
}

pub(crate) fn load_json(path: &Path) -> serde_json::Value {
    let json =
        fs::read_to_string(path).unwrap_or_else(|_| panic!("failed to read {}", path.display()));
    serde_json::from_str(&json).unwrap_or_else(|_| panic!("invalid json: {}", path.display()))
}

pub(crate) fn load_rule(path: &Path) -> rulemorph::RuleFile {
    let yaml =
        fs::read_to_string(path).unwrap_or_else(|_| panic!("failed to read {}", path.display()));
    parse_rule_file(&yaml)
        .unwrap_or_else(|err| panic!("failed to parse {}: {}", path.display(), err))
}

pub(crate) fn load_rule_with_format(path: &Path, format: RuleFormat) -> rulemorph::RuleFile {
    let source =
        fs::read_to_string(path).unwrap_or_else(|_| panic!("failed to read {}", path.display()));
    parse_rule_file_with_format(&source, format)
        .unwrap_or_else(|err| panic!("failed to parse {}: {}", path.display(), err))
}

pub(crate) fn assert_text_fixture(case: &str, input_file: &str) {
    let base = fixtures_dir().join(case);
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join(input_file))
        .unwrap_or_else(|_| panic!("failed to read {}", input_file));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

pub(crate) fn assert_xlsx_fixture(case: &str) {
    let base = fixtures_dir().join(case);
    let rule = load_rule(&base.join("rules.yaml"));
    let input =
        fs::read(base.join("input.xlsx")).unwrap_or_else(|_| panic!("failed to read input.xlsx"));
    let expected = load_json(&base.join("expected.json"));
    let output =
        transform_input(&rule, InputData::Bytes(&input), None).expect("transform excel input");
    assert_eq!(output, expected);
}

pub(crate) fn assert_json_fixture(case: &str) {
    let base = fixtures_dir().join(case);
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

pub(crate) fn assert_transform_error_fixture(case: &str) {
    let base = fixtures_dir().join(case);
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_expected_error(&base.join("expected_error.json"));

    let err = transform(&rule, &input, None).expect_err("expected transform error");
    assert_eq!(transform_kind_to_str(&err.kind), expected.kind);
    assert_eq!(err.path, expected.path);
}

pub(crate) fn load_optional_json(path: &Path) -> Option<serde_json::Value> {
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

#[derive(Debug, serde::Deserialize)]
struct ExpectedTransformError {
    kind: String,
    path: Option<String>,
}
