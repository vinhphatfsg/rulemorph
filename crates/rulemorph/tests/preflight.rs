use std::fs;
use std::path::{Path, PathBuf};

use rulemorph::{
    InputData, NormalizationOptions, TransformErrorKind, parse_rule_file, preflight_validate,
    preflight_validate_input_with_warnings_with_base_dir_and_options,
};

fn fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
}

fn load_rule(path: &Path) -> rulemorph::RuleFile {
    let yaml =
        fs::read_to_string(path).unwrap_or_else(|_| panic!("failed to read {}", path.display()));
    parse_rule_file(&yaml)
        .unwrap_or_else(|err| panic!("failed to parse {}: {}", path.display(), err))
}

fn load_expected_error(path: &Path) -> ExpectedTransformError {
    let json =
        fs::read_to_string(path).unwrap_or_else(|_| panic!("failed to read {}", path.display()));
    let value: serde_json::Value =
        serde_json::from_str(&json).unwrap_or_else(|_| panic!("invalid json: {}", path.display()));
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

include!("preflight/success.rs");
include!("preflight/errors.rs");
include!("preflight/input_contract.rs");
