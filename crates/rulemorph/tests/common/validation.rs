#![allow(dead_code)]

use std::fs;
use std::path::{Path, PathBuf};

use rulemorph::{RuleError, parse_rule_file};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct ExpectedError {
    code: String,
    path: Option<String>,
}

pub(crate) fn fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
}

pub(crate) fn load_rule(case: &str) -> rulemorph::RuleFile {
    let rules_path = fixtures_dir().join(case).join("rules.yaml");
    let yaml = fs::read_to_string(&rules_path)
        .unwrap_or_else(|_| panic!("failed to read {}", rules_path.display()));
    parse_rule_file(&yaml)
        .unwrap_or_else(|err| panic!("failed to parse YAML {}: {}", rules_path.display(), err))
}

pub(crate) fn load_expected_errors(case: &str) -> Vec<(String, Option<String>)> {
    let errors_path = fixtures_dir().join(case).join("expected_errors.json");
    let json = fs::read_to_string(&errors_path)
        .unwrap_or_else(|_| panic!("failed to read {}", errors_path.display()));
    let errors: Vec<ExpectedError> = serde_json::from_str(&json).unwrap_or_else(|err| {
        panic!(
            "failed to parse expected errors {}: {}",
            errors_path.display(),
            err
        )
    });
    normalize_expected(errors)
}

pub(crate) fn normalize_errors(errors: Vec<RuleError>) -> Vec<(String, Option<String>)> {
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
