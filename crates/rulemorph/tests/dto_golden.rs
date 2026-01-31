use std::fs;
use std::path::{Path, PathBuf};

use rulemorph::{DtoLanguage, generate_dto, parse_rule_file};

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

fn load_text(path: &Path) -> String {
    fs::read_to_string(path)
        .unwrap_or_else(|_| panic!("failed to read {}", path.display()))
        .trim_end()
        .to_string()
}

fn assert_golden_in_fixture(lang: DtoLanguage, fixture: &str, expected: &str) {
    let base = fixtures_dir().join(fixture);
    let rule = load_rule(&base.join("rules.yaml"));
    let output = generate_dto(&rule, lang, None).expect("dto failed");
    let expected = load_text(&base.join(expected));
    assert_eq!(output, expected);
}

#[test]
fn dto01_rust() {
    assert_golden_in_fixture(DtoLanguage::Rust, "dto01_basic", "expected_rust.rs");
}

#[test]
fn dto01_typescript() {
    assert_golden_in_fixture(
        DtoLanguage::TypeScript,
        "dto01_basic",
        "expected_typescript.ts",
    );
}

#[test]
fn dto01_python() {
    assert_golden_in_fixture(DtoLanguage::Python, "dto01_basic", "expected_python.py");
}

#[test]
fn dto01_go() {
    assert_golden_in_fixture(DtoLanguage::Go, "dto01_basic", "expected_go.go");
}

#[test]
fn dto01_java() {
    assert_golden_in_fixture(DtoLanguage::Java, "dto01_basic", "expected_java.java");
}

#[test]
fn dto01_kotlin() {
    assert_golden_in_fixture(DtoLanguage::Kotlin, "dto01_basic", "expected_kotlin.kt");
}

#[test]
fn dto01_swift() {
    assert_golden_in_fixture(DtoLanguage::Swift, "dto01_basic", "expected_swift.swift");
}

#[test]
fn dto02_steps_rust() {
    assert_golden_in_fixture(DtoLanguage::Rust, "dto02_steps", "expected_rust.rs");
}
