use std::fs;
use std::path::{Path, PathBuf};

use rulemorph::{RuleFormat, parse_rule_file_with_format, validate_rule_file};
use serde_json::{Value, json};

fn fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
}

fn load_json(path: &Path) -> Value {
    let json =
        fs::read_to_string(path).unwrap_or_else(|_| panic!("failed to read {}", path.display()));
    serde_json::from_str(&json).unwrap_or_else(|_| panic!("invalid json: {}", path.display()))
}

fn parse_rule_source(path: &Path) -> Value {
    let source =
        fs::read_to_string(path).unwrap_or_else(|_| panic!("failed to read {}", path.display()));
    let format = RuleFormat::from_path(path);
    let rule = parse_rule_file_with_format(&source, format)
        .unwrap_or_else(|err| panic!("failed to parse {}: {}", path.display(), err));
    validate_rule_file(&rule).unwrap_or_else(|errors| {
        panic!(
            "expected valid rules for {}, got {:?}",
            path.display(),
            errors
                .iter()
                .map(|err| (err.code.as_str(), err.path.as_deref()))
                .collect::<Vec<_>>()
        )
    });

    match format {
        RuleFormat::Yaml => serde_yaml::from_str(&source)
            .unwrap_or_else(|err| panic!("invalid yaml {}: {}", path.display(), err)),
        RuleFormat::Json => serde_json::from_str(&source)
            .unwrap_or_else(|err| panic!("invalid json {}: {}", path.display(), err)),
    }
}

fn rule_schema_summary(rule_source: Value) -> Value {
    json!({
        "version": rule_source["version"].clone(),
        "input": rule_source["input"].clone(),
        "mappings": rule_source["mappings"].clone(),
    })
}

fn assert_rule_schema_golden(case: &str, rules_file: &str) {
    let base = fixtures_dir().join(case);
    let actual = rule_schema_summary(parse_rule_source(&base.join(rules_file)));
    let expected = load_json(&base.join("expected_schema.json"));
    assert_eq!(actual, expected, "schema golden mismatch for {}", case);
}

#[test]
fn t30_json_rule_file_schema_golden() {
    assert_rule_schema_golden("t30_json_rule_file", "rules.json");
}

#[test]
fn t31_yaml_input_schema_golden() {
    assert_rule_schema_golden("t31_yaml_input", "rules.yaml");
}

#[test]
fn t32_toml_input_schema_golden() {
    assert_rule_schema_golden("t32_toml_input", "rules.yaml");
}

#[test]
fn t33_xml_input_schema_golden() {
    assert_rule_schema_golden("t33_xml_input", "rules.yaml");
}

#[test]
fn t34_excel_input_schema_golden() {
    assert_rule_schema_golden("t34_excel_input", "rules.yaml");
}

#[test]
fn t35_html_input_schema_golden() {
    assert_rule_schema_golden("t35_html_input", "rules.yaml");
}
