use std::fs;
use std::path::PathBuf;

use assert_cmd::cargo::cargo_bin_cmd;

fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("rulemorph")
        .join("tests")
        .join("fixtures")
}

fn read_json(path: &PathBuf) -> serde_json::Value {
    let data =
        fs::read_to_string(path).unwrap_or_else(|_| panic!("failed to read {}", path.display()));
    serde_json::from_str(&data).unwrap_or_else(|_| panic!("invalid json: {}", path.display()))
}

#[test]
fn validate_success_returns_zero() {
    let rules = fixtures_dir().join("t01_csv_basic").join("rules.yaml");
    let mut cmd = cargo_bin_cmd!("rulemorph");
    let output = cmd.arg("validate").arg("-r").arg(rules).output().unwrap();
    assert_eq!(output.status.code(), Some(0));
}

#[test]
fn validate_json_errors() {
    let rules = fixtures_dir()
        .join("v01_missing_mapping_value")
        .join("rules.yaml");
    let mut cmd = cargo_bin_cmd!("rulemorph");
    let output = cmd
        .arg("validate")
        .arg("-r")
        .arg(rules)
        .arg("-e")
        .arg("json")
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));

    let stderr = String::from_utf8(output.stderr).unwrap();
    let value: serde_json::Value =
        serde_json::from_str(&stderr).unwrap_or_else(|_| panic!("invalid json stderr: {}", stderr));
    assert_eq!(value[0]["type"], "validation");
    assert_eq!(value[0]["code"], "MissingMappingValue");
}

#[test]
fn preflight_success_returns_zero() {
    let base = fixtures_dir().join("p01_preflight_ok");
    let rules = base.join("rules.yaml");
    let input = base.join("input.json");
    let mut cmd = cargo_bin_cmd!("rulemorph");
    let output = cmd
        .arg("preflight")
        .arg("-r")
        .arg(rules)
        .arg("-i")
        .arg(input)
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(0));
}

#[test]
fn preflight_json_errors() {
    let base = fixtures_dir().join("p03_preflight_type_cast_failed");
    let rules = base.join("rules.yaml");
    let input = base.join("input.json");
    let mut cmd = cargo_bin_cmd!("rulemorph");
    let output = cmd
        .arg("preflight")
        .arg("-r")
        .arg(rules)
        .arg("-i")
        .arg(input)
        .arg("-e")
        .arg("json")
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(3));

    let stderr = String::from_utf8(output.stderr).unwrap();
    let value: serde_json::Value =
        serde_json::from_str(&stderr).unwrap_or_else(|_| panic!("invalid json stderr: {}", stderr));
    assert_eq!(value[0]["type"], "transform");
    assert_eq!(value[0]["kind"], "TypeCastFailed");
}

#[test]
fn transform_outputs_json() {
    let base = fixtures_dir().join("t03_json_out_context");
    let rules = base.join("rules.yaml");
    let input = base.join("input.json");
    let context = base.join("context.json");
    let expected = read_json(&base.join("expected.json"));

    let mut cmd = cargo_bin_cmd!("rulemorph");
    let output = cmd
        .arg("transform")
        .arg("-r")
        .arg(rules)
        .arg("-i")
        .arg(input)
        .arg("-c")
        .arg(context)
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(0));
    let stdout = String::from_utf8(output.stdout).unwrap();
    let actual: serde_json::Value =
        serde_json::from_str(&stdout).unwrap_or_else(|_| panic!("invalid json stdout: {}", stdout));
    assert_eq!(actual, expected);
}

#[test]
fn transform_accepts_json_rule_file_by_extension() {
    let base = fixtures_dir().join("t30_json_rule_file");
    let expected = read_json(&base.join("expected.json"));

    let mut cmd = cargo_bin_cmd!("rulemorph");
    let output = cmd
        .arg("transform")
        .arg("-r")
        .arg(base.join("rules.json"))
        .arg("-i")
        .arg(base.join("input.json"))
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(0));
    let stdout = String::from_utf8(output.stdout).unwrap();
    let actual: serde_json::Value =
        serde_json::from_str(&stdout).unwrap_or_else(|_| panic!("invalid json stdout: {}", stdout));
    assert_eq!(actual, expected);
}

#[test]
fn cli_limit_override_allows_more_records() {
    let base = fixtures_dir().join("t01_csv_basic");
    let mut cmd = cargo_bin_cmd!("rulemorph");
    let output = cmd
        .arg("transform")
        .arg("-r")
        .arg(base.join("rules.yaml"))
        .arg("-i")
        .arg(base.join("input.csv"))
        .arg("--limit")
        .arg("records=1000000")
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(0));
}

#[test]
fn cli_rejects_unknown_limit_override() {
    let base = fixtures_dir().join("t01_csv_basic");
    let mut cmd = cargo_bin_cmd!("rulemorph");
    let output = cmd
        .arg("transform")
        .arg("-r")
        .arg(base.join("rules.yaml"))
        .arg("-i")
        .arg(base.join("input.csv"))
        .arg("--limit")
        .arg("formula-eval=1")
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
}

#[test]
fn cli_limits_profile_large_is_accepted() {
    let base = fixtures_dir().join("t01_csv_basic");
    let mut cmd = cargo_bin_cmd!("rulemorph");
    let output = cmd
        .arg("transform")
        .arg("-r")
        .arg(base.join("rules.yaml"))
        .arg("-i")
        .arg(base.join("input.csv"))
        .arg("--limits-profile")
        .arg("large")
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(0));
}

#[test]
fn cli_rejects_limit_override_overflow() {
    let base = fixtures_dir().join("t01_csv_basic");
    let mut cmd = cargo_bin_cmd!("rulemorph");
    let output = cmd
        .arg("transform")
        .arg("-r")
        .arg(base.join("rules.yaml"))
        .arg("-i")
        .arg(base.join("input.csv"))
        .arg("--limit")
        .arg("records=999999999999999999999999999999")
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
}

#[test]
fn cli_rejects_input_over_byte_limit_before_transform() {
    let base = fixtures_dir().join("t01_csv_basic");
    let mut cmd = cargo_bin_cmd!("rulemorph");
    let output = cmd
        .arg("transform")
        .arg("-r")
        .arg(base.join("rules.yaml"))
        .arg("-i")
        .arg(base.join("input.csv"))
        .arg("--limit")
        .arg("input-bytes=4")
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("max_input_bytes"));
}

#[test]
fn cli_reports_invalid_utf8_as_transform_error() {
    let base = fixtures_dir().join("t01_csv_basic");
    let temp_dir = tempfile::tempdir().unwrap();
    let input = temp_dir.path().join("bad.csv");
    fs::write(&input, [0xff, 0xfe, b'\n']).unwrap();

    let mut cmd = cargo_bin_cmd!("rulemorph");
    let output = cmd
        .arg("transform")
        .arg("-r")
        .arg(base.join("rules.yaml"))
        .arg("-i")
        .arg(input)
        .arg("-e")
        .arg("json")
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(3));

    let stderr = String::from_utf8(output.stderr).unwrap();
    let value: serde_json::Value =
        serde_json::from_str(&stderr).unwrap_or_else(|_| panic!("invalid json stderr: {}", stderr));
    assert_eq!(value[0]["type"], "transform");
    assert_eq!(value[0]["kind"], "InvalidInput");
    assert!(value[0]["message"].as_str().unwrap().contains("UTF-8"));
}

#[test]
fn cli_limits_file_is_accepted() {
    let base = fixtures_dir().join("t01_csv_basic");
    let temp_dir = tempfile::tempdir().unwrap();
    let limits_path = temp_dir.path().join("limits.toml");
    fs::write(&limits_path, "records = 1000000\n").unwrap();

    let mut cmd = cargo_bin_cmd!("rulemorph");
    let output = cmd
        .arg("transform")
        .arg("-r")
        .arg(base.join("rules.yaml"))
        .arg("-i")
        .arg(base.join("input.csv"))
        .arg("--limits-file")
        .arg(limits_path)
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(0));
}

#[test]
fn transform_outputs_ndjson() {
    let base = fixtures_dir().join("t12_ndjson_csv");
    let rules = base.join("rules.yaml");
    let input = base.join("input.csv");
    let expected = fs::read_to_string(base.join("expected.ndjson"))
        .unwrap_or_else(|_| panic!("failed to read expected.ndjson"));

    let mut cmd = cargo_bin_cmd!("rulemorph");
    let output = cmd
        .arg("transform")
        .arg("-r")
        .arg(rules)
        .arg("-i")
        .arg(input)
        .arg("--ndjson")
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(0));
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert_eq!(stdout, expected);
}

#[test]
fn transform_writes_output_file() {
    let base = fixtures_dir().join("t01_csv_basic");
    let rules = base.join("rules.yaml");
    let input = base.join("input.csv");
    let expected = read_json(&base.join("expected.json"));

    let temp_dir = tempfile::tempdir().unwrap();
    let out_path = temp_dir.path().join("nested").join("out.json");

    let mut cmd = cargo_bin_cmd!("rulemorph");
    let output = cmd
        .arg("transform")
        .arg("-r")
        .arg(rules)
        .arg("-i")
        .arg(input)
        .arg("-o")
        .arg(&out_path)
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(0));
    let contents = fs::read_to_string(&out_path)
        .unwrap_or_else(|_| panic!("failed to read {}", out_path.display()));
    let actual: serde_json::Value = serde_json::from_str(&contents)
        .unwrap_or_else(|_| panic!("invalid json output: {}", contents));
    assert_eq!(actual, expected);
}

#[test]
fn transform_emits_warnings_json() {
    let base = fixtures_dir().join("t10_when_compare");
    let rules = base.join("rules.yaml");
    let input = base.join("input.json");

    let mut cmd = cargo_bin_cmd!("rulemorph");
    let output = cmd
        .arg("transform")
        .arg("-r")
        .arg(rules)
        .arg("-i")
        .arg(input)
        .arg("-e")
        .arg("json")
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(0));
    let stderr = String::from_utf8(output.stderr).unwrap();
    let value: serde_json::Value =
        serde_json::from_str(&stderr).unwrap_or_else(|_| panic!("invalid json stderr: {}", stderr));
    assert_eq!(value[0]["type"], "warning");
    assert_eq!(value[0]["kind"], "ExprError");
}

#[test]
fn transform_validate_flag_reports_validation_error() {
    let rules = fixtures_dir()
        .join("v01_missing_mapping_value")
        .join("rules.yaml");
    let input = fixtures_dir().join("t01_csv_basic").join("input.csv");

    let mut cmd = cargo_bin_cmd!("rulemorph");
    let output = cmd
        .arg("transform")
        .arg("-r")
        .arg(rules)
        .arg("-i")
        .arg(input)
        .arg("-v")
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(2));
}

#[test]
fn generate_outputs_rust_dto() {
    let rules = fixtures_dir().join("t01_csv_basic").join("rules.yaml");

    let mut cmd = cargo_bin_cmd!("rulemorph");
    let output = cmd
        .arg("generate")
        .arg("-r")
        .arg(rules)
        .arg("-l")
        .arg("rust")
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(0));
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("struct Record"));
}
