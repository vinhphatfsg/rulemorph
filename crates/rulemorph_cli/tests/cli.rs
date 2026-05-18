use std::fs;

use assert_cmd::cargo::cargo_bin_cmd;

#[path = "common/cli.rs"]
mod cli_common;

#[cfg(feature = "server")]
use cli_common::stdout_json;
use cli_common::{
    assert_json_stdout_eq, fixtures_dir, read_json, rulemorph_output, stderr_json, stderr_string,
    stdout_string,
};

#[test]
fn validate_success_returns_zero() {
    let rules = fixtures_dir().join("t01_csv_basic").join("rules.yaml");
    let output = rulemorph_output(|cmd| {
        cmd.arg("validate").arg("-r").arg(rules);
    });
    assert_eq!(output.status.code(), Some(0));
}

#[test]
fn validate_json_errors() {
    let rules = fixtures_dir()
        .join("v01_missing_mapping_value")
        .join("rules.yaml");
    let output = rulemorph_output(|cmd| {
        cmd.arg("validate")
            .arg("-r")
            .arg(rules)
            .arg("-e")
            .arg("json");
    });
    assert_eq!(output.status.code(), Some(2));

    let value = stderr_json(output);
    assert_eq!(value[0]["type"], "validation");
    assert_eq!(value[0]["code"], "MissingMappingValue");
}

#[test]
fn preflight_success_returns_zero() {
    let base = fixtures_dir().join("p01_preflight_ok");
    let rules = base.join("rules.yaml");
    let input = base.join("input.json");
    let output = rulemorph_output(|cmd| {
        cmd.arg("preflight")
            .arg("-r")
            .arg(rules)
            .arg("-i")
            .arg(input);
    });
    assert_eq!(output.status.code(), Some(0));
}

#[test]
fn preflight_rejects_input_over_limit_override_before_transform() {
    let base = fixtures_dir().join("p01_preflight_ok");
    let output = rulemorph_output(|cmd| {
        cmd.arg("preflight")
            .arg("-r")
            .arg(base.join("rules.yaml"))
            .arg("-i")
            .arg(base.join("input.json"))
            .arg("--limit")
            .arg("input-bytes=4");
    });
    assert_eq!(output.status.code(), Some(1));
    let stderr = stderr_string(output);
    assert!(stderr.contains("max_input_bytes"));
}

#[test]
fn preflight_limits_profile_large_is_accepted() {
    let base = fixtures_dir().join("p01_preflight_ok");
    let output = rulemorph_output(|cmd| {
        cmd.arg("preflight")
            .arg("-r")
            .arg(base.join("rules.yaml"))
            .arg("-i")
            .arg(base.join("input.json"))
            .arg("--limits-profile")
            .arg("large");
    });
    assert_eq!(output.status.code(), Some(0));
}

#[test]
fn preflight_limits_file_is_accepted() {
    let base = fixtures_dir().join("p01_preflight_ok");
    let temp_dir = tempfile::tempdir().unwrap();
    let limits_path = temp_dir.path().join("limits.toml");
    fs::write(&limits_path, "input-bytes = 1000000\n").unwrap();

    let output = rulemorph_output(|cmd| {
        cmd.arg("preflight")
            .arg("-r")
            .arg(base.join("rules.yaml"))
            .arg("-i")
            .arg(base.join("input.json"))
            .arg("--limits-file")
            .arg(limits_path);
    });
    assert_eq!(output.status.code(), Some(0));
}

#[test]
fn preflight_json_errors() {
    let base = fixtures_dir().join("p03_preflight_type_cast_failed");
    let rules = base.join("rules.yaml");
    let input = base.join("input.json");
    let output = rulemorph_output(|cmd| {
        cmd.arg("preflight")
            .arg("-r")
            .arg(rules)
            .arg("-i")
            .arg(input)
            .arg("-e")
            .arg("json");
    });
    assert_eq!(output.status.code(), Some(3));

    let value = stderr_json(output);
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

    let output = rulemorph_output(|cmd| {
        cmd.arg("transform")
            .arg("-r")
            .arg(rules)
            .arg("-i")
            .arg(input)
            .arg("-c")
            .arg(context);
    });

    assert_eq!(output.status.code(), Some(0));
    assert_json_stdout_eq(output, &expected);
}

#[test]
fn transform_accepts_json_rule_file_by_extension() {
    let base = fixtures_dir().join("t30_json_rule_file");
    let expected = read_json(&base.join("expected.json"));

    let output = rulemorph_output(|cmd| {
        cmd.arg("transform")
            .arg("-r")
            .arg(base.join("rules.json"))
            .arg("-i")
            .arg(base.join("input.json"));
    });

    assert_eq!(output.status.code(), Some(0));
    assert_json_stdout_eq(output, &expected);
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
    let stderr = stderr_string(output);
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

    let value = stderr_json(output);
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
fn cli_transform_excel_input() {
    let base = fixtures_dir().join("t34_excel_input");
    let mut cmd = cargo_bin_cmd!("rulemorph");
    let output = cmd
        .arg("transform")
        .arg("-r")
        .arg(base.join("rules.yaml"))
        .arg("-i")
        .arg(base.join("input.xlsx"))
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(0));
}

#[test]
fn cli_preflight_excel_input() {
    let base = fixtures_dir().join("t34_excel_input");
    let mut cmd = cargo_bin_cmd!("rulemorph");
    let output = cmd
        .arg("preflight")
        .arg("-r")
        .arg(base.join("rules.yaml"))
        .arg("-i")
        .arg(base.join("input.xlsx"))
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(0));
}

#[test]
fn cli_transform_yaml_input() {
    let temp_dir = tempfile::tempdir().unwrap();
    let rules = temp_dir.path().join("rules.yaml");
    let input = temp_dir.path().join("input.yaml");
    fs::write(
        &rules,
        r#"
version: 2
input:
  format: yaml
  yaml:
    records_path: users
mappings:
  - target: "id"
    source: "id"
  - target: "name"
    source: "name"
"#,
    )
    .unwrap();
    fs::write(
        &input,
        r#"
users:
  - id: "1"
    name: Alice
"#,
    )
    .unwrap();
    assert_simple_transform(&rules, &input);
}

#[test]
fn cli_transform_toml_input() {
    let temp_dir = tempfile::tempdir().unwrap();
    let rules = temp_dir.path().join("rules.yaml");
    let input = temp_dir.path().join("input.toml");
    fs::write(
        &rules,
        r#"
version: 2
input:
  format: toml
  toml:
    records_path: users
mappings:
  - target: "id"
    source: "id"
  - target: "name"
    source: "name"
"#,
    )
    .unwrap();
    fs::write(
        &input,
        r#"
[[users]]
id = "1"
name = "Alice"
"#,
    )
    .unwrap();
    assert_simple_transform(&rules, &input);
}

#[test]
fn cli_transform_xml_input() {
    let temp_dir = tempfile::tempdir().unwrap();
    let rules = temp_dir.path().join("rules.yaml");
    let input = temp_dir.path().join("input.xml");
    fs::write(
        &rules,
        r##"
version: 2
input:
  format: xml
  xml:
    records_path: users.user
    attr_prefix: "@"
    text_key: "#text"
mappings:
  - target: "id"
    source: 'input.["@id"]'
  - target: "name"
    source: 'input.name[0]["#text"]'
"##,
    )
    .unwrap();
    fs::write(
        &input,
        r#"<users><user id="1"><name>Alice</name></user></users>"#,
    )
    .unwrap();
    assert_simple_transform(&rules, &input);
}

#[test]
fn cli_transform_html_input() {
    let temp_dir = tempfile::tempdir().unwrap();
    let rules = temp_dir.path().join("rules.yaml");
    let input = temp_dir.path().join("input.html");
    fs::write(
        &rules,
        r#"
version: 2
input:
  format: html
  html:
    records_selector: "table#users tbody tr"
    fields:
      id:
        selector: "td:nth-child(1)"
        value: text
      name:
        selector: "td:nth-child(2)"
        value: text
mappings:
  - target: "id"
    source: "id"
  - target: "name"
    source: "name"
"#,
    )
    .unwrap();
    fs::write(
        &input,
        r#"<table id="users"><tbody><tr><td>1</td><td>Alice</td></tr></tbody></table>"#,
    )
    .unwrap();
    assert_simple_transform(&rules, &input);
}

fn assert_simple_transform(rules: &std::path::Path, input: &std::path::Path) {
    let mut cmd = cargo_bin_cmd!("rulemorph");
    let output = cmd
        .arg("transform")
        .arg("-r")
        .arg(rules)
        .arg("-i")
        .arg(input)
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(0));
    assert_json_stdout_eq(output, &serde_json::json!([{ "id": "1", "name": "Alice" }]));
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
    let stdout = stdout_string(output);
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
    let value = stderr_json(output);
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

    let output = rulemorph_output(|cmd| {
        cmd.arg("generate")
            .arg("-r")
            .arg(rules)
            .arg("-l")
            .arg("rust");
    });

    assert_eq!(output.status.code(), Some(0));
    let stdout = stdout_string(output);
    assert!(stdout.contains("struct Record"));
}

#[cfg(feature = "server")]
#[test]
fn api_keys_issue_list_and_revoke_json() {
    let temp_dir = tempfile::tempdir().unwrap();

    let mut issue_cmd = cargo_bin_cmd!("rulemorph");
    let issue = issue_cmd
        .arg("api-keys")
        .arg("issue")
        .arg("--tenant-id")
        .arg("tenant-a")
        .arg("--data-dir")
        .arg(temp_dir.path())
        .arg("--label")
        .arg("alpha")
        .arg("--json")
        .output()
        .unwrap();
    assert_eq!(issue.status.code(), Some(0));

    let issued = stdout_json(issue);
    let id = issued["id"].as_str().expect("issued id").to_string();
    assert_eq!(issued["label"], "alpha");
    assert!(issued["key"].as_str().is_some_and(|key| !key.is_empty()));

    let mut list_cmd = cargo_bin_cmd!("rulemorph");
    let list = list_cmd
        .arg("api-keys")
        .arg("list")
        .arg("--tenant-id")
        .arg("tenant-a")
        .arg("--data-dir")
        .arg(temp_dir.path())
        .arg("--json")
        .output()
        .unwrap();
    assert_eq!(list.status.code(), Some(0));

    let keys = stdout_json(list);
    assert_eq!(keys.as_array().expect("api key array").len(), 1);
    assert_eq!(keys[0]["id"], id);
    assert_eq!(keys[0]["label"], "alpha");
    assert!(keys[0]["revoked_at"].is_null());

    let mut revoke_cmd = cargo_bin_cmd!("rulemorph");
    let revoke = revoke_cmd
        .arg("api-keys")
        .arg("revoke")
        .arg("--tenant-id")
        .arg("tenant-a")
        .arg("--data-dir")
        .arg(temp_dir.path())
        .arg("--id")
        .arg(id)
        .arg("--json")
        .output()
        .unwrap();
    assert_eq!(revoke.status.code(), Some(0));

    let revoked = stdout_json(revoke);
    assert_eq!(revoked["revoked"], true);
}
