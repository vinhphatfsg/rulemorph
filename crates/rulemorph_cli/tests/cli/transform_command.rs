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
