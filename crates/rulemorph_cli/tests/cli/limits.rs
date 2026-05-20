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
