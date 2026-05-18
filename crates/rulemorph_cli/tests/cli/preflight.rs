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
