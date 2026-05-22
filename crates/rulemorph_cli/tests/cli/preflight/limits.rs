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
