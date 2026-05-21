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
