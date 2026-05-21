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
