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
