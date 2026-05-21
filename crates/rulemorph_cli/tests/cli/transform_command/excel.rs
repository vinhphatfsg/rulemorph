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
