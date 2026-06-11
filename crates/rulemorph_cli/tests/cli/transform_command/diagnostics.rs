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
    assert_eq!(value[0]["kind"], "InvalidInput");
    assert_eq!(value[0]["path"], "version");
    assert!(
        value
            .as_array()
            .expect("warnings")
            .iter()
            .any(|warning| warning["kind"] == "ExprError")
    );
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
