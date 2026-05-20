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
