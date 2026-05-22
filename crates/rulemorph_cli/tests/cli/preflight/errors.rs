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
