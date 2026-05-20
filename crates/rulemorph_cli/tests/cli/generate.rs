#[test]
fn generate_outputs_rust_dto() {
    let rules = fixtures_dir().join("t01_csv_basic").join("rules.yaml");

    let output = rulemorph_output(|cmd| {
        cmd.arg("generate")
            .arg("-r")
            .arg(rules)
            .arg("-l")
            .arg("rust");
    });

    assert_eq!(output.status.code(), Some(0));
    let stdout = stdout_string(output);
    assert!(stdout.contains("struct Record"));
}
