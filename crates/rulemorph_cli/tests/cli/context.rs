#[test]
fn transform_rejects_invalid_context_json() {
    let base = fixtures_dir().join("t03_json_out_context");
    let temp_dir = tempfile::tempdir().unwrap();
    let context_path = temp_dir.path().join("context.json");
    fs::write(&context_path, "{invalid").unwrap();

    let output = rulemorph_output(|cmd| {
        cmd.arg("transform")
            .arg("-r")
            .arg(base.join("rules.yaml"))
            .arg("-i")
            .arg(base.join("input.json"))
            .arg("-c")
            .arg(context_path);
    });

    assert_eq!(output.status.code(), Some(1));
    let stderr = stderr_string(output);
    assert!(stderr.contains("failed to parse context JSON:"));
}
