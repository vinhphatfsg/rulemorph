#[cfg(feature = "server")]
#[test]
fn ui_native_mode_rejects_no_ui() {
    let mut cmd = cargo_bin_cmd!("rulemorph");
    let output = cmd
        .arg("ui")
        .arg("--api-mode")
        .arg("native")
        .arg("--no-ui")
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1));
    let stderr = stderr_string(output);
    assert!(stderr.contains("ui-only mode cannot be used with --no-ui"));
}

#[cfg(feature = "server")]
#[test]
fn purge_traces_rejects_zero_retention_days() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut cmd = cargo_bin_cmd!("rulemorph");
    let output = cmd
        .arg("purge-traces")
        .arg("--data-dir")
        .arg(temp_dir.path())
        .arg("--retention-days")
        .arg("0")
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1));
    let stderr = stderr_string(output);
    assert!(stderr.contains("--retention-days must be greater than 0"));
}

#[cfg(feature = "server")]
#[test]
fn validate_rules_dir_json_errors() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut cmd = cargo_bin_cmd!("rulemorph");
    let output = cmd
        .arg("validate-rules-dir")
        .arg("-r")
        .arg(temp_dir.path())
        .arg("-e")
        .arg("json")
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(2));
    let value = stderr_json(output);
    assert_eq!(value[0]["type"], "rules_dir");
    assert_eq!(value[0]["code"], "ReadFailed");
    assert!(value[0]["file"]
        .as_str()
        .expect("rules-dir error file should be a string")
        .ends_with("endpoint.yaml"));
}
