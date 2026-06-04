#[test]
fn direct_rule_reads_json_from_stdin() {
    let output = rulemorph_output(|cmd| {
        cmd.arg("-rule")
            .arg("@input.test")
            .write_stdin(r#"{ "test": 1 }"#);
    });

    assert_eq!(output.status.code(), Some(0));
    assert_eq!(stdout_string(output), "1\n");
}

#[test]
fn direct_rule_accepts_equals_alias() {
    let output = rulemorph_output(|cmd| {
        cmd.arg("-rule=@input.test")
            .write_stdin(r#"{ "test": 1 }"#);
    });

    assert_eq!(output.status.code(), Some(0));
    assert_eq!(stdout_string(output), "1\n");
}

#[test]
fn direct_rule_preserves_singleton_json_array_shape() {
    let output = rulemorph_output(|cmd| {
        cmd.arg("--rule")
            .arg("@input.test")
            .write_stdin(r#"[{ "test": 1 }]"#);
    });

    assert_eq!(output.status.code(), Some(0));
    assert_eq!(stdout_string(output), "[1]\n");
}

#[test]
fn direct_rule_outputs_null_for_missing_object_value() {
    let output = rulemorph_output(|cmd| {
        cmd.arg("--rule")
            .arg("@input.missing")
            .write_stdin(r#"{ "test": 1 }"#);
    });

    assert_eq!(output.status.code(), Some(0));
    assert_eq!(stdout_string(output), "null\n");
}

#[test]
fn direct_rule_outputs_null_items_for_missing_array_values() {
    let output = rulemorph_output(|cmd| {
        cmd.arg("--rule")
            .arg("@input.missing")
            .write_stdin(r#"[{ "test": 1 }, { "test": 2 }]"#);
    });

    assert_eq!(output.status.code(), Some(0));
    assert_eq!(stdout_string(output), "[null,null]\n");
}

#[test]
fn direct_rule_distinguishes_missing_from_empty_object() {
    let output = rulemorph_output(|cmd| {
        cmd.arg("--rule").arg("{}").write_stdin(r#"{ "test": 1 }"#);
    });

    assert_eq!(output.status.code(), Some(0));
    assert_eq!(stdout_string(output), "{}\n");
}

#[test]
fn direct_rule_rejects_duplicate_keys_in_inline_json() {
    let output = rulemorph_output(|cmd| {
        cmd.arg("--rule")
            .arg(r#"{ "a": 1, "a": 2 }"#)
            .write_stdin(r#"{ "test": 1 }"#);
    });

    assert_eq!(output.status.code(), Some(1));
    assert!(stderr_string(output).contains("duplicate key"));
}

#[test]
fn direct_rule_unwraps_bom_prefixed_json_object() {
    let output = rulemorph_output(|cmd| {
        cmd.arg("--rule")
            .arg("@input.test")
            .write_stdin(Vec::from(b"\xef\xbb\xbf{ \"test\": 1 }".as_slice()));
    });

    assert_eq!(output.status.code(), Some(0));
    assert_eq!(stdout_string(output), "1\n");
}

#[test]
fn direct_options_cannot_be_ignored_before_subcommand() {
    let output = rulemorph_output(|cmd| {
        cmd.arg("-i")
            .arg("input.json")
            .arg("transform")
            .arg("-r")
            .arg("rules.yaml");
    });

    assert_eq!(output.status.code(), Some(2));
    assert!(stderr_string(output).contains("direct-mode options require --rule"));
}
