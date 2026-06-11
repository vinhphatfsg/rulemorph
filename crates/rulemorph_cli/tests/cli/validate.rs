#[test]
fn validate_success_returns_zero() {
    let cases = [
        "t01_csv_basic",
        "t36_spreadsheets_plugin_products",
        "tv35_finalize_wrap",
        "tv49_typed_value_firestore_document",
        "tv50_typed_value_mongo_extended_json",
    ];

    for case in cases {
        let rules = fixtures_dir().join(case).join("rules.yaml");
        let output = rulemorph_output(|cmd| {
            cmd.arg("validate").arg("-r").arg(rules);
        });
        assert_eq!(output.status.code(), Some(0), "{case} should validate");
    }
}

#[test]
fn validate_warns_for_version_one_rules() {
    let rules = fixtures_dir().join("t01_csv_basic").join("rules.yaml");
    let output = rulemorph_output(|cmd| {
        cmd.arg("validate")
            .arg("-r")
            .arg(rules)
            .arg("-e")
            .arg("json");
    });
    assert_eq!(output.status.code(), Some(0));

    let value = stderr_json(output);
    assert_eq!(value[0]["type"], "warning");
    assert_eq!(value[0]["kind"], "InvalidInput");
    assert_eq!(value[0]["path"], "version");
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

#[test]
fn validate_accepts_out_reference_produced_by_return_false_branch_child() {
    let dir = validate_temp_dir("branch-output-ok");
    write_rule(
        &dir,
        "main.yaml",
        r#"version: 2
input:
  format: json
  json: {}
steps:
  - mappings:
      - target: base
        value: 10
  - branch:
      when: { eq: [1, 1] }
      then: child.yaml
      return: false
  - mappings:
      - target: after
        expr: "@out.branch_value"
"#,
    );
    write_rule(
        &dir,
        "child.yaml",
        r#"version: 2
input:
  format: json
  json: {}
mappings:
  - target: branch_value
    value: 20
"#,
    );

    let output = rulemorph_output(|cmd| {
        cmd.arg("validate").arg("-r").arg(dir.join("main.yaml"));
    });
    assert_eq!(output.status.code(), Some(0));
}

#[test]
fn validate_accepts_return_false_branch_child_output_update() {
    let dir = validate_temp_dir("branch-output-update");
    write_rule(
        &dir,
        "main.yaml",
        r#"version: 2
input:
  format: json
  json: {}
steps:
  - branch:
      when: { eq: [1, 1] }
      then: child.yaml
      return: false
  - mappings:
      - target: count
        expr:
          - "@out.count"
          - add: [1]
"#,
    );
    write_rule(
        &dir,
        "child.yaml",
        r#"version: 2
input:
  format: json
  json: {}
mappings:
  - target: count
    value: 10
"#,
    );

    let output = rulemorph_output(|cmd| {
        cmd.arg("validate")
            .arg("-r")
            .arg(dir.join("main.yaml"))
            .arg("-e")
            .arg("json");
    });
    assert_eq!(
        output.status.code(),
        Some(0),
        "branch child outputs should be @out-visible without becoming parent duplicates: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[cfg(unix)]
#[test]
fn validate_uses_effective_base_dir_for_symlinked_branch_rules() {
    let dir = validate_temp_dir("branch-symlink-base");
    std::fs::create_dir_all(dir.join("a")).expect("create a dir");
    std::fs::create_dir_all(dir.join("b")).expect("create b dir");
    std::fs::create_dir_all(dir.join("shared")).expect("create shared dir");

    write_rule(
        &dir,
        "main.yaml",
        r#"version: 2
input:
  format: json
  json: {}
steps:
  - branch:
      when: { eq: [1, 1] }
      then: a/link.yaml
      return: false
  - branch:
      when: { eq: [1, 1] }
      then: b/link.yaml
      return: false
  - mappings:
      - target: after
        expr: "@out.b_only"
"#,
    );
    write_rule(
        &dir.join("shared"),
        "rule.yaml",
        r#"version: 2
input:
  format: json
  json: {}
steps:
  - branch:
      when: { eq: [1, 1] }
      then: child.yaml
      return: false
"#,
    );
    write_rule(
        &dir.join("a"),
        "child.yaml",
        r#"version: 2
input:
  format: json
  json: {}
mappings:
  - target: a_only
    value: true
"#,
    );
    write_rule(
        &dir.join("b"),
        "child.yaml",
        r#"version: 2
input:
  format: json
  json: {}
mappings:
  - target: b_only
    value: true
"#,
    );
    std::os::unix::fs::symlink("../shared/rule.yaml", dir.join("a/link.yaml"))
        .expect("symlink a/link.yaml");
    std::os::unix::fs::symlink("../shared/rule.yaml", dir.join("b/link.yaml"))
        .expect("symlink b/link.yaml");

    let output = rulemorph_output(|cmd| {
        cmd.arg("validate")
            .arg("-r")
            .arg(dir.join("main.yaml"))
            .arg("-e")
            .arg("json");
    });
    assert_eq!(
        output.status.code(),
        Some(0),
        "symlinked branch rules must use the link parent as effective base dir: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn validate_rejects_out_reference_not_produced_by_return_false_branch_child() {
    let dir = validate_temp_dir("branch-output-missing");
    write_rule(
        &dir,
        "main.yaml",
        r#"version: 2
input:
  format: json
  json: {}
steps:
  - mappings:
      - target: base
        value: 10
  - branch:
      when: { eq: [1, 1] }
      then: child.yaml
      return: false
  - mappings:
      - target: after
        expr: "@out.never_created"
"#,
    );
    write_rule(
        &dir,
        "child.yaml",
        r#"version: 2
input:
  format: json
  json: {}
mappings:
  - target: branch_value
    value: 20
"#,
    );

    let output = rulemorph_output(|cmd| {
        cmd.arg("validate")
            .arg("-r")
            .arg(dir.join("main.yaml"))
            .arg("-e")
            .arg("json");
    });
    assert_eq!(output.status.code(), Some(2));
    let value = stderr_json(output);
    assert!(
        value
            .as_array()
            .expect("validation errors")
            .iter()
            .any(|err| err["code"] == "ForwardOutReference")
    );
}

#[test]
fn validate_rejects_return_false_branch_child_with_non_object_output() {
    let dir = validate_temp_dir("branch-non-object-output");
    write_rule(
        &dir,
        "main.yaml",
        r#"version: 2
input:
  format: json
  json: {}
steps:
  - branch:
      when: { eq: [1, 1] }
      then: child.yaml
      return: false
"#,
    );
    write_rule(
        &dir,
        "child.yaml",
        r#"version: 2
input:
  format: json
  json: {}
mappings:
  - target: ignored
    value: true
finalize:
  wrap: "@input.name"
"#,
    );

    let output = rulemorph_output(|cmd| {
        cmd.arg("validate")
            .arg("-r")
            .arg(dir.join("main.yaml"))
            .arg("-e")
            .arg("json");
    });
    assert_eq!(output.status.code(), Some(2));
    let value = stderr_json(output);
    assert!(
        value
            .as_array()
            .expect("validation errors")
            .iter()
            .any(|err| {
                err["code"] == "InvalidStep" && err["path"] == "steps[0].branch.then"
            })
    );
}

#[test]
fn validate_rejects_return_false_branch_child_finalize_without_wrap() {
    let dir = validate_temp_dir("branch-finalize-array-output");
    write_rule(
        &dir,
        "main.yaml",
        r#"version: 2
input:
  format: json
  json: {}
steps:
  - branch:
      when: { eq: [1, 1] }
      then: child.yaml
      return: false
"#,
    );
    write_rule(
        &dir,
        "child.yaml",
        r#"version: 2
input:
  format: json
  json: {}
mappings:
  - target: value
    value: 1
finalize:
  limit: 1
"#,
    );

    let output = rulemorph_output(|cmd| {
        cmd.arg("validate")
            .arg("-r")
            .arg(dir.join("main.yaml"))
            .arg("-e")
            .arg("json");
    });
    assert_eq!(output.status.code(), Some(2));
    let value = stderr_json(output);
    assert!(
        value
            .as_array()
            .expect("validation errors")
            .iter()
            .any(|err| err["code"] == "InvalidStep"
                && err["path"] == "steps[0].branch.then")
    );
}

#[test]
fn validate_rejects_nested_branch_out_reference_not_produced_by_child() {
    let dir = validate_temp_dir("branch-nested-output-missing");
    write_rule(
        &dir,
        "main.yaml",
        r#"version: 2
input:
  format: json
  json: {}
steps:
  - branch:
      when: { eq: [1, 1] }
      then: child.yaml
      return: false
"#,
    );
    write_rule(
        &dir,
        "child.yaml",
        r#"version: 2
input:
  format: json
  json: {}
steps:
  - branch:
      when: { eq: [1, 1] }
      then: nested.yaml
      return: false
  - mappings:
      - target: after
        expr: "@out.never_created"
"#,
    );
    write_rule(
        &dir,
        "nested.yaml",
        r#"version: 2
input:
  format: json
  json: {}
mappings:
  - target: nested_value
    value: 20
"#,
    );

    let output = rulemorph_output(|cmd| {
        cmd.arg("validate")
            .arg("-r")
            .arg(dir.join("main.yaml"))
            .arg("-e")
            .arg("json");
    });
    assert_eq!(output.status.code(), Some(2));
    let value = stderr_json(output);
    assert!(
        value
            .as_array()
            .expect("validation errors")
            .iter()
            .any(|err| err["code"] == "InvalidStep"
                && err["path"] == "steps[0].branch.then")
    );
}

#[test]
fn validate_rejects_return_false_branch_child_with_nested_return_true_non_object_output() {
    let dir = validate_temp_dir("branch-nested-return-non-object");
    write_rule(
        &dir,
        "main.yaml",
        r#"version: 2
input:
  format: json
  json: {}
steps:
  - branch:
      when: { eq: [1, 1] }
      then: child.yaml
      return: false
"#,
    );
    write_rule(
        &dir,
        "child.yaml",
        r#"version: 2
input:
  format: json
  json: {}
steps:
  - branch:
      when: { eq: [1, 1] }
      then: scalar.yaml
      return: true
"#,
    );
    write_rule(
        &dir,
        "scalar.yaml",
        r#"version: 2
input:
  format: json
  json: {}
mappings:
  - target: ignored
    value: true
finalize:
  wrap: "@input.name"
"#,
    );

    let output = rulemorph_output(|cmd| {
        cmd.arg("validate")
            .arg("-r")
            .arg(dir.join("main.yaml"))
            .arg("-e")
            .arg("json");
    });
    assert_eq!(output.status.code(), Some(2));
    let value = stderr_json(output);
    assert!(
        value
            .as_array()
            .expect("validation errors")
            .iter()
            .any(|err| err["code"] == "InvalidStep"
                && err["path"] == "steps[0].branch.then")
    );
}

#[test]
fn validate_rejects_branch_cycle() {
    let dir = validate_temp_dir("branch-cycle");
    write_rule(
        &dir,
        "main.yaml",
        r#"version: 2
input:
  format: json
  json: {}
steps:
  - branch:
      when: { eq: [1, 1] }
      then: child.yaml
      return: false
"#,
    );
    write_rule(
        &dir,
        "child.yaml",
        r#"version: 2
input:
  format: json
  json: {}
steps:
  - branch:
      when: { eq: [1, 1] }
      then: main.yaml
      return: false
"#,
    );

    let output = rulemorph_output(|cmd| {
        cmd.arg("validate")
            .arg("-r")
            .arg(dir.join("main.yaml"))
            .arg("-e")
            .arg("json");
    });
    assert_eq!(output.status.code(), Some(2));
    let value = stderr_json(output);
    assert!(
        value
            .as_array()
            .expect("validation errors")
            .iter()
            .any(|err| err["code"] == "InvalidStep"
                && err["message"] == "branch rule cycle detected")
    );
}

#[test]
fn validate_rejects_branch_base_dir_escape() {
    let dir = validate_temp_dir("branch-base");
    let outside = validate_temp_dir("branch-outside");
    write_rule(
        &outside,
        "outside.yaml",
        r#"version: 2
input:
  format: json
  json: {}
mappings:
  - target: ok
    value: true
"#,
    );
    write_rule(
        &dir,
        "main.yaml",
        &format!(
            r#"version: 2
input:
  format: json
  json: {{}}
steps:
  - branch:
      when: {{ eq: [1, 1] }}
      then: {}
      return: false
"#,
            outside.join("outside.yaml").display()
        ),
    );

    let output = rulemorph_output(|cmd| {
        cmd.arg("validate")
            .arg("-r")
            .arg(dir.join("main.yaml"))
            .arg("-e")
            .arg("json");
    });
    assert_eq!(output.status.code(), Some(2));
    let value = stderr_json(output);
    assert!(
        value
            .as_array()
            .expect("validation errors")
            .iter()
            .any(|err| err["code"] == "InvalidStep"
                && err["path"] == "steps[0].branch.then")
    );
}

#[test]
fn validate_rejects_branch_base_dir_escape_with_bare_relative_rules_path() {
    let dir = validate_temp_dir("branch-base-bare-rules");
    let outside = validate_temp_dir("branch-outside-bare-rules");
    write_rule(
        &outside,
        "outside.yaml",
        r#"version: 2
input:
  format: json
  json: {}
mappings:
  - target: ok
    value: true
"#,
    );
    write_rule(
        &dir,
        "main.yaml",
        &format!(
            r#"version: 2
input:
  format: json
  json: {{}}
steps:
  - branch:
      when: {{ eq: [1, 1] }}
      then: {}
      return: false
"#,
            outside.join("outside.yaml").display()
        ),
    );

    let output = rulemorph_output(|cmd| {
        cmd.current_dir(&dir)
            .arg("validate")
            .arg("-r")
            .arg("main.yaml")
            .arg("-e")
            .arg("json");
    });
    assert_eq!(output.status.code(), Some(2));
    let value = stderr_json(output);
    assert!(
        value
            .as_array()
            .expect("validation errors")
            .iter()
            .any(|err| err["code"] == "InvalidStep"
                && err["path"] == "steps[0].branch.then")
    );
}

#[test]
fn validate_rejects_empty_branch_then_without_resolving_directory() {
    let dir = validate_temp_dir("branch-empty-then");
    write_rule(
        &dir,
        "main.yaml",
        r#"version: 2
input:
  format: json
  json: {}
steps:
  - branch:
      when: { eq: [1, 1] }
      then: ""
      return: false
"#,
    );

    let output = rulemorph_output(|cmd| {
        cmd.arg("validate")
            .arg("-r")
            .arg(dir.join("main.yaml"))
            .arg("-e")
            .arg("json");
    });
    assert_eq!(output.status.code(), Some(2));
    let value = stderr_json(output);
    let errors = value.as_array().expect("validation errors");
    assert!(
        errors.iter().any(|err| {
            err["code"] == "InvalidStep"
                && err["path"] == "steps[0].branch.then"
                && err["message"] == "branch.then is required"
        }),
        "expected required branch.then error, got {errors:?}"
    );
    assert!(
        !errors.iter().any(|err| err["message"]
            .as_str()
            .is_some_and(|message| message.contains("failed to read branch rule")
                || message.contains("failed to resolve branch rule"))),
        "empty branch.then should not be resolved as a file path: {errors:?}"
    );
}

#[test]
fn validate_prefixes_branch_child_validation_errors_with_parent_branch_path() {
    let dir = validate_temp_dir("branch-child-error-path");
    write_rule(
        &dir,
        "main.yaml",
        r#"version: 2
input:
  format: json
  json: {}
steps:
  - branch:
      when: { eq: [1, 1] }
      then: child.yaml
      return: false
"#,
    );
    write_rule(
        &dir,
        "child.yaml",
        r#"version: 2
input:
  format: json
  json: {}
mappings:
  - target: value
"#,
    );

    let output = rulemorph_output(|cmd| {
        cmd.arg("validate")
            .arg("-r")
            .arg(dir.join("main.yaml"))
            .arg("-e")
            .arg("json");
    });
    assert_eq!(output.status.code(), Some(2));
    let value = stderr_json(output);
    let errors = value.as_array().expect("validation errors");
    assert!(
        errors.iter().any(|err| {
            err["code"] == "MissingMappingValue"
                && err["path"] == "steps[0].branch.then.mappings[0]"
        }),
        "expected child error path under branch.then, got {errors:?}"
    );
    assert!(
        !errors
            .iter()
            .any(|err| err["path"].as_str() == Some("mappings[0]")),
        "child errors must not look like parent top-level paths: {errors:?}"
    );
}

fn validate_temp_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "rulemorph-cli-validate-{}-{}",
        name,
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

fn write_rule(dir: &std::path::Path, name: &str, contents: &str) {
    std::fs::write(dir.join(name), contents).expect("write rule");
}
