#[test]
fn custom_ops_flow_through_cli_validate_transform_and_generate() {
    let temp_dir = tempfile::tempdir().unwrap();
    let rules = temp_dir.path().join("rules.yaml");
    let input = temp_dir.path().join("input.json");

    fs::write(
        &rules,
        r#"
version: 2
input:
  format: json
  json: {}
defs:
  line_total:
    input: { qty: int, unit_price: number }
    returns: number
    expr:
      - "$"
      - let:
          qty: ["$.qty", float]
          price: ["$.unit_price", float]
      - "@qty"
      - "*": ["@price"]
mappings:
  - target: total
    expr:
      - "@input.line"
      - line_total:
          - with: { qty: "$.quantity", unit_price: "$.price" }
    required: true
"#,
    )
    .unwrap();
    fs::write(&input, r#"[{"line":{"quantity":3,"price":19.5}}]"#).unwrap();

    let validate = rulemorph_output(|cmd| {
        cmd.arg("validate").arg("-r").arg(&rules);
    });
    assert_eq!(validate.status.code(), Some(0), "{}", stderr_string(validate));

    let transform = rulemorph_output(|cmd| {
        cmd.arg("transform")
            .arg("-r")
            .arg(&rules)
            .arg("-i")
            .arg(&input);
    });
    assert_eq!(transform.status.code(), Some(0));
    assert_json_stdout_eq(transform, &serde_json::json!([{ "total": 58.5 }]));

    let generate = rulemorph_output(|cmd| {
        cmd.arg("generate")
            .arg("-r")
            .arg(&rules)
            .arg("-l")
            .arg("ts")
            .arg("-n")
            .arg("Record");
    });
    assert_eq!(generate.status.code(), Some(0));
    let dto = stdout_string(generate);
    assert!(dto.contains("interface Record"));
    assert!(dto.contains("total: number;"));
}
