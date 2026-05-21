#[test]
fn cli_transform_yaml_input() {
    let temp_dir = tempfile::tempdir().unwrap();
    let rules = temp_dir.path().join("rules.yaml");
    let input = temp_dir.path().join("input.yaml");
    std::fs::write(
        &rules,
        r#"
version: 2
input:
  format: yaml
  yaml:
    records_path: users
mappings:
  - target: "id"
    source: "id"
  - target: "name"
    source: "name"
"#,
    )
    .unwrap();
    std::fs::write(
        &input,
        r#"
users:
  - id: "1"
    name: Alice
"#,
    )
    .unwrap();
    assert_simple_transform(&rules, &input);
}

#[test]
fn cli_transform_toml_input() {
    let temp_dir = tempfile::tempdir().unwrap();
    let rules = temp_dir.path().join("rules.yaml");
    let input = temp_dir.path().join("input.toml");
    std::fs::write(
        &rules,
        r#"
version: 2
input:
  format: toml
  toml:
    records_path: users
mappings:
  - target: "id"
    source: "id"
  - target: "name"
    source: "name"
"#,
    )
    .unwrap();
    std::fs::write(
        &input,
        r#"
[[users]]
id = "1"
name = "Alice"
"#,
    )
    .unwrap();
    assert_simple_transform(&rules, &input);
}
