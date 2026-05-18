#[test]
fn cli_transform_yaml_input() {
    let temp_dir = tempfile::tempdir().unwrap();
    let rules = temp_dir.path().join("rules.yaml");
    let input = temp_dir.path().join("input.yaml");
    fs::write(
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
    fs::write(
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
    fs::write(
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
    fs::write(
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

#[test]
fn cli_transform_xml_input() {
    let temp_dir = tempfile::tempdir().unwrap();
    let rules = temp_dir.path().join("rules.yaml");
    let input = temp_dir.path().join("input.xml");
    fs::write(
        &rules,
        r##"
version: 2
input:
  format: xml
  xml:
    records_path: users.user
    attr_prefix: "@"
    text_key: "#text"
mappings:
  - target: "id"
    source: 'input.["@id"]'
  - target: "name"
    source: 'input.name[0]["#text"]'
"##,
    )
    .unwrap();
    fs::write(
        &input,
        r#"<users><user id="1"><name>Alice</name></user></users>"#,
    )
    .unwrap();
    assert_simple_transform(&rules, &input);
}

#[test]
fn cli_transform_html_input() {
    let temp_dir = tempfile::tempdir().unwrap();
    let rules = temp_dir.path().join("rules.yaml");
    let input = temp_dir.path().join("input.html");
    fs::write(
        &rules,
        r#"
version: 2
input:
  format: html
  html:
    records_selector: "table#users tbody tr"
    fields:
      id:
        selector: "td:nth-child(1)"
        value: text
      name:
        selector: "td:nth-child(2)"
        value: text
mappings:
  - target: "id"
    source: "id"
  - target: "name"
    source: "name"
"#,
    )
    .unwrap();
    fs::write(
        &input,
        r#"<table id="users"><tbody><tr><td>1</td><td>Alice</td></tr></tbody></table>"#,
    )
    .unwrap();
    assert_simple_transform(&rules, &input);
}

fn assert_simple_transform(rules: &std::path::Path, input: &std::path::Path) {
    let mut cmd = cargo_bin_cmd!("rulemorph");
    let output = cmd
        .arg("transform")
        .arg("-r")
        .arg(rules)
        .arg("-i")
        .arg(input)
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(0));
    assert_json_stdout_eq(output, &serde_json::json!([{ "id": "1", "name": "Alice" }]));
}
