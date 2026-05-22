#[test]
fn xml_input_normalizes_attributes_text_and_repeated_children() {
    let yaml = r##"
version: 2
input:
  format: xml
  xml:
    records_path: users.user
    attr_prefix: "@"
    text_key: "#text"
    child_policy: array
mappings:
  - target: "id"
    source: 'input.["@id"]'
  - target: "name"
    source: 'input.name[0]["#text"]'
  - target: "first_role"
    source: 'input.role[0]["#text"]'
"##;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let input = r#"<users><user id="1"><name>Alice</name><role>admin</role><role>editor</role></user></users>"#;
    let output = transform(&rule, input, None).expect("transform");
    assert_eq!(
        output,
        serde_json::json!([{ "id": "1", "name": "Alice", "first_role": "admin" }])
    );
}

#[test]
fn xml_mixed_content_preserves_token_separators_before_normalization() {
    let yaml = r##"
version: 2
input:
  format: xml
  xml:
    records_path: root
    text_key: "#text"
mappings:
  - target: "text"
    source: 'input.["#text"]'
"##;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let input = r#"<root>hello <b>ignored</b> world</root>"#;
    let output = transform(&rule, input, None).expect("transform");
    assert_eq!(output, serde_json::json!([{ "text": "hello world" }]));
}
