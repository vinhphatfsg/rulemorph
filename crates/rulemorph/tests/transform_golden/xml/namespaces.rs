#[test]
fn xml_allows_scoped_namespace_prefix_shadowing() {
    let rule = parse_rule_file(
        r##"
version: 2
input:
  format: xml
  xml:
    records_path: users.user
mappings:
  - target: "outer_name"
    source: 'input.["a:name"][0]["#text"]'
  - target: "inner_name"
    source: 'input.group[0]["a:name"][0]["#text"]'
"##,
    )
    .expect("parse rule");
    let output = transform(
        &rule,
        r#"<users xmlns:a="urn:outer"><user><a:name>Outer</a:name><group xmlns:a="urn:inner"><a:name>Inner</a:name></group></user></users>"#,
        None,
    )
    .expect("scoped namespace shadowing should be valid");
    assert_eq!(
        output,
        serde_json::json!([{ "outer_name": "Outer", "inner_name": "Inner" }])
    );
}

#[test]
fn xml_allows_scoped_default_namespace_shadowing() {
    let rule = parse_rule_file(
        r##"
version: 2
input:
  format: xml
  xml:
    records_path: users.user
mappings:
  - target: "outer_name"
    source: 'input.name[0]["#text"]'
  - target: "inner_name"
    source: 'input.group[0].name[0]["#text"]'
"##,
    )
    .expect("parse rule");
    let output = transform(
        &rule,
        r#"<users xmlns="urn:outer"><user><name>Outer</name><group xmlns="urn:inner"><name>Inner</name></group></user></users>"#,
        None,
    )
    .expect("scoped default namespace shadowing should be valid");
    assert_eq!(
        output,
        serde_json::json!([{ "outer_name": "Outer", "inner_name": "Inner" }])
    );
}
