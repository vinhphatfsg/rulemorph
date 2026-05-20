#[test]
fn html_input_extracts_selector_fields() {
    let yaml = r#"
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
      profile_url:
        selector: "a.profile"
        value: attr
        attr: href
mappings:
  - target: "id"
    source: "id"
  - target: "name"
    source: "name"
  - target: "profile_url"
    source: "profile_url"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let input = r#"<table id="users"><tbody><tr><td>1</td><td>Alice</td><td><a class="profile" href="/users/1">Profile</a></td></tr></tbody></table>"#;
    let output = transform(&rule, input, None).expect("transform");
    assert_eq!(
        output,
        serde_json::json!([{ "id": "1", "name": "Alice", "profile_url": "/users/1" }])
    );
}

#[test]
fn html_field_without_selector_uses_record_element() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: html
  html:
    records_selector: ".item"
    fields:
      name:
        value: text
mappings:
  - target: "name"
    source: "name"
"#,
    )
    .expect("parse rule");
    let output =
        transform(&rule, r#"<p class="item"> Alice <b>Smith</b> </p>"#, None).expect("transform");
    assert_eq!(output, serde_json::json!([{ "name": "Alice Smith" }]));
}
