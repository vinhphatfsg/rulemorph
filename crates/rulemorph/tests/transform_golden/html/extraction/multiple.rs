#[test]
fn html_multiple_no_match_returns_empty_array() {
    let yaml = r#"
version: 2
input:
  format: html
  html:
    records_selector: ".article"
    fields:
      tags:
        selector: ".tag"
        value: text
        multiple: true
mappings:
  - target: "tags"
    source: "tags"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let output =
        transform(&rule, r#"<article class="article"></article>"#, None).expect("transform");
    assert_eq!(output, serde_json::json!([{ "tags": [] }]));
}

#[test]
fn html_multiple_missing_attrs_are_excluded() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: html
  html:
    records_selector: ".article"
    fields:
      urls:
        selector: "a"
        value: attr
        attr: href
        multiple: true
mappings:
  - target: "urls"
    source: "urls"
"#,
    )
    .expect("parse rule");
    let output = transform(
        &rule,
        r#"<article class="article"><a>missing</a><a href="/ok">ok</a></article>"#,
        None,
    )
    .expect("transform");
    assert_eq!(output, serde_json::json!([{ "urls": ["/ok"] }]));
}
