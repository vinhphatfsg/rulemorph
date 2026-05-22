#[test]
fn html_inner_html_is_extracted_without_execution() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: html
  html:
    records_selector: ".article"
    fields:
      body:
        selector: ".body"
        value: html
mappings:
  - target: "body"
    source: "body"
"#,
    )
    .expect("parse rule");
    let output = transform(
        &rule,
        r#"<article class="article"><div class="body"><b>Alice</b><script>fetch("/x")</script></div></article>"#,
        None,
    )
    .expect("transform");
    assert_eq!(
        output,
        serde_json::json!([{ "body": "<b>Alice</b><script>fetch(\"/x\")</script>" }])
    );
}

#[test]
fn html_inner_html_preserves_raw_spacing() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: html
  html:
    records_selector: ".article"
    fields:
      body:
        selector: ".body"
        value: html
mappings:
  - target: "body"
    source: "body"
"#,
    )
    .expect("parse rule");
    let output = transform(
        &rule,
        r#"<article class="article"><div class="body"><span> Alice   Smith </span></div></article>"#,
        None,
    )
    .expect("transform");
    assert_eq!(
        output,
        serde_json::json!([{ "body": "<span> Alice   Smith </span>" }])
    );
}
