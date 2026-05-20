#[test]
fn html_rejects_invalid_selector() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: html
  html:
    records_selector: ".item["
    fields:
      name:
        value: text
mappings:
  - target: "name"
    source: "name"
"#,
    )
    .expect("parse rule");
    let err = transform(&rule, r#"<p class="item">Alice</p>"#, None)
        .expect_err("selector parse should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}
