#[test]
fn html_allows_literal_less_than_sequences_when_dom_nodes_within_limit() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: html
  html:
    records_selector: ".item"
    fields:
      name:
        selector: ".name"
        value: text
mappings:
  - target: "name"
    source: "name"
"#,
    )
    .expect("parse rule");
    let options = NormalizationOptions {
        max_html_nodes: 20,
        ..NormalizationOptions::default()
    };
    let literal_tags = "<article><aside><a><address><abbr><area><audio><bdi><bdo><base><button>";
    let input = format!(
        r#"<article class="item"><script>const sample = "{literal_tags}";</script><span class="name">Alice</span></article>"#
    );
    let output = normalize_records_with_options(&rule, InputData::Text(&input), &options)
        .expect("literal less-than sequences should not count as parsed DOM nodes")
        .collect::<Result<Vec<_>, _>>()
        .expect("records should normalize");
    assert_eq!(output, vec![serde_json::json!({ "name": "Alice" })]);
}
