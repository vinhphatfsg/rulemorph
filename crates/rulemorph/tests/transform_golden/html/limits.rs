#[test]
fn html_rejects_array_limit_exceeded() {
    let rule = parse_rule_file(
        r#"
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
"#,
    )
    .expect("parse rule");
    let options = NormalizationOptions {
        max_array_len: 1,
        ..NormalizationOptions::default()
    };
    let err = normalize_records_with_options(
        &rule,
        InputData::Text(
            r#"<article class="article"><span class="tag">a</span><span class="tag">b</span></article>"#,
        ),
        &options,
    )
    .expect_err("array limit should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn html_rejects_node_limit_exceeded() {
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
    let options = NormalizationOptions {
        max_html_nodes: 1,
        ..NormalizationOptions::default()
    };
    let err = normalize_records_with_options(
        &rule,
        InputData::Text(r#"<div><p class="item">Alice</p></div>"#),
        &options,
    )
    .expect_err("node limit should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn html_rejects_parser_created_node_limit_exceeded() {
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
    let options = NormalizationOptions {
        max_html_nodes: 1,
        ..NormalizationOptions::default()
    };
    let err = normalize_records_with_options(
        &rule,
        InputData::Text(r#"<p class="item">Alice</p>"#),
        &options,
    )
    .expect_err("parsed DOM node limit should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

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
