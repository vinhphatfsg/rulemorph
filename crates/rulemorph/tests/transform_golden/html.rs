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
