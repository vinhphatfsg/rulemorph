#[test]
fn markdown_omits_oversized_inline_raw_html_when_disabled() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: markdown
  markdown:
    include:
      raw_html: false
mappings:
  - target: "body_text"
    source: "input.body_text"
  - target: "blocks"
    source: "input.blocks"
  - target: "raw_html"
    source: "input.raw_html"
"#,
    )
    .expect("parse markdown rule");
    let options = NormalizationOptions {
        max_text_bytes: 16,
        ..NormalizationOptions::default()
    };

    let output = transform_input_with_options(
        &rule,
        InputData::Text("ok <span data-x=\"oversized-raw-literal\"></span>"),
        None,
        &options,
    )
    .expect("disabled inline raw HTML should not fail on omitted literal size");

    assert_eq!(
        output,
        serde_json::json!([{
            "body_text": "ok",
            "blocks": [{
                "id": "b1",
                "type": "paragraph",
                "section_id": "preamble",
                "parent_block_id": null,
                "text": "ok",
                "inlines": [{ "type": "text", "text": "ok " }]
            }]
        }])
    );
}

#[test]
fn markdown_omits_oversized_block_raw_html_when_disabled() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: markdown
  markdown:
    include:
      raw_html: false
mappings:
  - target: "body_text"
    source: "input.body_text"
  - target: "blocks"
    source: "input.blocks"
  - target: "raw_html"
    source: "input.raw_html"
"#,
    )
    .expect("parse markdown rule");
    let options = NormalizationOptions {
        max_text_bytes: 16,
        ..NormalizationOptions::default()
    };

    let output = transform_input_with_options(
        &rule,
        InputData::Text("<div data-x=\"oversized-raw-literal\">ok</div>"),
        None,
        &options,
    )
    .expect("disabled block raw HTML should not fail on omitted literal size");

    assert_eq!(
        output,
        serde_json::json!([{
            "body_text": "ok",
            "blocks": [{
                "id": "b1",
                "type": "html_block",
                "section_id": "preamble",
                "parent_block_id": null,
                "text": "ok",
                "inlines": []
            }]
        }])
    );
}

#[test]
fn markdown_raw_html_is_preserved_by_default_and_can_be_omitted() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: markdown
  markdown:
    include:
      raw_html: false
mappings:
  - target: "body_text"
    source: "input.body_text"
  - target: "blocks"
    source: "input.blocks"
  - target: "raw_html"
    source: "input.raw_html"
"#,
    )
    .expect("parse markdown rule");
    let output = transform(&rule, "# Guide\n\n<span>raw</span>", None).expect("transform");
    assert_eq!(
        output,
        serde_json::json!([{
            "body_text": "Guide raw",
            "blocks": [{
                "id": "b1",
                "type": "heading",
                "section_id": "s1-1",
                "parent_block_id": null,
                "level": 1,
                "text": "Guide",
                "inlines": [{ "type": "text", "text": "Guide" }]
            }, {
                "id": "b2",
                "type": "paragraph",
                "section_id": "s1-1",
                "parent_block_id": null,
                "text": "raw",
                "inlines": [{ "type": "text", "text": "raw" }]
            }]
        }])
    );
}

#[test]
fn markdown_raw_html_disabled_keeps_html_block_structure() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: markdown
  markdown:
    include:
      raw_html: false
mappings:
  - target: "body_text"
    source: "input.body_text"
  - target: "blocks"
    source: "input.blocks"
  - target: "raw_html"
    source: "input.raw_html"
"#,
    )
    .expect("parse markdown rule");
    let output = transform(&rule, "# Guide\n\n<div>note</div>\n\nAfter", None).expect("transform");
    assert_eq!(
        output,
        serde_json::json!([{
            "body_text": "Guide note After",
            "blocks": [{
                "id": "b1",
                "type": "heading",
                "section_id": "s1-1",
                "parent_block_id": null,
                "level": 1,
                "text": "Guide",
                "inlines": [{ "type": "text", "text": "Guide" }]
            }, {
                "id": "b2",
                "type": "html_block",
                "section_id": "s1-1",
                "parent_block_id": null,
                "text": "note",
                "inlines": []
            }, {
                "id": "b3",
                "type": "paragraph",
                "section_id": "s1-1",
                "parent_block_id": null,
                "text": "After",
                "inlines": [{ "type": "text", "text": "After" }]
            }]
        }])
    );
}
