#[test]
fn t51_markdown_document_structure() {
    assert_text_fixture("t51_markdown_document_structure", "input.md");
}

#[test]
fn t52_markdown_blocks_and_lists() {
    assert_text_fixture("t52_markdown_blocks_and_lists", "input.md");
}

#[test]
fn t53_markdown_inline_structure() {
    assert_text_fixture("t53_markdown_inline_structure", "input.md");
}

#[test]
fn t54_markdown_table_document() {
    assert_text_fixture("t54_markdown_table_document", "input.md");
}

#[test]
fn t55_markdown_sections_projection() {
    assert_text_fixture("t55_markdown_sections_projection", "input.md");
}

#[test]
fn t56_markdown_table_rows_projection() {
    assert_text_fixture("t56_markdown_table_rows_projection", "input.md");
}

#[test]
fn t57_markdown_frontmatter() {
    assert_text_fixture("t57_markdown_frontmatter", "input.md");
}

#[test]
fn t58_markdown_raw_html() {
    assert_text_fixture("t58_markdown_raw_html", "input.md");
}

#[test]
fn markdown_rejects_excessive_preflight_nodes() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: markdown
  markdown: {}
mappings:
  - target: "title"
    source: "input.title"
"#,
    )
    .expect("parse markdown rule");
    let options = NormalizationOptions {
        max_markdown_nodes: 1,
        ..NormalizationOptions::default()
    };
    let err = transform_input_with_options(
        &rule,
        InputData::Text("# One\n\n## Two\n\ntext"),
        None,
        &options,
    )
    .expect_err("markdown node budget should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("max_markdown_nodes"));
}

#[test]
fn markdown_rejects_excessive_table_cells() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: markdown
  markdown:
    records: table_rows
mappings:
  - target: "field"
    source: "input.object.Field"
"#,
    )
    .expect("parse markdown rule");
    let options = NormalizationOptions {
        max_markdown_table_cells: 1,
        ..NormalizationOptions::default()
    };
    let err = transform_input_with_options(
        &rule,
        InputData::Text("| Field | Type |\n| --- | --- |\n| id | string |"),
        None,
        &options,
    )
    .expect_err("markdown table cell budget should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("max_markdown_table_cells"));
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
                "section_id": "s1",
                "parent_block_id": null,
                "level": 1,
                "text": "Guide",
                "inlines": [{ "type": "text", "text": "Guide" }]
            }, {
                "id": "b2",
                "type": "paragraph",
                "section_id": "s1",
                "parent_block_id": null,
                "text": "raw",
                "inlines": [{ "type": "text", "text": "raw" }]
            }]
        }])
    );
}

#[test]
fn markdown_duplicate_table_headers_fail_in_strict_mode() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: markdown
  markdown:
    records: table_rows
    table_header_policy: strict
mappings:
  - target: "object"
    source: "input.object"
"#,
    )
    .expect("parse markdown rule");
    let err = transform(
        &rule,
        "| Field | Field |\n| --- | --- |\n| id | duplicate |",
        None,
    )
    .expect_err("duplicate strict headers should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("table headers"));
}

#[test]
fn markdown_duplicate_table_headers_can_use_index_policy() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: markdown
  markdown:
    records: table_rows
    table_header_policy: index
mappings:
  - target: "first"
    source: "input.object.col_0"
  - target: "second"
    source: "input.object.col_1"
"#,
    )
    .expect("parse markdown rule");
    let output = transform(
        &rule,
        "| Field | Field |\n| --- | --- |\n| id | duplicate |",
        None,
    )
    .expect("index policy should pass");
    assert_eq!(
        output,
        serde_json::json!([{ "first": "id", "second": "duplicate" }])
    );
}

#[test]
fn markdown_frontmatter_must_be_object() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: markdown
  markdown: {}
mappings:
  - target: "title"
    source: "input.title"
"#,
    )
    .expect("parse markdown rule");
    let err = transform(&rule, "---\n- bad\n---\n# Guide", None)
        .expect_err("array frontmatter should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("frontmatter must be an object"));
}
