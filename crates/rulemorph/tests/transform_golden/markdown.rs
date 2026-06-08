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
fn markdown_rejects_oversized_paragraph_text_during_collection() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: markdown
  markdown:
    include:
      body_text: false
      blocks: false
      links: false
      images: false
      code_blocks: false
      tables: false
      raw_html: false
mappings:
  - target: "record_type"
    source: "input.record_type"
"#,
    )
    .expect("parse markdown rule");
    let options = NormalizationOptions {
        max_text_bytes: 8,
        ..NormalizationOptions::default()
    };

    let err = transform_input_with_options(
        &rule,
        InputData::Text("oversized"),
        None,
        &options,
    )
    .expect_err("oversized markdown paragraph text should fail during collection");

    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("max_text_bytes"));
}

#[test]
fn markdown_does_not_reject_hidden_body_text_aggregate() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: markdown
  markdown:
    include:
      body_text: false
      blocks: false
      links: false
      images: false
      code_blocks: false
      tables: false
      raw_html: false
mappings:
  - target: "record_type"
    source: "input.record_type"
"#,
    )
    .expect("parse markdown rule");
    let options = NormalizationOptions {
        max_text_bytes: 8,
        ..NormalizationOptions::default()
    };

    let output = transform_input_with_options(
        &rule,
        InputData::Text("small\n\nsmall\n\nsmall"),
        None,
        &options,
    )
    .expect("hidden document body_text aggregate should not fail");

    assert_eq!(output, serde_json::json!([{ "record_type": "document" }]));
}

#[test]
fn markdown_does_not_reject_hidden_list_text_aggregate() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: markdown
  markdown:
    include:
      body_text: false
      blocks: false
      links: false
      images: false
      code_blocks: false
      tables: false
      raw_html: false
mappings:
  - target: "record_type"
    source: "input.record_type"
"#,
    )
    .expect("parse markdown rule");
    let options = NormalizationOptions {
        max_text_bytes: 8,
        ..NormalizationOptions::default()
    };

    let output = transform_input_with_options(
        &rule,
        InputData::Text("- aa\n- aa\n- aa\n- aa\n- aa"),
        None,
        &options,
    )
    .expect("hidden list aggregate text should not fail");

    assert_eq!(output, serde_json::json!([{ "record_type": "document" }]));
}

#[test]
fn markdown_does_not_reject_hidden_link_url() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: markdown
  markdown:
    include:
      blocks: false
      links: false
mappings:
  - target: "body_text"
    source: "input.body_text"
"#,
    )
    .expect("parse markdown rule");
    let options = NormalizationOptions {
        max_text_bytes: 8,
        ..NormalizationOptions::default()
    };

    let output = transform_input_with_options(
        &rule,
        InputData::Text("[ok](https://example.com/oversized-link-destination)"),
        None,
        &options,
    )
    .expect("hidden link URL should not fail text limits");

    assert_eq!(output, serde_json::json!([{ "body_text": "ok" }]));
}

#[test]
fn markdown_does_not_reject_hidden_image_url() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: markdown
  markdown:
    include:
      blocks: false
      images: false
mappings:
  - target: "body_text"
    source: "input.body_text"
"#,
    )
    .expect("parse markdown rule");
    let options = NormalizationOptions {
        max_text_bytes: 8,
        ..NormalizationOptions::default()
    };

    let output = transform_input_with_options(
        &rule,
        InputData::Text("![ok](https://example.com/oversized-image-destination)"),
        None,
        &options,
    )
    .expect("hidden image URL should not fail text limits");

    assert_eq!(output, serde_json::json!([{ "body_text": "ok" }]));
}

#[test]
fn markdown_does_not_reject_hidden_image_title() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: markdown
  markdown:
    include:
      blocks: false
      images: false
mappings:
  - target: "body_text"
    source: "input.body_text"
"#,
    )
    .expect("parse markdown rule");
    let options = NormalizationOptions {
        max_text_bytes: 8,
        ..NormalizationOptions::default()
    };

    let output = transform_input_with_options(
        &rule,
        InputData::Text("![ok](x \"oversized-title\")"),
        None,
        &options,
    )
    .expect("hidden image title should not fail text limits");

    assert_eq!(output, serde_json::json!([{ "body_text": "ok" }]));
}

#[test]
fn markdown_sections_do_not_reject_hidden_document_body_text_aggregate() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: markdown
  markdown:
    records: sections
    section_levels: [2]
mappings:
  - target: "heading"
    source: "input.heading"
  - target: "body_text"
    source: "input.body_text"
"#,
    )
    .expect("parse markdown rule");
    let options = NormalizationOptions {
        max_text_bytes: 16,
        ..NormalizationOptions::default()
    };

    let output = transform_input_with_options(
        &rule,
        InputData::Text("# Top\n\n## A\nsmall\n\n## B\nsmall"),
        None,
        &options,
    )
    .expect("hidden document body_text aggregate should not fail section projection");

    assert_eq!(
        output,
        serde_json::json!([
            { "heading": "A", "body_text": "small" },
            { "heading": "B", "body_text": "small" }
        ])
    );
}

#[test]
fn markdown_table_rows_do_not_reject_hidden_document_body_text_aggregate() {
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
  - target: "type"
    source: "input.object.Type"
"#,
    )
    .expect("parse markdown rule");
    let options = NormalizationOptions {
        max_text_bytes: 16,
        ..NormalizationOptions::default()
    };

    let output = transform_input_with_options(
        &rule,
        InputData::Text("| Field | Type |\n| --- | --- |\n| id | str |\n| nm | str |"),
        None,
        &options,
    )
    .expect("hidden document body_text aggregate should not fail table row projection");

    assert_eq!(
        output,
        serde_json::json!([
            { "field": "id", "type": "str" },
            { "field": "nm", "type": "str" }
        ])
    );
}

#[test]
fn markdown_does_not_reject_hidden_table_aggregate_text() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: markdown
  markdown:
    records: table_rows
    include:
      body_text: false
      blocks: false
      tables: true
mappings:
  - target: "field"
    source: "input.object.Field"
  - target: "type"
    source: "input.object.Type"
"#,
    )
    .expect("parse markdown rule");
    let options = NormalizationOptions {
        max_text_bytes: 16,
        ..NormalizationOptions::default()
    };

    let output = transform_input_with_options(
        &rule,
        InputData::Text("| Field | Type |\n| --- | --- |\n| id | string |\n| nm | string |"),
        None,
        &options,
    )
    .expect("hidden table aggregate text should not fail text limits");

    assert_eq!(
        output,
        serde_json::json!([
            { "field": "id", "type": "string" },
            { "field": "nm", "type": "string" }
        ])
    );
}

#[test]
fn markdown_rejects_oversized_code_block_text_during_collection() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: markdown
  markdown:
    include:
      body_text: false
      blocks: false
      links: false
      images: false
      code_blocks: false
      tables: false
      raw_html: false
mappings:
  - target: "record_type"
    source: "input.record_type"
"#,
    )
    .expect("parse markdown rule");
    let options = NormalizationOptions {
        max_text_bytes: 8,
        ..NormalizationOptions::default()
    };

    let err = transform_input_with_options(
        &rule,
        InputData::Text("```\noversized\n```"),
        None,
        &options,
    )
    .expect_err("oversized markdown code block text should fail during collection");

    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("max_text_bytes"));
}

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
fn markdown_commonmark_pipe_text_does_not_count_as_table_cells() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: markdown
  markdown:
    flavor: commonmark
mappings:
  - target: "body_text"
    source: "input.body_text"
"#,
    )
    .expect("parse markdown rule");
    let options = NormalizationOptions {
        max_markdown_table_cells: 1,
        ..NormalizationOptions::default()
    };

    transform_input_with_options(
        &rule,
        InputData::Text("| Field | Type |\n| --- | --- |"),
        None,
        &options,
    )
    .expect("commonmark pipe text should not be counted as table cells");
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

#[test]
fn markdown_unsupported_body_markdown_is_rejected_during_transform() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: markdown
  markdown:
    include:
      body_markdown: true
mappings:
  - target: "body_text"
    source: "input.body_text"
"#,
    )
    .expect("parse markdown rule");

    let err = transform(&rule, "# Guide", None)
        .expect_err("unsupported body_markdown should fail during transform");

    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("body_markdown"));
}

#[test]
fn markdown_unsupported_sourcepos_is_rejected_during_transform() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: markdown
  markdown:
    include:
      sourcepos: true
mappings:
  - target: "body_text"
    source: "input.body_text"
"#,
    )
    .expect("parse markdown rule");

    let err =
        transform(&rule, "# Guide", None).expect_err("unsupported sourcepos should fail during transform");

    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("sourcepos"));
}

#[test]
fn markdown_section_levels_are_rejected_during_transform() {
    for (section_levels, expected) in [
        ("[]", "section_levels must not be empty"),
        ("[0]", "section_levels entries must be 1..=6"),
        ("[7]", "section_levels entries must be 1..=6"),
        ("[2, 2]", "section_levels entries must be unique"),
    ] {
        let rule = parse_rule_file(&format!(
            r#"
version: 2
input:
  format: markdown
  markdown:
    records: sections
    section_levels: {section_levels}
mappings:
  - target: "heading"
    source: "input.heading"
"#
        ))
        .expect("parse markdown rule");

        let err = transform(&rule, "# Guide", None)
            .expect_err("invalid section_levels should fail during transform");

        assert_eq!(err.kind, TransformErrorKind::InvalidInput);
        assert!(
            err.message.contains(expected),
            "expected {expected:?}, got {:?}",
            err.message
        );
    }
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
fn markdown_blank_table_headers_fail_in_strict_mode_even_when_text_is_not_trimmed() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: markdown
  markdown:
    records: table_rows
    table_header_policy: strict
    trim_text: false
    collapse_whitespace: false
mappings:
  - target: "object"
    source: "input.object"
"#,
    )
    .expect("parse markdown rule");
    let err = transform(&rule, "| `   ` | Type |\n| --- | --- |\n| id | string |", None)
        .expect_err("blank strict headers should fail");
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

#[test]
fn markdown_yaml_frontmatter_obeys_alias_limit() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: markdown
  markdown: {}
mappings:
  - target: "frontmatter"
    source: "input.frontmatter"
"#,
    )
    .expect("parse markdown rule");
    let options = NormalizationOptions {
        max_yaml_aliases: 1,
        ..NormalizationOptions::default()
    };
    let err = transform_input_with_options(
        &rule,
        InputData::Text("---\nbase: &base value\none: *base\ntwo: *base\n---\n# Guide"),
        None,
        &options,
    )
    .expect_err("yaml frontmatter should obey alias limits");

    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("max_yaml_aliases"));
}

#[test]
fn markdown_frontmatter_accepts_crlf_delimiters_in_auto_mode() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: markdown
  markdown: {}
mappings:
  - target: "owner"
    source: "input.frontmatter.owner"
  - target: "title"
    source: "input.title"
"#,
    )
    .expect("parse markdown rule");
    let output = transform(&rule, "---\r\nowner: docs\r\n---\r\n# Guide", None)
        .expect("crlf frontmatter should parse");
    assert_eq!(
        output,
        serde_json::json!([{ "owner": "docs", "title": "Guide" }])
    );
}

#[test]
fn markdown_frontmatter_accepts_eof_closing_delimiter() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: markdown
  markdown:
    frontmatter: toml
mappings:
  - target: "owner"
    source: "input.frontmatter.owner"
  - target: "title"
    source: "input.title"
"#,
    )
    .expect("parse markdown rule");
    let output = transform(&rule, "+++\nowner = \"docs\"\ntitle = \"Guide\"\n+++", None)
        .expect("eof frontmatter closing delimiter should parse");
    assert_eq!(
        output,
        serde_json::json!([{ "owner": "docs", "title": "Guide" }])
    );
}

#[test]
fn markdown_frontmatter_auto_treats_unclosed_delimiter_as_body() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: markdown
  markdown: {}
mappings:
  - target: "frontmatter"
    source: "input.frontmatter"
  - target: "title"
    source: "input.title"
  - target: "body_text"
    source: "input.body_text"
"#,
    )
    .expect("parse markdown rule");
    let output =
        transform(&rule, "---\n# Guide", None).expect("unclosed auto delimiter should be body");
    assert_eq!(
        output,
        serde_json::json!([{ "frontmatter": {}, "title": "Guide", "body_text": "Guide" }])
    );
}

#[test]
fn markdown_table_rows_projection_ignores_document_table_output_flag() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: markdown
  markdown:
    records: table_rows
    include:
      tables: false
mappings:
  - target: "field"
    source: "input.object.Field"
  - target: "type"
    source: "input.object.Type"
"#,
    )
    .expect("parse markdown rule");
    let output = transform(
        &rule,
        "| Field | Type |\n| --- | --- |\n| id | string |",
        None,
    )
    .expect("table_rows projection should not depend on include.tables");
    assert_eq!(
        output,
        serde_json::json!([{ "field": "id", "type": "string" }])
    );
}

#[test]
fn markdown_table_rows_projection_obeys_record_limit() {
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
        max_records: 1,
        ..NormalizationOptions::default()
    };
    let err = transform_input_with_options(
        &rule,
        InputData::Text("| Field |\n| --- |\n| id |\n| name |"),
        None,
        &options,
    )
    .expect_err("table_rows projection should obey max_records");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("max_records"));
}

#[test]
fn markdown_sections_projection_obeys_record_limit() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: markdown
  markdown:
    records: sections
    section_levels: [1]
mappings:
  - target: "heading"
    source: "input.heading"
"#,
    )
    .expect("parse markdown rule");
    let options = NormalizationOptions {
        max_records: 1,
        ..NormalizationOptions::default()
    };
    let err = transform_input_with_options(
        &rule,
        InputData::Text("# One\n\n# Two"),
        None,
        &options,
    )
    .expect_err("sections projection should obey max_records");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("max_records"));
}

#[test]
fn markdown_section_blocks_include_nested_container_children() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: markdown
  markdown:
    records: sections
    section_levels: [1]
mappings:
  - target: "blocks"
    source: "input.blocks"
"#,
    )
    .expect("parse markdown rule");
    let output = transform(&rule, "# Tasks\n\n- Write tests\n\n> Keep notes", None)
        .expect("section projection should include resolvable nested container blocks");
    assert_eq!(
        output,
        serde_json::json!([{
            "blocks": [
                {
                    "id": "b1",
                    "inlines": [{ "type": "text", "text": "Tasks" }],
                    "level": 1,
                    "parent_block_id": null,
                    "section_id": "s1-1",
                    "text": "Tasks",
                    "type": "heading"
                },
                {
                    "id": "b2",
                    "inlines": [],
                    "item_ids": ["b3"],
                    "ordered": false,
                    "parent_block_id": null,
                    "section_id": "s1-1",
                    "start": null,
                    "text": "Write tests",
                    "tight": true,
                    "type": "list"
                },
                {
                    "checked": null,
                    "child_block_ids": ["b4"],
                    "id": "b3",
                    "inlines": [],
                    "ordinal": null,
                    "parent_block_id": "b2",
                    "section_id": "s1-1",
                    "text": "Write tests",
                    "type": "list_item"
                },
                {
                    "id": "b4",
                    "inlines": [{ "type": "text", "text": "Write tests" }],
                    "parent_block_id": "b3",
                    "section_id": "s1-1",
                    "text": "Write tests",
                    "type": "paragraph"
                },
                {
                    "child_block_ids": ["b6"],
                    "id": "b5",
                    "inlines": [],
                    "parent_block_id": null,
                    "section_id": "s1-1",
                    "text": "Keep notes",
                    "type": "blockquote"
                },
                {
                    "id": "b6",
                    "inlines": [{ "type": "text", "text": "Keep notes" }],
                    "parent_block_id": "b5",
                    "section_id": "s1-1",
                    "text": "Keep notes",
                    "type": "paragraph"
                }
            ]
        }])
    );
}

#[test]
fn markdown_section_blocks_include_own_heading_block() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: markdown
  markdown:
    records: sections
    section_levels: [2]
    include:
      blocks: true
mappings:
  - target: "heading_block_id"
    source: "input.heading_block_id"
  - target: "blocks"
    source: "input.blocks"
"#,
    )
    .expect("parse markdown rule");
    let output = transform(&rule, "# Guide\n\n## Usage\n\nBody", None)
        .expect("section projection should include heading block");
    assert_eq!(
        output,
        serde_json::json!([{
            "heading_block_id": "b2",
            "blocks": [{
                "id": "b2",
                "inlines": [{ "type": "text", "text": "Usage" }],
                "level": 2,
                "parent_block_id": null,
                "section_id": "s1-1.s2-1",
                "text": "Usage",
                "type": "heading"
            }, {
                "id": "b3",
                "inlines": [{ "type": "text", "text": "Body" }],
                "parent_block_id": null,
                "section_id": "s1-1.s2-1",
                "text": "Body",
                "type": "paragraph"
            }]
        }])
    );
}

#[test]
fn markdown_nested_headings_open_document_sections() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: markdown
  markdown:
    include:
      blocks: true
mappings:
  - target: "section_index"
    source: "input.section_index"
  - target: "blocks"
    source: "input.blocks"
"#,
    )
    .expect("parse markdown rule");
    let output = transform(&rule, "# Top\n\n> ## Quoted\n> inside\n\nAfter", None)
        .expect("nested headings should transform");
    assert_eq!(
        output,
        serde_json::json!([{
            "section_index": [
                { "id": "s1-1", "level": 1, "heading": "Top", "path": ["Top"], "ordinal_path": [1] },
                { "id": "s1-1.s2-1", "level": 2, "heading": "Quoted", "path": ["Top", "Quoted"], "ordinal_path": [1, 1] }
            ],
            "blocks": [
                {
                    "id": "b1",
                    "inlines": [{ "type": "text", "text": "Top" }],
                    "level": 1,
                    "parent_block_id": null,
                    "section_id": "s1-1",
                    "text": "Top",
                    "type": "heading"
                },
                {
                    "child_block_ids": ["b3", "b4"],
                    "id": "b2",
                    "inlines": [],
                    "parent_block_id": null,
                    "section_id": "s1-1",
                    "text": "Quoted inside",
                    "type": "blockquote"
                },
                {
                    "id": "b3",
                    "inlines": [{ "type": "text", "text": "Quoted" }],
                    "level": 2,
                    "parent_block_id": "b2",
                    "section_id": "s1-1.s2-1",
                    "text": "Quoted",
                    "type": "heading"
                },
                {
                    "id": "b4",
                    "inlines": [{ "type": "text", "text": "inside" }],
                    "parent_block_id": "b2",
                    "section_id": "s1-1.s2-1",
                    "text": "inside",
                    "type": "paragraph"
                },
                {
                    "id": "b5",
                    "inlines": [{ "type": "text", "text": "After" }],
                    "parent_block_id": null,
                    "section_id": "s1-1.s2-1",
                    "text": "After",
                    "type": "paragraph"
                }
            ]
        }])
    );
}

#[test]
fn markdown_nested_heading_section_projection_keeps_nested_body() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: markdown
  markdown:
    records: sections
    section_levels: [2]
    include:
      blocks: true
mappings:
  - target: "heading"
    source: "input.heading"
  - target: "body_text"
    source: "input.body_text"
  - target: "content_block_ids"
    source: "input.content_block_ids"
  - target: "blocks"
    source: "input.blocks"
"#,
    )
    .expect("parse markdown rule");
    let output = transform(&rule, "# Top\n\n> ## Quoted\n> inside\n\nAfter", None)
        .expect("nested heading sections should transform");
    assert_eq!(
        output,
        serde_json::json!([{
            "heading": "Quoted",
            "body_text": "inside After",
            "content_block_ids": ["b4", "b5"],
            "blocks": [
                {
                    "child_block_ids": ["b3", "b4"],
                    "id": "b2",
                    "inlines": [],
                    "parent_block_id": null,
                    "section_id": "s1-1",
                    "text": "Quoted inside",
                    "type": "blockquote"
                },
                {
                    "id": "b3",
                    "inlines": [{ "type": "text", "text": "Quoted" }],
                    "level": 2,
                    "parent_block_id": "b2",
                    "section_id": "s1-1.s2-1",
                    "text": "Quoted",
                    "type": "heading"
                },
                {
                    "id": "b4",
                    "inlines": [{ "type": "text", "text": "inside" }],
                    "parent_block_id": "b2",
                    "section_id": "s1-1.s2-1",
                    "text": "inside",
                    "type": "paragraph"
                },
                {
                    "id": "b5",
                    "inlines": [{ "type": "text", "text": "After" }],
                    "parent_block_id": null,
                    "section_id": "s1-1.s2-1",
                    "text": "After",
                    "type": "paragraph"
                }
            ]
        }])
    );
}

#[test]
fn markdown_section_projection_filters_container_refs_to_projected_blocks() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: markdown
  markdown:
    records: sections
    section_levels: [2]
    include:
      blocks: true
mappings:
  - target: "blocks"
    source: "input.blocks"
"#,
    )
    .expect("parse markdown rule");
    let output = transform(&rule, "# Top\n\n- Before\n- ## Nested\n  Inside", None)
        .expect("nested list heading section should transform");
    assert_eq!(
        output,
        serde_json::json!([{
            "blocks": [
                {
                    "id": "b2",
                    "inlines": [],
                    "item_ids": ["b5"],
                    "ordered": false,
                    "parent_block_id": null,
                    "section_id": "s1-1",
                    "start": null,
                    "text": "Before Nested Inside",
                    "tight": true,
                    "type": "list"
                },
                {
                    "checked": null,
                    "child_block_ids": ["b6", "b7"],
                    "id": "b5",
                    "inlines": [],
                    "ordinal": null,
                    "parent_block_id": "b2",
                    "section_id": "s1-1",
                    "text": "Nested Inside",
                    "type": "list_item"
                },
                {
                    "id": "b6",
                    "inlines": [{ "type": "text", "text": "Nested" }],
                    "level": 2,
                    "parent_block_id": "b5",
                    "section_id": "s1-1.s2-1",
                    "text": "Nested",
                    "type": "heading"
                },
                {
                    "id": "b7",
                    "inlines": [{ "type": "text", "text": "Inside" }],
                    "parent_block_id": "b5",
                    "section_id": "s1-1.s2-1",
                    "text": "Inside",
                    "type": "paragraph"
                }
            ]
        }])
    );
}

#[test]
fn markdown_section_body_text_includes_nested_code_blocks() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: markdown
  markdown:
    records: sections
    section_levels: [1]
mappings:
  - target: "body_text"
    source: "input.body_text"
"#,
    )
    .expect("parse markdown rule");
    let output = transform(&rule, "# Guide\n\n- Step\n\n  ```sh\n  cargo test\n  ```", None)
        .expect("section projection should include code block text in container text");
    assert_eq!(
        output,
        serde_json::json!([{ "body_text": "Step cargo test" }])
    );
}

#[test]
fn markdown_strict_table_keys_match_headers_when_text_is_not_trimmed() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: markdown
  markdown:
    records: table_rows
    table_header_policy: strict
    trim_text: false
    collapse_whitespace: false
mappings:
  - target: "headers"
    source: "input.headers"
  - target: "object"
    source: "input.object"
"#,
    )
    .expect("parse markdown rule");
    let output = transform(
        &rule,
        "| `  Field  ` | Type |\n| --- | --- |\n| id | string |",
        None,
    )
        .expect("strict header keys should match headers");
    assert_eq!(
        output,
        serde_json::json!([{
            "headers": [" Field ", "Type"],
            "object": {
                " Field ": "id",
                "Type": "string"
            }
        }])
    );
}

#[test]
fn markdown_section_ids_include_heading_levels_to_avoid_collisions() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: markdown
  markdown: {}
mappings:
  - target: "section_index"
    source: "input.section_index"
"#,
    )
    .expect("parse markdown rule");
    let output = transform(
        &rule,
        "## First\n\n# Second\n\n# Third\n\n### Deep\n\n## Shallow",
        None,
    )
    .expect("transform");
    assert_eq!(
        output,
        serde_json::json!([{
            "section_index": [
                { "id": "s2-1", "level": 2, "heading": "First", "path": ["First"], "ordinal_path": [1] },
                { "id": "s1-1", "level": 1, "heading": "Second", "path": ["Second"], "ordinal_path": [1] },
                { "id": "s1-2", "level": 1, "heading": "Third", "path": ["Third"], "ordinal_path": [2] },
                { "id": "s1-2.s3-1", "level": 3, "heading": "Deep", "path": ["Third", "Deep"], "ordinal_path": [2, 1] },
                { "id": "s1-2.s2-1", "level": 2, "heading": "Shallow", "path": ["Third", "Shallow"], "ordinal_path": [2, 1] }
            ]
        }])
    );
}

#[test]
fn markdown_toml_frontmatter_obeys_text_limit() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: markdown
  markdown:
    frontmatter: toml
mappings:
  - target: "frontmatter"
    source: "input.frontmatter"
"#,
    )
    .expect("parse markdown rule");
    let options = NormalizationOptions {
        max_text_bytes: 4,
        ..NormalizationOptions::default()
    };
    let err = transform_input_with_options(
        &rule,
        InputData::Text("+++\nowner = \"docs-team\"\n+++\n# Guide"),
        None,
        &options,
    )
    .expect_err("toml frontmatter should obey text limits");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("max_text_bytes"));
}
