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
