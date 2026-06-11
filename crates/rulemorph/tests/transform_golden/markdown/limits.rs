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
