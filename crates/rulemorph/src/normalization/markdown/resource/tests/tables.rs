use super::super::*;

#[test]
fn preflight_counts_blockquote_list_items_before_parsing() {
    let input = "> - x\n".repeat(4);
    let options = NormalizationOptions {
        max_markdown_nodes: 12,
        ..NormalizationOptions::default()
    };

    let err = enforce_markdown_structural_preflight(&input, true, &options)
        .expect_err("blockquote list items should exceed the preflight node estimate");

    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("max_markdown_nodes"));
}

#[test]
fn preflight_rejects_tab_delimited_list_items_before_parsing() {
    let input = "-\tx\n".repeat(4);
    let options = NormalizationOptions {
        max_markdown_nodes: 8,
        ..NormalizationOptions::default()
    };

    let err = enforce_markdown_structural_preflight(&input, true, &options)
        .expect_err("tab-delimited list items should exceed the preflight node estimate");

    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("max_markdown_nodes"));
}

#[test]
fn preflight_allows_pipe_text_without_table_cells() {
    let input = "echo a | sed s/a/b/ | wc\n".repeat(2);
    let options = NormalizationOptions {
        max_markdown_table_cells: 1,
        ..NormalizationOptions::default()
    };

    enforce_markdown_structural_preflight(&input, true, &options)
        .expect("ordinary pipe text should not count as table cells before parsing");
}

#[test]
fn preflight_ignores_escaped_and_code_span_pipes_in_table_cells() {
    let input = "| Field | Value |\n| --- | --- |\n| a \\| b | `c | d` |";
    let options = NormalizationOptions {
        max_markdown_table_cells: 5,
        ..NormalizationOptions::default()
    };

    enforce_markdown_structural_preflight(input, true, &options)
        .expect("escaped and code-span pipes should stay inside their table cells");
}

#[test]
fn preflight_counts_unmatched_backtick_table_pipes_before_parsing() {
    let input = format!(
        "| Field | Type |\n| --- | --- |\n{}",
        "| `id | string |\n".repeat(3)
    );
    let options = NormalizationOptions {
        max_markdown_table_cells: 5,
        ..NormalizationOptions::default()
    };

    let err = enforce_markdown_structural_preflight(&input, true, &options)
        .expect_err("unmatched backticks should not hide later table separators");

    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("max_markdown_table_cells"));
}

#[test]
fn preflight_keeps_html_block_open_on_closing_tag_prefix() {
    let input = "<script>\n</scripture>\n- item\n- item\n</script>";
    let options = NormalizationOptions {
        max_markdown_nodes: 3,
        ..NormalizationOptions::default()
    };

    enforce_markdown_structural_preflight(input, true, &options)
        .expect("closing tag prefixes should not end an HTML block");
}

#[test]
fn preflight_counts_blockquote_gfm_table_cells_before_parsing() {
    let input = "> | Field | Type |\n> | --- | --- |\n> | id | string |";
    let options = NormalizationOptions {
        max_markdown_table_cells: 3,
        ..NormalizationOptions::default()
    };

    let err = enforce_markdown_structural_preflight(input, true, &options)
        .expect_err("blockquote tables should count table cells before parsing");

    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("max_markdown_table_cells"));
}

#[test]
fn preflight_does_not_merge_table_header_across_blockquote_boundary() {
    let input = "| Field | Type |\n> | --- | --- |\n> | id | string |";
    let options = NormalizationOptions {
        max_markdown_table_cells: 1,
        ..NormalizationOptions::default()
    };

    enforce_markdown_structural_preflight(input, true, &options)
        .expect("table header and delimiter should not merge across quote boundaries");
}

#[test]
fn preflight_does_not_continue_blockquote_table_after_quote_boundary() {
    let input = "> | Field | Type |\n> | --- | --- |\n| id | string |\n| name | string |";
    let options = NormalizationOptions {
        max_markdown_table_cells: 3,
        ..NormalizationOptions::default()
    };

    enforce_markdown_structural_preflight(input, true, &options)
        .expect("blockquote table rows should not continue after the quote boundary");
}

#[test]
fn preflight_ignores_header_delimiter_mismatch_as_non_table() {
    let input = "| A | B |\n| --- | --- | --- |\n| x | y |";
    let options = NormalizationOptions {
        max_markdown_table_cells: 1,
        ..NormalizationOptions::default()
    };

    enforce_markdown_structural_preflight(input, true, &options)
        .expect("mismatched header and delimiter columns should not count as a table");
}

#[test]
fn preflight_counts_short_table_rows_as_header_width() {
    let input = "| A | B | C |\n| --- | --- | --- |\n| x |";
    let options = NormalizationOptions {
        max_markdown_table_cells: 5,
        ..NormalizationOptions::default()
    };

    let err = enforce_markdown_structural_preflight(input, true, &options)
        .expect_err("short table rows should count padded cells before parsing");

    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("max_markdown_table_cells"));
}

#[test]
fn preflight_counts_long_table_rows_as_header_width() {
    let input = "| A | B |\n| --- | --- |\n| x | y | ignored | ignored |";
    let options = NormalizationOptions {
        max_markdown_table_cells: 4,
        ..NormalizationOptions::default()
    };

    enforce_markdown_structural_preflight(input, true, &options)
        .expect("extra table row cells should not overcount parsed table cells");
}

#[test]
fn preflight_does_not_double_count_gfm_table_header_nodes() {
    let input = "| A | B |\n| --- | --- |";
    let options = NormalizationOptions {
        max_markdown_nodes: 7,
        max_markdown_table_cells: 100,
        ..NormalizationOptions::default()
    };

    enforce_markdown_structural_preflight(input, true, &options)
        .expect("table header and delimiter fallback nodes should not be double-counted");
}

#[test]
fn preflight_does_not_double_count_gfm_table_body_row_nodes() {
    let input = "| A | B |\n| --- | --- |\n| x | y |";
    let options = NormalizationOptions {
        max_markdown_nodes: 12,
        max_markdown_table_cells: 100,
        ..NormalizationOptions::default()
    };

    enforce_markdown_structural_preflight(input, true, &options)
        .expect("table body row fallback nodes should not be double-counted");
}

#[test]
fn preflight_counts_blockquote_gfm_table_container_node() {
    let input = "> | A | B |\n> | --- | --- |";
    let options = NormalizationOptions {
        max_markdown_nodes: 7,
        max_markdown_table_cells: 100,
        ..NormalizationOptions::default()
    };

    let err = enforce_markdown_structural_preflight(input, true, &options)
        .expect_err("blockquote table container should count before parsing");

    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("max_markdown_nodes"));
}

#[test]
fn preflight_counts_blockquote_gfm_table_body_row_nodes() {
    let input = "> | A | B |\n> | --- | --- |\n> | x | y |";
    let options = NormalizationOptions {
        max_markdown_nodes: 12,
        max_markdown_table_cells: 100,
        ..NormalizationOptions::default()
    };

    let err = enforce_markdown_structural_preflight(input, true, &options)
        .expect_err("blockquote table body rows should count before parsing");

    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("max_markdown_nodes"));
}

#[test]
fn preflight_rejects_gfm_table_cells_before_parsing() {
    let input = "| Field | Type |\n| --- | --- |\n| id | string |";
    let options = NormalizationOptions {
        max_markdown_table_cells: 1,
        ..NormalizationOptions::default()
    };

    let err = enforce_markdown_structural_preflight(input, true, &options)
        .expect_err("table cells should exceed the preflight table-cell estimate");

    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("max_markdown_table_cells"));
}

#[test]
fn preflight_counts_gfm_table_rows_toward_nodes() {
    let input = format!(
        "| Field | Type |\n| --- | --- |\n{}",
        "| id | string |\n".repeat(4)
    );
    let options = NormalizationOptions {
        max_markdown_nodes: 8,
        max_markdown_table_cells: 100,
        ..NormalizationOptions::default()
    };

    let err = enforce_markdown_structural_preflight(&input, true, &options)
        .expect_err("table rows should exceed the preflight node estimate");

    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("max_markdown_nodes"));
}

#[test]
fn preflight_counts_plain_paragraphs_toward_nodes() {
    let input = "plain\n\n".repeat(4);
    let options = NormalizationOptions {
        max_markdown_nodes: 8,
        ..NormalizationOptions::default()
    };

    let err = enforce_markdown_structural_preflight(&input, true, &options)
        .expect_err("plain paragraphs should exceed the preflight node estimate");

    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("max_markdown_nodes"));
}
