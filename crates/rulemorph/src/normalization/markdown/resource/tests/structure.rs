use super::super::*;

#[test]
fn preflight_rejects_inline_heavy_input_before_parsing() {
    let input = "[x](https://example.com)".repeat(16);
    let options = NormalizationOptions {
        max_markdown_nodes: 8,
        ..NormalizationOptions::default()
    };

    let err = enforce_markdown_structural_preflight(&input, true, &options)
        .expect_err("inline-heavy input should exceed the preflight node estimate");

    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("max_markdown_nodes"));
}

#[test]
fn preflight_skips_inline_estimate_inside_fenced_code() {
    let input = format!("```\n{}\n```", "[x](https://example.com)".repeat(16));
    let options = NormalizationOptions {
        max_markdown_nodes: 8,
        ..NormalizationOptions::default()
    };

    enforce_markdown_structural_preflight(&input, true, &options)
        .expect("link-like code text should not count as inline nodes");
}

#[test]
fn preflight_skips_inline_estimate_inside_html_block() {
    let input = format!("<pre>\n{}\n</pre>", "[x](https://example.com)\n".repeat(8));
    let options = NormalizationOptions {
        max_markdown_nodes: 2,
        ..NormalizationOptions::default()
    };

    enforce_markdown_structural_preflight(&input, true, &options)
        .expect("link-like text inside an HTML block should not count as Markdown nodes");
}

#[test]
fn preflight_skips_inline_estimate_inside_block_html_tag() {
    let input = format!("<div>\n{}</div>", "[x](https://example.com)\n".repeat(8));
    let options = NormalizationOptions {
        max_markdown_nodes: 2,
        ..NormalizationOptions::default()
    };

    enforce_markdown_structural_preflight(&input, true, &options)
        .expect("link-like text inside a block HTML tag should not count as Markdown nodes");
}

#[test]
fn preflight_skips_inline_estimate_inside_complete_html_tag_block() {
    let input = format!(
        "<custom-element>\n{}</custom-element>",
        "[x](https://example.com)\n".repeat(8)
    );
    let options = NormalizationOptions {
        max_markdown_nodes: 2,
        ..NormalizationOptions::default()
    };

    enforce_markdown_structural_preflight(&input, true, &options).expect(
        "link-like text inside a complete HTML tag block should not count as Markdown nodes",
    );
}

#[test]
fn preflight_does_not_start_complete_html_tag_block_inside_paragraph() {
    let input = format!(
        "paragraph\n<custom-element>\n{}</custom-element>",
        "[x](https://example.com)\n".repeat(4)
    );
    let options = NormalizationOptions {
        max_markdown_nodes: 8,
        ..NormalizationOptions::default()
    };

    let err = enforce_markdown_structural_preflight(&input, true, &options)
        .expect_err("type 7 HTML blocks should not hide Markdown while a paragraph is active");

    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("max_markdown_nodes"));
}

#[test]
fn preflight_does_not_start_complete_html_tag_block_inside_blockquote_paragraph() {
    let input = format!(
        "> paragraph\n> <custom-element>\n> {}</custom-element>",
        "[x](https://example.com)\n".repeat(4)
    );
    let options = NormalizationOptions {
        max_markdown_nodes: 10,
        ..NormalizationOptions::default()
    };

    let err = enforce_markdown_structural_preflight(&input, true, &options)
        .expect_err("type 7 HTML blocks should not hide Markdown inside a blockquote paragraph");

    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("max_markdown_nodes"));
}

#[test]
fn preflight_does_not_start_complete_html_tag_block_after_reference_definition() {
    let input =
        "[x]: https://example.com\n<custom-element>\n[x](https://example.com)\n</custom-element>";
    let options = NormalizationOptions {
        max_markdown_nodes: 4,
        ..NormalizationOptions::default()
    };

    let err = enforce_markdown_structural_preflight(input, true, &options).expect_err(
        "reference definitions should keep type 7 HTML tags inside the paragraph preflight",
    );

    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("max_markdown_nodes"));
}

#[test]
fn preflight_does_not_start_complete_html_tag_block_after_blockquote_reference_definition() {
    let input = "> [x]: https://example.com\n> <custom-element>\n> [x](https://example.com)\n> </custom-element>";
    let options = NormalizationOptions {
        max_markdown_nodes: 6,
        ..NormalizationOptions::default()
    };

    let err = enforce_markdown_structural_preflight(input, true, &options).expect_err(
            "blockquote reference definitions should keep type 7 HTML tags inside the paragraph preflight",
        );

    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("max_markdown_nodes"));
}

#[test]
fn preflight_skips_table_like_text_inside_blockquote_fenced_code() {
    let input = "> ```\n> | A | B |\n> | --- | --- |\n> | x | y |\n> ```";
    let options = NormalizationOptions {
        max_markdown_table_cells: 1,
        ..NormalizationOptions::default()
    };

    enforce_markdown_structural_preflight(input, true, &options)
        .expect("table-like code text inside a blockquote fence should not count as table cells");
}

#[test]
fn preflight_does_not_skip_content_after_invalid_backtick_fence_info() {
    let input = format!(
        "``` invalid ` info\n{}",
        "[x](https://example.com)\n".repeat(4)
    );
    let options = NormalizationOptions {
        max_markdown_nodes: 8,
        ..NormalizationOptions::default()
    };

    let err = enforce_markdown_structural_preflight(&input, true, &options)
        .expect_err("invalid backtick fence info should not hide later content");

    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("max_markdown_nodes"));
}

#[test]
fn preflight_rejects_content_after_four_space_indented_fence() {
    let input = format!("    ```\n{}", "[x](https://example.com)\n".repeat(4));
    let options = NormalizationOptions {
        max_markdown_nodes: 8,
        ..NormalizationOptions::default()
    };

    let err = enforce_markdown_structural_preflight(&input, true, &options)
        .expect_err("four-space indented fence marker should not hide later content");

    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("max_markdown_nodes"));
}

#[test]
fn preflight_allows_markdown_markers_inside_indented_code() {
    let input = format!("{}{}", "    - x\n".repeat(8), "    # title\n".repeat(8));
    let options = NormalizationOptions {
        max_markdown_nodes: 8,
        ..NormalizationOptions::default()
    };

    enforce_markdown_structural_preflight(&input, true, &options)
        .expect("indented code markers should not count as active Markdown structure");
}

#[test]
fn preflight_allows_blank_lines_without_node_growth() {
    let input = "\n".repeat(20);
    let options = NormalizationOptions {
        max_markdown_nodes: 1,
        ..NormalizationOptions::default()
    };

    enforce_markdown_structural_preflight(&input, true, &options)
        .expect("blank lines should not count as parsed Markdown nodes");
}

#[test]
fn preflight_counts_non_heading_hash_lines_as_paragraphs() {
    let input = "#tag\n".repeat(4);
    let options = NormalizationOptions {
        max_markdown_nodes: 8,
        ..NormalizationOptions::default()
    };

    let err = enforce_markdown_structural_preflight(&input, true, &options)
        .expect_err("non-heading hash lines should count as paragraph/text nodes");

    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("max_markdown_nodes"));
}

#[test]
fn preflight_counts_atx_heading_text_nodes_before_parsing() {
    let input = "# title\n".repeat(5);
    let options = NormalizationOptions {
        max_markdown_nodes: 8,
        ..NormalizationOptions::default()
    };

    let err = enforce_markdown_structural_preflight(&input, true, &options)
        .expect_err("heading text nodes should count toward the preflight node estimate");

    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("max_markdown_nodes"));
}

#[test]
fn preflight_counts_thematic_breaks_as_single_nodes() {
    let input = "***\n---\n_ _ _\n";
    let options = NormalizationOptions {
        max_markdown_nodes: 4,
        ..NormalizationOptions::default()
    };

    enforce_markdown_structural_preflight(input, true, &options)
        .expect("thematic breaks should count as one parsed block node each");
}

#[test]
fn preflight_allows_setext_headings_without_underline_node_growth() {
    let input = "Title\n===\nSubtitle\n---\n";
    let options = NormalizationOptions {
        max_markdown_nodes: 5,
        ..NormalizationOptions::default()
    };

    enforce_markdown_structural_preflight(input, true, &options)
        .expect("setext underline lines should not add paragraph nodes");
}

#[test]
fn preflight_allows_empty_atx_headings_with_closing_markers() {
    let input = "# #\n".repeat(2);
    let options = NormalizationOptions {
        max_markdown_nodes: 3,
        ..NormalizationOptions::default()
    };

    enforce_markdown_structural_preflight(&input, true, &options)
        .expect("closing markers without heading content should not add text nodes");
}

#[test]
fn preflight_rejects_list_heavy_input_before_parsing() {
    let input = "- x\n".repeat(4);
    let options = NormalizationOptions {
        max_markdown_nodes: 8,
        ..NormalizationOptions::default()
    };

    let err = enforce_markdown_structural_preflight(&input, true, &options)
        .expect_err("compact list-heavy input should exceed the preflight node estimate");

    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("max_markdown_nodes"));
}

#[test]
fn preflight_shares_list_container_across_adjacent_items() {
    let input = "- a\n- b\n- c";
    let options = NormalizationOptions {
        max_markdown_nodes: 11,
        ..NormalizationOptions::default()
    };

    enforce_markdown_structural_preflight(input, true, &options)
        .expect("adjacent list items should share one list container estimate");
}

#[test]
fn preflight_treats_non_one_ordered_marker_inside_paragraph_as_text() {
    let input = "intro\n2. not a list\n3. still paragraph\n";
    let options = NormalizationOptions {
        max_markdown_nodes: 7,
        ..NormalizationOptions::default()
    };

    enforce_markdown_structural_preflight(input, true, &options)
        .expect("ordered markers above one should not interrupt an active paragraph");
}

#[test]
fn preflight_keeps_one_ordered_marker_as_paragraph_interrupting_list() {
    let input = "intro\n1. list\n";
    let options = NormalizationOptions {
        max_markdown_nodes: 5,
        ..NormalizationOptions::default()
    };

    let err = enforce_markdown_structural_preflight(input, true, &options)
        .expect_err("ordered marker one can interrupt an active paragraph");

    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("max_markdown_nodes"));
}
