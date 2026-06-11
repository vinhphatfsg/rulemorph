use super::super::*;

#[test]
fn preflight_ignores_reference_definitions_inside_fenced_code() {
    let input = "A [x]\n```\n[x]: https://example.com\n```";
    let options = NormalizationOptions {
        max_markdown_nodes: 4,
        ..NormalizationOptions::default()
    };

    enforce_markdown_structural_preflight(input, true, &options)
        .expect("reference definitions inside fenced code should not enable shortcut links");
}

#[test]
fn preflight_ignores_reference_definitions_inside_html_block() {
    let input = "A [x]\n<div>\n[x]: https://example.com\n</div>";
    let options = NormalizationOptions {
        max_markdown_nodes: 4,
        ..NormalizationOptions::default()
    };

    enforce_markdown_structural_preflight(input, true, &options)
        .expect("reference definitions inside HTML blocks should not enable shortcut links");
}

#[test]
fn preflight_ignores_reference_definition_without_destination() {
    let input = "A [x]\n[x]:";
    let options = NormalizationOptions {
        max_markdown_nodes: 5,
        ..NormalizationOptions::default()
    };

    enforce_markdown_structural_preflight(input, true, &options)
        .expect("reference labels without destinations should not enable shortcut links");
}

#[test]
fn preflight_ignores_blockquote_reference_definition_without_destination() {
    let input = "> A [x]\n> [x]:";
    let options = NormalizationOptions {
        max_markdown_nodes: 7,
        ..NormalizationOptions::default()
    };

    enforce_markdown_structural_preflight(input, true, &options).expect(
        "blockquote reference labels without destinations should not enable shortcut links",
    );
}

#[test]
fn preflight_ignores_reference_definition_with_trailing_text() {
    let input = "A [x] [x] [x] [x] [x]\n[x]: foo bar";
    let options = NormalizationOptions {
        max_markdown_nodes: 5,
        ..NormalizationOptions::default()
    };

    enforce_markdown_structural_preflight(input, true, &options)
        .expect("reference definitions with trailing text should not enable shortcut links");
}

#[test]
fn preflight_ignores_reference_definition_with_unbalanced_destination_parens() {
    let input = "[x]: foo(bar\nA [x]";
    let options = NormalizationOptions {
        max_markdown_nodes: 5,
        ..NormalizationOptions::default()
    };

    enforce_markdown_structural_preflight(input, true, &options)
        .expect("unbalanced destination parens should not enable shortcut links");
}

#[test]
fn preflight_ignores_reference_definition_with_nested_bracketed_destination_start() {
    let input = "[x]: <foo<bar>\nA [x] [x] [x] [x] [x]";
    let options = NormalizationOptions {
        max_markdown_nodes: 7,
        ..NormalizationOptions::default()
    };

    enforce_markdown_structural_preflight(input, true, &options)
        .expect("nested bracketed destination starts should not enable shortcut links");
}

#[test]
fn preflight_ignores_reference_definition_with_escaped_destination_space() {
    let input = "[x]: foo\\ bar\nA [x] [x] [x] [x] [x]";
    let options = NormalizationOptions {
        max_markdown_nodes: 5,
        ..NormalizationOptions::default()
    };

    enforce_markdown_structural_preflight(input, true, &options)
        .expect("escaped spaces should not make unbracketed destinations valid");
}

#[test]
fn preflight_counts_reference_definition_with_balanced_destination_parens() {
    let input = "[x]: foo(bar)\nA [x] [x]";
    let options = NormalizationOptions {
        max_markdown_nodes: 5,
        ..NormalizationOptions::default()
    };

    let err = enforce_markdown_structural_preflight(input, true, &options)
        .expect_err("balanced destination parens should enable shortcut links");

    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("max_markdown_nodes"));
}

#[test]
fn preflight_counts_reference_definition_with_escaped_destination_parens() {
    let input = "[x]: foo\\(bar\\)\nA [x] [x]";
    let options = NormalizationOptions {
        max_markdown_nodes: 5,
        ..NormalizationOptions::default()
    };

    let err = enforce_markdown_structural_preflight(input, true, &options)
        .expect_err("escaped destination parens should keep the reference definition valid");

    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("max_markdown_nodes"));
}

#[test]
fn preflight_does_not_count_reference_definitions_as_paragraphs() {
    let input =
        "[a]: https://example.com/a\n[b]: https://example.com/b\n[c]: https://example.com/c";
    let options = NormalizationOptions {
        max_markdown_nodes: 1,
        ..NormalizationOptions::default()
    };

    enforce_markdown_structural_preflight(input, true, &options)
        .expect("reference definition lines should not add paragraph/text nodes");
}

#[test]
fn preflight_does_not_count_multiline_reference_definition_title_as_paragraph() {
    let input = "[a]: https://example.com/a\n\"title\"";
    let options = NormalizationOptions {
        max_markdown_nodes: 1,
        ..NormalizationOptions::default()
    };

    enforce_markdown_structural_preflight(input, true, &options)
        .expect("reference definition title continuation should not add paragraph/text nodes");
}

#[test]
fn preflight_does_not_count_multiline_reference_definition_title_lines_as_paragraphs() {
    let input = "[a]: https://example.com/a\n\"one\ntwo\"";
    let options = NormalizationOptions {
        max_markdown_nodes: 1,
        ..NormalizationOptions::default()
    };

    enforce_markdown_structural_preflight(input, true, &options)
        .expect("multiline reference definition title should not add paragraph/text nodes");
}

#[test]
fn preflight_ignores_table_text_inside_multiline_reference_definition_title() {
    let input = "[a]: https://example.com/a\n\"title\n| A | B |\n| --- | --- |\nend\"";
    let options = NormalizationOptions {
        max_markdown_table_cells: 1,
        ..NormalizationOptions::default()
    };

    enforce_markdown_structural_preflight(input, true, &options)
        .expect("table-like text inside a multiline reference title should not count cells");
}

#[test]
fn preflight_ignores_table_text_inside_blockquote_lazy_multiline_reference_title() {
    let input = "> [a]: https://example.com/a\n\"title\n| A | B |\n| --- | --- |\nend\"";
    let options = NormalizationOptions {
        max_markdown_table_cells: 1,
        ..NormalizationOptions::default()
    };

    enforce_markdown_structural_preflight(input, true, &options)
        .expect("table-like lazy continuation title text should not count cells");
}

#[test]
fn preflight_counts_table_after_blank_line_in_reference_title_candidate() {
    let input = "[a]: https://example.com/a\n\"title\n\n| A | B |\n| --- | --- |\nend\"";
    let options = NormalizationOptions {
        max_markdown_table_cells: 1,
        ..NormalizationOptions::default()
    };

    let err = enforce_markdown_structural_preflight(input, true, &options)
        .expect_err("blank line should end pending reference title continuation");

    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("max_markdown_table_cells"));
}

#[test]
fn preflight_counts_table_after_blank_line_in_blockquote_lazy_reference_title_candidate() {
    let input = "> [a]: https://example.com/a\n\"title\n\n| A | B |\n| --- | --- |\nend\"";
    let options = NormalizationOptions {
        max_markdown_table_cells: 1,
        ..NormalizationOptions::default()
    };

    let err = enforce_markdown_structural_preflight(input, true, &options)
        .expect_err("blank line should end blockquote lazy reference title continuation");

    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("max_markdown_table_cells"));
}

#[test]
fn preflight_counts_table_after_fence_in_reference_title_candidate() {
    let input =
        "[a]: https://example.com/a\n\"title\n```\ncode\n```\n| A | B |\n| --- | --- |\nend\"";
    let options = NormalizationOptions {
        max_markdown_table_cells: 1,
        ..NormalizationOptions::default()
    };

    let err = enforce_markdown_structural_preflight(input, true, &options)
        .expect_err("code fence should end pending reference title continuation");

    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("max_markdown_table_cells"));
}

#[test]
fn preflight_counts_table_after_fence_in_blockquote_lazy_reference_title_candidate() {
    let input =
        "> [a]: https://example.com/a\n\"title\n```\ncode\n```\n| A | B |\n| --- | --- |\nend\"";
    let options = NormalizationOptions {
        max_markdown_table_cells: 1,
        ..NormalizationOptions::default()
    };

    let err = enforce_markdown_structural_preflight(input, true, &options)
        .expect_err("code fence should end blockquote lazy reference title continuation");

    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("max_markdown_table_cells"));
}

#[test]
fn preflight_counts_table_after_heading_in_reference_title_candidate() {
    let input = "[a]: https://example.com/a\n\"title\n# Heading\n| A | B |\n| --- | --- |\nend\"";
    let options = NormalizationOptions {
        max_markdown_table_cells: 1,
        ..NormalizationOptions::default()
    };

    let err = enforce_markdown_structural_preflight(input, true, &options)
        .expect_err("ATX heading should end pending reference title continuation");

    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("max_markdown_table_cells"));
}

#[test]
fn preflight_counts_table_after_heading_in_blockquote_lazy_reference_title_candidate() {
    let input = "> [a]: https://example.com/a\n\"title\n# Heading\n| A | B |\n| --- | --- |\nend\"";
    let options = NormalizationOptions {
        max_markdown_table_cells: 1,
        ..NormalizationOptions::default()
    };

    let err = enforce_markdown_structural_preflight(input, true, &options)
        .expect_err("ATX heading should end blockquote lazy reference title continuation");

    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("max_markdown_table_cells"));
}

#[test]
fn preflight_counts_table_after_thematic_break_in_reference_title_candidate() {
    let input = "[a]: https://example.com/a\n\"title\n---\n| A | B |\n| --- | --- |\nend\"";
    let options = NormalizationOptions {
        max_markdown_table_cells: 1,
        ..NormalizationOptions::default()
    };

    let err = enforce_markdown_structural_preflight(input, true, &options)
        .expect_err("thematic break should end pending reference title continuation");

    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("max_markdown_table_cells"));
}

#[test]
fn preflight_counts_table_after_list_item_in_reference_title_candidate() {
    let input = "[a]: https://example.com/a\n\"title\n- item\n| A | B |\n| --- | --- |\nend\"";
    let options = NormalizationOptions {
        max_markdown_table_cells: 1,
        ..NormalizationOptions::default()
    };

    let err = enforce_markdown_structural_preflight(input, true, &options)
        .expect_err("list item should end pending reference title continuation");

    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("max_markdown_table_cells"));
}

#[test]
fn preflight_counts_table_after_html_block_in_reference_title_candidate() {
    let input =
        "[a]: https://example.com/a\n\"title\n<script>\n</script>\n| A | B |\n| --- | --- |\nend\"";
    let options = NormalizationOptions {
        max_markdown_table_cells: 1,
        ..NormalizationOptions::default()
    };

    let err = enforce_markdown_structural_preflight(input, true, &options)
        .expect_err("HTML block should end pending reference title continuation");

    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("max_markdown_table_cells"));
}

#[test]
fn preflight_keeps_non_one_ordered_marker_inside_reference_title_candidate() {
    let input = "[a]: https://example.com/a\n\"title\n2. item\n| A | B |\n| --- | --- |\nend\"";
    let options = NormalizationOptions {
        max_markdown_table_cells: 1,
        ..NormalizationOptions::default()
    };

    enforce_markdown_structural_preflight(input, true, &options)
        .expect("ordered list markers above one should stay inside the reference title");
}

#[test]
fn preflight_counts_table_after_setext_underline_in_reference_title_candidate() {
    let input = "[a]: https://example.com/a\n\"title\n===\n| A | B |\n| --- | --- |\nend\"";
    let options = NormalizationOptions {
        max_markdown_table_cells: 1,
        ..NormalizationOptions::default()
    };

    let err = enforce_markdown_structural_preflight(input, true, &options)
        .expect_err("setext underline should end pending reference title continuation");

    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("max_markdown_table_cells"));
}

#[test]
fn preflight_keeps_empty_unordered_marker_inside_reference_title_candidate() {
    let input = "[a]: https://example.com/a\n\"title\n* \n| A | B |\n| --- | --- |\nend\"";
    let options = NormalizationOptions {
        max_markdown_table_cells: 1,
        ..NormalizationOptions::default()
    };

    enforce_markdown_structural_preflight(input, true, &options)
        .expect("empty unordered list marker should stay inside the reference title");
}

#[test]
fn preflight_keeps_empty_ordered_marker_inside_reference_title_candidate() {
    let input = "[a]: https://example.com/a\n\"title\n1. \n| A | B |\n| --- | --- |\nend\"";
    let options = NormalizationOptions {
        max_markdown_table_cells: 1,
        ..NormalizationOptions::default()
    };

    enforce_markdown_structural_preflight(input, true, &options)
        .expect("empty ordered list marker should stay inside the reference title");
}

#[test]
fn preflight_counts_table_after_reference_definition_without_title_continuation() {
    let input = "[a]: https://example.com/a\n| A | B |\n| --- | --- |";
    let options = NormalizationOptions {
        max_markdown_table_cells: 1,
        ..NormalizationOptions::default()
    };

    let err = enforce_markdown_structural_preflight(input, true, &options)
        .expect_err("table text after a reference definition should still count as table cells");

    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("max_markdown_table_cells"));
}

#[test]
fn preflight_counts_reference_definition_with_escaped_bracketed_destination_start() {
    let input = "[x]: <foo\\<bar>\nA [x] [x]";
    let options = NormalizationOptions {
        max_markdown_nodes: 5,
        ..NormalizationOptions::default()
    };

    let err = enforce_markdown_structural_preflight(input, true, &options)
        .expect_err("escaped bracketed destination starts should keep the definition valid");

    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("max_markdown_nodes"));
}

#[test]
fn preflight_ignores_blockquote_reference_definition_with_trailing_text() {
    let input = "> A [x] [x] [x] [x] [x]\n> [x]: foo bar";
    let options = NormalizationOptions {
        max_markdown_nodes: 7,
        ..NormalizationOptions::default()
    };

    enforce_markdown_structural_preflight(input, true, &options).expect(
        "blockquote reference definitions with trailing text should not enable shortcut links",
    );
}

#[test]
fn preflight_ignores_reference_definition_inside_paragraph() {
    let input = "intro\n[x]: not-url\nA [x] [x] [x] [x] [x]";
    let options = NormalizationOptions {
        max_markdown_nodes: 7,
        ..NormalizationOptions::default()
    };

    enforce_markdown_structural_preflight(input, true, &options)
        .expect("reference-like text inside a paragraph should not enable shortcut links");
}

#[test]
fn preflight_counts_reference_links_on_reference_like_paragraph_line() {
    let input = "intro\n[x]: https://example.com [y] [y]\n\n[y]: https://example.com";
    let options = NormalizationOptions {
        max_markdown_nodes: 8,
        ..NormalizationOptions::default()
    };

    let err = enforce_markdown_structural_preflight(input, true, &options)
        .expect_err("reference links on paragraph text should count before parsing");

    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("max_markdown_nodes"));
}

#[test]
fn preflight_ignores_blockquote_reference_definition_inside_paragraph() {
    let input = "> intro\n> [x]: not-url\n> A [x] [x] [x] [x] [x]";
    let options = NormalizationOptions {
        max_markdown_nodes: 10,
        ..NormalizationOptions::default()
    };

    enforce_markdown_structural_preflight(input, true, &options).expect(
        "reference-like text inside a blockquote paragraph should not enable shortcut links",
    );
}

#[test]
fn preflight_ignores_blockquote_lazy_continuation_reference_definition_inside_paragraph() {
    let input = "> intro\n[x]: not-url\nA [x] [x] [x] [x] [x]";
    let options = NormalizationOptions {
        max_markdown_nodes: 8,
        ..NormalizationOptions::default()
    };

    enforce_markdown_structural_preflight(input, true, &options).expect(
        "lazy continuation text inside a blockquote paragraph should not enable shortcut links",
    );
}

#[test]
fn preflight_counts_reference_links_after_blockquote_reference_lazy_continuation() {
    let input = "> [x]: https://example.com\n[y]: https://example.com\nA [y] [y]";
    let options = NormalizationOptions {
        max_markdown_nodes: 6,
        ..NormalizationOptions::default()
    };

    let err = enforce_markdown_structural_preflight(input, true, &options).expect_err(
        "blockquote reference-definition-only continuation should enable shortcut links",
    );

    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("max_markdown_nodes"));
}

#[test]
fn preflight_counts_reference_links_on_blockquote_lazy_continuation_line() {
    let input = "> intro\n[x]: https://example.com [y] [y]\n\n[y]: https://example.com";
    let options = NormalizationOptions {
        max_markdown_nodes: 9,
        ..NormalizationOptions::default()
    };

    let err = enforce_markdown_structural_preflight(input, true, &options).expect_err(
        "reference links on blockquote lazy continuation text should count before parsing",
    );

    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("max_markdown_nodes"));
}
