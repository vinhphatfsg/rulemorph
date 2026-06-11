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
fn t59_markdown_table_alignment_contract() {
    assert_text_fixture("t59_markdown_table_alignment_contract", "input.md");
}
