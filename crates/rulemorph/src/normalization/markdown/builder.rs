use super::document::{
    MarkdownDocument, Section, SectionSummary, strings_to_value, table_alignments,
};
use super::text::{html_to_text, no_space_before, normalize_raw_html, normalize_text, plain_text};
use super::*;

mod blocks;
mod tables;
mod text_sections;

#[derive(Clone)]
struct FlatSection {
    id: String,
    level: u8,
    heading: String,
    heading_block_id: Option<String>,
    path: Vec<String>,
    ordinal_path: Vec<usize>,
    content_block_ids: Vec<String>,
    child_ids: Vec<String>,
    parent_id: Option<String>,
}

#[derive(Clone)]
struct StackSection {
    id: String,
    level: u8,
    heading: String,
    ordinal: usize,
}

pub(super) struct DocumentBuilder<'a> {
    markdown: &'a MarkdownInput,
    options: &'a NormalizationOptions,
    frontmatter: Map<String, JsonValue>,
    title: Option<String>,
    body_text: String,
    flat_sections: Vec<FlatSection>,
    section_by_id: HashMap<String, usize>,
    section_stack: Vec<StackSection>,
    counters: [usize; 7],
    active_section_id: Option<String>,
    blocks: Vec<Map<String, JsonValue>>,
    block_index_by_id: HashMap<String, usize>,
    next_block_number: usize,
    links: Vec<JsonValue>,
    images: Vec<JsonValue>,
    code_blocks: Vec<JsonValue>,
    tables: Vec<JsonValue>,
    raw_html: Vec<JsonValue>,
    next_table_index: usize,
}

impl<'a> DocumentBuilder<'a> {
    pub(super) fn new(
        markdown: &'a MarkdownInput,
        options: &'a NormalizationOptions,
        frontmatter: Map<String, JsonValue>,
    ) -> Self {
        Self {
            markdown,
            options,
            frontmatter,
            title: None,
            body_text: String::new(),
            flat_sections: Vec::new(),
            section_by_id: HashMap::new(),
            section_stack: Vec::new(),
            counters: [0; 7],
            active_section_id: None,
            blocks: Vec::new(),
            block_index_by_id: HashMap::new(),
            next_block_number: 1,
            links: Vec::new(),
            images: Vec::new(),
            code_blocks: Vec::new(),
            tables: Vec::new(),
            raw_html: Vec::new(),
            next_table_index: 0,
        }
    }

    pub(super) fn collect(&mut self, root: Node<'_>) -> Result<(), TransformError> {
        for child in root.children() {
            self.process_block(child, None, true)?;
        }
        Ok(())
    }

    pub(super) fn finish(self) -> MarkdownDocument {
        let title = self
            .title
            .clone()
            .or_else(|| {
                self.frontmatter
                    .get("title")
                    .and_then(JsonValue::as_str)
                    .map(ToOwned::to_owned)
            })
            .unwrap_or_default();
        let sections = self.nested_sections();
        let section_index = self
            .flat_sections
            .iter()
            .map(|section| SectionSummary {
                id: section.id.clone(),
                level: section.level,
                heading: section.heading.clone(),
                path: section.path.clone(),
                ordinal_path: section.ordinal_path.clone(),
            })
            .collect();
        MarkdownDocument {
            frontmatter: self.frontmatter,
            title,
            body_text: self.body_text,
            sections,
            section_index,
            blocks: self.blocks,
            links: self.links,
            images: self.images,
            code_blocks: self.code_blocks,
            tables: self.tables,
            raw_html: self.raw_html,
        }
    }

    fn process_block(
        &mut self,
        node: Node<'_>,
        parent_block_id: Option<String>,
        top_level_content: bool,
    ) -> Result<Option<String>, TransformError> {
        enum BlockKind {
            Heading(u8),
            Paragraph,
            List,
            ListItem,
            TaskItem(Option<char>),
            BlockQuote,
            CodeBlock,
            HtmlBlock,
            ThematicBreak,
            Table,
            Skip,
            Recurse,
        }

        let kind = {
            let data = node.data.borrow();
            match &data.value {
                NodeValue::Heading(heading) => BlockKind::Heading(heading.level),
                NodeValue::Paragraph => BlockKind::Paragraph,
                NodeValue::List(_) => BlockKind::List,
                NodeValue::Item(_) => BlockKind::ListItem,
                NodeValue::TaskItem(task) => BlockKind::TaskItem(task.symbol),
                NodeValue::BlockQuote => BlockKind::BlockQuote,
                NodeValue::CodeBlock(_) => BlockKind::CodeBlock,
                NodeValue::HtmlBlock(_) => BlockKind::HtmlBlock,
                NodeValue::ThematicBreak => BlockKind::ThematicBreak,
                NodeValue::Table(_) => BlockKind::Table,
                NodeValue::TableRow(_) | NodeValue::TableCell => BlockKind::Skip,
                _ => BlockKind::Recurse,
            }
        };

        match kind {
            BlockKind::Heading(level) => self
                .add_heading(node, level, parent_block_id, top_level_content)
                .map(Some),
            BlockKind::Paragraph => self
                .add_paragraph(node, parent_block_id, top_level_content)
                .map(Some),
            BlockKind::List => self.add_list(node, parent_block_id, top_level_content),
            BlockKind::ListItem => self.add_list_item(node, parent_block_id, None, None),
            BlockKind::TaskItem(symbol) => {
                self.add_list_item(node, parent_block_id, None, Some(symbol.is_some()))
            }
            BlockKind::BlockQuote => self.add_blockquote(node, parent_block_id, top_level_content),
            BlockKind::CodeBlock => self
                .add_code_block(node, parent_block_id, top_level_content)
                .map(Some),
            BlockKind::HtmlBlock => self.add_html_block(node, parent_block_id, top_level_content),
            BlockKind::ThematicBreak => Ok(Some(
                self.add_thematic_break(parent_block_id, top_level_content),
            )),
            BlockKind::Table => self
                .add_table(node, parent_block_id, top_level_content)
                .map(Some),
            BlockKind::Skip => Ok(None),
            BlockKind::Recurse => {
                let mut last = None;
                for child in node.children() {
                    last = self.process_block(child, parent_block_id.clone(), top_level_content)?;
                }
                Ok(last)
            }
        }
    }
}
