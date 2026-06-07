use std::collections::HashMap;

use comrak::nodes::{ListType, Node, NodeValue, TableAlignment};
use comrak::{Arena, Options, parse_document};
use serde_json::{Map, Value as JsonValue, json};

use crate::error::{TransformError, TransformErrorKind};
use crate::model::{MarkdownFlavor, MarkdownInput, MarkdownRecordsMode, RuleFile};

use super::{NormalizationOptions, enforce_json_limits, enforce_records_limit};

mod frontmatter;
mod project;
mod resource;

pub fn normalize_markdown_records(
    rule: &RuleFile,
    input: &str,
    options: &NormalizationOptions,
) -> Result<Vec<JsonValue>, TransformError> {
    let markdown = rule.input.markdown.as_ref().ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::InvalidInput,
            "input.markdown is required when format=markdown",
        )
    })?;

    let split = frontmatter::split_frontmatter(markdown.frontmatter, input, options)?;
    resource::enforce_markdown_structural_preflight(split.body, options)?;
    let arena = Arena::new();
    let parser_options = parser_options(markdown);
    let root = parse_document(&arena, split.body, &parser_options);
    resource::count_parsed_markdown_nodes(root, options)?;
    let mut builder = DocumentBuilder::new(markdown, split.frontmatter);
    builder.collect(root)?;
    let document = builder.finish();

    let records = match markdown.records {
        MarkdownRecordsMode::Document => vec![document_record(document, markdown)],
        MarkdownRecordsMode::Sections => project::project_sections(&document, markdown),
        MarkdownRecordsMode::TableRows => project::project_table_rows(&document, markdown)?,
    };
    enforce_records_limit(records.len(), options)?;
    for record in &records {
        enforce_json_limits(record, options)?;
    }
    Ok(records)
}

#[derive(Clone)]
struct MarkdownDocument {
    frontmatter: Map<String, JsonValue>,
    title: String,
    body_text: String,
    sections: Vec<Section>,
    section_index: Vec<SectionSummary>,
    blocks: Vec<Map<String, JsonValue>>,
    links: Vec<JsonValue>,
    images: Vec<JsonValue>,
    code_blocks: Vec<JsonValue>,
    tables: Vec<JsonValue>,
    raw_html: Vec<JsonValue>,
}

#[derive(Clone)]
struct Section {
    id: String,
    level: u8,
    heading: String,
    heading_block_id: Option<String>,
    path: Vec<String>,
    ordinal_path: Vec<usize>,
    content_block_ids: Vec<String>,
    child_ids: Vec<String>,
    children: Vec<Section>,
}

#[derive(Clone)]
struct SectionSummary {
    id: String,
    level: u8,
    heading: String,
    path: Vec<String>,
    ordinal_path: Vec<usize>,
}

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

struct DocumentBuilder<'a> {
    markdown: &'a MarkdownInput,
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

fn parser_options(markdown: &MarkdownInput) -> Options<'static> {
    let mut options = Options::default();
    if markdown.flavor == MarkdownFlavor::Gfm {
        options.extension.table = true;
        options.extension.strikethrough = true;
        options.extension.autolink = true;
        options.extension.tasklist = true;
    }
    options
}

fn document_record(document: MarkdownDocument, markdown: &MarkdownInput) -> JsonValue {
    let mut record = Map::new();
    record.insert(
        "record_type".to_string(),
        JsonValue::String("document".to_string()),
    );
    record.insert(
        "frontmatter".to_string(),
        JsonValue::Object(document.frontmatter.clone()),
    );
    record.insert("title".to_string(), JsonValue::String(document.title));
    if markdown.include.body_text {
        record.insert(
            "body_text".to_string(),
            JsonValue::String(normalize_text(&document.body_text, markdown)),
        );
    }
    record.insert(
        "sections".to_string(),
        JsonValue::Array(document.sections.iter().map(section_to_json).collect()),
    );
    record.insert(
        "section_index".to_string(),
        JsonValue::Array(
            document
                .section_index
                .iter()
                .map(section_summary_to_json)
                .collect(),
        ),
    );
    if markdown.include.blocks {
        record.insert(
            "blocks".to_string(),
            JsonValue::Array(document.blocks.into_iter().map(JsonValue::Object).collect()),
        );
    }
    if markdown.include.links {
        record.insert("links".to_string(), JsonValue::Array(document.links));
    }
    if markdown.include.images {
        record.insert("images".to_string(), JsonValue::Array(document.images));
    }
    if markdown.include.code_blocks {
        record.insert(
            "code_blocks".to_string(),
            JsonValue::Array(document.code_blocks),
        );
    }
    if markdown.include.tables {
        record.insert("tables".to_string(), JsonValue::Array(document.tables));
    }
    if markdown.include.raw_html {
        record.insert("raw_html".to_string(), JsonValue::Array(document.raw_html));
    }
    JsonValue::Object(record)
}

impl<'a> DocumentBuilder<'a> {
    fn new(markdown: &'a MarkdownInput, frontmatter: Map<String, JsonValue>) -> Self {
        Self {
            markdown,
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

    fn collect(&mut self, root: Node<'_>) -> Result<(), TransformError> {
        for child in root.children() {
            self.process_block(child, None, true)?;
        }
        Ok(())
    }

    fn finish(self) -> MarkdownDocument {
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
            BlockKind::Heading(level) => Ok(Some(self.add_heading(node, level, parent_block_id))),
            BlockKind::Paragraph => Ok(Some(self.add_paragraph(
                node,
                parent_block_id,
                top_level_content,
            ))),
            BlockKind::List => self.add_list(node, parent_block_id, top_level_content),
            BlockKind::ListItem => self.add_list_item(node, parent_block_id, None, None),
            BlockKind::TaskItem(symbol) => {
                self.add_list_item(node, parent_block_id, None, Some(symbol.is_some()))
            }
            BlockKind::BlockQuote => self.add_blockquote(node, parent_block_id, top_level_content),
            BlockKind::CodeBlock => Ok(Some(self.add_code_block(
                node,
                parent_block_id,
                top_level_content,
            ))),
            BlockKind::HtmlBlock => {
                Ok(self.add_html_block(node, parent_block_id, top_level_content))
            }
            BlockKind::ThematicBreak => Ok(Some(
                self.add_thematic_break(parent_block_id, top_level_content),
            )),
            BlockKind::Table => Ok(Some(self.add_table(
                node,
                parent_block_id,
                top_level_content,
            ))),
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

    fn add_heading(
        &mut self,
        node: Node<'_>,
        level: u8,
        parent_block_id: Option<String>,
    ) -> String {
        let id = self.next_block_id();
        let text = normalize_text(&plain_text(node), self.markdown);
        let section_id = self.open_heading_section(level, text.clone(), id.clone());
        if level == 1 && self.title.is_none() && !text.is_empty() {
            self.title = Some(text.clone());
        }
        push_body_text(&mut self.body_text, &text, self.markdown);
        let inlines = self.collect_inlines(node, &id);
        let mut block = self.common_block(
            id.clone(),
            "heading",
            section_id,
            parent_block_id,
            text,
            inlines,
        );
        block.insert("level".to_string(), json!(level));
        self.push_block(block, false);
        id
    }

    fn add_paragraph(
        &mut self,
        node: Node<'_>,
        parent_block_id: Option<String>,
        top_level_content: bool,
    ) -> String {
        let id = self.next_block_id();
        let section_id = self.current_section_id();
        let text = normalize_text(&plain_text(node), self.markdown);
        push_body_text(&mut self.body_text, &text, self.markdown);
        let inlines = self.collect_inlines(node, &id);
        let block = self.common_block(
            id.clone(),
            "paragraph",
            section_id,
            parent_block_id,
            text,
            inlines,
        );
        self.push_block(block, top_level_content);
        id
    }

    fn add_list(
        &mut self,
        node: Node<'_>,
        parent_block_id: Option<String>,
        top_level_content: bool,
    ) -> Result<Option<String>, TransformError> {
        let id = self.next_block_id();
        let section_id = self.current_section_id();
        let (ordered, start, tight) = {
            let data = node.data.borrow();
            match &data.value {
                NodeValue::List(list) => {
                    (list.list_type == ListType::Ordered, list.start, list.tight)
                }
                _ => (false, 0, false),
            }
        };
        let text = normalize_text(&plain_text(node), self.markdown);
        let mut block = self.common_block(
            id.clone(),
            "list",
            section_id,
            parent_block_id,
            text,
            Vec::new(),
        );
        block.insert("ordered".to_string(), json!(ordered));
        block.insert(
            "start".to_string(),
            if ordered {
                json!(start)
            } else {
                JsonValue::Null
            },
        );
        block.insert("tight".to_string(), json!(tight));
        block.insert("item_ids".to_string(), JsonValue::Array(Vec::new()));
        let block_index = self.push_block(block, top_level_content);

        let mut item_ids = Vec::new();
        for (offset, child) in node.children().enumerate() {
            let ordinal = if ordered { Some(start + offset) } else { None };
            if let Some(item_id) = self.process_list_child(child, id.clone(), ordinal)? {
                item_ids.push(JsonValue::String(item_id));
            }
        }
        self.blocks[block_index].insert("item_ids".to_string(), JsonValue::Array(item_ids));
        Ok(Some(id))
    }

    fn process_list_child(
        &mut self,
        node: Node<'_>,
        parent_block_id: String,
        ordinal: Option<usize>,
    ) -> Result<Option<String>, TransformError> {
        enum ListChild {
            Item,
            Task(Option<char>),
            Other,
        }
        let child = {
            let data = node.data.borrow();
            match &data.value {
                NodeValue::Item(_) => ListChild::Item,
                NodeValue::TaskItem(task) => ListChild::Task(task.symbol),
                _ => ListChild::Other,
            }
        };
        match child {
            ListChild::Item => self.add_list_item(node, Some(parent_block_id), ordinal, None),
            ListChild::Task(symbol) => {
                self.add_list_item(node, Some(parent_block_id), ordinal, Some(symbol.is_some()))
            }
            ListChild::Other => self.process_block(node, Some(parent_block_id), false),
        }
    }

    fn add_list_item(
        &mut self,
        node: Node<'_>,
        parent_block_id: Option<String>,
        ordinal: Option<usize>,
        checked: Option<bool>,
    ) -> Result<Option<String>, TransformError> {
        let id = self.next_block_id();
        let section_id = self.current_section_id();
        let text = normalize_text(&plain_text(node), self.markdown);
        let mut block = self.common_block(
            id.clone(),
            "list_item",
            section_id,
            parent_block_id,
            text,
            Vec::new(),
        );
        block.insert(
            "ordinal".to_string(),
            ordinal.map_or(JsonValue::Null, |value| json!(value)),
        );
        block.insert(
            "checked".to_string(),
            checked.map_or(JsonValue::Null, JsonValue::Bool),
        );
        block.insert("child_block_ids".to_string(), JsonValue::Array(Vec::new()));
        let block_index = self.push_block(block, false);

        let mut child_ids = Vec::new();
        for child in node.children() {
            if let Some(child_id) = self.process_block(child, Some(id.clone()), false)? {
                child_ids.push(JsonValue::String(child_id));
            }
        }
        self.blocks[block_index].insert("child_block_ids".to_string(), JsonValue::Array(child_ids));
        Ok(Some(id))
    }

    fn add_blockquote(
        &mut self,
        node: Node<'_>,
        parent_block_id: Option<String>,
        top_level_content: bool,
    ) -> Result<Option<String>, TransformError> {
        let id = self.next_block_id();
        let section_id = self.current_section_id();
        let text = normalize_text(&plain_text(node), self.markdown);
        let mut block = self.common_block(
            id.clone(),
            "blockquote",
            section_id,
            parent_block_id,
            text,
            Vec::new(),
        );
        block.insert("child_block_ids".to_string(), JsonValue::Array(Vec::new()));
        let block_index = self.push_block(block, top_level_content);

        let mut child_ids = Vec::new();
        for child in node.children() {
            if let Some(child_id) = self.process_block(child, Some(id.clone()), false)? {
                child_ids.push(JsonValue::String(child_id));
            }
        }
        self.blocks[block_index].insert("child_block_ids".to_string(), JsonValue::Array(child_ids));
        Ok(Some(id))
    }

    fn add_code_block(
        &mut self,
        node: Node<'_>,
        parent_block_id: Option<String>,
        top_level_content: bool,
    ) -> String {
        let id = self.next_block_id();
        let section_id = self.current_section_id();
        let (info, literal) = {
            let data = node.data.borrow();
            match &data.value {
                NodeValue::CodeBlock(code) => (code.info.clone(), code.literal.clone()),
                _ => (String::new(), String::new()),
            }
        };
        let language = info.split_whitespace().next().unwrap_or("").to_string();
        let text = literal.trim_end_matches('\n').to_string();
        push_body_text(&mut self.body_text, &text, self.markdown);
        let mut block = self.common_block(
            id.clone(),
            "code_block",
            section_id.clone(),
            parent_block_id,
            text.clone(),
            Vec::new(),
        );
        block.insert("language".to_string(), JsonValue::String(language.clone()));
        block.insert("info".to_string(), JsonValue::String(info.clone()));
        self.push_block(block, top_level_content);
        if self.markdown.include.code_blocks {
            self.code_blocks.push(json!({
                "block_id": id,
                "section_id": section_id,
                "language": language,
                "info": info,
                "text": text,
            }));
        }
        id
    }

    fn add_html_block(
        &mut self,
        node: Node<'_>,
        parent_block_id: Option<String>,
        top_level_content: bool,
    ) -> Option<String> {
        let html = {
            let data = node.data.borrow();
            match &data.value {
                NodeValue::HtmlBlock(html) => normalize_raw_html(&html.literal, self.markdown),
                _ => String::new(),
            }
        };
        let text = normalize_text(&html_to_text(&html), self.markdown);
        push_body_text(&mut self.body_text, &text, self.markdown);
        if !self.markdown.include.raw_html {
            return None;
        }
        let id = self.next_block_id();
        let section_id = self.current_section_id();
        let mut block = self.common_block(
            id.clone(),
            "html_block",
            section_id,
            parent_block_id,
            text,
            Vec::new(),
        );
        block.insert("html".to_string(), JsonValue::String(html.clone()));
        self.push_block(block, top_level_content);
        self.raw_html.push(json!({
            "block_id": id,
            "kind": "block",
            "html": html,
        }));
        Some(id)
    }

    fn add_thematic_break(
        &mut self,
        parent_block_id: Option<String>,
        top_level_content: bool,
    ) -> String {
        let id = self.next_block_id();
        let section_id = self.current_section_id();
        let block = self.common_block(
            id.clone(),
            "thematic_break",
            section_id,
            parent_block_id,
            String::new(),
            Vec::new(),
        );
        self.push_block(block, top_level_content);
        id
    }

    fn add_table(
        &mut self,
        node: Node<'_>,
        parent_block_id: Option<String>,
        top_level_content: bool,
    ) -> String {
        let id = self.next_block_id();
        let section_id = self.current_section_id();
        let table_index = self.next_table_index;
        self.next_table_index += 1;
        let alignments = table_alignments(node);
        let (header_row, rows) = self.table_rows(node, &id);
        let text = normalize_text(&plain_text(node), self.markdown);
        push_body_text(&mut self.body_text, &text, self.markdown);
        let mut block = self.common_block(
            id.clone(),
            "table",
            section_id.clone(),
            parent_block_id,
            text,
            Vec::new(),
        );
        block.insert("table_index".to_string(), json!(table_index));
        block.insert("alignments".to_string(), strings_to_value(&alignments));
        block.insert("header_row".to_string(), header_row.clone());
        block.insert("rows".to_string(), JsonValue::Array(rows.clone()));
        self.push_block(block, top_level_content);
        self.tables.push(json!({
            "block_id": id,
            "section_id": section_id,
            "table_index": table_index,
            "alignments": alignments,
            "header_row": header_row,
            "rows": rows,
        }));
        id
    }

    fn table_rows(&mut self, node: Node<'_>, block_id: &str) -> (JsonValue, Vec<JsonValue>) {
        let mut header_row = json!({ "cells": [] });
        let mut rows = Vec::new();
        for row in node.children() {
            let is_header = matches!(row.data.borrow().value, NodeValue::TableRow(true));
            let cells = row
                .children()
                .filter(|cell| matches!(cell.data.borrow().value, NodeValue::TableCell))
                .enumerate()
                .map(|(column_index, cell)| {
                    json!({
                        "column_index": column_index,
                        "text": normalize_text(&plain_text(cell), self.markdown),
                        "inlines": self.collect_inlines(cell, block_id),
                    })
                })
                .collect::<Vec<_>>();
            if is_header {
                header_row = json!({ "cells": cells });
            } else {
                rows.push(json!({
                    "row_index": rows.len(),
                    "cells": cells,
                }));
            }
        }
        (header_row, rows)
    }

    fn collect_inlines(&mut self, node: Node<'_>, block_id: &str) -> Vec<JsonValue> {
        let mut out = Vec::new();
        for child in node.children() {
            self.collect_inline_node(child, block_id, &mut out);
        }
        out
    }

    fn collect_inline_node(&mut self, node: Node<'_>, block_id: &str, out: &mut Vec<JsonValue>) {
        enum InlineKind {
            Text(String),
            SoftBreak,
            LineBreak,
            Code(String),
            Html(String),
            Emphasis,
            Strong,
            Strikethrough,
            Link { url: String, title: String },
            Image { url: String, title: String },
            Recurse,
            Skip,
        }

        let kind = {
            let data = node.data.borrow();
            match &data.value {
                NodeValue::Text(text) => InlineKind::Text(text.to_string()),
                NodeValue::SoftBreak => InlineKind::SoftBreak,
                NodeValue::LineBreak => InlineKind::LineBreak,
                NodeValue::Code(code) => InlineKind::Code(code.literal.clone()),
                NodeValue::HtmlInline(html) => {
                    InlineKind::Html(normalize_raw_html(html, self.markdown))
                }
                NodeValue::Emph => InlineKind::Emphasis,
                NodeValue::Strong => InlineKind::Strong,
                NodeValue::Strikethrough => InlineKind::Strikethrough,
                NodeValue::Link(link) => InlineKind::Link {
                    url: link.url.to_string(),
                    title: link.title.to_string(),
                },
                NodeValue::Image(image) => InlineKind::Image {
                    url: image.url.to_string(),
                    title: image.title.to_string(),
                },
                NodeValue::Paragraph
                | NodeValue::Heading(_)
                | NodeValue::TableCell
                | NodeValue::Document => InlineKind::Recurse,
                _ if node.children().next().is_some() => InlineKind::Recurse,
                _ => InlineKind::Skip,
            }
        };

        match kind {
            InlineKind::Text(text) => out.push(json!({ "type": "text", "text": text })),
            InlineKind::SoftBreak => out.push(json!({ "type": "soft_break" })),
            InlineKind::LineBreak => out.push(json!({ "type": "line_break" })),
            InlineKind::Code(text) => out.push(json!({ "type": "code", "text": text })),
            InlineKind::Html(html) => {
                if self.markdown.include.raw_html {
                    self.raw_html.push(json!({
                        "block_id": block_id,
                        "kind": "inline",
                        "html": html,
                    }));
                    out.push(json!({ "type": "html_inline", "html": html }));
                }
            }
            InlineKind::Emphasis => {
                out.push(
                    json!({ "type": "emphasis", "children": self.collect_inlines(node, block_id) }),
                );
            }
            InlineKind::Strong => {
                out.push(
                    json!({ "type": "strong", "children": self.collect_inlines(node, block_id) }),
                );
            }
            InlineKind::Strikethrough => {
                out.push(json!({ "type": "strikethrough", "children": self.collect_inlines(node, block_id) }));
            }
            InlineKind::Link { url, title } => {
                let children = self.collect_inlines(node, block_id);
                let text = normalize_text(&plain_text(node), self.markdown);
                if self.markdown.include.links {
                    self.links.push(json!({
                        "block_id": block_id,
                        "text": text,
                        "url": url,
                        "title": title,
                    }));
                }
                out.push(json!({
                    "type": "link",
                    "url": url,
                    "title": title,
                    "text": text,
                    "children": children,
                }));
            }
            InlineKind::Image { url, title } => {
                let children = self.collect_inlines(node, block_id);
                let alt = normalize_text(&plain_text(node), self.markdown);
                if self.markdown.include.images {
                    self.images.push(json!({
                        "block_id": block_id,
                        "alt": alt,
                        "url": url,
                        "title": title,
                    }));
                }
                out.push(json!({
                    "type": "image",
                    "url": url,
                    "title": title,
                    "alt": alt,
                    "children": children,
                }));
            }
            InlineKind::Recurse => {
                for child in node.children() {
                    self.collect_inline_node(child, block_id, out);
                }
            }
            InlineKind::Skip => {}
        }
    }

    fn common_block(
        &self,
        id: String,
        block_type: &str,
        section_id: String,
        parent_block_id: Option<String>,
        text: String,
        inlines: Vec<JsonValue>,
    ) -> Map<String, JsonValue> {
        let mut block = Map::new();
        block.insert("id".to_string(), JsonValue::String(id));
        block.insert(
            "type".to_string(),
            JsonValue::String(block_type.to_string()),
        );
        block.insert("section_id".to_string(), JsonValue::String(section_id));
        block.insert(
            "parent_block_id".to_string(),
            parent_block_id.map_or(JsonValue::Null, JsonValue::String),
        );
        block.insert("text".to_string(), JsonValue::String(text));
        block.insert("inlines".to_string(), JsonValue::Array(inlines));
        block
    }

    fn push_block(&mut self, block: Map<String, JsonValue>, section_content: bool) -> usize {
        let index = self.blocks.len();
        let id = block
            .get("id")
            .and_then(JsonValue::as_str)
            .unwrap_or_default()
            .to_string();
        if section_content {
            self.push_content_block_id(&id);
        }
        self.block_index_by_id.insert(id, index);
        self.blocks.push(block);
        index
    }

    fn next_block_id(&mut self) -> String {
        let id = format!("b{}", self.next_block_number);
        self.next_block_number += 1;
        id
    }

    fn current_section_id(&mut self) -> String {
        if self.active_section_id.is_none() {
            self.ensure_preamble();
        }
        self.active_section_id.clone().unwrap_or_default()
    }

    fn ensure_preamble(&mut self) {
        if self.section_by_id.contains_key("preamble") {
            self.active_section_id = Some("preamble".to_string());
            return;
        }
        let section = FlatSection {
            id: "preamble".to_string(),
            level: 0,
            heading: String::new(),
            heading_block_id: None,
            path: Vec::new(),
            ordinal_path: Vec::new(),
            content_block_ids: Vec::new(),
            child_ids: Vec::new(),
            parent_id: None,
        };
        self.section_by_id
            .insert(section.id.clone(), self.flat_sections.len());
        self.flat_sections.push(section);
        self.active_section_id = Some("preamble".to_string());
    }

    fn open_heading_section(
        &mut self,
        level: u8,
        heading: String,
        heading_block_id: String,
    ) -> String {
        let level_index = usize::from(level);
        self.counters[level_index] += 1;
        for counter in self.counters.iter_mut().skip(level_index + 1) {
            *counter = 0;
        }
        self.section_stack.retain(|item| item.level < level);
        let mut path = self
            .section_stack
            .iter()
            .map(|item| item.heading.clone())
            .collect::<Vec<_>>();
        path.push(heading.clone());
        let mut ordinal_path = self
            .section_stack
            .iter()
            .map(|item| item.ordinal)
            .collect::<Vec<_>>();
        ordinal_path.push(self.counters[level_index]);
        let id = self.section_id(level, self.counters[level_index]);
        let parent_id = self.section_stack.last().map(|item| item.id.clone());
        if let Some(parent_id) = &parent_id
            && let Some(parent) = self.section_mut(parent_id)
        {
            parent.child_ids.push(id.clone());
        }
        let section = FlatSection {
            id: id.clone(),
            level,
            heading: heading.clone(),
            heading_block_id: Some(heading_block_id),
            path,
            ordinal_path,
            content_block_ids: Vec::new(),
            child_ids: Vec::new(),
            parent_id,
        };
        self.section_by_id
            .insert(section.id.clone(), self.flat_sections.len());
        self.flat_sections.push(section);
        self.section_stack.push(StackSection {
            id: id.clone(),
            level,
            heading,
            ordinal: self.counters[level_index],
        });
        self.active_section_id = Some(id.clone());
        id
    }

    fn section_id(&self, level: u8, ordinal: usize) -> String {
        let mut parts = self
            .section_stack
            .iter()
            .map(|item| format!("s{}-{}", item.level, item.ordinal))
            .collect::<Vec<_>>();
        parts.push(format!("s{}-{}", level, ordinal));
        parts.join(".")
    }

    fn push_content_block_id(&mut self, block_id: &str) {
        let Some(section_id) = self.active_section_id.clone() else {
            return;
        };
        if let Some(section) = self.section_mut(&section_id) {
            section.content_block_ids.push(block_id.to_string());
        }
    }

    fn section_mut(&mut self, id: &str) -> Option<&mut FlatSection> {
        self.section_by_id
            .get(id)
            .copied()
            .and_then(|index| self.flat_sections.get_mut(index))
    }

    fn nested_sections(&self) -> Vec<Section> {
        self.flat_sections
            .iter()
            .filter(|section| section.parent_id.is_none())
            .map(|section| self.nested_section(section))
            .collect()
    }

    fn nested_section(&self, section: &FlatSection) -> Section {
        let children = section
            .child_ids
            .iter()
            .filter_map(|child_id| {
                self.section_by_id
                    .get(child_id)
                    .and_then(|index| self.flat_sections.get(*index))
            })
            .map(|child| self.nested_section(child))
            .collect();
        Section {
            id: section.id.clone(),
            level: section.level,
            heading: section.heading.clone(),
            heading_block_id: section.heading_block_id.clone(),
            path: section.path.clone(),
            ordinal_path: section.ordinal_path.clone(),
            content_block_ids: section.content_block_ids.clone(),
            child_ids: section.child_ids.clone(),
            children,
        }
    }
}

pub(super) fn plain_text(node: Node<'_>) -> String {
    let mut out = String::new();
    collect_plain_text(node, &mut out);
    out
}

fn collect_plain_text(node: Node<'_>, out: &mut String) {
    let separate_children = {
        let data = node.data.borrow();
        matches!(
            data.value,
            NodeValue::Document
                | NodeValue::BlockQuote
                | NodeValue::List(_)
                | NodeValue::Item(_)
                | NodeValue::TaskItem(_)
                | NodeValue::Table(_)
                | NodeValue::TableRow(_)
        )
    };
    {
        let data = node.data.borrow();
        match &data.value {
            NodeValue::Text(text) => out.push_str(text),
            NodeValue::Code(code) => out.push_str(&code.literal),
            NodeValue::SoftBreak | NodeValue::LineBreak => out.push(' '),
            NodeValue::HtmlBlock(html) => out.push_str(&html_to_text(&html.literal)),
            NodeValue::HtmlInline(html) => out.push_str(&html_to_text(html)),
            _ => {}
        }
    }
    let mut first_child = true;
    for child in node.children() {
        if separate_children && !first_child && !out.ends_with(char::is_whitespace) {
            out.push(' ');
        }
        let before = out.len();
        collect_plain_text(child, out);
        if separate_children && before != out.len() && !out.ends_with(char::is_whitespace) {
            out.push(' ');
        }
        first_child = false;
    }
}

pub(super) fn push_body_text(out: &mut String, value: &str, markdown: &MarkdownInput) {
    let value = normalize_text(value, markdown);
    if value.is_empty() {
        return;
    }
    if !out.is_empty() && !out.ends_with(char::is_whitespace) && !value.starts_with(no_space_before)
    {
        out.push(' ');
    }
    out.push_str(&value);
}

pub(super) fn normalize_text(value: &str, markdown: &MarkdownInput) -> String {
    let value = if markdown.trim_text {
        value.trim()
    } else {
        value
    };
    if markdown.collapse_whitespace {
        value.split_whitespace().collect::<Vec<_>>().join(" ")
    } else {
        value.to_string()
    }
}

pub(super) fn html_to_text(value: &str) -> String {
    let mut out = String::new();
    let mut in_tag = false;
    for ch in value.chars() {
        match ch {
            '<' => in_tag = true,
            '>' => in_tag = false,
            _ if !in_tag => out.push(ch),
            _ => {}
        }
    }
    out
}

fn normalize_raw_html(value: &str, markdown: &MarkdownInput) -> String {
    if markdown.trim_text {
        value.trim().to_string()
    } else {
        value.to_string()
    }
}

fn no_space_before(ch: char) -> bool {
    matches!(ch, '.' | ',' | ';' | ':' | '?' | '!' | ')' | ']')
}

fn section_to_json(section: &Section) -> JsonValue {
    json!({
        "id": section.id,
        "level": section.level,
        "heading": section.heading,
        "heading_block_id": section.heading_block_id,
        "path": section.path,
        "ordinal_path": section.ordinal_path,
        "content_block_ids": section.content_block_ids,
        "child_ids": section.child_ids,
        "children": section.children.iter().map(section_to_json).collect::<Vec<_>>(),
    })
}

fn block_section_to_json(section: &Section) -> JsonValue {
    json!({
        "id": section.id,
        "heading": section.heading,
        "path": section.path,
    })
}

fn section_summary_to_json(section: &SectionSummary) -> JsonValue {
    json!({
        "id": section.id,
        "level": section.level,
        "heading": section.heading,
        "path": section.path,
        "ordinal_path": section.ordinal_path,
    })
}

fn table_alignments(node: Node<'_>) -> Vec<String> {
    let data = node.data.borrow();
    match &data.value {
        NodeValue::Table(table) => table
            .alignments
            .iter()
            .map(|alignment| match alignment {
                TableAlignment::Left => "left",
                TableAlignment::Center => "center",
                TableAlignment::Right => "right",
                TableAlignment::None => "left",
            })
            .map(ToOwned::to_owned)
            .collect(),
        _ => Vec::new(),
    }
}

fn strings_to_value(values: &[String]) -> JsonValue {
    JsonValue::Array(values.iter().cloned().map(JsonValue::String).collect())
}
