use super::*;

impl<'a> DocumentBuilder<'a> {
    pub(super) fn add_table(
        &mut self,
        node: Node<'_>,
        parent_block_id: Option<String>,
        top_level_content: bool,
    ) -> Result<String, TransformError> {
        let id = self.next_block_id();
        let section_id = self.current_section_id();
        let table_index = self.next_table_index;
        self.next_table_index += 1;
        let alignments = table_alignments(node);
        let (header_row, rows) = self.table_rows(node, &id)?;
        let text = self.normalized_container_text(node)?;
        self.push_body_text(&text)?;
        let section_content = self.is_section_content_block(
            &section_id,
            parent_block_id.as_deref(),
            top_level_content,
        );
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
        self.push_block(block, section_content);
        self.tables.push(json!({
            "block_id": id,
            "section_id": section_id,
            "table_index": table_index,
            "alignments": alignments,
            "header_row": header_row,
            "rows": rows,
        }));
        Ok(id)
    }

    pub(super) fn table_rows(
        &mut self,
        node: Node<'_>,
        block_id: &str,
    ) -> Result<(JsonValue, Vec<JsonValue>), TransformError> {
        let mut header_row = json!({ "cells": [] });
        let mut rows = Vec::new();
        for row in node.children() {
            let is_header = matches!(row.data.borrow().value, NodeValue::TableRow(true));
            let mut cells = Vec::new();
            for (column_index, cell) in row
                .children()
                .filter(|cell| matches!(cell.data.borrow().value, NodeValue::TableCell))
                .enumerate()
            {
                cells.push(json!({
                    "column_index": column_index,
                    "text": self.normalized_text(&plain_text(cell))?,
                    "inlines": self.collect_inlines(cell, block_id)?,
                }));
            }
            if is_header {
                header_row = json!({ "cells": cells });
            } else {
                rows.push(json!({
                    "row_index": rows.len(),
                    "cells": cells,
                }));
            }
        }
        Ok((header_row, rows))
    }

    pub(super) fn collect_inlines(
        &mut self,
        node: Node<'_>,
        block_id: &str,
    ) -> Result<Vec<JsonValue>, TransformError> {
        let mut out = Vec::new();
        for child in node.children() {
            self.collect_inline_node(child, block_id, &mut out)?;
        }
        Ok(out)
    }

    pub(super) fn collect_inline_node(
        &mut self,
        node: Node<'_>,
        block_id: &str,
        out: &mut Vec<JsonValue>,
    ) -> Result<(), TransformError> {
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
                    if self.markdown.include.raw_html {
                        InlineKind::Html(self.raw_html(html)?)
                    } else {
                        InlineKind::Skip
                    }
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
            InlineKind::Text(text) => {
                self.check_text_bytes(&text)?;
                out.push(json!({ "type": "text", "text": text }));
            }
            InlineKind::SoftBreak => out.push(json!({ "type": "soft_break" })),
            InlineKind::LineBreak => out.push(json!({ "type": "line_break" })),
            InlineKind::Code(text) => {
                self.check_text_bytes(&text)?;
                out.push(json!({ "type": "code", "text": text }));
            }
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
                out.push(json!({
                    "type": "emphasis",
                    "children": self.collect_inlines(node, block_id)?,
                }));
            }
            InlineKind::Strong => {
                out.push(json!({
                    "type": "strong",
                    "children": self.collect_inlines(node, block_id)?,
                }));
            }
            InlineKind::Strikethrough => {
                out.push(json!({
                    "type": "strikethrough",
                    "children": self.collect_inlines(node, block_id)?,
                }));
            }
            InlineKind::Link { url, title } => {
                if self.markdown.include.blocks || self.markdown.include.links {
                    self.check_text_bytes(&url)?;
                    self.check_text_bytes(&title)?;
                }
                let children = self.collect_inlines(node, block_id)?;
                let text = self.normalized_text(&plain_text(node))?;
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
                if self.markdown.include.blocks || self.markdown.include.images {
                    self.check_text_bytes(&url)?;
                    self.check_text_bytes(&title)?;
                }
                let children = self.collect_inlines(node, block_id)?;
                let alt = self.normalized_text(&plain_text(node))?;
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
                    self.collect_inline_node(child, block_id, out)?;
                }
            }
            InlineKind::Skip => {}
        }
        Ok(())
    }
}
