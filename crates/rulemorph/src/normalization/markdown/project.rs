use serde_json::{Map, Value as JsonValue, json};

use crate::error::{TransformError, TransformErrorKind};
use crate::model::{MarkdownInput, MarkdownTableHeaderPolicy};

use super::super::{NormalizationOptions, enforce_records_limit};
use super::{
    MarkdownDocument, Section, block_section_to_json, normalize_text, push_body_text,
    section_to_json, strings_to_value,
};

pub(super) fn project_sections(
    document: &MarkdownDocument,
    markdown: &MarkdownInput,
    options: &NormalizationOptions,
) -> Result<Vec<JsonValue>, TransformError> {
    let selected = markdown
        .section_levels
        .clone()
        .unwrap_or_else(|| vec![1, 2, 3, 4, 5, 6]);
    let mut out = Vec::new();
    for section in &document.sections {
        collect_projected_sections(section, document, &selected, markdown, options, &mut out)?;
    }
    Ok(out)
}

fn collect_projected_sections(
    section: &Section,
    document: &MarkdownDocument,
    selected: &[u8],
    markdown: &MarkdownInput,
    options: &NormalizationOptions,
    out: &mut Vec<JsonValue>,
) -> Result<(), TransformError> {
    if selected.contains(&section.level) {
        enforce_records_limit(out.len().saturating_add(1), options)?;
        out.push(project_section_record(section, document, markdown));
    }
    for child in &section.children {
        collect_projected_sections(child, document, selected, markdown, options, out)?;
    }
    Ok(())
}

fn project_section_record(
    section: &Section,
    document: &MarkdownDocument,
    markdown: &MarkdownInput,
) -> JsonValue {
    let mut record = Map::new();
    record.insert(
        "record_type".to_string(),
        JsonValue::String("section".to_string()),
    );
    record.insert(
        "document".to_string(),
        json!({
            "title": document.title,
            "frontmatter": document.frontmatter,
        }),
    );
    record.insert("id".to_string(), JsonValue::String(section.id.clone()));
    record.insert("level".to_string(), json!(section.level));
    record.insert(
        "heading".to_string(),
        JsonValue::String(section.heading.clone()),
    );
    record.insert("path".to_string(), strings_to_value(&section.path));
    record.insert("ordinal_path".to_string(), json!(section.ordinal_path));
    if markdown.include.body_text {
        record.insert(
            "body_text".to_string(),
            JsonValue::String(section_body_text(section, document, markdown)),
        );
    }
    record.insert(
        "heading_block_id".to_string(),
        section
            .heading_block_id
            .clone()
            .map_or(JsonValue::Null, JsonValue::String),
    );
    record.insert(
        "content_block_ids".to_string(),
        strings_to_value(&section.content_block_ids),
    );
    if markdown.include.blocks {
        record.insert(
            "blocks".to_string(),
            JsonValue::Array(section_blocks(section, document)),
        );
    }
    record.insert(
        "children".to_string(),
        JsonValue::Array(section.children.iter().map(section_to_json).collect()),
    );
    JsonValue::Object(record)
}

pub(super) fn project_table_rows(
    document: &MarkdownDocument,
    markdown: &MarkdownInput,
    options: &NormalizationOptions,
) -> Result<Vec<JsonValue>, TransformError> {
    let mut records = Vec::new();
    for table in &document.tables {
        let table_index = table
            .get("table_index")
            .and_then(JsonValue::as_u64)
            .unwrap_or_default() as usize;
        let block_id = table
            .get("block_id")
            .and_then(JsonValue::as_str)
            .unwrap_or_default()
            .to_string();
        let section_id = table
            .get("section_id")
            .and_then(JsonValue::as_str)
            .unwrap_or_default()
            .to_string();
        let alignments = table
            .get("alignments")
            .cloned()
            .unwrap_or_else(|| JsonValue::Array(Vec::new()));
        let headers = table_headers(table);
        let keys = table_keys(&headers, markdown)?;
        let Some(rows) = table.get("rows").and_then(JsonValue::as_array) else {
            continue;
        };
        for row in rows {
            enforce_records_limit(records.len().saturating_add(1), options)?;
            let row_index = row
                .get("row_index")
                .and_then(JsonValue::as_u64)
                .unwrap_or_default() as usize;
            let cells = row
                .get("cells")
                .and_then(JsonValue::as_array)
                .cloned()
                .unwrap_or_default();
            let mut object = Map::new();
            for (index, cell) in cells.iter().enumerate() {
                let key = keys
                    .get(index)
                    .cloned()
                    .unwrap_or_else(|| format!("col_{}", index));
                let value = cell
                    .get("text")
                    .and_then(JsonValue::as_str)
                    .unwrap_or_default()
                    .to_string();
                object.insert(key, JsonValue::String(value));
            }
            records.push(json!({
                "record_type": "table_row",
                "document": {
                    "title": document.title,
                    "frontmatter": document.frontmatter,
                },
                "section": table_row_section(document, &section_id),
                "table": {
                    "block_id": block_id,
                    "table_index": table_index,
                    "alignments": alignments,
                },
                "row_index": row_index,
                "headers": headers,
                "cells": cells,
                "object": object,
            }));
        }
    }
    Ok(records)
}

fn table_row_section(document: &MarkdownDocument, section_id: &str) -> JsonValue {
    find_section(&document.sections, section_id)
        .map(block_section_to_json)
        .unwrap_or_else(|| {
            json!({
                "id": section_id,
                "heading": JsonValue::Null,
                "path": [],
            })
        })
}

fn find_section<'a>(sections: &'a [Section], section_id: &str) -> Option<&'a Section> {
    for section in sections {
        if section.id == section_id {
            return Some(section);
        }
        if let Some(found) = find_section(&section.children, section_id) {
            return Some(found);
        }
    }
    None
}

fn table_headers(table: &JsonValue) -> Vec<String> {
    table
        .get("header_row")
        .and_then(|row| row.get("cells"))
        .and_then(JsonValue::as_array)
        .map(|cells| {
            cells
                .iter()
                .map(|cell| {
                    cell.get("text")
                        .and_then(JsonValue::as_str)
                        .unwrap_or_default()
                        .to_string()
                })
                .collect()
        })
        .unwrap_or_default()
}

fn table_keys(headers: &[String], markdown: &MarkdownInput) -> Result<Vec<String>, TransformError> {
    match markdown.table_header_policy {
        MarkdownTableHeaderPolicy::Index => Ok((0..headers.len())
            .map(|index| format!("col_{}", index))
            .collect()),
        MarkdownTableHeaderPolicy::Strict => {
            let mut seen = std::collections::HashSet::new();
            let mut keys = Vec::with_capacity(headers.len());
            for header in headers {
                let key = header.clone();
                if key.trim().is_empty() || !seen.insert(key.clone()) {
                    return Err(TransformError::new(
                        TransformErrorKind::InvalidInput,
                        "markdown table headers must be non-empty and unique when table_header_policy=strict",
                    ));
                }
                keys.push(key);
            }
            Ok(keys)
        }
    }
}

fn section_body_text(
    section: &Section,
    document: &MarkdownDocument,
    markdown: &MarkdownInput,
) -> String {
    let mut out = String::new();
    append_section_body_text(section, document, markdown, &mut out, false);
    normalize_text(&out, markdown)
}

fn append_section_body_text(
    section: &Section,
    document: &MarkdownDocument,
    markdown: &MarkdownInput,
    out: &mut String,
    include_heading: bool,
) {
    if include_heading {
        push_body_text(out, &section.heading, markdown);
    }
    for block_id in &section.content_block_ids {
        if let Some(block) = block_by_id(document, block_id)
            && let Some(text) = block.get("text").and_then(JsonValue::as_str)
        {
            push_body_text(out, text, markdown);
        }
    }
    for child in &section.children {
        append_section_body_text(child, document, markdown, out, true);
    }
}

fn section_blocks(section: &Section, document: &MarkdownDocument) -> Vec<JsonValue> {
    let mut block_ids = std::collections::HashSet::<String>::new();
    collect_section_block_ids(section, document, &mut block_ids);
    document
        .blocks
        .iter()
        .filter(|block| {
            block
                .get("id")
                .and_then(JsonValue::as_str)
                .is_some_and(|id| block_ids.contains(id))
        })
        .cloned()
        .map(JsonValue::Object)
        .collect()
}

fn collect_section_block_ids(
    section: &Section,
    document: &MarkdownDocument,
    block_ids: &mut std::collections::HashSet<String>,
) {
    for block_id in &section.content_block_ids {
        collect_block_tree_ids(document, block_id, block_ids);
    }
    for child in &section.children {
        if let Some(heading_block_id) = &child.heading_block_id {
            collect_block_tree_ids(document, heading_block_id, block_ids);
        }
        collect_section_block_ids(child, document, block_ids);
    }
}

fn collect_block_tree_ids(
    document: &MarkdownDocument,
    block_id: &str,
    block_ids: &mut std::collections::HashSet<String>,
) {
    if !block_ids.insert(block_id.to_string()) {
        return;
    }
    let Some(block) = block_by_id(document, block_id) else {
        return;
    };
    for field in ["item_ids", "child_block_ids"] {
        if let Some(children) = block.get(field).and_then(JsonValue::as_array) {
            for child in children {
                if let Some(child_id) = child.as_str() {
                    collect_block_tree_ids(document, child_id, block_ids);
                }
            }
        }
    }
}

fn block_by_id<'a>(
    document: &'a MarkdownDocument,
    block_id: &str,
) -> Option<&'a Map<String, JsonValue>> {
    document.blocks.iter().find(|block| {
        block
            .get("id")
            .and_then(JsonValue::as_str)
            .is_some_and(|id| id == block_id)
    })
}
