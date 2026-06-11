use std::collections::{HashMap, HashSet};

use comrak::nodes::{ListType, Node, NodeValue, TableAlignment};
use comrak::{Arena, Options, parse_document};
use serde_json::{Map, Value as JsonValue, json};

use crate::error::{TransformError, TransformErrorKind};
use crate::model::{MarkdownFlavor, MarkdownInput, MarkdownRecordsMode, RuleFile};

use super::{NormalizationOptions, enforce_json_limits, enforce_records_limit};

mod builder;
mod document;
mod frontmatter;
mod project;
mod resource;
mod text;

use builder::DocumentBuilder;
use document::document_record;

use document::{
    MarkdownDocument, Section, block_section_to_json, section_to_json, strings_to_value,
};
use text::{normalize_text, push_body_text};

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
    validate_markdown_runtime_options(markdown)?;

    let split = frontmatter::split_frontmatter(markdown.frontmatter, input, options)?;
    resource::enforce_markdown_structural_preflight(
        split.body,
        markdown.flavor == MarkdownFlavor::Gfm,
        options,
    )?;
    let arena = Arena::new();
    let parser_options = parser_options(markdown);
    let root = parse_document(&arena, split.body, &parser_options);
    resource::count_parsed_markdown_nodes(root, options)?;
    let mut builder = DocumentBuilder::new(markdown, options, split.frontmatter);
    builder.collect(root)?;
    let document = builder.finish();

    let records = match markdown.records {
        MarkdownRecordsMode::Document => vec![document_record(document, markdown)],
        MarkdownRecordsMode::Sections => project::project_sections(&document, markdown, options)?,
        MarkdownRecordsMode::TableRows => {
            project::project_table_rows(&document, markdown, options)?
        }
    };
    enforce_records_limit(records.len(), options)?;
    for record in &records {
        enforce_json_limits(record, options)?;
    }
    Ok(records)
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

fn validate_markdown_runtime_options(markdown: &MarkdownInput) -> Result<(), TransformError> {
    if markdown.include.body_markdown {
        return Err(TransformError::new(
            TransformErrorKind::InvalidInput,
            "markdown.include.body_markdown is not currently supported",
        ));
    }
    if markdown.include.sourcepos {
        return Err(TransformError::new(
            TransformErrorKind::InvalidInput,
            "markdown.include.sourcepos is not currently supported",
        ));
    }
    if let Some(levels) = markdown.section_levels.as_deref() {
        if levels.is_empty() {
            return Err(TransformError::new(
                TransformErrorKind::InvalidInput,
                "markdown.section_levels must not be empty",
            ));
        }
        let mut seen = HashSet::new();
        for level in levels {
            if !(1..=6).contains(level) {
                return Err(TransformError::new(
                    TransformErrorKind::InvalidInput,
                    "markdown.section_levels entries must be 1..=6",
                ));
            }
            if !seen.insert(*level) {
                return Err(TransformError::new(
                    TransformErrorKind::InvalidInput,
                    "markdown.section_levels entries must be unique",
                ));
            }
        }
    }
    Ok(())
}

fn invalid(message: impl Into<String>) -> TransformError {
    TransformError::new(TransformErrorKind::InvalidInput, message)
}
