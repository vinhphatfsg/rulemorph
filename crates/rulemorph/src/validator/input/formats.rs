use std::collections::HashSet;

use crate::error::ErrorCode;
use crate::excel_ref::{column_letters_to_index, parse_cell_window};
use crate::model::{
    Column, ExcelCellErrorPolicy, ExcelColumn, ExcelEmptyCellPolicy, ExcelInput, HtmlInput,
    HtmlValueKind, MarkdownInput, XmlInput,
};
use crate::xml_name::is_xml_name;

use crate::validator::ValidationCtx;

pub(super) fn validate_columns(columns: &[Column], base_path: &str, ctx: &mut ValidationCtx<'_>) {
    let mut names = HashSet::new();
    for (index, column) in columns.iter().enumerate() {
        let path = format!("{}[{}].name", base_path, index);
        let name = column.name.trim();
        if name.is_empty() {
            ctx.push(
                ErrorCode::InvalidInputOption,
                "column name is required",
                path,
            );
            continue;
        }
        if !names.insert(name.to_string()) {
            ctx.push(
                ErrorCode::DuplicateInputField,
                "column name must be unique",
                path,
            );
        }
    }
}

fn validate_excel_columns(columns: &[ExcelColumn], base_path: &str, ctx: &mut ValidationCtx<'_>) {
    let mut names = HashSet::new();
    for (index, column) in columns.iter().enumerate() {
        let name_path = format!("{}[{}].name", base_path, index);
        let name = column.name.trim();
        if name.is_empty() {
            ctx.push(
                ErrorCode::InvalidInputOption,
                "column name is required",
                name_path,
            );
        } else if !names.insert(name.to_string()) {
            ctx.push(
                ErrorCode::DuplicateInputField,
                "column name must be unique",
                name_path,
            );
        }
        if column.column.trim().is_empty() {
            ctx.push(
                ErrorCode::InvalidInputOption,
                "excel column reference is required",
                format!("{}[{}].column", base_path, index),
            );
        } else if let Err(err) = column_letters_to_index(&column.column) {
            ctx.push(
                ErrorCode::InvalidInputOption,
                &err.message,
                format!("{}[{}].column", base_path, index),
            );
        }
    }
}

pub(super) fn validate_xml_input(xml: &XmlInput, ctx: &mut ValidationCtx<'_>) {
    if !is_valid_xml_records_path(&xml.records_path) {
        ctx.push(
            ErrorCode::InvalidPath,
            "xml.records_path must be a dot-separated element path",
            "input.xml.records_path",
        );
    }
    if xml.attr_prefix.is_empty() {
        ctx.push(
            ErrorCode::InvalidInputOption,
            "xml.attr_prefix must not be empty",
            "input.xml.attr_prefix",
        );
    }
    if xml.text_key.is_empty() {
        ctx.push(
            ErrorCode::InvalidInputOption,
            "xml.text_key must not be empty",
            "input.xml.text_key",
        );
    }
    if xml.attr_prefix == xml.text_key {
        ctx.push(
            ErrorCode::InvalidInputOption,
            "xml.attr_prefix and text_key must be distinct",
            "input.xml.text_key",
        );
    }
}

fn is_valid_xml_records_path(path: &str) -> bool {
    if path.trim().is_empty() || path.contains('[') || path.contains(']') {
        return false;
    }
    path.split('.').all(is_xml_name)
}

pub(super) fn validate_html_input(html: &HtmlInput, ctx: &mut ValidationCtx<'_>) {
    if html.records_selector.trim().is_empty() {
        ctx.push(
            ErrorCode::InvalidInputOption,
            "html.records_selector must not be empty",
            "input.html.records_selector",
        );
    }
    if html.fields.is_empty() {
        ctx.push(
            ErrorCode::InvalidInputOption,
            "html.fields must not be empty",
            "input.html.fields",
        );
    }
    for (name, field) in &html.fields {
        if name.trim().is_empty() {
            ctx.push(
                ErrorCode::InvalidInputOption,
                "html field name must not be empty",
                "input.html.fields",
            );
        }
        if field.value == HtmlValueKind::Attr
            && field
                .attr
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .is_none()
        {
            ctx.push(
                ErrorCode::InvalidInputOption,
                "html.fields.*.attr is required when value=attr",
                format!("input.html.fields.{}.attr", name),
            );
        }
    }
}

pub(super) fn validate_excel_input(excel: &ExcelInput, ctx: &mut ValidationCtx<'_>) {
    let cell_window = match parse_cell_window(excel.range.as_deref()) {
        Ok(window) => Some(window),
        Err(err) => {
            ctx.push(
                ErrorCode::InvalidInputOption,
                &err.message,
                "input.excel.range",
            );
            None
        }
    };
    if excel.header_row == 0 {
        ctx.push(
            ErrorCode::InvalidInputOption,
            "excel.header_row must be 1-based",
            "input.excel.header_row",
        );
    } else if excel.has_header
        && let Some(window) = cell_window
        && excel.header_row - 1 < window.start_row
    {
        ctx.push(
            ErrorCode::InvalidInputOption,
            "excel.header_row is before selected range",
            "input.excel.header_row",
        );
    }
    if let Some(data_start_row) = excel.data_start_row
        && data_start_row == 0
    {
        ctx.push(
            ErrorCode::InvalidInputOption,
            "excel.data_start_row must be 1-based",
            "input.excel.data_start_row",
        );
    }
    if !excel.has_header && excel.columns.as_ref().is_none_or(Vec::is_empty) {
        ctx.push(
            ErrorCode::MissingExcelColumns,
            "excel.columns is required when has_header=false",
            "input.excel.columns",
        );
    }
    if let Some(columns) = excel.columns.as_deref() {
        validate_excel_columns(columns, "input.excel.columns", ctx);
    }
    if excel.empty_cell != ExcelEmptyCellPolicy::Missing {
        ctx.push(
            ErrorCode::InvalidInputOption,
            "excel.empty_cell must be missing",
            "input.excel.empty_cell",
        );
    }
    if excel.cell_error != ExcelCellErrorPolicy::Error {
        ctx.push(
            ErrorCode::InvalidInputOption,
            "excel.cell_error must be error",
            "input.excel.cell_error",
        );
    }
}

pub(super) fn validate_markdown_input(markdown: &MarkdownInput, ctx: &mut ValidationCtx<'_>) {
    if let Some(levels) = markdown.section_levels.as_deref() {
        if levels.is_empty() {
            ctx.push(
                ErrorCode::InvalidInputOption,
                "markdown.section_levels must not be empty",
                "input.markdown.section_levels",
            );
        }
        let mut seen = HashSet::new();
        for (index, level) in levels.iter().enumerate() {
            let path = format!("input.markdown.section_levels[{}]", index);
            if !(1..=6).contains(level) {
                ctx.push(
                    ErrorCode::InvalidInputOption,
                    "markdown.section_levels entries must be 1..=6",
                    path,
                );
                continue;
            }
            if !seen.insert(*level) {
                ctx.push(
                    ErrorCode::DuplicateInputField,
                    "markdown.section_levels entries must be unique",
                    path,
                );
            }
        }
    }
    if markdown.include.body_markdown {
        ctx.push(
            ErrorCode::InvalidInputOption,
            "markdown.include.body_markdown is not currently supported",
            "input.markdown.include.body_markdown",
        );
    }
    if markdown.include.sourcepos {
        ctx.push(
            ErrorCode::InvalidInputOption,
            "markdown.include.sourcepos is not currently supported",
            "input.markdown.include.sourcepos",
        );
    }
}
