use std::collections::HashSet;

use crate::error::ErrorCode;
use crate::model::{
    Column, ExcelCellErrorPolicy, ExcelColumn, ExcelEmptyCellPolicy, ExcelInput, HtmlInput,
    HtmlValueKind, InputFormat, RuleFile, XmlInput,
};
use crate::path::parse_path;
use crate::xml_name::is_xml_name;

use super::ValidationCtx;

pub(super) fn validate_input(rule: &RuleFile, ctx: &mut ValidationCtx<'_>) {
    match rule.input.format {
        InputFormat::Csv => {
            if rule.input.csv.is_none() {
                ctx.push(
                    ErrorCode::MissingCsvSection,
                    "input.csv is required when format=csv",
                    "input.csv",
                );
            }
        }
        InputFormat::Json => {
            if rule.input.json.is_none() {
                ctx.push(
                    ErrorCode::MissingJsonSection,
                    "input.json is required when format=json",
                    "input.json",
                );
            }
        }
        InputFormat::Yaml => {
            if rule.input.yaml.is_none() {
                ctx.push(
                    ErrorCode::MissingYamlSection,
                    "input.yaml is required when format=yaml",
                    "input.yaml",
                );
            }
        }
        InputFormat::Toml => {
            if rule.input.toml.is_none() {
                ctx.push(
                    ErrorCode::MissingTomlSection,
                    "input.toml is required when format=toml",
                    "input.toml",
                );
            }
        }
        InputFormat::Xml => {
            if rule.input.xml.is_none() {
                ctx.push(
                    ErrorCode::MissingXmlSection,
                    "input.xml is required when format=xml",
                    "input.xml",
                );
            }
        }
        InputFormat::Html => {
            if rule.input.html.is_none() {
                ctx.push(
                    ErrorCode::MissingHtmlSection,
                    "input.html is required when format=html",
                    "input.html",
                );
            }
        }
        InputFormat::Excel => {
            if rule.input.excel.is_none() {
                ctx.push(
                    ErrorCode::MissingExcelSection,
                    "input.excel is required when format=excel",
                    "input.excel",
                );
            }
        }
    }

    if let InputFormat::Csv = rule.input.format {
        if let Some(csv) = &rule.input.csv {
            if csv.delimiter.len() != 1 {
                ctx.push(
                    ErrorCode::InvalidDelimiterLength,
                    "csv.delimiter must be a single-byte character",
                    "input.csv.delimiter",
                );
            }
            if !csv.has_header && csv.columns.as_ref().is_none_or(Vec::is_empty) {
                ctx.push(
                    ErrorCode::MissingCsvColumns,
                    "csv.columns is required when has_header=false",
                    "input.csv.columns",
                );
            }
            if let Some(columns) = csv.columns.as_deref() {
                validate_columns(columns, "input.csv.columns", ctx);
            }
        }
    }

    if let InputFormat::Json = rule.input.format {
        if let Some(json) = &rule.input.json {
            if let Some(path) = json.records_path.as_deref() {
                if parse_path(path).is_err() {
                    ctx.push(
                        ErrorCode::InvalidPath,
                        "records_path is invalid",
                        "input.json.records_path",
                    );
                }
            }
        }
    }

    if let InputFormat::Yaml = rule.input.format {
        if let Some(yaml) = &rule.input.yaml {
            if let Some(path) = yaml.records_path.as_deref() {
                validate_records_path(path, "input.yaml.records_path", ctx);
            }
        }
    }

    if let InputFormat::Toml = rule.input.format {
        if let Some(toml) = &rule.input.toml {
            if let Some(path) = toml.records_path.as_deref() {
                validate_records_path(path, "input.toml.records_path", ctx);
            }
        }
    }

    if let InputFormat::Xml = rule.input.format {
        if let Some(xml) = &rule.input.xml {
            validate_xml_input(xml, ctx);
        }
    }

    if let InputFormat::Html = rule.input.format {
        if let Some(html) = &rule.input.html {
            validate_html_input(html, ctx);
        }
    }

    if let InputFormat::Excel = rule.input.format {
        if let Some(excel) = &rule.input.excel {
            validate_excel_input(excel, ctx);
        }
    }
}

fn validate_records_path(path: &str, error_path: &str, ctx: &mut ValidationCtx<'_>) {
    if parse_path(path).is_err() {
        ctx.push(
            ErrorCode::InvalidPath,
            "records_path is invalid",
            error_path,
        );
    }
}

fn validate_columns(columns: &[Column], base_path: &str, ctx: &mut ValidationCtx<'_>) {
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
        }
    }
}

fn validate_xml_input(xml: &XmlInput, ctx: &mut ValidationCtx<'_>) {
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

fn validate_html_input(html: &HtmlInput, ctx: &mut ValidationCtx<'_>) {
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

fn validate_excel_input(excel: &ExcelInput, ctx: &mut ValidationCtx<'_>) {
    if excel.header_row == 0 {
        ctx.push(
            ErrorCode::InvalidInputOption,
            "excel.header_row must be 1-based",
            "input.excel.header_row",
        );
    }
    if let Some(data_start_row) = excel.data_start_row {
        if data_start_row == 0 {
            ctx.push(
                ErrorCode::InvalidInputOption,
                "excel.data_start_row must be 1-based",
                "input.excel.data_start_row",
            );
        }
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
