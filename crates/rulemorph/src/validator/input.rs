use crate::error::ErrorCode;
use crate::model::{InputFormat, RuleFile};
use crate::path::parse_path;

use super::ValidationCtx;

mod formats;

use formats::{
    validate_columns, validate_excel_input, validate_html_input, validate_markdown_input,
    validate_xml_input,
};

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
        InputFormat::Markdown => {
            if rule.input.markdown.is_none() {
                ctx.push(
                    ErrorCode::MissingMarkdownSection,
                    "input.markdown is required when format=markdown",
                    "input.markdown",
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
                validate_records_path(path, "input.json.records_path", ctx);
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

    if let InputFormat::Markdown = rule.input.format {
        if let Some(markdown) = &rule.input.markdown {
            validate_markdown_input(markdown, ctx);
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
