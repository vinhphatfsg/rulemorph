mod csv;
#[cfg(feature = "excel")]
mod excel;
#[cfg(feature = "html")]
mod html;
mod input;
mod json;
mod limits;
mod options;
mod records;
mod toml;
mod xml;
mod yaml;

pub use input::InputData;
pub use options::NormalizationOptions;
pub use records::NormalizedRecords;

use crate::error::TransformError;
#[cfg(any(not(feature = "html"), not(feature = "excel")))]
use crate::error::TransformErrorKind;
use crate::model::{InputFormat, RuleFile};

use input::text_input;
pub(crate) use limits::{
    enforce_json_limits, enforce_records_limit, select_records_from_document,
    select_records_from_owned_document,
};

pub fn normalize_records<'a>(
    rule: &RuleFile,
    input: InputData<'a>,
) -> Result<NormalizedRecords<'a>, TransformError> {
    normalize_records_with_options(rule, input, &NormalizationOptions::default())
}

pub fn normalize_records_with_options<'a>(
    rule: &RuleFile,
    input: InputData<'a>,
    options: &NormalizationOptions,
) -> Result<NormalizedRecords<'a>, TransformError> {
    if rule.input.format == InputFormat::Csv {
        return Ok(NormalizedRecords::Streaming(Box::new(
            csv::normalize_csv_records_iter(rule, text_input(input, options)?, options)?,
        )));
    }

    let records = match rule.input.format {
        InputFormat::Csv => unreachable!("CSV records are returned as a streaming iterator"),
        InputFormat::Json => {
            json::normalize_json_records(rule, text_input(input, options)?, options)?
        }
        InputFormat::Yaml => {
            yaml::normalize_yaml_records(rule, text_input(input, options)?, options)?
        }
        InputFormat::Toml => {
            toml::normalize_toml_records(rule, text_input(input, options)?, options)?
        }
        InputFormat::Xml => xml::normalize_xml_records(rule, text_input(input, options)?, options)?,
        #[cfg(feature = "html")]
        InputFormat::Html => {
            html::normalize_html_records(rule, text_input(input, options)?, options)?
        }
        #[cfg(not(feature = "html"))]
        InputFormat::Html => return Err(unsupported_input_format("html")),
        #[cfg(feature = "excel")]
        InputFormat::Excel => excel::normalize_excel_records(rule, input, options)?,
        #[cfg(not(feature = "excel"))]
        InputFormat::Excel => return Err(unsupported_input_format("excel")),
    };
    Ok(NormalizedRecords::Materialized(records.into_iter()))
}

#[cfg(any(not(feature = "html"), not(feature = "excel")))]
fn unsupported_input_format(format: &str) -> TransformError {
    TransformError::new(
        TransformErrorKind::InvalidInput,
        format!("input format {} is not enabled in this build", format),
    )
}
