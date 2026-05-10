use csv::{ReaderBuilder, StringRecordsIntoIter};
use serde_json::{Map, Value as JsonValue};
use std::collections::HashSet;

use crate::error::{TransformError, TransformErrorKind};
use crate::model::RuleFile;

use super::{NormalizationOptions, enforce_records_limit};

pub(crate) fn normalize_csv_records_iter<'a>(
    rule: &RuleFile,
    input: &'a str,
    options: &NormalizationOptions,
) -> Result<CsvRecords<'a>, TransformError> {
    let csv_spec = rule.input.csv.as_ref().ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::InvalidInput,
            "input.csv is required when format=csv",
        )
    })?;
    if csv_spec.delimiter.len() != 1 {
        return Err(TransformError::new(
            TransformErrorKind::InvalidInput,
            "csv.delimiter must be a single-byte character",
        ));
    }

    let mut reader = ReaderBuilder::new()
        .delimiter(csv_spec.delimiter.as_bytes()[0])
        .has_headers(csv_spec.has_header)
        .from_reader(input.as_bytes());

    let headers = if csv_spec.has_header {
        let header_record = reader.headers().map_err(|err| {
            TransformError::new(
                TransformErrorKind::InvalidInput,
                format!("failed to read csv header: {}", err),
            )
        })?;
        header_record
            .iter()
            .enumerate()
            .map(|(index, value)| {
                if index == 0 {
                    value.trim_start_matches('\u{feff}').to_string()
                } else {
                    value.to_string()
                }
            })
            .collect::<Vec<_>>()
    } else {
        let columns = csv_spec
            .columns
            .as_ref()
            .filter(|columns| !columns.is_empty())
            .ok_or_else(|| {
                TransformError::new(
                    TransformErrorKind::InvalidInput,
                    "csv.columns is required when has_header=false",
                )
            })?;
        columns.iter().map(|column| column.name.clone()).collect()
    };
    validate_headers(&headers)?;

    Ok(CsvRecords {
        records: reader.into_records(),
        headers,
        options: options.clone(),
        count: 0,
    })
}

pub(crate) struct CsvRecords<'a> {
    records: StringRecordsIntoIter<&'a [u8]>,
    headers: Vec<String>,
    options: NormalizationOptions,
    count: usize,
}

impl Iterator for CsvRecords<'_> {
    type Item = Result<JsonValue, TransformError>;

    fn next(&mut self) -> Option<Self::Item> {
        let record = match self.records.next()? {
            Ok(record) => record,
            Err(err) => {
                return Some(Err(TransformError::new(
                    TransformErrorKind::InvalidInput,
                    format!("failed to read csv record: {}", err),
                )));
            }
        };
        if record.len() != self.headers.len() {
            return Some(Err(TransformError::new(
                TransformErrorKind::InvalidInput,
                format!(
                    "csv record has {} fields but expected {}",
                    record.len(),
                    self.headers.len()
                ),
            )));
        }

        let mut obj = Map::new();
        for (index, name) in self.headers.iter().enumerate() {
            if let Some(value) = record.get(index) {
                if value.len() > self.options.max_text_bytes {
                    return Some(Err(TransformError::new(
                        TransformErrorKind::InvalidInput,
                        "input exceeds max_text_bytes",
                    )));
                }
                obj.insert(name.clone(), JsonValue::String(value.to_string()));
            }
        }

        self.count += 1;
        if let Err(err) = enforce_records_limit(self.count, &self.options) {
            return Some(Err(err));
        }

        Some(Ok(JsonValue::Object(obj)))
    }
}

fn validate_headers(headers: &[String]) -> Result<(), TransformError> {
    let mut seen = HashSet::new();
    for header in headers {
        if header.trim().is_empty() {
            return Err(TransformError::new(
                TransformErrorKind::InvalidInput,
                "csv header must not be blank",
            ));
        }
        if !seen.insert(header.clone()) {
            return Err(TransformError::new(
                TransformErrorKind::InvalidInput,
                "csv header must be unique",
            ));
        }
    }
    Ok(())
}
