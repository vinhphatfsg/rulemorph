use serde_json::Value as JsonValue;

use crate::error::TransformError;
use crate::model::RuleFile;
use crate::normalization::{
    InputData, NormalizationOptions, NormalizedRecords, normalize_records_with_options,
};

pub(super) fn input_records_iter_with_options<'a>(
    rule: &RuleFile,
    input: InputData<'a>,
    options: &NormalizationOptions,
) -> Result<InputRecordsIter<'a>, TransformError> {
    Ok(InputRecordsIter::Normalized(
        normalize_records_with_options(rule, input, options)?,
    ))
}

pub(super) enum InputRecordsIter<'a> {
    Normalized(NormalizedRecords<'a>),
}

impl Iterator for InputRecordsIter<'_> {
    type Item = Result<JsonValue, TransformError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            InputRecordsIter::Normalized(iter) => iter.next(),
        }
    }
}
