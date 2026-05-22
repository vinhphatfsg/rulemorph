use std::fmt;

use serde_json::Value as JsonValue;

use crate::error::TransformError;

/// Normalized input records.
///
/// Streaming formats can report record-level parse or limit errors while the
/// iterator is consumed, so callers must drain or collect the iterator when
/// they need full input validation.
pub enum NormalizedRecords<'a> {
    Materialized(std::vec::IntoIter<JsonValue>),
    Streaming(Box<dyn Iterator<Item = Result<JsonValue, TransformError>> + 'a>),
}

impl fmt::Debug for NormalizedRecords<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NormalizedRecords::Materialized(_) => formatter.write_str("Materialized(..)"),
            NormalizedRecords::Streaming(_) => formatter.write_str("Streaming(..)"),
        }
    }
}

impl Iterator for NormalizedRecords<'_> {
    type Item = Result<JsonValue, TransformError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            NormalizedRecords::Materialized(iter) => iter.next().map(Ok),
            NormalizedRecords::Streaming(iter) => iter.next(),
        }
    }
}
