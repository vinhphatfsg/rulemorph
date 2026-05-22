use std::cell::Cell;
use std::fmt;

use serde::de::DeserializeSeed;
use serde_yaml::Value as YamlValue;

mod value_seed;

use value_seed::YamlValueSeed;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StrictYamlError {
    message: String,
    line: Option<usize>,
    column: Option<usize>,
}

impl StrictYamlError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            line: None,
            column: None,
        }
    }

    fn from_serde_yaml(err: serde_yaml::Error) -> Self {
        let location = err.location();
        Self {
            message: err.to_string(),
            line: location.as_ref().map(|loc| loc.line()),
            column: location.as_ref().map(|loc| loc.column()),
        }
    }

    pub fn location(&self) -> Option<(usize, usize)> {
        self.line.zip(self.column)
    }
}

impl fmt::Display for StrictYamlError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for StrictYamlError {}

pub fn parse_yaml_value_strict(source: &str) -> Result<YamlValue, StrictYamlError> {
    let mut documents = serde_yaml::Deserializer::from_str(source);
    let deserializer = documents
        .next()
        .ok_or_else(|| StrictYamlError::new("YAML stream must contain one document"))?;
    YamlValueSeed::unbounded()
        .deserialize(deserializer)
        .map_err(StrictYamlError::from_serde_yaml)
        .and_then(|value| reject_trailing_yaml_documents(documents).map(|_| value))
}

pub fn parse_yaml_value_strict_with_limits(
    source: &str,
    max_depth: usize,
    max_nodes: usize,
    max_array_len: usize,
    max_text_bytes: usize,
) -> Result<YamlValue, StrictYamlError> {
    let mut documents = serde_yaml::Deserializer::from_str(source);
    let deserializer = documents
        .next()
        .ok_or_else(|| StrictYamlError::new("YAML stream must contain one document"))?;
    let node_count = Cell::new(0usize);
    YamlValueSeed::bounded(
        max_depth,
        max_nodes,
        max_array_len,
        max_text_bytes,
        &node_count,
    )
    .deserialize(deserializer)
    .map_err(StrictYamlError::from_serde_yaml)
    .and_then(|value| reject_trailing_yaml_documents(documents).map(|_| value))
}

fn reject_trailing_yaml_documents<'de, I>(documents: I) -> Result<(), StrictYamlError>
where
    I: IntoIterator<Item = serde_yaml::Deserializer<'de>>,
{
    if documents.into_iter().next().is_some() {
        return Err(StrictYamlError::new(
            "YAML stream must contain exactly one document",
        ));
    }
    Ok(())
}
