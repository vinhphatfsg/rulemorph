use std::collections::HashSet;
use std::fmt;

use serde::Deserializer;
use serde::de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor};
use serde_json::{Map, Number as JsonNumber, Value as JsonValue};

use crate::error::{TransformError, TransformErrorKind};
use crate::model::RuleFile;

use super::{
    NormalizationOptions, enforce_json_limits, enforce_records_limit, select_records_from_document,
};

pub fn normalize_toml_records(
    rule: &RuleFile,
    input: &str,
    options: &NormalizationOptions,
) -> Result<Vec<JsonValue>, TransformError> {
    let json = parse_toml_json_with_limits(input, options).map_err(|err| {
        TransformError::new(
            TransformErrorKind::InvalidInput,
            format!("failed to parse TOML input: {}", err),
        )
    })?;
    enforce_json_limits(&json, options)?;
    let records = select_records_from_document(
        &json,
        rule.input
            .toml
            .as_ref()
            .and_then(|toml| toml.records_path.as_deref()),
        "input.toml.records_path",
    )?;
    enforce_records_limit(records.len(), options)?;
    Ok(records)
}

fn parse_toml_json_with_limits(
    input: &str,
    options: &NormalizationOptions,
) -> Result<JsonValue, toml::de::Error> {
    TomlJsonSeed { options, depth: 0 }.deserialize(toml::Deserializer::new(input))
}

#[derive(Clone, Copy)]
struct TomlJsonSeed<'a> {
    options: &'a NormalizationOptions,
    depth: usize,
}

impl<'a> TomlJsonSeed<'a> {
    fn child(self) -> Self {
        Self {
            options: self.options,
            depth: self.depth + 1,
        }
    }
}

impl<'de> DeserializeSeed<'de> for TomlJsonSeed<'_> {
    type Value = JsonValue;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        if self.depth > self.options.max_depth {
            return Err(de::Error::custom("input exceeds max_depth"));
        }
        deserializer.deserialize_any(TomlJsonVisitor { seed: self })
    }
}

struct TomlJsonVisitor<'a> {
    seed: TomlJsonSeed<'a>,
}

impl<'de> Visitor<'de> for TomlJsonVisitor<'_> {
    type Value = JsonValue;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a TOML value")
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
        Ok(JsonValue::Bool(value))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
        Ok(JsonValue::Number(value.into()))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
        Ok(JsonValue::Number(value.into()))
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        JsonNumber::from_f64(value)
            .map(JsonValue::Number)
            .ok_or_else(|| E::custom("TOML float is not JSON-compatible"))
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if value.len() > self.seed.options.max_text_bytes {
            return Err(E::custom("input exceeds max_text_bytes"));
        }
        Ok(JsonValue::String(value.to_string()))
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if value.len() > self.seed.options.max_text_bytes {
            return Err(E::custom("input exceeds max_text_bytes"));
        }
        Ok(JsonValue::String(value))
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        if seq
            .size_hint()
            .is_some_and(|hint| hint > self.seed.options.max_array_len)
        {
            return Err(de::Error::custom("input exceeds max_array_len"));
        }
        let capacity = seq
            .size_hint()
            .map(|hint| hint.min(self.seed.options.max_array_len))
            .unwrap_or(0);
        let mut values = Vec::with_capacity(capacity);
        while let Some(value) = seq.next_element_seed(self.seed.child())? {
            values.push(value);
            if values.len() > self.seed.options.max_array_len {
                return Err(de::Error::custom("input exceeds max_array_len"));
            }
        }
        Ok(JsonValue::Array(values))
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut values = Map::new();
        let mut keys = HashSet::new();
        while let Some(key) = map.next_key::<String>()? {
            if !keys.insert(key.clone()) {
                return Err(de::Error::custom(format!("duplicate key `{}`", key)));
            }
            let value = map.next_value_seed(self.seed.child())?;
            values.insert(key, value);
        }
        if values.len() == 1 {
            if let Some(JsonValue::String(value)) = values.remove("$__toml_private_datetime") {
                return Ok(JsonValue::String(value));
            }
        }
        Ok(JsonValue::Object(values))
    }
}
