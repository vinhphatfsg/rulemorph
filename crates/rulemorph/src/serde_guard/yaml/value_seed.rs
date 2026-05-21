use std::cell::Cell;
use std::collections::HashSet;
use std::fmt;

use serde::Deserializer;
use serde::de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor};
use serde_yaml::{Mapping as YamlMapping, Number as YamlNumber, Value as YamlValue};

#[derive(Clone, Copy)]
pub(super) struct YamlValueSeed<'a> {
    depth: usize,
    max_depth: Option<usize>,
    max_nodes: Option<usize>,
    max_array_len: Option<usize>,
    max_text_bytes: Option<usize>,
    node_count: Option<&'a Cell<usize>>,
}

impl<'a> YamlValueSeed<'a> {
    pub(super) fn unbounded() -> Self {
        Self {
            depth: 0,
            max_depth: None,
            max_nodes: None,
            max_array_len: None,
            max_text_bytes: None,
            node_count: None,
        }
    }

    pub(super) fn bounded(
        max_depth: usize,
        max_nodes: usize,
        max_array_len: usize,
        max_text_bytes: usize,
        node_count: &'a Cell<usize>,
    ) -> Self {
        Self {
            depth: 0,
            max_depth: Some(max_depth),
            max_nodes: Some(max_nodes),
            max_array_len: Some(max_array_len),
            max_text_bytes: Some(max_text_bytes),
            node_count: Some(node_count),
        }
    }

    fn child(self) -> Self {
        Self {
            depth: self.depth + 1,
            ..self
        }
    }
}

impl<'de> DeserializeSeed<'de> for YamlValueSeed<'_> {
    type Value = YamlValue;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        if self
            .max_depth
            .is_some_and(|max_depth| self.depth > max_depth)
        {
            return Err(de::Error::custom("input exceeds max_depth"));
        }
        if let (Some(max_nodes), Some(node_count)) = (self.max_nodes, self.node_count) {
            let next = node_count.get().saturating_add(1);
            if next > max_nodes {
                return Err(de::Error::custom("input exceeds max_yaml_expanded_nodes"));
            }
            node_count.set(next);
        }
        deserializer.deserialize_any(YamlValueVisitor { seed: self })
    }
}

struct YamlValueVisitor<'a> {
    seed: YamlValueSeed<'a>,
}

impl<'de> Visitor<'de> for YamlValueVisitor<'_> {
    type Value = YamlValue;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a YAML value")
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
        Ok(YamlValue::Bool(value))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
        Ok(YamlValue::Number(value.into()))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
        Ok(YamlValue::Number(value.into()))
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if !value.is_finite() {
            return Err(E::custom("non-finite YAML number is not allowed"));
        }
        Ok(YamlValue::Number(YamlNumber::from(value)))
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if self
            .seed
            .max_text_bytes
            .is_some_and(|max_text_bytes| value.len() > max_text_bytes)
        {
            return Err(E::custom("input exceeds max_text_bytes"));
        }
        Ok(YamlValue::String(value.to_string()))
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if self
            .seed
            .max_text_bytes
            .is_some_and(|max_text_bytes| value.len() > max_text_bytes)
        {
            return Err(E::custom("input exceeds max_text_bytes"));
        }
        Ok(YamlValue::String(value))
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(YamlValue::Null)
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(YamlValue::Null)
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        if let (Some(hint), Some(max_array_len)) = (seq.size_hint(), self.seed.max_array_len)
            && hint > max_array_len
        {
            return Err(de::Error::custom("input exceeds max_array_len"));
        }
        let capacity = match (seq.size_hint(), self.seed.max_array_len) {
            (Some(hint), Some(max_array_len)) => hint.min(max_array_len),
            (Some(hint), None) => hint,
            (None, _) => 0,
        };
        let mut values = Vec::with_capacity(capacity);
        while let Some(value) = seq.next_element_seed(self.seed.child())? {
            values.push(value);
            if self
                .seed
                .max_array_len
                .is_some_and(|max_array_len| values.len() > max_array_len)
            {
                return Err(de::Error::custom("input exceeds max_array_len"));
            }
        }
        Ok(YamlValue::Sequence(values))
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut values = YamlMapping::new();
        let mut keys = HashSet::new();
        while let Some(key) = map.next_key_seed(self.seed.child())? {
            let signature = yaml_key_signature(&key);
            if !keys.insert(signature.clone()) {
                return Err(de::Error::custom(format!("duplicate key `{}`", signature)));
            }
            let value = map.next_value_seed(self.seed.child())?;
            values.insert(key, value);
        }
        Ok(YamlValue::Mapping(values))
    }
}

fn yaml_key_signature(value: &YamlValue) -> String {
    match value {
        YamlValue::Null => "~".to_string(),
        YamlValue::Bool(value) => value.to_string(),
        YamlValue::Number(value) => value.to_string(),
        YamlValue::String(value) => value.clone(),
        _ => format!("{:?}", value),
    }
}
