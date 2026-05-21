use serde_json::Value as JsonValue;

use super::PathToken;

pub fn get_path<'a>(value: &'a JsonValue, tokens: &[PathToken]) -> Option<&'a JsonValue> {
    let mut current = value;
    for token in tokens {
        match token {
            PathToken::Key(key) => match current {
                JsonValue::Object(map) => current = map.get(key)?,
                _ => return None,
            },
            PathToken::Index(index) => match current {
                JsonValue::Array(items) => current = items.get(*index)?,
                _ => return None,
            },
        }
    }
    Some(current)
}
