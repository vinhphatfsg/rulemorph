use std::collections::{HashMap, HashSet};

use super::schema::{FieldType, SchemaNode};

mod identifiers;
mod literals;

pub(super) use self::identifiers::{field_identifier, safe_type_name};
pub(super) use self::literals::{
    go_json_tag_literal, json_string_literal, rust_string_literal, safe_comment_text,
    swift_string_literal,
};

pub(super) struct TypeDef<'a> {
    pub(super) name: String,
    pub(super) node: &'a SchemaNode,
    pub(super) path: Vec<String>,
}

pub(super) struct NameRegistry {
    base: String,
    used: HashSet<String>,
    names: HashMap<Vec<String>, String>,
}

impl NameRegistry {
    pub(super) fn new(base: &str) -> Self {
        Self {
            base: base.to_string(),
            used: HashSet::new(),
            names: HashMap::new(),
        }
    }

    fn type_name_for_path(&mut self, path: &[String]) -> String {
        if let Some(name) = self.names.get(path) {
            return name.clone();
        }

        let mut name = self.base.clone();
        for segment in path {
            name.push_str(&pascal_case(&words_from_key(segment)));
        }

        if name.is_empty() {
            name = "Record".to_string();
        }

        let mut unique = name.clone();
        let mut suffix = 2;
        while self.used.contains(&unique) {
            unique = format!("{}_{}", name, suffix);
            suffix += 1;
        }
        self.used.insert(unique.clone());
        self.names.insert(path.to_vec(), unique.clone());
        unique
    }

    pub(super) fn get(&self, path: &[String]) -> Option<&String> {
        self.names.get(path)
    }
}

pub(super) fn collect_types<'a>(
    node: &'a SchemaNode,
    path: Vec<String>,
    registry: &mut NameRegistry,
    out: &mut Vec<TypeDef<'a>>,
) {
    for field in &node.fields {
        let mut child_path = path.clone();
        child_path.push(field.key.clone());
        collect_nested_types(&field.field_type, child_path, registry, out);
    }

    let name = registry.type_name_for_path(&path);
    out.push(TypeDef { name, node, path });
}

fn collect_nested_types<'a>(
    field_type: &'a FieldType,
    path: Vec<String>,
    registry: &mut NameRegistry,
    out: &mut Vec<TypeDef<'a>>,
) {
    match field_type {
        FieldType::Object(child) => {
            registry.type_name_for_path(&path);
            collect_types(child, path, registry, out);
        }
        FieldType::Array(inner) | FieldType::Map(inner) | FieldType::Nullable(inner) => {
            collect_nested_types(inner, path, registry, out);
        }
        FieldType::Primitive(_) | FieldType::JsonValue => {}
    }
}

fn words_from_key(key: &str) -> Vec<String> {
    let mut words = Vec::new();
    let mut current = String::new();
    for ch in key.chars() {
        if ch.is_ascii_alphanumeric() {
            if ch.is_ascii_uppercase()
                && !current.is_empty()
                && current
                    .chars()
                    .last()
                    .is_some_and(|prev| prev.is_ascii_lowercase() || prev.is_ascii_digit())
            {
                words.push(current);
                current = String::new();
            }
            current.push(ch);
        } else if !current.is_empty() {
            words.push(current);
            current = String::new();
        }
    }
    if !current.is_empty() {
        words.push(current);
    }
    if words.is_empty() {
        words.push("field".to_string());
    }
    words
}

fn snake_case(words: &[String]) -> String {
    words
        .iter()
        .map(|word| word.to_lowercase())
        .collect::<Vec<String>>()
        .join("_")
}

fn lower_camel(words: &[String]) -> String {
    if words.is_empty() {
        return String::new();
    }

    let mut iter = words.iter();
    let first = iter
        .next()
        .map(|word| word.to_lowercase())
        .unwrap_or_default();
    let mut result = first;
    for word in iter {
        result.push_str(&capitalize(word));
    }
    result
}

fn pascal_case(words: &[String]) -> String {
    let mut result = String::new();
    for word in words {
        result.push_str(&capitalize(word));
    }
    result
}

fn capitalize(value: &str) -> String {
    let mut chars = value.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().collect::<String>() + &chars.as_str().to_lowercase(),
        None => String::new(),
    }
}
