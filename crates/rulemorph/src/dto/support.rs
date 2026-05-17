use std::collections::{HashMap, HashSet};

use super::schema::{FieldType, SchemaNode};

mod naming;

pub(super) use naming::{field_identifier, safe_type_name};
use naming::{pascal_case, words_from_key};

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
        if let FieldType::Object(child) = &field.field_type {
            let mut child_path = path.clone();
            child_path.push(field.key.clone());
            registry.type_name_for_path(&child_path);
            collect_types(child, child_path, registry, out);
        }
    }

    let name = registry.type_name_for_path(&path);
    out.push(TypeDef { name, node, path });
}

pub(super) fn rust_string_literal(value: &str) -> String {
    format!("{:?}", value)
}

pub(super) fn json_string_literal(value: &str) -> String {
    serde_json::to_string(value).expect("string serialization should not fail")
}

pub(super) fn swift_string_literal(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            ch if ch.is_control() => out.push_str(&format!("\\u{{{:x}}}", ch as u32)),
            ch => out.push(ch),
        }
    }
    out.push('"');
    out
}

pub(super) fn safe_comment_text(value: &str) -> String {
    value.replace("*/", "* /").replace(['\r', '\n'], "\\n")
}

pub(super) fn go_json_tag_literal(key: &str, optional: bool) -> String {
    if !key.contains('`')
        && !key.contains('"')
        && !key.contains('\\')
        && !key.contains('\r')
        && !key.contains('\n')
    {
        if optional {
            return format!("`json:\"{},omitempty\"`", key);
        }
        return format!("`json:\"{}\"`", key);
    }
    let mut tag_key = String::with_capacity(key.len());
    for ch in key.chars() {
        match ch {
            '\\' => tag_key.push_str("\\\\"),
            '"' => tag_key.push_str("\\\""),
            '\n' => tag_key.push_str("\\n"),
            '\r' => tag_key.push_str("\\r"),
            '\t' => tag_key.push_str("\\t"),
            ch => tag_key.push(ch),
        }
    }
    let tag = if optional {
        format!("json:\"{},omitempty\"", tag_key)
    } else {
        format!("json:\"{}\"", tag_key)
    };
    json_string_literal(&tag)
}
