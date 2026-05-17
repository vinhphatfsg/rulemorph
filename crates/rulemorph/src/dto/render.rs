use std::collections::HashMap;

mod jvm;
mod python;
mod rust;
mod typescript;

use super::schema::{
    Field, FieldType, PrimitiveType, SchemaNode, node_has_required, node_uses_json,
};
use super::support::{
    NameRegistry, collect_types, field_identifier, go_json_tag_literal, swift_string_literal,
};
use super::{DtoError, DtoLanguage};

pub(super) use self::jvm::{render_java, render_kotlin};
pub(super) use self::python::render_python;
pub(super) use self::rust::render_rust;
pub(super) use self::typescript::render_typescript;

pub(super) fn render_go(schema: &SchemaNode, name: &str) -> Result<String, DtoError> {
    let mut registry = NameRegistry::new(name);
    let mut defs = Vec::new();
    collect_types(schema, Vec::new(), &mut registry, &mut defs);

    let uses_json = node_uses_json(schema);

    let mut out = String::new();
    out.push_str("package dto\n\n");
    if uses_json {
        out.push_str("import \"encoding/json\"\n\n");
    }

    for def in defs {
        out.push_str(&format!("type {} struct {{\n", def.name));
        let mut used = HashMap::new();
        for field in &def.node.fields {
            let ident = field_identifier(DtoLanguage::Go, &field.key, &mut used);
            let optional = match &field.field_type {
                FieldType::Object(child) => !node_has_required(child),
                _ => field.optional,
            };
            let field_type = go_type_for_field(field, &def.path, &registry, optional);
            let tag = go_json_tag_literal(&field.key, optional);
            out.push_str(&format!("    {} {} {}\n", ident, field_type, tag));
        }
        out.push_str("}\n\n");
    }

    Ok(out.trim_end().to_string())
}

fn go_type_for_field(
    field: &Field,
    parent_path: &[String],
    registry: &NameRegistry,
    optional: bool,
) -> String {
    let base = match &field.field_type {
        FieldType::Primitive(PrimitiveType::String) => "string".to_string(),
        FieldType::Primitive(PrimitiveType::Int) => "int64".to_string(),
        FieldType::Primitive(PrimitiveType::Float) => "float64".to_string(),
        FieldType::Primitive(PrimitiveType::Bool) => "bool".to_string(),
        FieldType::JsonValue => "json.RawMessage".to_string(),
        FieldType::Object(_) => {
            let mut path = parent_path.to_vec();
            path.push(field.key.clone());
            registry
                .get(&path)
                .cloned()
                .unwrap_or_else(|| "Record".to_string())
        }
    };

    if optional { format!("*{}", base) } else { base }
}

pub(super) fn render_swift(schema: &SchemaNode, name: &str) -> Result<String, DtoError> {
    let mut registry = NameRegistry::new(name);
    let mut defs = Vec::new();
    collect_types(schema, Vec::new(), &mut registry, &mut defs);

    let uses_json = node_uses_json(schema);

    let mut out = String::new();
    for def in defs {
        out.push_str(&format!("struct {}: Codable {{\n", def.name));
        let mut used = HashMap::new();
        let mut coding_keys = Vec::new();
        for field in &def.node.fields {
            let ident = field_identifier(DtoLanguage::Swift, &field.key, &mut used);
            let rename = ident != field.key;
            let optional = match &field.field_type {
                FieldType::Object(child) => !node_has_required(child),
                _ => field.optional,
            };
            let field_type = swift_type_for_field(field, &def.path, &registry, optional);

            out.push_str(&format!("    let {}: {}\n", ident, field_type));
            if rename {
                coding_keys.push(format!(
                    "        case {} = {}",
                    ident,
                    swift_string_literal(&field.key)
                ));
            }
        }

        if !coding_keys.is_empty() {
            out.push('\n');
            out.push_str("    enum CodingKeys: String, CodingKey {\n");
            for line in coding_keys {
                out.push_str(&format!("{}\n", line));
            }
            out.push_str("    }\n");
        }
        out.push_str("}\n\n");
    }

    if uses_json {
        out.push_str(SWIFT_JSON_VALUE);
        out.push('\n');
    }

    Ok(out.trim_end().to_string())
}

fn swift_type_for_field(
    field: &Field,
    parent_path: &[String],
    registry: &NameRegistry,
    optional: bool,
) -> String {
    let base = match &field.field_type {
        FieldType::Primitive(PrimitiveType::String) => "String".to_string(),
        FieldType::Primitive(PrimitiveType::Int) => "Int".to_string(),
        FieldType::Primitive(PrimitiveType::Float) => "Double".to_string(),
        FieldType::Primitive(PrimitiveType::Bool) => "Bool".to_string(),
        FieldType::JsonValue => "JSONValue".to_string(),
        FieldType::Object(_) => {
            let mut path = parent_path.to_vec();
            path.push(field.key.clone());
            registry
                .get(&path)
                .cloned()
                .unwrap_or_else(|| "Record".to_string())
        }
    };

    if optional { format!("{}?", base) } else { base }
}

fn schema_has_optional(node: &SchemaNode) -> bool {
    for field in &node.fields {
        match &field.field_type {
            FieldType::Object(child) => {
                if !node_has_required(child) || schema_has_optional(child) {
                    return true;
                }
            }
            _ => {
                if field.optional {
                    return true;
                }
            }
        }
    }
    false
}

fn schema_has_rename(node: &SchemaNode, lang: DtoLanguage) -> bool {
    let mut used = HashMap::new();
    for field in &node.fields {
        let ident = field_identifier(lang, &field.key, &mut used);
        if ident != field.key {
            return true;
        }
        if let FieldType::Object(child) = &field.field_type {
            if schema_has_rename(child, lang) {
                return true;
            }
        }
    }
    false
}

const SWIFT_JSON_VALUE: &str = "enum JSONValue: Codable {\n    case string(String)\n    case number(Double)\n    case bool(Bool)\n    case object([String: JSONValue])\n    case array([JSONValue])\n    case null\n\n    init(from decoder: Decoder) throws {\n        let container = try decoder.singleValueContainer()\n        if container.decodeNil() {\n            self = .null\n        } else if let value = try? container.decode(Bool.self) {\n            self = .bool(value)\n        } else if let value = try? container.decode(Double.self) {\n            self = .number(value)\n        } else if let value = try? container.decode(String.self) {\n            self = .string(value)\n        } else if let value = try? container.decode([String: JSONValue].self) {\n            self = .object(value)\n        } else if let value = try? container.decode([JSONValue].self) {\n            self = .array(value)\n        } else {\n            throw DecodingError.typeMismatch(JSONValue.self, DecodingError.Context(codingPath: decoder.codingPath, debugDescription: \"Unsupported JSON value\"))\n        }\n    }\n\n    func encode(to encoder: Encoder) throws {\n        var container = encoder.singleValueContainer()\n        switch self {\n        case .string(let value):\n            try container.encode(value)\n        case .number(let value):\n            try container.encode(value)\n        case .bool(let value):\n            try container.encode(value)\n        case .object(let value):\n            try container.encode(value)\n        case .array(let value):\n            try container.encode(value)\n        case .null:\n            try container.encodeNil()\n        }\n    }\n}\n";
