use std::collections::HashMap;

use crate::dto::schema::{
    Field, FieldType, PrimitiveType, SchemaNode, field_is_optional, node_uses_json,
};
use crate::dto::support::{NameRegistry, collect_types, field_identifier, rust_string_literal};
use crate::dto::{DtoError, DtoLanguage};

pub(in crate::dto) fn render_rust(schema: &SchemaNode, name: &str) -> Result<String, DtoError> {
    let mut registry = NameRegistry::new(name);
    let mut defs = Vec::new();
    collect_types(schema, Vec::new(), &mut registry, &mut defs);

    let mut out = String::new();
    out.push_str("use serde::{Deserialize, Serialize};\n");
    if schema_uses_map(schema) {
        out.push_str("use std::collections::HashMap;\n");
    }
    if node_uses_json(schema) {
        out.push_str("use serde_json::Value;\n");
    }
    out.push('\n');

    for def in defs {
        out.push_str("#[derive(Debug, Clone, Serialize, Deserialize)]\n");
        out.push_str(&format!("pub struct {} {{\n", def.name));

        let mut used = HashMap::new();
        for field in &def.node.fields {
            let ident = field_identifier(DtoLanguage::Rust, &field.key, &mut used);
            let rename = ident != field.key;
            let optional = field_is_optional(field);
            let field_type = rust_type_for_field(field, &def.path, &registry);

            let mut attrs = Vec::new();
            if optional {
                attrs.push("default".to_string());
                attrs.push("skip_serializing_if = \"Option::is_none\"".to_string());
            }
            if rename {
                attrs.push(format!("rename = {}", rust_string_literal(&field.key)));
            }

            if !attrs.is_empty() {
                out.push_str(&format!("    #[serde({})]\n", attrs.join(", ")));
            }

            let final_type = if optional {
                format!("Option<{}>", field_type)
            } else {
                field_type
            };
            out.push_str(&format!("    pub {}: {},\n", ident, final_type));
        }

        out.push_str("}\n\n");
    }

    Ok(out.trim_end().to_string())
}

fn rust_type_for_field(field: &Field, parent_path: &[String], registry: &NameRegistry) -> String {
    let mut path = parent_path.to_vec();
    path.push(field.key.clone());
    rust_type_for_type(&field.field_type, &path, registry)
}

fn rust_type_for_type(field_type: &FieldType, path: &[String], registry: &NameRegistry) -> String {
    match field_type {
        FieldType::Primitive(PrimitiveType::String) => "String".to_string(),
        FieldType::Primitive(PrimitiveType::Int) => "i64".to_string(),
        FieldType::Primitive(PrimitiveType::Float) => "f64".to_string(),
        FieldType::Primitive(PrimitiveType::Bool) => "bool".to_string(),
        FieldType::Array(inner) => format!("Vec<{}>", rust_type_for_type(inner, path, registry)),
        FieldType::Map(inner) => {
            format!(
                "HashMap<String, {}>",
                rust_type_for_type(inner, path, registry)
            )
        }
        FieldType::Nullable(inner) => {
            format!("Option<{}>", rust_type_for_type(inner, path, registry))
        }
        FieldType::JsonValue => "Value".to_string(),
        FieldType::Object(_) => registry
            .get(path)
            .cloned()
            .unwrap_or_else(|| "Record".to_string()),
    }
}

fn schema_uses_map(node: &SchemaNode) -> bool {
    node.fields
        .iter()
        .any(|field| field_type_uses_map(&field.field_type))
}

fn field_type_uses_map(field_type: &FieldType) -> bool {
    match field_type {
        FieldType::Map(_) => true,
        FieldType::Array(inner) | FieldType::Nullable(inner) => field_type_uses_map(inner),
        FieldType::Object(child) => schema_uses_map(child),
        FieldType::Primitive(_) | FieldType::JsonValue => false,
    }
}
