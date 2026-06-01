use std::collections::HashMap;

use crate::dto::schema::{
    Field, FieldType, PrimitiveType, SchemaNode, field_is_optional, node_uses_json,
};
use crate::dto::support::{NameRegistry, collect_types, field_identifier, json_string_literal};
use crate::dto::{DtoError, DtoLanguage};

use super::{schema_has_optional, schema_has_rename};

pub(in crate::dto) fn render_java(schema: &SchemaNode, name: &str) -> Result<String, DtoError> {
    let mut registry = NameRegistry::new(name);
    let mut defs = Vec::new();
    collect_types(schema, Vec::new(), &mut registry, &mut defs);

    let uses_json = node_uses_json(schema);
    let uses_optional = schema_has_optional(schema);
    let uses_rename = schema_has_rename(schema, DtoLanguage::Java);
    let uses_list = schema_uses_array(schema);
    let uses_map = schema_uses_map(schema);

    let mut out = String::new();
    if uses_rename {
        out.push_str("import com.fasterxml.jackson.annotation.JsonProperty;\n");
    }
    if uses_json {
        out.push_str("import com.fasterxml.jackson.databind.JsonNode;\n");
    }
    if uses_optional {
        out.push_str("import java.util.Optional;\n");
    }
    if uses_list {
        out.push_str("import java.util.List;\n");
    }
    if uses_map {
        out.push_str("import java.util.Map;\n");
    }
    if uses_rename || uses_json || uses_optional || uses_list || uses_map {
        out.push('\n');
    }

    for def in defs {
        let visibility = if def.path.is_empty() { "public " } else { "" };
        out.push_str(&format!("{}class {} {{\n", visibility, def.name));
        let mut used = HashMap::new();
        for field in &def.node.fields {
            let ident = field_identifier(DtoLanguage::Java, &field.key, &mut used);
            let rename = ident != field.key;
            let optional = field_is_optional(field);
            let field_type = java_type_for_field(field, &def.path, &registry, optional);

            if rename {
                out.push_str(&format!(
                    "    @JsonProperty({})\n",
                    json_string_literal(&field.key)
                ));
            }
            out.push_str(&format!("    public {} {};\n", field_type, ident));
        }
        out.push_str("}\n\n");
    }

    Ok(out.trim_end().to_string())
}

fn java_type_for_field(
    field: &Field,
    parent_path: &[String],
    registry: &NameRegistry,
    optional: bool,
) -> String {
    let mut path = parent_path.to_vec();
    path.push(field.key.clone());
    let base = java_type_for_type(&field.field_type, &path, registry);

    if optional && !base.starts_with("Optional<") {
        format!("Optional<{}>", base)
    } else {
        base
    }
}

fn java_type_for_type(field_type: &FieldType, path: &[String], registry: &NameRegistry) -> String {
    match field_type {
        FieldType::Primitive(PrimitiveType::String) => "String".to_string(),
        FieldType::Primitive(PrimitiveType::Int) => "Long".to_string(),
        FieldType::Primitive(PrimitiveType::Float) => "Double".to_string(),
        FieldType::Primitive(PrimitiveType::Bool) => "Boolean".to_string(),
        FieldType::Array(inner) => format!("List<{}>", java_type_for_type(inner, path, registry)),
        FieldType::Map(inner) => {
            format!("Map<String, {}>", java_type_for_type(inner, path, registry))
        }
        FieldType::Nullable(inner) => {
            let inner_type = java_type_for_type(inner, path, registry);
            if inner_type.starts_with("Optional<") {
                inner_type
            } else {
                format!("Optional<{}>", inner_type)
            }
        }
        FieldType::JsonValue => "JsonNode".to_string(),
        FieldType::Object(_) => registry
            .get(path)
            .cloned()
            .unwrap_or_else(|| "Record".to_string()),
    }
}

pub(in crate::dto) fn render_kotlin(schema: &SchemaNode, name: &str) -> Result<String, DtoError> {
    let mut registry = NameRegistry::new(name);
    let mut defs = Vec::new();
    collect_types(schema, Vec::new(), &mut registry, &mut defs);

    let uses_json = node_uses_json(schema);
    let uses_rename = schema_has_rename(schema, DtoLanguage::Kotlin);

    let mut out = String::new();
    if uses_rename {
        out.push_str("import com.fasterxml.jackson.annotation.JsonProperty\n");
    }
    if uses_json {
        out.push_str("import com.fasterxml.jackson.databind.JsonNode\n");
    }
    if uses_rename || uses_json {
        out.push('\n');
    }

    for def in defs {
        out.push_str(&format!("data class {}(\n", def.name));
        let mut used = HashMap::new();
        for (index, field) in def.node.fields.iter().enumerate() {
            let ident = field_identifier(DtoLanguage::Kotlin, &field.key, &mut used);
            let rename = ident != field.key;
            let optional = field_is_optional(field);
            let field_type = kotlin_type_for_field(field, &def.path, &registry, optional);

            if rename {
                out.push_str(&format!(
                    "    @JsonProperty({})\n",
                    json_string_literal(&field.key)
                ));
            }
            let suffix = if index + 1 == def.node.fields.len() {
                ""
            } else {
                ","
            };
            out.push_str(&format!("    val {}: {}{}\n", ident, field_type, suffix));
        }
        out.push_str(")\n\n");
    }

    Ok(out.trim_end().to_string())
}

fn kotlin_type_for_field(
    field: &Field,
    parent_path: &[String],
    registry: &NameRegistry,
    optional: bool,
) -> String {
    let mut path = parent_path.to_vec();
    path.push(field.key.clone());
    let base = kotlin_type_for_type(&field.field_type, &path, registry);

    if optional && !base.ends_with('?') {
        format!("{}?", base)
    } else {
        base
    }
}

fn kotlin_type_for_type(
    field_type: &FieldType,
    path: &[String],
    registry: &NameRegistry,
) -> String {
    match field_type {
        FieldType::Primitive(PrimitiveType::String) => "String".to_string(),
        FieldType::Primitive(PrimitiveType::Int) => "Long".to_string(),
        FieldType::Primitive(PrimitiveType::Float) => "Double".to_string(),
        FieldType::Primitive(PrimitiveType::Bool) => "Boolean".to_string(),
        FieldType::Array(inner) => format!("List<{}>", kotlin_type_for_type(inner, path, registry)),
        FieldType::Map(inner) => {
            format!(
                "Map<String, {}>",
                kotlin_type_for_type(inner, path, registry)
            )
        }
        FieldType::Nullable(inner) => {
            let inner_type = kotlin_type_for_type(inner, path, registry);
            if inner_type.ends_with('?') {
                inner_type
            } else {
                format!("{}?", inner_type)
            }
        }
        FieldType::JsonValue => "JsonNode".to_string(),
        FieldType::Object(_) => registry
            .get(path)
            .cloned()
            .unwrap_or_else(|| "Record".to_string()),
    }
}

fn schema_uses_array(node: &SchemaNode) -> bool {
    node.fields
        .iter()
        .any(|field| field_type_uses_array(&field.field_type))
}

fn field_type_uses_array(field_type: &FieldType) -> bool {
    match field_type {
        FieldType::Array(_) => true,
        FieldType::Map(inner) | FieldType::Nullable(inner) => field_type_uses_array(inner),
        FieldType::Object(child) => schema_uses_array(child),
        FieldType::Primitive(_) | FieldType::JsonValue => false,
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
