use std::collections::HashMap;

use crate::dto::schema::{Field, FieldType, PrimitiveType, SchemaNode, node_has_required};
use crate::dto::support::{
    NameRegistry, collect_types, field_identifier, json_string_literal, safe_comment_text,
};
use crate::dto::{DtoError, DtoLanguage};

pub(in crate::dto) fn render_typescript(
    schema: &SchemaNode,
    name: &str,
) -> Result<String, DtoError> {
    let mut registry = NameRegistry::new(name);
    let mut defs = Vec::new();
    collect_types(schema, Vec::new(), &mut registry, &mut defs);

    let mut out = String::new();
    for def in defs {
        out.push_str(&format!("export interface {} {{\n", def.name));
        let mut used = HashMap::new();
        for field in &def.node.fields {
            let ident = field_identifier(DtoLanguage::TypeScript, &field.key, &mut used);
            let rename = ident != field.key;
            let optional = match &field.field_type {
                FieldType::Object(child) => !node_has_required(child),
                _ => field.optional,
            };
            let field_type = typescript_type_for_field(field, &def.path, &registry);
            if rename {
                out.push_str(&format!(
                    "  /** json: {} */\n",
                    json_string_literal(&safe_comment_text(&field.key))
                ));
            }
            let suffix = if optional { "?" } else { "" };
            out.push_str(&format!("  {}{}: {};\n", ident, suffix, field_type));
        }
        out.push_str("}\n\n");
    }

    Ok(out.trim_end().to_string())
}

fn typescript_type_for_field(
    field: &Field,
    parent_path: &[String],
    registry: &NameRegistry,
) -> String {
    match &field.field_type {
        FieldType::Primitive(PrimitiveType::String) => "string".to_string(),
        FieldType::Primitive(PrimitiveType::Int) => "number".to_string(),
        FieldType::Primitive(PrimitiveType::Float) => "number".to_string(),
        FieldType::Primitive(PrimitiveType::Bool) => "boolean".to_string(),
        FieldType::JsonValue => "unknown".to_string(),
        FieldType::Object(_) => {
            let mut path = parent_path.to_vec();
            path.push(field.key.clone());
            registry
                .get(&path)
                .cloned()
                .unwrap_or_else(|| "Record".to_string())
        }
    }
}
