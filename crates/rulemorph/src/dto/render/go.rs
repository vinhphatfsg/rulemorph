use std::collections::HashMap;

use crate::dto::schema::{
    Field, FieldType, PrimitiveType, SchemaNode, node_has_required, node_uses_json,
};
use crate::dto::support::{NameRegistry, collect_types, field_identifier, go_json_tag_literal};
use crate::dto::{DtoError, DtoLanguage};

pub(in crate::dto) fn render_go(schema: &SchemaNode, name: &str) -> Result<String, DtoError> {
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
