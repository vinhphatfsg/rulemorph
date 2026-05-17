use std::collections::HashMap;

use crate::dto::schema::{
    Field, FieldType, PrimitiveType, SchemaNode, node_has_required, node_uses_json,
};
use crate::dto::support::{
    NameRegistry, collect_types, field_identifier, json_string_literal, safe_comment_text,
};
use crate::dto::{DtoError, DtoLanguage};

use super::{schema_has_optional, schema_has_rename};

pub(in crate::dto) fn render_python(schema: &SchemaNode, name: &str) -> Result<String, DtoError> {
    let mut registry = NameRegistry::new(name);
    let mut defs = Vec::new();
    collect_types(schema, Vec::new(), &mut registry, &mut defs);

    let uses_json = node_uses_json(schema);
    let uses_optional = schema_has_optional(schema);
    let uses_rename = schema_has_rename(schema, DtoLanguage::Python);

    let mut out = String::new();
    out.push_str("from dataclasses import dataclass");
    if uses_rename {
        out.push_str(", field");
    }
    out.push('\n');

    if uses_json || uses_optional {
        let mut parts = Vec::new();
        if uses_optional {
            parts.push("Optional");
        }
        if uses_json {
            parts.push("Any");
        }
        out.push_str(&format!("from typing import {}\n", parts.join(", ")));
    }
    out.push('\n');

    for def in defs {
        out.push_str("@dataclass\n");
        out.push_str(&format!("class {}:\n", def.name));
        if def.node.fields.is_empty() {
            out.push_str("    pass\n\n");
            continue;
        }

        struct RenderField {
            key: String,
            ident: String,
            field_type: String,
            optional: bool,
            rename: bool,
        }

        let mut used = HashMap::new();
        let mut fields = Vec::new();
        for field in &def.node.fields {
            let ident = field_identifier(DtoLanguage::Python, &field.key, &mut used);
            let rename = ident != field.key;
            let optional = match &field.field_type {
                FieldType::Object(child) => !node_has_required(child),
                _ => field.optional,
            };
            let field_type = python_type_for_field(field, &def.path, &registry, optional);
            fields.push(RenderField {
                key: field.key.clone(),
                ident,
                field_type,
                optional,
                rename,
            });
        }

        for field in fields
            .iter()
            .filter(|field| !field.optional)
            .chain(fields.iter().filter(|field| field.optional))
        {
            if field.rename {
                out.push_str(&format!(
                    "    # json: {}\n",
                    json_string_literal(&safe_comment_text(&field.key))
                ));
            }

            if field.rename {
                if field.optional {
                    out.push_str(&format!(
                        "    {}: {} = field(default=None, metadata={{\"json_key\": {}}})\n",
                        field.ident,
                        field.field_type,
                        json_string_literal(&field.key)
                    ));
                } else {
                    out.push_str(&format!(
                        "    {}: {} = field(metadata={{\"json_key\": {}}})\n",
                        field.ident,
                        field.field_type,
                        json_string_literal(&field.key)
                    ));
                }
            } else if field.optional {
                out.push_str(&format!(
                    "    {}: {} = None\n",
                    field.ident, field.field_type
                ));
            } else {
                out.push_str(&format!("    {}: {}\n", field.ident, field.field_type));
            }
        }
        out.push('\n');
    }

    Ok(out.trim_end().to_string())
}

fn python_type_for_field(
    field: &Field,
    parent_path: &[String],
    registry: &NameRegistry,
    optional: bool,
) -> String {
    let base = match &field.field_type {
        FieldType::Primitive(PrimitiveType::String) => "str".to_string(),
        FieldType::Primitive(PrimitiveType::Int) => "int".to_string(),
        FieldType::Primitive(PrimitiveType::Float) => "float".to_string(),
        FieldType::Primitive(PrimitiveType::Bool) => "bool".to_string(),
        FieldType::JsonValue => "Any".to_string(),
        FieldType::Object(_) => {
            let mut path = parent_path.to_vec();
            path.push(field.key.clone());
            registry
                .get(&path)
                .cloned()
                .unwrap_or_else(|| "Record".to_string())
        }
    };

    if optional {
        format!("Optional[{}]", base)
    } else {
        base
    }
}
