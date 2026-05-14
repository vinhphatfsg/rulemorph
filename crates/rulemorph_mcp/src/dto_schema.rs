use std::collections::{HashMap, HashSet};

use crate::path_expr::append_path;

pub(crate) struct DtoSchema {
    pub(crate) root: String,
    pub(crate) types: HashMap<String, DtoType>,
}

pub(crate) struct DtoType {
    pub(crate) fields: Vec<DtoField>,
}

pub(crate) struct DtoField {
    pub(crate) json_key: String,
    pub(crate) field_type: DtoFieldType,
    pub(crate) optional: bool,
}

pub(crate) enum DtoFieldType {
    Primitive(PrimitiveKind),
    Object(String),
    Unknown,
}

pub(crate) enum PrimitiveKind {
    String,
    Int,
    Float,
    Bool,
}

pub(crate) struct GeneratedMapping {
    pub(crate) target: String,
    pub(crate) value_type: Option<String>,
    pub(crate) required: bool,
}

pub(crate) fn generate_mappings_from_schema(
    schema: &DtoSchema,
) -> Result<Vec<GeneratedMapping>, String> {
    let mut mappings = Vec::new();
    let mut visiting = HashSet::new();
    build_mappings_for_type(
        schema,
        &schema.root,
        "",
        false,
        &mut visiting,
        &mut mappings,
    )?;
    Ok(mappings)
}

fn build_mappings_for_type(
    schema: &DtoSchema,
    type_name: &str,
    prefix: &str,
    parent_optional: bool,
    visiting: &mut HashSet<String>,
    out: &mut Vec<GeneratedMapping>,
) -> Result<(), String> {
    if !visiting.insert(type_name.to_string()) {
        return Ok(());
    }
    let dto_type = schema
        .types
        .get(type_name)
        .ok_or_else(|| format!("unknown dto type: {}", type_name))?;

    for field in &dto_type.fields {
        let target = append_path(prefix, &field.json_key);
        let optional = parent_optional || field.optional;
        match &field.field_type {
            DtoFieldType::Primitive(kind) => {
                let value_type = primitive_to_value_type(kind);
                out.push(GeneratedMapping {
                    target,
                    value_type,
                    required: !optional,
                });
            }
            DtoFieldType::Unknown => {
                out.push(GeneratedMapping {
                    target,
                    value_type: None,
                    required: !optional,
                });
            }
            DtoFieldType::Object(child) => {
                build_mappings_for_type(schema, child, &target, optional, visiting, out)?;
            }
        }
    }

    visiting.remove(type_name);
    Ok(())
}

fn primitive_to_value_type(kind: &PrimitiveKind) -> Option<String> {
    match kind {
        PrimitiveKind::String => Some("string".to_string()),
        PrimitiveKind::Int => Some("int".to_string()),
        PrimitiveKind::Float => Some("float".to_string()),
        PrimitiveKind::Bool => Some("bool".to_string()),
    }
}
