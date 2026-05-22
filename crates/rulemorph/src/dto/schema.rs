use serde_json::Value as JsonValue;

use super::DtoError;
use crate::model::{Expr, RuleFile};
use crate::path::{PathToken, parse_path};

#[derive(Clone)]
pub(super) struct SchemaNode {
    pub(super) fields: Vec<Field>,
}

#[derive(Clone)]
pub(super) struct Field {
    pub(super) key: String,
    pub(super) field_type: FieldType,
    pub(super) optional: bool,
}

#[derive(Clone)]
pub(super) enum FieldType {
    Primitive(PrimitiveType),
    Object(Box<SchemaNode>),
    JsonValue,
}

#[derive(Clone, Copy)]
pub(super) enum PrimitiveType {
    String,
    Int,
    Float,
    Bool,
}

pub(super) fn build_schema(rule: &RuleFile) -> Result<SchemaNode, DtoError> {
    let mut root = SchemaNode { fields: Vec::new() };

    let step_mappings = rule
        .steps
        .iter()
        .flat_map(|steps| steps.iter())
        .flat_map(|step| step.mappings.iter())
        .flat_map(|mappings| mappings.iter());

    for mapping in rule.mappings.iter().chain(step_mappings) {
        let tokens =
            parse_path(&mapping.target).map_err(|_| DtoError::new("target path is invalid"))?;
        if tokens
            .iter()
            .any(|token| matches!(token, PathToken::Index(_)))
        {
            return Err(DtoError::new("target path must not include indexes"));
        }

        let mut keys = Vec::new();
        for token in tokens {
            match token {
                PathToken::Key(key) => keys.push(key),
                PathToken::Index(_) => {}
            }
        }

        if keys.is_empty() {
            return Err(DtoError::new("target path is invalid"));
        }

        let field_type = match mapping.value_type.as_deref() {
            Some("string") => FieldType::Primitive(PrimitiveType::String),
            Some("int") => FieldType::Primitive(PrimitiveType::Int),
            Some("float") => FieldType::Primitive(PrimitiveType::Float),
            Some("bool") => FieldType::Primitive(PrimitiveType::Bool),
            Some(_) => return Err(DtoError::new("unsupported type in mapping")),
            None => FieldType::JsonValue,
        };
        let conditional = match &mapping.when {
            None => false,
            Some(Expr::Literal(JsonValue::Bool(true))) => false,
            _ => true,
        };
        let optional = conditional
            || !(mapping.required || mapping.value.is_some() || mapping.default.is_some());

        insert_field(&mut root, &keys, field_type, optional)?;
    }

    Ok(root)
}

fn insert_field(
    node: &mut SchemaNode,
    keys: &[String],
    field_type: FieldType,
    optional: bool,
) -> Result<(), DtoError> {
    if keys.is_empty() {
        return Err(DtoError::new("target path is invalid"));
    }

    let key = &keys[0];
    if keys.len() == 1 {
        if node.fields.iter().any(|field| field.key == *key) {
            return Err(DtoError::new("duplicate target in dto"));
        }
        node.fields.push(Field {
            key: key.clone(),
            field_type,
            optional,
        });
        return Ok(());
    }

    if let Some(field) = node.fields.iter_mut().find(|field| field.key == *key) {
        match &mut field.field_type {
            FieldType::Object(child) => {
                return insert_field(child, &keys[1..], field_type, optional);
            }
            _ => return Err(DtoError::new("target conflicts with non-object")),
        }
    }

    let mut child = SchemaNode { fields: Vec::new() };
    insert_field(&mut child, &keys[1..], field_type, optional)?;
    node.fields.push(Field {
        key: key.clone(),
        field_type: FieldType::Object(Box::new(child)),
        optional: false,
    });
    Ok(())
}

pub(super) fn node_has_required(node: &SchemaNode) -> bool {
    for field in &node.fields {
        match &field.field_type {
            FieldType::Object(child) => {
                if node_has_required(child) {
                    return true;
                }
            }
            _ => {
                if !field.optional {
                    return true;
                }
            }
        }
    }
    false
}

pub(super) fn node_uses_json(node: &SchemaNode) -> bool {
    for field in &node.fields {
        match &field.field_type {
            FieldType::JsonValue => return true,
            FieldType::Object(child) => {
                if node_uses_json(child) {
                    return true;
                }
            }
            _ => {}
        }
    }
    false
}
