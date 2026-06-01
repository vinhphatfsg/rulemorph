use serde_json::Value as JsonValue;

use super::DtoError;
use super::infer::{
    InferenceState, infer_mapping_field_type, remember_mapping_type, type_from_mapping_type,
};
use crate::model::{Expr, RuleFile};
use crate::path::{PathToken, parse_path};

#[derive(Clone, PartialEq)]
pub(super) struct SchemaNode {
    pub(super) fields: Vec<Field>,
}

#[derive(Clone, PartialEq)]
pub(super) struct Field {
    pub(super) key: String,
    pub(super) field_type: FieldType,
    pub(super) optional: bool,
    pub(super) synthetic: bool,
}

#[derive(Clone, PartialEq)]
pub(super) enum FieldType {
    Primitive(PrimitiveType),
    Array(Box<FieldType>),
    Map(Box<FieldType>),
    Nullable(Box<FieldType>),
    Object(Box<SchemaNode>),
    JsonValue,
}

#[derive(Clone, Copy, PartialEq)]
pub(super) enum PrimitiveType {
    String,
    Int,
    Float,
    Bool,
}

pub(super) fn build_schema(rule: &RuleFile) -> Result<SchemaNode, DtoError> {
    let mut root = SchemaNode { fields: Vec::new() };
    let mut inference = InferenceState::default();

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

        if mapping
            .value_type
            .as_deref()
            .is_some_and(|value_type| type_from_mapping_type(value_type).is_none())
        {
            return Err(DtoError::new("unsupported type in mapping"));
        }
        let field_type = infer_mapping_field_type(mapping, rule, &mut inference);
        let conditional = match &mapping.when {
            None => false,
            Some(Expr::Literal(JsonValue::Bool(true))) => false,
            _ => true,
        };
        let optional = conditional
            || !(mapping.required || mapping.value.is_some() || mapping.default.is_some());

        insert_field(&mut root, &keys, field_type, optional)?;
        remember_output_prefixes(&mut inference, &root, &keys);
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
            synthetic: false,
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
        synthetic: true,
    });
    Ok(())
}

fn remember_output_prefixes(inference: &mut InferenceState, root: &SchemaNode, keys: &[String]) {
    for prefix_len in 1..=keys.len() {
        let prefix = &keys[..prefix_len];
        if let Some(field_type) = field_type_at_keys(root, prefix) {
            remember_mapping_type(inference, prefix, field_type);
        }
    }
}

fn field_type_at_keys<'a>(node: &'a SchemaNode, keys: &[String]) -> Option<&'a FieldType> {
    let key = keys.first()?;
    let field = node.fields.iter().find(|field| field.key == *key)?;
    if keys.len() == 1 {
        return Some(&field.field_type);
    }
    match &field.field_type {
        FieldType::Object(child) => field_type_at_keys(child, &keys[1..]),
        _ => None,
    }
}

pub(super) fn field_is_optional(field: &Field) -> bool {
    match &field.field_type {
        FieldType::Object(child) if field.synthetic => field.optional || !node_has_required(child),
        _ => field.optional,
    }
}

pub(super) fn node_has_required(node: &SchemaNode) -> bool {
    for field in &node.fields {
        match &field.field_type {
            FieldType::Object(child) => {
                if !field_is_optional(field) || node_has_required(child) {
                    return true;
                }
            }
            _ => {
                if !field_is_optional(field) {
                    return true;
                }
            }
        }
    }
    false
}

pub(super) fn node_uses_json(node: &SchemaNode) -> bool {
    node.fields
        .iter()
        .any(|field| field_type_uses_json(&field.field_type))
}

pub(super) fn field_type_uses_json(field_type: &FieldType) -> bool {
    match field_type {
        FieldType::JsonValue => true,
        FieldType::Array(inner) | FieldType::Map(inner) | FieldType::Nullable(inner) => {
            field_type_uses_json(inner)
        }
        FieldType::Object(child) => node_uses_json(child),
        FieldType::Primitive(_) => false,
    }
}
