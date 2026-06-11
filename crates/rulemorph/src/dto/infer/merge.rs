use super::*;

pub(super) fn generated_type_count(field_type: &FieldType) -> usize {
    match field_type {
        FieldType::Object(node) => {
            1 + node
                .fields
                .iter()
                .map(|field| generated_type_count(&field.field_type))
                .sum::<usize>()
        }
        FieldType::Array(inner) | FieldType::Map(inner) | FieldType::Nullable(inner) => {
            generated_type_count(inner)
        }
        FieldType::Primitive(_) | FieldType::JsonValue => 0,
    }
}

pub(super) fn merge_types(left: FieldType, right: FieldType) -> FieldType {
    use FieldType::*;
    match (left, right) {
        (JsonValue, _) | (_, JsonValue) => JsonValue,
        (Primitive(a), Primitive(b)) if a == b => Primitive(a),
        (Primitive(PrimitiveType::Int), Primitive(PrimitiveType::Float))
        | (Primitive(PrimitiveType::Float), Primitive(PrimitiveType::Int)) => {
            Primitive(PrimitiveType::Float)
        }
        (Nullable(a), Nullable(b)) => Nullable(Box::new(merge_types(*a, *b))),
        (Nullable(a), _) if *a == JsonValue => JsonValue,
        (_, Nullable(b)) if *b == JsonValue => JsonValue,
        (Nullable(a), b) | (b, Nullable(a)) => Nullable(Box::new(merge_types(*a, b))),
        (Array(a), Array(b)) => Array(Box::new(merge_types(*a, *b))),
        (Map(a), Map(b)) => Map(Box::new(merge_types(*a, *b))),
        (Object(a), Object(b)) => match merge_object_nodes(*a, *b) {
            Some(node) => Object(Box::new(node)),
            None => JsonValue,
        },
        (Primitive(_), Primitive(_)) => JsonValue,
        (Array(_), _)
        | (_, Array(_))
        | (Map(_), _)
        | (_, Map(_))
        | (Object(_), _)
        | (_, Object(_)) => JsonValue,
    }
}

pub(super) fn merge_object_nodes(left: SchemaNode, right: SchemaNode) -> Option<SchemaNode> {
    let mut right_fields = right.fields;
    let mut fields = Vec::with_capacity(left.fields.len() + right_fields.len());

    for mut left_field in left.fields {
        if let Some(index) = right_fields
            .iter()
            .position(|right_field| right_field.key == left_field.key)
        {
            let right_field = right_fields.remove(index);
            left_field.field_type = merge_types(left_field.field_type, right_field.field_type);
            left_field.optional = left_field.optional || right_field.optional;
            fields.push(left_field);
        } else {
            left_field.optional = true;
            fields.push(left_field);
        }
    }

    for mut right_field in right_fields {
        right_field.optional = true;
        fields.push(right_field);
    }

    if fields.len() > DTO_INFER_MAX_OBJECT_FIELDS {
        None
    } else {
        Some(SchemaNode { fields })
    }
}

pub(super) fn merge_object_types_for_operation(left: FieldType, right: FieldType) -> FieldType {
    match (left, right) {
        (FieldType::Object(left), FieldType::Object(right)) => {
            match merge_object_nodes_for_operation(*left, *right) {
                Some(node) => FieldType::Object(Box::new(node)),
                None => FieldType::JsonValue,
            }
        }
        _ => FieldType::JsonValue,
    }
}

pub(super) fn merge_object_nodes_for_operation(
    left: SchemaNode,
    right: SchemaNode,
) -> Option<SchemaNode> {
    let mut right_fields = right.fields;
    let mut fields = Vec::with_capacity(left.fields.len() + right_fields.len());

    for mut left_field in left.fields {
        if let Some(index) = right_fields
            .iter()
            .position(|right_field| right_field.key == left_field.key)
        {
            let right_field = right_fields.remove(index);
            left_field.field_type = merge_types(left_field.field_type, right_field.field_type);
            left_field.optional = left_field.optional || right_field.optional;
        }
        fields.push(left_field);
    }

    fields.extend(right_fields);

    if fields.len() > DTO_INFER_MAX_OBJECT_FIELDS {
        None
    } else {
        Some(SchemaNode { fields })
    }
}
