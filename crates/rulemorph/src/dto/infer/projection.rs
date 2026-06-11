use super::*;

pub(super) fn infer_get(input_type: &FieldType, args: &[V2Expr]) -> FieldType {
    let Some(path) = args.first().and_then(literal_string_arg) else {
        return FieldType::JsonValue;
    };
    field_type_at_path(input_type, &path).unwrap_or(FieldType::JsonValue)
}

pub(super) fn infer_pick(input_type: &FieldType, args: &[V2Expr]) -> FieldType {
    let FieldType::Object(node) = input_type else {
        return FieldType::JsonValue;
    };
    let paths = literal_path_args(args);
    if paths.len() != args.len() {
        return FieldType::JsonValue;
    }
    let mut fields = Vec::new();
    for path in paths {
        let Some(keys) = key_path(&path) else {
            return FieldType::JsonValue;
        };
        let Some(field) = pick_field_at_keys(&node.fields, &keys) else {
            continue;
        };
        if !merge_projected_field(&mut fields, field) {
            return FieldType::JsonValue;
        }
    }
    FieldType::Object(Box::new(SchemaNode { fields }))
}

pub(super) fn infer_omit(input_type: &FieldType, args: &[V2Expr]) -> FieldType {
    let FieldType::Object(node) = input_type else {
        return FieldType::JsonValue;
    };
    let paths = literal_path_args(args);
    if paths.len() != args.len() {
        return FieldType::JsonValue;
    }
    let mut omit_keys = Vec::new();
    for path in paths {
        let Some(keys) = key_path(&path) else {
            return FieldType::JsonValue;
        };
        if keys.is_empty() {
            return FieldType::JsonValue;
        }
        omit_keys.push(keys);
    }
    let mut fields = node.fields.clone();
    for keys in omit_keys {
        omit_path_from_fields(&mut fields, &keys);
    }
    FieldType::Object(Box::new(SchemaNode { fields }))
}

pub(super) fn pick_field_at_keys(fields: &[Field], keys: &[String]) -> Option<Field> {
    let key = keys.first()?;
    let field = fields.iter().find(|field| field.key == *key)?;
    if keys.len() == 1 {
        return Some(field.clone());
    }

    let FieldType::Object(child) = &field.field_type else {
        return None;
    };
    let child_field = pick_field_at_keys(&child.fields, &keys[1..])?;
    Some(Field {
        key: field.key.clone(),
        field_type: FieldType::Object(Box::new(SchemaNode {
            fields: vec![child_field],
        })),
        optional: field.optional,
        synthetic: field.synthetic,
    })
}

pub(super) fn merge_projected_field(fields: &mut Vec<Field>, projected: Field) -> bool {
    if let Some(existing) = fields.iter_mut().find(|field| field.key == projected.key) {
        existing.field_type =
            merge_projected_types(existing.field_type.clone(), projected.field_type);
        existing.optional = existing.optional || projected.optional;
        !matches!(existing.field_type, FieldType::JsonValue)
    } else if fields.len() >= DTO_INFER_MAX_OBJECT_FIELDS {
        false
    } else {
        fields.push(projected);
        true
    }
}

pub(super) fn merge_projected_types(left: FieldType, right: FieldType) -> FieldType {
    match (&left, &right) {
        (FieldType::Object(_), FieldType::Object(_)) => {
            merge_object_types_for_operation(left, right)
        }
        _ => merge_types(left, right),
    }
}

pub(super) fn omit_path_from_fields(fields: &mut Vec<Field>, keys: &[String]) {
    let Some(key) = keys.first() else {
        return;
    };
    if keys.len() == 1 {
        fields.retain(|field| field.key != *key);
        return;
    }

    let Some(field) = fields.iter_mut().find(|field| field.key == *key) else {
        return;
    };
    if let FieldType::Object(child) = &mut field.field_type {
        omit_path_from_fields(&mut child.fields, &keys[1..]);
    }
}

pub(super) fn infer_merge(
    input_type: &FieldType,
    args: &[V2Expr],
    rule: &RuleFile,
    state: &mut InferenceState,
    scope: &Scope,
    depth: usize,
) -> FieldType {
    let mut merged = match input_type {
        FieldType::Object(_) => input_type.clone(),
        _ => return FieldType::JsonValue,
    };
    for arg in args {
        let arg_type = infer_arg_expr(
            arg,
            rule,
            state,
            scope.clone().with_pipe(merged.clone()),
            depth + 1,
        );
        if !matches!(arg_type, FieldType::Object(_)) {
            return FieldType::JsonValue;
        }
        merged = merge_object_types_for_operation(merged, arg_type);
        if matches!(merged, FieldType::JsonValue) {
            return FieldType::JsonValue;
        }
    }
    merged
}

pub(super) fn infer_fold(
    input_type: &FieldType,
    args: &[V2Expr],
    rule: &RuleFile,
    state: &mut InferenceState,
    scope: &Scope,
    depth: usize,
) -> FieldType {
    let [initial_arg, fold_arg] = args else {
        return FieldType::JsonValue;
    };
    let element = array_element_type(input_type);
    let initial_type = infer_arg_expr(
        initial_arg,
        rule,
        state,
        scope.clone().with_pipe(input_type.clone()),
        depth + 1,
    );
    let fold_scope = scope
        .clone()
        .with_pipe(element.clone())
        .with_item(Some(element))
        .with_acc(Some(initial_type.clone()));
    let fold_type = infer_arg_expr(fold_arg, rule, state, fold_scope, depth + 1);
    merge_types(initial_type, fold_type)
}

pub(super) fn literal_path_args(args: &[V2Expr]) -> Vec<String> {
    args.iter().filter_map(literal_string_arg).collect()
}

pub(super) fn literal_string_arg(expr: &V2Expr) -> Option<String> {
    match expr {
        V2Expr::Pipe(pipe) if pipe.steps.is_empty() => match &pipe.start {
            V2Start::Literal(JsonValue::String(value)) => Some(value.clone()),
            _ => None,
        },
        _ => None,
    }
}

pub(super) fn array_element_type(field_type: &FieldType) -> FieldType {
    match field_type {
        FieldType::Array(inner) => (**inner).clone(),
        FieldType::Nullable(inner) => array_element_type(inner),
        _ => FieldType::JsonValue,
    }
}

pub(super) fn object_value_union(field_type: &FieldType) -> FieldType {
    match field_type {
        FieldType::Object(node) => node
            .fields
            .iter()
            .map(|field| field.field_type.clone())
            .reduce(merge_types)
            .unwrap_or(FieldType::JsonValue),
        FieldType::Map(inner) => (**inner).clone(),
        FieldType::Nullable(inner) => object_value_union(inner),
        _ => FieldType::JsonValue,
    }
}
