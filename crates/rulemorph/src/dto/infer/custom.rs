use super::*;

pub(super) fn infer_custom_call(
    op: &str,
    rule: &RuleFile,
    state: &mut InferenceState,
    depth: usize,
) -> FieldType {
    if !state.enter_node(depth) {
        return FieldType::JsonValue;
    }
    let Some(def) = rule.defs.get(op) else {
        return FieldType::JsonValue;
    };
    infer_custom_op_return(def, rule, state, depth + 1)
}

pub(super) fn infer_custom_op_return(
    def: &CustomOpDef,
    rule: &RuleFile,
    state: &mut InferenceState,
    depth: usize,
) -> FieldType {
    if !state.enter_node(depth) {
        return FieldType::JsonValue;
    }
    if let Some(returns) = &def.returns {
        return rule_type_to_field_type(returns);
    }
    let Some(mappings) = &def.mappings else {
        return FieldType::JsonValue;
    };
    synthesize_custom_mappings_return_type(def, mappings, rule, state, depth + 1)
}

pub(super) fn synthesize_custom_mappings_return_type(
    def: &CustomOpDef,
    mappings: &[Mapping],
    rule: &RuleFile,
    state: &mut InferenceState,
    depth: usize,
) -> FieldType {
    if !state.enter_node(depth) || !state.reserve_generated_type() {
        return FieldType::JsonValue;
    }
    let input_type = rule_type_to_field_type(&def.input);
    let mut root = SchemaNode { fields: Vec::new() };
    for mapping in mappings {
        let Some(keys) = key_path(&mapping.target) else {
            return FieldType::JsonValue;
        };
        if keys.is_empty() {
            return FieldType::JsonValue;
        }
        let output_type = FieldType::Object(Box::new(root.clone()));
        let field_type = infer_custom_mapping_field_type(
            mapping,
            rule,
            state,
            &input_type,
            &output_type,
            depth + 1,
        );
        let conditional = !matches!(
            &mapping.when,
            None | Some(Expr::Literal(JsonValue::Bool(true)))
        );
        let optional = conditional
            || !(mapping.required || mapping.value.is_some() || mapping.default.is_some());
        if !insert_custom_return_field(&mut root, &keys, field_type, optional) {
            return FieldType::JsonValue;
        }
    }
    FieldType::Object(Box::new(root))
}

pub(super) fn infer_custom_mapping_field_type(
    mapping: &Mapping,
    rule: &RuleFile,
    state: &mut InferenceState,
    input_type: &FieldType,
    output_type: &FieldType,
    depth: usize,
) -> FieldType {
    if !state.enter_node(depth) {
        return FieldType::JsonValue;
    }
    if let Some(explicit) = mapping
        .value_type
        .as_deref()
        .and_then(type_from_mapping_type)
    {
        return explicit;
    }

    let primary = if let Some(value) = &mapping.value {
        infer_json_value(value, state, depth + 1)
    } else if let Some(source) = &mapping.source {
        infer_custom_mapping_source_type(source, input_type, output_type)
    } else if let Some(expr) = &mapping.expr {
        infer_expr_with_scope(
            expr,
            rule,
            state,
            Scope::new()
                .with_input(input_type.clone())
                .with_out(output_type.clone())
                .with_pipe(input_type.clone()),
            depth + 1,
        )
    } else {
        FieldType::JsonValue
    };

    match (&primary, &mapping.default) {
        (FieldType::JsonValue, _) => FieldType::JsonValue,
        (_, Some(default)) => merge_types(primary, infer_json_value(default, state, depth + 1)),
        (_, None) => primary,
    }
}

pub(super) fn infer_custom_mapping_source_type(
    source: &str,
    input_type: &FieldType,
    output_type: &FieldType,
) -> FieldType {
    match parse_custom_mapping_source(source) {
        Some((CustomMappingSource::Input, path)) => scoped_path_type(Some(input_type), path),
        Some((CustomMappingSource::Out, path)) => scoped_path_type(Some(output_type), path),
        Some((CustomMappingSource::Context, _)) | None => FieldType::JsonValue,
    }
}

pub(super) fn parse_custom_mapping_source(value: &str) -> Option<(CustomMappingSource, &str)> {
    if let Some((prefix, path)) = value.split_once('.') {
        if path.is_empty() {
            return None;
        }
        let namespace = match prefix {
            "input" => CustomMappingSource::Input,
            "context" => CustomMappingSource::Context,
            "out" => CustomMappingSource::Out,
            _ => return None,
        };
        Some((namespace, path))
    } else {
        if value.is_empty() {
            return None;
        }
        Some((CustomMappingSource::Input, value))
    }
}

#[derive(Clone, Copy)]
pub(super) enum CustomMappingSource {
    Input,
    Context,
    Out,
}

pub(super) fn insert_custom_return_field(
    node: &mut SchemaNode,
    keys: &[String],
    field_type: FieldType,
    optional: bool,
) -> bool {
    let Some(key) = keys.first() else {
        return false;
    };
    if keys.len() == 1 {
        if let Some(field) = node.fields.iter_mut().find(|field| field.key == *key) {
            field.field_type = field_type;
            field.optional = field.optional && optional;
            field.synthetic = false;
            return true;
        }
        if node.fields.len() >= DTO_INFER_MAX_OBJECT_FIELDS {
            return false;
        }
        node.fields.push(Field {
            key: key.clone(),
            field_type,
            optional,
            synthetic: false,
        });
        return true;
    }

    if let Some(field) = node.fields.iter_mut().find(|field| field.key == *key) {
        let FieldType::Object(child) = &mut field.field_type else {
            return false;
        };
        return insert_custom_return_field(child, &keys[1..], field_type, optional);
    }

    if node.fields.len() >= DTO_INFER_MAX_OBJECT_FIELDS {
        return false;
    }
    let mut child = SchemaNode { fields: Vec::new() };
    if !insert_custom_return_field(&mut child, &keys[1..], field_type, optional) {
        return false;
    }
    node.fields.push(Field {
        key: key.clone(),
        field_type: FieldType::Object(Box::new(child)),
        optional: false,
        synthetic: true,
    });
    true
}

pub(super) fn rule_type_to_field_type(rule_type: &RuleType) -> FieldType {
    let field_type = match &rule_type.kind {
        RuleTypeKind::String => FieldType::Primitive(PrimitiveType::String),
        RuleTypeKind::Int => FieldType::Primitive(PrimitiveType::Int),
        RuleTypeKind::Float | RuleTypeKind::Number => FieldType::Primitive(PrimitiveType::Float),
        RuleTypeKind::Bool => FieldType::Primitive(PrimitiveType::Bool),
        RuleTypeKind::Json => FieldType::JsonValue,
        RuleTypeKind::Array(item) => FieldType::Array(Box::new(rule_type_to_field_type(item))),
        RuleTypeKind::Object(fields) => FieldType::Object(Box::new(SchemaNode {
            fields: fields
                .iter()
                .map(|(key, field)| Field {
                    key: key.clone(),
                    field_type: rule_type_to_field_type(&field.ty),
                    optional: field.optional,
                    synthetic: false,
                })
                .collect(),
        })),
    };
    if rule_type.nullable {
        FieldType::Nullable(Box::new(field_type))
    } else {
        field_type
    }
}
