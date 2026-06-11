use super::*;

pub(super) fn insert_return_path(
    fields: &mut BTreeMap<String, RuleTypeField>,
    tokens: &[PathToken],
    optional: bool,
) -> bool {
    let Some(PathToken::Key(key)) = tokens.first() else {
        return !optional;
    };
    if tokens.len() == 1 {
        let field = fields.entry(key.clone()).or_insert_with(|| RuleTypeField {
            ty: json_rule_type(),
            optional,
        });
        field.ty = json_rule_type();
        field.optional &= optional;
        return !optional;
    }

    let field = fields.entry(key.clone()).or_insert_with(|| RuleTypeField {
        ty: RuleType {
            kind: RuleTypeKind::Object(BTreeMap::new()),
            nullable: false,
        },
        optional,
    });
    let child_required = match &mut field.ty.kind {
        RuleTypeKind::Object(child_fields) => {
            insert_return_path(child_fields, &tokens[1..], optional)
        }
        _ => {
            field.ty = json_rule_type();
            !optional
        }
    };
    field.optional &= !child_required;
    child_required
}

pub(super) fn shadowed_custom_op_error(name: &str, path: &str) -> TransformError {
    TransformError::new(
        TransformErrorKind::ExprError,
        format!(
            "custom op `{}` must not shadow a built-in or reserved op",
            name
        ),
    )
    .with_path(path)
}

pub(super) fn normalize_custom_body_when_error_path(
    path: Option<String>,
    mapping_path: &str,
    when_path: &str,
) -> Option<String> {
    let path = path?;
    if path == when_path {
        return Some(mapping_path.to_string());
    }
    if let Some(suffix) = path.strip_prefix(when_path)
        && (suffix.starts_with('.') || suffix.starts_with('['))
    {
        return Some(format!("{mapping_path}{suffix}"));
    }
    Some(path)
}

pub(super) fn mapping_may_be_absent(mapping: &Mapping) -> bool {
    let conditional = !matches!(
        &mapping.when,
        None | Some(crate::model::Expr::Literal(JsonValue::Bool(true)))
    );
    conditional || !(mapping.required || mapping.value.is_some() || mapping.default.is_some())
}

pub(super) fn json_rule_type() -> RuleType {
    RuleType {
        kind: RuleTypeKind::Json,
        nullable: true,
    }
}
