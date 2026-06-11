use super::*;

pub(super) fn validate_type_limits(
    ty: &RuleType,
    path: &str,
    locator: Option<&YamlLocator>,
    errors: &mut Vec<RuleError>,
) {
    let mut fields = 0usize;
    let depth = type_depth_and_fields(ty, &mut fields);
    if depth > MAX_TYPE_DEPTH {
        push_rule_error(
            errors,
            locator,
            ErrorCode::InvalidTypeName,
            "custom op type exceeds configured depth limit",
            path,
        );
    }
    if fields > MAX_TYPE_FIELDS {
        push_rule_error(
            errors,
            locator,
            ErrorCode::InvalidTypeName,
            "custom op type exceeds configured field limit",
            path,
        );
    }
}

pub(super) fn type_depth_and_fields(ty: &RuleType, fields: &mut usize) -> usize {
    match &ty.kind {
        RuleTypeKind::Array(item) => 1 + type_depth_and_fields(item, fields),
        RuleTypeKind::Object(map) => {
            *fields += map.len();
            1 + map
                .values()
                .map(|field| type_depth_and_fields(&field.ty, fields))
                .max()
                .unwrap_or(0)
        }
        _ => 1,
    }
}

pub(super) fn custom_op_body_nodes(def: &CustomOpDef) -> usize {
    let mut count = 0usize;
    if let Some(expr) = &def.expr {
        count += expr_node_count(expr);
    }
    if let Some(mappings) = &def.mappings {
        count += mappings.len();
        for mapping in mappings {
            if let Some(expr) = &mapping.expr {
                count += expr_node_count(expr);
            }
            if let Some(when) = &mapping.when {
                count += condition_node_count(when);
            }
            if let Some(value) = &mapping.value {
                count += json_node_count(value);
            }
            if let Some(default) = &mapping.default {
                count += json_node_count(default);
            }
        }
    }
    count
}

pub(super) fn expr_node_count(expr: &crate::model::Expr) -> usize {
    let Some(value) = crate::expr_json::expr_to_json_for_v2_pipe(expr) else {
        return 1;
    };
    json_node_count(&value)
}

pub(super) fn condition_node_count(expr: &crate::model::Expr) -> usize {
    let Some(value) = crate::expr_json::expr_to_json_for_v2_condition(expr) else {
        return 1;
    };
    json_node_count(&value)
}

pub(super) fn json_node_count(value: &JsonValue) -> usize {
    match value {
        JsonValue::Array(items) => 1 + items.iter().map(json_node_count).sum::<usize>(),
        JsonValue::Object(map) => 1 + map.values().map(json_node_count).sum::<usize>(),
        _ => 1,
    }
}

pub(super) fn is_valid_custom_op_name(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(ch) if ch.is_ascii_alphabetic() || ch == '_' => {}
        _ => return false,
    }
    chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
}

pub(crate) fn is_reserved_or_builtin_custom_op_name(name: &str) -> bool {
    is_valid_operator(name) || is_reserved_custom_op_name(name)
}

pub(super) fn is_reserved_custom_op_name(name: &str) -> bool {
    matches!(
        name,
        "op" | "let" | "if" | "map" | "then" | "else" | "cond" | "ref"
    )
}

pub(super) fn push_rule_error(
    errors: &mut Vec<RuleError>,
    locator: Option<&YamlLocator>,
    code: ErrorCode,
    message: impl Into<String>,
    path: &str,
) {
    let mut err = RuleError::new(code, message).with_path(path);
    if let Some(locator) = locator
        && let Some(location) = locator.location_for(path)
    {
        err = err.with_location(location.line, location.column);
    }
    errors.push(err);
}

pub(super) fn type_name(ty: &RuleType) -> &'static str {
    match &ty.kind {
        RuleTypeKind::String => "string",
        RuleTypeKind::Int => "int",
        RuleTypeKind::Float => "float",
        RuleTypeKind::Number => "number",
        RuleTypeKind::Bool => "bool",
        RuleTypeKind::Json => "json",
        RuleTypeKind::Array(_) => "array",
        RuleTypeKind::Object(_) => "object",
    }
}

pub(super) fn json_type(value: &JsonValue) -> &'static str {
    match value {
        JsonValue::Null => "null",
        JsonValue::Bool(_) => "bool",
        JsonValue::Number(_) => "number",
        JsonValue::String(_) => "string",
        JsonValue::Array(_) => "array",
        JsonValue::Object(_) => "object",
    }
}
