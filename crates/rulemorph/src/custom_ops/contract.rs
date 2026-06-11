use super::*;

pub(crate) fn check_contract(
    value: &JsonValue,
    ty: &RuleType,
    mode: ContractMode,
    path: &str,
) -> Result<(), TransformError> {
    check_contract_inner(value, ty, mode, path).map_err(|message| {
        TransformError::new(TransformErrorKind::ExprError, message).with_path(path)
    })
}

pub(super) fn check_contract_inner(
    value: &JsonValue,
    ty: &RuleType,
    mode: ContractMode,
    path: &str,
) -> Result<(), String> {
    if matches!(ty.kind, RuleTypeKind::Json) {
        return Ok(());
    }
    if value.is_null() {
        return if ty.nullable {
            Ok(())
        } else {
            Err(format!("{} expected {}, got null", path, type_name(ty)))
        };
    }
    match &ty.kind {
        RuleTypeKind::String => value
            .is_string()
            .then_some(())
            .ok_or_else(|| format!("{} expected string, got {}", path, json_type(value))),
        RuleTypeKind::Int => value
            .as_i64()
            .or_else(|| value.as_u64().and_then(|v| i64::try_from(v).ok()))
            .map(|_| ())
            .ok_or_else(|| format!("{} expected int, got {}", path, json_type(value))),
        RuleTypeKind::Float | RuleTypeKind::Number => value
            .as_f64()
            .filter(|value| value.is_finite())
            .map(|_| ())
            .ok_or_else(|| format!("{} expected number, got {}", path, json_type(value))),
        RuleTypeKind::Bool => value
            .is_boolean()
            .then_some(())
            .ok_or_else(|| format!("{} expected bool, got {}", path, json_type(value))),
        RuleTypeKind::Json => Ok(()),
        RuleTypeKind::Array(item_ty) => {
            let JsonValue::Array(items) = value else {
                return Err(format!("{} expected array, got {}", path, json_type(value)));
            };
            for (index, item) in items.iter().enumerate() {
                check_contract_inner(item, item_ty, mode, &format!("{}[{}]", path, index))?;
            }
            Ok(())
        }
        RuleTypeKind::Object(fields) => check_object_contract(value, fields, mode, path),
    }
}

pub(super) fn check_object_contract(
    value: &JsonValue,
    fields: &BTreeMap<String, RuleTypeField>,
    mode: ContractMode,
    path: &str,
) -> Result<(), String> {
    let JsonValue::Object(object) = value else {
        return Err(format!(
            "{} expected object, got {}",
            path,
            json_type(value)
        ));
    };
    for (name, field) in fields {
        match object.get(name) {
            Some(value) => {
                check_contract_inner(value, &field.ty, mode, &format!("{}.{}", path, name))?
            }
            None if field.optional => {}
            None => return Err(format!("{} missing required field `{}`", path, name)),
        }
    }
    if matches!(mode, ContractMode::AdapterExact | ContractMode::OutputExact) {
        for name in object.keys() {
            if !fields.contains_key(name) {
                return Err(format!("{} contains unexpected field `{}`", path, name));
            }
        }
    }
    Ok(())
}

pub(crate) fn build_with_object(items: impl IntoIterator<Item = (String, JsonValue)>) -> JsonValue {
    let mut object = JsonMap::new();
    for (key, value) in items {
        object.insert(key, value);
    }
    JsonValue::Object(object)
}
