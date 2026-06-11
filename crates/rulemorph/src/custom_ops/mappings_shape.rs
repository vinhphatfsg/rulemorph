use super::*;

pub(super) fn validate_custom_mappings_shape(
    def: &CustomOpDef,
    base_path: &str,
    locator: Option<&YamlLocator>,
    errors: &mut Vec<RuleError>,
) {
    let Some(mappings) = &def.mappings else {
        return;
    };

    if let Some(returns) = &def.returns
        && !matches!(returns.kind, RuleTypeKind::Json | RuleTypeKind::Object(_))
    {
        push_rule_error(
            errors,
            locator,
            ErrorCode::InvalidTypeName,
            "custom op mappings body returns must be object or json",
            &format!("{}.returns", base_path),
        );
    }

    let mut produced_targets: HashSet<Vec<PathToken>> = HashSet::new();
    for (index, mapping) in mappings.iter().enumerate() {
        let base = format!("{}.mappings[{}]", base_path, index);

        if mapping.target.trim().is_empty() {
            push_rule_error(
                errors,
                locator,
                ErrorCode::MissingTarget,
                "mapping.target is required",
                &format!("{}.target", base),
            );
        }

        let target_tokens = match parse_path(&mapping.target) {
            Ok(tokens) => tokens,
            Err(_) => {
                push_rule_error(
                    errors,
                    locator,
                    ErrorCode::InvalidPath,
                    "target path is invalid",
                    &format!("{}.target", base),
                );
                continue;
            }
        };

        if target_tokens
            .iter()
            .any(|token| matches!(token, PathToken::Index(_)))
        {
            push_rule_error(
                errors,
                locator,
                ErrorCode::InvalidPath,
                "target path must not include indexes",
                &format!("{}.target", base),
            );
            continue;
        }

        if produced_targets.contains(&target_tokens) {
            push_rule_error(
                errors,
                locator,
                ErrorCode::DuplicateTarget,
                "mapping.target is duplicated",
                &format!("{}.target", base),
            );
        }

        let value_count = custom_mapping_value_count(mapping);
        if value_count == 0 {
            push_rule_error(
                errors,
                locator,
                ErrorCode::MissingMappingValue,
                "mapping must define source, value, or expr",
                &base,
            );
        } else if value_count > 1 {
            push_rule_error(
                errors,
                locator,
                ErrorCode::SourceValueExprExclusive,
                "exactly one of source/value/expr is required",
                &base,
            );
        }

        if let Some(type_name) = &mapping.value_type
            && !matches!(type_name.as_str(), "string" | "int" | "float" | "bool")
        {
            push_rule_error(
                errors,
                locator,
                ErrorCode::InvalidTypeName,
                "type must be string|int|float|bool",
                &format!("{}.type", base),
            );
        }

        if let Some(source) = &mapping.source {
            validate_custom_mapping_source(source, &base, &produced_targets, locator, errors);
        }

        produced_targets.insert(target_tokens);
    }
}

pub(super) fn custom_mapping_value_count(mapping: &Mapping) -> usize {
    usize::from(mapping.source.is_some())
        + usize::from(mapping.value.is_some())
        + usize::from(mapping.expr.is_some())
}

pub(super) fn validate_custom_mapping_source(
    source: &str,
    base_path: &str,
    produced_targets: &HashSet<Vec<PathToken>>,
    locator: Option<&YamlLocator>,
    errors: &mut Vec<RuleError>,
) {
    let full_path = format!("{}.source", base_path);
    let Some((namespace, path)) = parse_mapping_source_ref(source) else {
        push_rule_error(
            errors,
            locator,
            ErrorCode::InvalidRefNamespace,
            "ref namespace must be input|out",
            &full_path,
        );
        return;
    };

    if namespace == CustomSourceNamespace::Context {
        push_rule_error(
            errors,
            locator,
            ErrorCode::InvalidRefNamespace,
            "@context is not available inside custom op bodies",
            &full_path,
        );
        return;
    }

    let tokens = match parse_path(path) {
        Ok(tokens) => tokens,
        Err(_) => {
            push_rule_error(
                errors,
                locator,
                ErrorCode::InvalidPath,
                "path is invalid",
                &full_path,
            );
            return;
        }
    };

    if namespace == CustomSourceNamespace::Out && !out_ref_resolves(&tokens, produced_targets) {
        push_rule_error(
            errors,
            locator,
            ErrorCode::ForwardOutReference,
            "out reference must point to previous mappings",
            &full_path,
        );
    }
}

pub(super) fn parse_mapping_source_ref(value: &str) -> Option<(CustomSourceNamespace, &str)> {
    if let Some((prefix, path)) = value.split_once('.') {
        if path.is_empty() {
            return None;
        }
        let namespace = match prefix {
            "input" => CustomSourceNamespace::Input,
            "context" => CustomSourceNamespace::Context,
            "out" => CustomSourceNamespace::Out,
            _ => return None,
        };
        Some((namespace, path))
    } else {
        if value.is_empty() {
            return None;
        }
        Some((CustomSourceNamespace::Input, value))
    }
}

pub(super) fn out_ref_resolves(
    tokens: &[PathToken],
    produced_targets: &HashSet<Vec<PathToken>>,
) -> bool {
    if !tokens
        .iter()
        .any(|token| matches!(token, PathToken::Key(_)))
    {
        return false;
    }

    for produced in produced_targets {
        if is_path_prefix(produced, tokens) || is_path_prefix(tokens, produced) {
            return true;
        }
    }
    false
}

pub(super) fn is_path_prefix(prefix: &[PathToken], tokens: &[PathToken]) -> bool {
    prefix.len() <= tokens.len() && prefix.iter().zip(tokens).all(|(left, right)| left == right)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CustomSourceNamespace {
    Input,
    Context,
    Out,
}
