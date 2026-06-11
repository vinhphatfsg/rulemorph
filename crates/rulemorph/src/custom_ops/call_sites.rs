use super::*;

pub(crate) fn validate_custom_call_sites(
    rule: &RuleFile,
    locator: Option<&YamlLocator>,
) -> Vec<RuleError> {
    let mut errors = Vec::new();
    for (name, def) in &rule.defs {
        let base = format!("defs.{}", name);
        validate_def_body_call_sites(rule, def, &base, locator, &mut errors, true);
    }
    for (index, mapping) in rule.mappings.iter().enumerate() {
        validate_mapping_call_sites(
            rule,
            mapping,
            &format!("mappings[{}]", index),
            locator,
            &mut errors,
            false,
            &HashSet::new(),
        );
    }
    if let Some(expr) = &rule.record_when {
        validate_condition_expr_call_sites(
            rule,
            expr,
            "record_when",
            locator,
            &mut errors,
            false,
            &HashSet::new(),
        );
    }
    if let Some(steps) = &rule.steps {
        for (step_index, step) in steps.iter().enumerate() {
            let base = format!("steps[{}]", step_index);
            if let Some(mappings) = &step.mappings {
                for (index, mapping) in mappings.iter().enumerate() {
                    validate_mapping_call_sites(
                        rule,
                        mapping,
                        &format!("{}.mappings[{}]", base, index),
                        locator,
                        &mut errors,
                        false,
                        &HashSet::new(),
                    );
                }
            }
            if let Some(expr) = &step.record_when {
                validate_condition_expr_call_sites(
                    rule,
                    expr,
                    &format!("{}.record_when", base),
                    locator,
                    &mut errors,
                    false,
                    &HashSet::new(),
                );
            }
            if let Some(asserts) = &step.asserts {
                for (index, assert) in asserts.iter().enumerate() {
                    validate_condition_expr_call_sites(
                        rule,
                        &assert.when,
                        &format!("{}.asserts[{}].when", base, index),
                        locator,
                        &mut errors,
                        false,
                        &HashSet::new(),
                    );
                }
            }
            if let Some(branch) = &step.branch {
                validate_condition_expr_call_sites(
                    rule,
                    &branch.when,
                    &format!("{}.branch.when", base),
                    locator,
                    &mut errors,
                    false,
                    &HashSet::new(),
                );
            }
        }
    }
    if let Some(finalize) = &rule.finalize {
        if let Some(filter) = &finalize.filter {
            validate_condition_expr_call_sites(
                rule,
                filter,
                "finalize.filter",
                locator,
                &mut errors,
                false,
                &HashSet::new(),
            );
        }
        if let Some(wrap) = &finalize.wrap {
            validate_finalize_wrap_call_sites(rule, wrap, "finalize.wrap", locator, &mut errors);
        }
    }
    errors
}

pub(super) fn validate_finalize_wrap_call_sites(
    rule: &RuleFile,
    value: &JsonValue,
    path: &str,
    locator: Option<&YamlLocator>,
    errors: &mut Vec<RuleError>,
) {
    match value {
        JsonValue::Object(map) => {
            for (key, value) in map {
                validate_finalize_wrap_call_sites(
                    rule,
                    value,
                    &format!("{}.{}", path, key),
                    locator,
                    errors,
                );
            }
        }
        _ => {
            let Ok(expr) = parse_v2_expr(value) else {
                return;
            };
            validate_v2_expr_call_sites(rule, &expr, path, locator, errors, false);
        }
    }
}

pub(super) fn validate_mapping_call_sites(
    rule: &RuleFile,
    mapping: &Mapping,
    base_path: &str,
    locator: Option<&YamlLocator>,
    errors: &mut Vec<RuleError>,
    in_custom_body: bool,
    produced_targets: &HashSet<Vec<PathToken>>,
) {
    if let Some(expr) = &mapping.expr {
        validate_expr_call_sites(
            rule,
            expr,
            &format!("{}.expr", base_path),
            locator,
            errors,
            in_custom_body,
            produced_targets,
        );
    }
    if let Some(when) = &mapping.when {
        validate_condition_expr_call_sites(
            rule,
            when,
            &format!("{}.when", base_path),
            locator,
            errors,
            in_custom_body,
            produced_targets,
        );
    }
}

pub(super) fn validate_def_body_call_sites(
    rule: &RuleFile,
    def: &CustomOpDef,
    base_path: &str,
    locator: Option<&YamlLocator>,
    errors: &mut Vec<RuleError>,
    in_custom_body: bool,
) {
    if let Some(expr) = &def.expr {
        validate_expr_call_sites(
            rule,
            expr,
            &format!("{}.expr", base_path),
            locator,
            errors,
            in_custom_body,
            &HashSet::new(),
        );
    }
    if let Some(mappings) = &def.mappings {
        let mut produced_targets = HashSet::new();
        for (index, mapping) in mappings.iter().enumerate() {
            validate_mapping_call_sites(
                rule,
                mapping,
                &format!("{}.mappings[{}]", base_path, index),
                locator,
                errors,
                in_custom_body,
                &produced_targets,
            );
            if let Ok(tokens) = parse_path(&mapping.target) {
                produced_targets.insert(tokens);
            }
        }
    }
}
