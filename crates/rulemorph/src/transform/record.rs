use super::*;

fn apply_mappings(
    rule: &RuleFile,
    record: &JsonValue,
    context: Option<&JsonValue>,
    warnings: &mut Vec<TransformWarning>,
) -> Result<JsonValue, TransformError> {
    let mut out = JsonValue::Object(Map::new());
    apply_mappings_into(
        &rule.mappings,
        record,
        context,
        &mut out,
        warnings,
        rule.version,
        "mappings",
    )?;
    Ok(out)
}

fn apply_mappings_into(
    mappings: &[Mapping],
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &mut JsonValue,
    warnings: &mut Vec<TransformWarning>,
    rule_version: u8,
    base_path: &str,
) -> Result<(), TransformError> {
    for (index, mapping) in mappings.iter().enumerate() {
        let mapping_path = format!("{}[{}]", base_path, index);
        if !eval_when(
            mapping,
            record,
            context,
            out,
            &mapping_path,
            warnings,
            rule_version,
        ) {
            continue;
        }
        let value = eval_mapping(mapping, record, context, out, &mapping_path, rule_version)?;
        if let Some(value) = value {
            set_path(out, &mapping.target, value, &mapping_path)?;
        }
    }
    Ok(())
}

pub(super) fn apply_rule_to_record(
    rule: &RuleFile,
    record: &JsonValue,
    context: Option<&JsonValue>,
    warnings: &mut Vec<TransformWarning>,
    base_dir: Option<&Path>,
    branch_context: &mut BranchContext,
) -> Result<Option<JsonValue>, TransformError> {
    if let Some(steps) = &rule.steps {
        return apply_steps(
            steps,
            record,
            context,
            warnings,
            rule.version,
            base_dir,
            branch_context,
        );
    }

    if !eval_record_when(rule, record, context, warnings) {
        return Ok(None);
    }

    let output = apply_mappings(rule, record, context, warnings)?;
    Ok(Some(output))
}

fn apply_steps(
    steps: &[V2RuleStep],
    record: &JsonValue,
    context: Option<&JsonValue>,
    warnings: &mut Vec<TransformWarning>,
    rule_version: u8,
    base_dir: Option<&Path>,
    branch_context: &mut BranchContext,
) -> Result<Option<JsonValue>, TransformError> {
    let mut out = JsonValue::Object(Map::new());

    for (step_index, step) in steps.iter().enumerate() {
        let base_path = format!("steps[{}]", step_index);

        if let Some(mappings) = &step.mappings {
            apply_mappings_into(
                mappings,
                record,
                context,
                &mut out,
                warnings,
                rule_version,
                &format!("{}.mappings", base_path),
            )?;
            continue;
        }

        if let Some(expr) = &step.record_when {
            let when_path = format!("{}.record_when", base_path);
            let keep = eval_when_expr(expr, record, context, &out, &when_path, rule_version)?;
            if !keep {
                return Ok(None);
            }
            continue;
        }

        if let Some(asserts) = &step.asserts {
            for (assert_index, assert) in asserts.iter().enumerate() {
                let assert_path = format!("{}.asserts[{}]", base_path, assert_index);
                let ok = eval_when_expr(
                    &assert.when,
                    record,
                    context,
                    &out,
                    &format!("{}.when", assert_path),
                    rule_version,
                )?;
                if !ok {
                    return Err(TransformError::new(
                        TransformErrorKind::AssertionFailed,
                        format!(
                            "assert failed: {}: {}",
                            assert.error.code, assert.error.message
                        ),
                    )
                    .with_path(assert_path));
                }
            }
            continue;
        }

        if let Some(branch) = &step.branch {
            let branch_path = format!("{}.branch", base_path);
            let take = eval_when_expr(
                &branch.when,
                record,
                context,
                &out,
                &format!("{}.when", branch_path),
                rule_version,
            )?;
            let (target, target_field) = if take {
                (Some(branch.then.as_str()), "then")
            } else {
                (branch.r#else.as_deref(), "else")
            };
            if let Some(target) = target {
                let branch_path_guard = branch_context
                    .enter(base_dir, target)
                    .map_err(|err| err.with_path(format!("{}.{}", branch_path, target_field)))?;
                let (branch_rule, branch_base_dir) =
                    load_rule_from_path(base_dir, target, branch_context.allowed_root()).map_err(
                        |err| err.with_path(format!("{}.{}", branch_path, target_field)),
                    )?;
                let branch_input = out.clone();
                let (branch_output, branch_warnings) = transform_record_with_warnings_inner(
                    &branch_rule,
                    &branch_input,
                    context,
                    Some(&branch_base_dir),
                    branch_context,
                )?;
                branch_context.exit(branch_path_guard);
                warnings.extend(branch_warnings);
                let Some(branch_output) = branch_output else {
                    return Ok(None);
                };

                if branch.return_ {
                    return Ok(Some(branch_output));
                }
                merge_branch_output(&mut out, &branch_output, &branch_path)?;
            }
            continue;
        }
    }

    Ok(Some(out))
}
