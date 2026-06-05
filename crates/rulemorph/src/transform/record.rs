use super::*;

fn apply_mappings(
    rule: &RuleFile,
    record: &JsonValue,
    context: Option<&JsonValue>,
    warnings: &mut Vec<TransformWarning>,
    limits: EvalLimits,
    base_v2_ctx: &V2EvalContext<'_>,
    compiled_rule: Option<&CompiledRule>,
) -> Result<JsonValue, TransformError> {
    let mut out = JsonValue::Object(Map::new());
    apply_mappings_into(
        rule,
        &rule.mappings,
        record,
        context,
        &mut out,
        warnings,
        rule.version,
        "mappings",
        limits,
        base_v2_ctx,
        compiled_rule,
    )?;
    Ok(out)
}

fn apply_mappings_into(
    rule: &RuleFile,
    mappings: &[Mapping],
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &mut JsonValue,
    warnings: &mut Vec<TransformWarning>,
    rule_version: u8,
    base_path: &str,
    limits: EvalLimits,
    base_v2_ctx: &V2EvalContext<'_>,
    compiled_rule: Option<&CompiledRule>,
) -> Result<(), TransformError> {
    for (index, mapping) in mappings.iter().enumerate() {
        let compiled_mapping = compiled_rule.and_then(|compiled| compiled.mapping(index));
        let mapping_path_storage;
        let mapping_path = if let Some(compiled) = compiled_mapping {
            compiled.mapping_path()
        } else {
            mapping_path_storage = format!("{}[{}]", base_path, index);
            &mapping_path_storage
        };
        if !eval_when(
            mapping,
            record,
            context,
            out,
            &mapping_path,
            warnings,
            rule_version,
            limits,
            base_v2_ctx,
        ) {
            continue;
        }
        let value = eval_mapping_with_v2_context(
            rule,
            mapping,
            record,
            context,
            out,
            &mapping_path,
            rule_version,
            limits,
            base_v2_ctx,
            compiled_mapping,
        )?;
        if let Some(value) = value {
            match compiled_mapping {
                Some(compiled) => {
                    let tokens = compiled.target_tokens(mapping)?;
                    set_path_tokens(out, tokens, value, &mapping_path)?;
                }
                None => set_path(out, &mapping.target, value, &mapping_path)?,
            }
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
    limits: EvalLimits,
    compiled_rule: Option<&CompiledRule>,
) -> Result<Option<JsonValue>, TransformError> {
    let base_v2_ctx = V2EvalContext::new()
        .with_limits(limits)
        .with_rule(rule)
        .with_shared_custom_op_counter();
    if let Some(steps) = &rule.steps {
        return apply_steps(
            rule,
            steps,
            record,
            context,
            warnings,
            rule.version,
            base_dir,
            branch_context,
            limits,
            &base_v2_ctx,
        );
    }

    if !eval_record_when(rule, record, context, warnings, limits, &base_v2_ctx) {
        return Ok(None);
    }

    let output = apply_mappings(
        rule,
        record,
        context,
        warnings,
        limits,
        &base_v2_ctx,
        compiled_rule,
    )?;
    Ok(Some(output))
}

fn apply_steps(
    rule: &RuleFile,
    steps: &[V2RuleStep],
    record: &JsonValue,
    context: Option<&JsonValue>,
    warnings: &mut Vec<TransformWarning>,
    rule_version: u8,
    base_dir: Option<&Path>,
    branch_context: &mut BranchContext,
    limits: EvalLimits,
    base_v2_ctx: &V2EvalContext<'_>,
) -> Result<Option<JsonValue>, TransformError> {
    let mut out = JsonValue::Object(Map::new());

    for (step_index, step) in steps.iter().enumerate() {
        let base_path = format!("steps[{}]", step_index);

        if let Some(mappings) = &step.mappings {
            apply_mappings_into(
                rule,
                mappings,
                record,
                context,
                &mut out,
                warnings,
                rule_version,
                &format!("{}.mappings", base_path),
                limits,
                base_v2_ctx,
                None,
            )?;
            continue;
        }

        if let Some(expr) = &step.record_when {
            let when_path = format!("{}.record_when", base_path);
            let keep = eval_when_expr_with_v2_context(
                expr,
                record,
                context,
                &out,
                &when_path,
                rule_version,
                limits,
                base_v2_ctx,
            )?;
            if !keep {
                return Ok(None);
            }
            continue;
        }

        if let Some(asserts) = &step.asserts {
            for (assert_index, assert) in asserts.iter().enumerate() {
                let assert_path = format!("{}.asserts[{}]", base_path, assert_index);
                let ok = eval_when_expr_with_v2_context(
                    &assert.when,
                    record,
                    context,
                    &out,
                    &format!("{}.when", assert_path),
                    rule_version,
                    limits,
                    base_v2_ctx,
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
            let take = eval_when_expr_with_v2_context(
                &branch.when,
                record,
                context,
                &out,
                &format!("{}.when", branch_path),
                rule_version,
                limits,
                base_v2_ctx,
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
                    limits,
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn custom_op_call_limit_is_shared_across_mappings_in_one_record() {
        let rule = crate::parse_rule_file(
            r#"
version: 2
input:
  format: json
  json: {}
defs:
  id:
    input: int
    returns: int
    expr: "$"
mappings:
  - target: a
    expr: ["@input.a", { map: [id] }]
  - target: b
    expr: ["@input.b", { map: [id] }]
"#,
        )
        .expect("rule parses");
        let record = json!({ "a": [1, 2], "b": [3, 4] });
        let mut warnings = Vec::new();
        let mut branch_context = BranchContext::default();
        let limits = EvalLimits {
            max_custom_op_calls_per_record: 3,
            ..EvalLimits::default()
        };

        let err = apply_rule_to_record(
            &rule,
            &record,
            None,
            &mut warnings,
            None,
            &mut branch_context,
            limits,
            None,
        )
        .expect_err("four calls across two mappings exceed shared per-record limit");

        assert!(
            err.message
                .contains("custom op calls per record exceed configured limit")
        );
    }
}
