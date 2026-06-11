use rulemorph::{NormalizationOptions, PathToken, serde_guard::parse_json_value_strict};

use super::DirectArgs;

mod ref_scan;
mod validation;

pub(super) use ref_scan::expr_has_evaluated_root_ref;
use validation::{
    checked_add_limit, validate_direct_output_cells, validate_expr_limits, validate_field_specs,
    validate_output_spec_size, validate_target,
};

pub(super) struct DirectCliError {
    pub(super) message: String,
    pub(super) exit_code: i32,
}

impl DirectCliError {
    pub(super) fn validation(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            exit_code: 2,
        }
    }

    fn parse(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            exit_code: 1,
        }
    }
}

#[derive(Clone)]
pub(super) struct FieldSpec {
    pub(super) target: String,
    pub(super) expr: serde_json::Value,
    pub(super) tokens: Vec<PathToken>,
}

pub(super) enum DirectOutputSpec {
    Rule(serde_json::Value),
    Fields(Vec<FieldSpec>),
    OutputMap(Vec<FieldSpec>),
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum DirectOutputMode {
    Value,
    RecordObject,
}

impl DirectOutputSpec {
    pub(super) fn output_mode(&self) -> DirectOutputMode {
        match self {
            DirectOutputSpec::Rule(_) => DirectOutputMode::Value,
            DirectOutputSpec::Fields(_) | DirectOutputSpec::OutputMap(_) => {
                DirectOutputMode::RecordObject
            }
        }
    }

    pub(super) fn exprs(&self) -> Vec<&serde_json::Value> {
        match self {
            DirectOutputSpec::Rule(expr) => vec![expr],
            DirectOutputSpec::Fields(fields) | DirectOutputSpec::OutputMap(fields) => {
                fields.iter().map(|field| &field.expr).collect()
            }
        }
    }

    fn output_field_count(&self) -> usize {
        match self {
            DirectOutputSpec::Rule(_) => 1,
            DirectOutputSpec::Fields(fields) | DirectOutputSpec::OutputMap(fields) => fields.len(),
        }
    }
}

pub(super) fn parse_direct_output_spec(
    args: &DirectArgs,
    options: &NormalizationOptions,
    output_cell_record_budget: usize,
) -> Result<DirectOutputSpec, DirectCliError> {
    let spec_count = usize::from(args.rule.is_some())
        + usize::from(!args.fields.is_empty())
        + usize::from(args.output_map.is_some());
    if spec_count != 1 {
        return Err(DirectCliError::validation(
            "exactly one of --rule, --output-map, or --field is required",
        ));
    }

    if let Some(rule) = args.rule.as_deref() {
        return parse_inline_expr(rule)
            .map(DirectOutputSpec::Rule)
            .map_err(DirectCliError::parse);
    }

    let spec = if !args.fields.is_empty() {
        let fields = parse_field_specs(&args.fields)?;
        DirectOutputSpec::Fields(fields)
    } else if let Some(output_map) = args.output_map.as_deref() {
        let fields = parse_output_map_specs(output_map)?;
        DirectOutputSpec::OutputMap(fields)
    } else {
        unreachable!("spec_count verified exactly one direct output spec");
    };
    validate_direct_output_cells(&spec, options, output_cell_record_budget)?;
    Ok(spec)
}

fn parse_field_specs(items: &[String]) -> Result<Vec<FieldSpec>, DirectCliError> {
    let spec_bytes = items
        .iter()
        .try_fold(0usize, |sum, item| checked_add_limit(sum, item.len()))
        .map_err(DirectCliError::validation)?;
    validate_output_spec_size(spec_bytes).map_err(DirectCliError::validation)?;

    let mut fields = Vec::with_capacity(items.len());
    for item in items {
        let (target, expr) = item
            .split_once('=')
            .ok_or_else(|| DirectCliError::validation("--field must be TARGET=EXPR"))?;
        if target.is_empty() {
            return Err(DirectCliError::validation(
                "--field target must not be blank",
            ));
        }
        if expr.is_empty() {
            return Err(DirectCliError::validation("--field expr must not be blank"));
        }
        let expr = parse_field_expr(expr).map_err(DirectCliError::validation)?;
        validate_expr_limits(&expr).map_err(DirectCliError::validation)?;
        fields.push(FieldSpec {
            target: target.to_string(),
            expr,
            tokens: validate_target(target).map_err(DirectCliError::validation)?,
        });
    }
    validate_field_specs(&fields).map_err(DirectCliError::validation)?;
    Ok(fields)
}

fn parse_output_map_specs(output_map: &str) -> Result<Vec<FieldSpec>, DirectCliError> {
    validate_output_spec_size(output_map.len()).map_err(DirectCliError::validation)?;
    let value = parse_json_value_strict(output_map).map_err(|err| {
        DirectCliError::validation(format!("failed to parse --output-map JSON: {}", err))
    })?;
    let serde_json::Value::Object(object) = value else {
        return Err(DirectCliError::validation(
            "--output-map must be a JSON object",
        ));
    };
    if object.is_empty() {
        return Err(DirectCliError::validation(
            "--output-map must define at least one field",
        ));
    }
    let mut fields = Vec::with_capacity(object.len());
    for (target, expr) in object {
        validate_expr_limits(&expr).map_err(DirectCliError::validation)?;
        if expr_has_evaluated_root_ref(&expr, "out") {
            return Err(DirectCliError::validation(
                "--output-map does not support @out references; use --field for ordered mappings",
            ));
        }
        fields.push(FieldSpec {
            tokens: validate_target(&target).map_err(DirectCliError::validation)?,
            target,
            expr,
        });
    }
    validate_field_specs(&fields).map_err(DirectCliError::validation)?;
    Ok(fields)
}

fn parse_field_expr(expr: &str) -> Result<serde_json::Value, String> {
    match parse_json_value_strict(expr) {
        Ok(value) => Ok(value),
        Err(err) if looks_like_json(expr) => Err(format!(
            "--field expr looks like JSON but failed to parse: {}",
            err
        )),
        Err(_) => Ok(serde_json::Value::String(expr.to_string())),
    }
}

fn looks_like_json(value: &str) -> bool {
    matches!(
        strip_utf8_bom(value.as_bytes())
            .iter()
            .copied()
            .find(|byte| !byte.is_ascii_whitespace()),
        Some(b'{') | Some(b'[')
    )
}

fn strip_utf8_bom(input: &[u8]) -> &[u8] {
    input.strip_prefix(b"\xef\xbb\xbf").unwrap_or(input)
}

fn parse_inline_expr(inline_rule: &str) -> Result<serde_json::Value, String> {
    match parse_json_value_strict(inline_rule) {
        Ok(value) => Ok(value),
        Err(err) if serde_json::from_str::<serde_json::Value>(inline_rule).is_ok() => {
            Err(format!("failed to parse inline JSON rule: {}", err))
        }
        Err(_) => Ok(serde_json::Value::String(inline_rule.to_string())),
    }
}
