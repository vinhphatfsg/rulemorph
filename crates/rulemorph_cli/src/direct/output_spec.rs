use std::collections::HashSet;

use rulemorph::{
    Expr, ExprChain, ExprOp, ExprRef, NormalizationOptions, PathToken, parse_path,
    serde_guard::parse_json_value_strict,
    v2_model::{V2CallArg, V2Comparison, V2Condition, V2Expr, V2Pipe, V2Ref, V2Start, V2Step},
    v2_parser::parse_v2_expr,
};

use super::DirectArgs;

const MAX_DIRECT_OUTPUT_FIELDS: usize = 10_000;
const MAX_DIRECT_OUTPUT_SPEC_BYTES: usize = 8 * 1024 * 1024;
const MAX_DIRECT_OUTPUT_TARGET_BYTES: usize = 256 * 1024;
const MAX_DIRECT_OUTPUT_TARGET_BYTES_TOTAL: usize = 8 * 1024 * 1024;
const MAX_DIRECT_OUTPUT_TARGET_DEPTH: usize = 256;
const MAX_DIRECT_OUTPUT_TARGET_TOKENS_TOTAL: usize = 1_000_000;
const MAX_DIRECT_OUTPUT_EXPR_DEPTH: usize = 256;
const MAX_DIRECT_OUTPUT_EXPR_NODES: usize = 1_000_000;
const MAX_DIRECT_OUTPUT_EXPR_STRING_BYTES: usize = 8 * 1024 * 1024;
const MAX_DIRECT_OUTPUT_CELLS: usize = 10_000_000;

pub(super) struct DirectCliError {
    pub(super) message: String,
    pub(super) exit_code: i32,
}

impl DirectCliError {
    fn validation(message: impl Into<String>) -> Self {
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
    tokens: Vec<PathToken>,
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

fn validate_output_spec_size(bytes: usize) -> Result<(), String> {
    if bytes > MAX_DIRECT_OUTPUT_SPEC_BYTES {
        return Err(format!(
            "direct output spec must be at most {} bytes",
            MAX_DIRECT_OUTPUT_SPEC_BYTES
        ));
    }
    Ok(())
}

fn validate_target(target: &str) -> Result<Vec<PathToken>, String> {
    if target.len() > MAX_DIRECT_OUTPUT_TARGET_BYTES {
        return Err(format!(
            "direct output target names must be at most {} bytes each",
            MAX_DIRECT_OUTPUT_TARGET_BYTES
        ));
    }
    let tokens = parse_path(target)
        .map_err(|err| format!("direct output target path is invalid: {}", err.message()))?;
    if tokens
        .iter()
        .any(|token| matches!(token, PathToken::Index(_)))
    {
        return Err("direct output target path must not include indexes".to_string());
    }
    if tokens.len() > MAX_DIRECT_OUTPUT_TARGET_DEPTH {
        return Err(format!(
            "direct output target path exceeds configured depth limit ({})",
            MAX_DIRECT_OUTPUT_TARGET_DEPTH
        ));
    }
    Ok(tokens)
}

fn validate_field_specs(fields: &[FieldSpec]) -> Result<(), String> {
    if fields.len() > MAX_DIRECT_OUTPUT_FIELDS {
        return Err(format!(
            "direct output has too many fields; maximum is {}",
            MAX_DIRECT_OUTPUT_FIELDS
        ));
    }
    let mut total_target_bytes = 0usize;
    let mut total_tokens = 0usize;
    let mut seen = HashSet::new();
    for field in fields {
        total_target_bytes = checked_add_limit(total_target_bytes, field.target.len())?;
        if total_target_bytes > MAX_DIRECT_OUTPUT_TARGET_BYTES_TOTAL {
            return Err(format!(
                "direct output target names total size must be at most {} bytes",
                MAX_DIRECT_OUTPUT_TARGET_BYTES_TOTAL
            ));
        }
        total_tokens = checked_add_limit(total_tokens, field.tokens.len())?;
        if total_tokens > MAX_DIRECT_OUTPUT_TARGET_TOKENS_TOTAL {
            return Err(format!(
                "direct output target paths have too many tokens; maximum is {}",
                MAX_DIRECT_OUTPUT_TARGET_TOKENS_TOTAL
            ));
        }
        if !seen.insert(field.tokens.clone()) {
            return Err("direct output target is duplicated".to_string());
        }
    }
    for (index, field) in fields.iter().enumerate() {
        if fields.iter().enumerate().any(|(other_index, other)| {
            index != other_index
                && (is_path_prefix(&field.tokens, &other.tokens)
                    || is_path_prefix(&other.tokens, &field.tokens))
        }) {
            return Err("direct output target conflicts with another target".to_string());
        }
    }
    Ok(())
}

fn is_path_prefix(prefix: &[PathToken], tokens: &[PathToken]) -> bool {
    prefix.len() < tokens.len() && tokens.starts_with(prefix)
}

fn validate_direct_output_cells(
    spec: &DirectOutputSpec,
    options: &NormalizationOptions,
    output_cell_record_budget: usize,
) -> Result<(), DirectCliError> {
    if spec.output_mode() != DirectOutputMode::RecordObject {
        return Ok(());
    }
    let records = output_cell_record_budget.min(options.max_records);
    let cells = records
        .checked_mul(spec.output_field_count())
        .ok_or_else(|| DirectCliError::validation("direct output cells are too large"))?;
    if cells > MAX_DIRECT_OUTPUT_CELLS {
        return Err(DirectCliError::validation(format!(
            "direct output cells must be at most {}",
            MAX_DIRECT_OUTPUT_CELLS
        )));
    }
    Ok(())
}

fn validate_expr_limits(expr: &serde_json::Value) -> Result<(), String> {
    let mut stats = ExprStats::default();
    collect_expr_stats(expr, 0, &mut stats)?;
    if stats.max_depth > MAX_DIRECT_OUTPUT_EXPR_DEPTH {
        return Err(format!(
            "direct output expr exceeds configured depth limit ({})",
            MAX_DIRECT_OUTPUT_EXPR_DEPTH
        ));
    }
    if stats.nodes > MAX_DIRECT_OUTPUT_EXPR_NODES {
        return Err(format!(
            "direct output expr has too many nodes; maximum is {}",
            MAX_DIRECT_OUTPUT_EXPR_NODES
        ));
    }
    Ok(())
}

#[derive(Default)]
struct ExprStats {
    max_depth: usize,
    nodes: usize,
}

fn collect_expr_stats(
    value: &serde_json::Value,
    depth: usize,
    stats: &mut ExprStats,
) -> Result<(), String> {
    stats.nodes = checked_add_limit(stats.nodes, 1)?;
    stats.max_depth = stats.max_depth.max(depth);
    match value {
        serde_json::Value::String(text) if text.len() > MAX_DIRECT_OUTPUT_EXPR_STRING_BYTES => {
            Err(format!(
                "direct output expr string values must be at most {} bytes each",
                MAX_DIRECT_OUTPUT_EXPR_STRING_BYTES
            ))
        }
        serde_json::Value::Array(items) => {
            for item in items {
                collect_expr_stats(item, depth + 1, stats)?;
            }
            Ok(())
        }
        serde_json::Value::Object(object) => {
            for value in object.values() {
                collect_expr_stats(value, depth + 1, stats)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn checked_add_limit(left: usize, right: usize) -> Result<usize, String> {
    left.checked_add(right)
        .ok_or_else(|| "direct output spec is too large".to_string())
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

pub(super) fn expr_has_evaluated_root_ref(expr: &serde_json::Value, root: &str) -> bool {
    scan_expr_refs(expr, root)
}

fn scan_expr_refs(expr: &serde_json::Value, root: &str) -> bool {
    if let Some(expr) = parse_v1_expr_ref_shape(expr) {
        return scan_v1_expr_refs(&expr, root);
    }
    parse_v2_expr(expr)
        .ok()
        .is_some_and(|expr| scan_v2_expr_refs(&expr, root))
}

fn parse_v1_expr_ref_shape(expr: &serde_json::Value) -> Option<Expr> {
    match expr {
        serde_json::Value::Object(values)
            if values.contains_key("ref")
                || values.contains_key("op")
                || values.contains_key("chain") =>
        {
            serde_json::from_value(expr.clone()).ok()
        }
        _ => None,
    }
}

fn scan_v1_expr_refs(expr: &Expr, root: &str) -> bool {
    match expr {
        Expr::Ref(ExprRef { ref_path }) => is_v1_root_ref(ref_path, root),
        Expr::Op(ExprOp { args, .. }) => args.iter().any(|expr| scan_v1_expr_refs(expr, root)),
        Expr::Chain(ExprChain { chain }) => chain.iter().any(|expr| scan_v1_expr_refs(expr, root)),
        Expr::Literal(_) => false,
    }
}

fn scan_v2_expr_refs(expr: &V2Expr, root: &str) -> bool {
    match expr {
        V2Expr::Pipe(pipe) => scan_v2_pipe_refs(pipe, root),
        V2Expr::V1Fallback(expr) => scan_v1_expr_refs(expr, root),
    }
}

fn scan_v2_pipe_refs(pipe: &V2Pipe, root: &str) -> bool {
    scan_v2_start_refs(&pipe.start, root)
        || pipe.steps.iter().any(|step| scan_v2_step_refs(step, root))
}

fn scan_v2_start_refs(start: &V2Start, root: &str) -> bool {
    match start {
        V2Start::Ref(reference) => is_v2_root_ref(reference, root),
        V2Start::V1Expr(expr) => scan_v1_expr_refs(expr, root),
        V2Start::PipeValue | V2Start::ImplicitPipeValue | V2Start::Literal(_) => false,
    }
}

fn scan_v2_step_refs(step: &V2Step, root: &str) -> bool {
    match step {
        V2Step::Ref(reference) => is_v2_root_ref(reference, root),
        V2Step::Op(op) => op.args.iter().any(|expr| scan_v2_expr_refs(expr, root)),
        V2Step::CustomCall(call) => call.with.as_ref().is_some_and(|args| {
            args.iter().any(|(_, arg)| match arg {
                V2CallArg::Expr(expr) => scan_v2_expr_refs(expr, root),
                V2CallArg::Value(_) => false,
            })
        }),
        V2Step::Let(let_step) => let_step
            .bindings
            .iter()
            .any(|(_, expr)| scan_v2_expr_refs(expr, root)),
        V2Step::If(if_step) => {
            scan_v2_condition_refs(&if_step.cond, root)
                || scan_v2_pipe_refs(&if_step.then_branch, root)
                || if_step
                    .else_branch
                    .as_ref()
                    .is_some_and(|pipe| scan_v2_pipe_refs(pipe, root))
        }
        V2Step::Map(map_step) => map_step
            .steps
            .iter()
            .any(|step| scan_v2_step_refs(step, root)),
    }
}

fn scan_v2_condition_refs(condition: &V2Condition, root: &str) -> bool {
    match condition {
        V2Condition::All(conditions) | V2Condition::Any(conditions) => conditions
            .iter()
            .any(|condition| scan_v2_condition_refs(condition, root)),
        V2Condition::Comparison(V2Comparison { args, .. }) => {
            args.iter().any(|expr| scan_v2_expr_refs(expr, root))
        }
        V2Condition::Expr(expr) => scan_v2_expr_refs(expr, root),
    }
}

fn is_v2_root_ref(reference: &V2Ref, root: &str) -> bool {
    match (reference, root) {
        (V2Ref::Input(path), "input") => is_canonical_numeric_root_path(path),
        (V2Ref::Out(_), "out") => true,
        (V2Ref::Context(_), "context") => true,
        _ => false,
    }
}

fn is_v1_root_ref(value: &str, root: &str) -> bool {
    if value == root {
        return root != "input";
    }
    let prefix = format!("{}.", root);
    let Some(rest) = value.strip_prefix(&prefix) else {
        return false;
    };
    if root == "input" {
        return is_canonical_numeric_root_path(rest);
    }
    !rest.is_empty()
}

fn is_canonical_numeric_root_path(rest: &str) -> bool {
    let digit_count = rest
        .as_bytes()
        .iter()
        .take_while(|byte| byte.is_ascii_digit())
        .count();
    if digit_count == 0 {
        return false;
    }
    let digits = &rest[..digit_count];
    if digits.len() > 1 && digits.starts_with('0') {
        return false;
    }
    matches!(
        rest.as_bytes().get(digit_count),
        None | Some(b'.') | Some(b'[')
    )
}

fn strip_utf8_bom(input: &[u8]) -> &[u8] {
    input.strip_prefix(b"\xef\xbb\xbf").unwrap_or(input)
}
