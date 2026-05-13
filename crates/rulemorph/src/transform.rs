use regex::Regex;
use serde_json::{Map, Value as JsonValue};
use std::cmp::Ordering;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use crate::cache::LruCache;
use crate::error::{TransformError, TransformErrorKind, TransformWarning};
use crate::model::{Expr, ExprChain, ExprOp, ExprRef, FinalizeSpec, Mapping, RuleFile, V2RuleStep};
use crate::path::{PathToken, get_path, parse_path};
use crate::trace::{
    TraceCollector, TraceEventKind, TracePhase, canonical_acc_path, canonical_context_path,
    canonical_input_path, canonical_item_path, canonical_out_path, canonical_output_path,
};
use crate::v2_eval::{
    EvalItem as V2EvalItem, EvalValue as V2EvalValue, V2EvalContext, eval_v2_condition,
    eval_v2_expr, eval_v2_let_step, eval_v2_op_step, eval_v2_pipe, eval_v2_ref, eval_v2_start,
};
use crate::v2_model::{V2ComparisonOp, V2Condition, V2Pipe, V2Ref, V2Start, V2Step};
use crate::v2_parser::{parse_v2_condition, parse_v2_expr, parse_v2_pipe_from_value};

const REGEX_CACHE_CAPACITY: usize = 128;
const BRANCH_MAX_DEPTH: usize = 64;

fn regex_cache() -> &'static Mutex<LruCache<String, Regex>> {
    static REGEX_CACHE: OnceLock<Mutex<LruCache<String, Regex>>> = OnceLock::new();
    REGEX_CACHE.get_or_init(|| Mutex::new(LruCache::new(REGEX_CACHE_CAPACITY)))
}

fn cached_regex(pattern: &str, path: &str) -> Result<Regex, TransformError> {
    let key = pattern.to_string();
    if let Some(regex) = {
        let mut cache = regex_cache().lock().unwrap_or_else(|err| err.into_inner());
        cache.get_cloned(&key)
    } {
        return Ok(regex);
    }

    let regex = Regex::new(pattern).map_err(|_| {
        TransformError::new(TransformErrorKind::ExprError, "regex pattern is invalid")
            .with_path(path)
    })?;
    {
        let mut cache = regex_cache().lock().unwrap_or_else(|err| err.into_inner());
        cache.insert(key, regex.clone());
    }
    Ok(regex)
}

mod api;
mod branch;
mod expr_json;
mod finalize;
mod operators;
mod path_ops;
mod record;
mod record_trace;
mod records;
mod stream;
mod types;
mod v1_expr;
mod v1_trace;
mod v2_trace;

use self::api::transform_record_with_warnings_inner;
use self::branch::{BranchContext, load_rule_from_path, merge_branch_output};
use self::expr_json::{expr_to_json_for_v2_condition, expr_to_json_for_v2_pipe, literal_string};
use self::finalize::{apply_finalize, apply_finalize_traced, sort_key_from_value};
pub(crate) use self::operators::eval_op;
use self::operators::{
    SortKey, arg_expr_at, args_len, cast_value, compare_sort_keys, locals_with_item,
    locals_with_precomputed_args, value_as_bool, value_to_string,
};
use self::path_ops::{
    flatten_object, has_duplicate_path, has_path_conflict, merge_object, parse_path_tokens,
    parse_ref, parse_source, remove_path, set_path, set_path_object_only, set_path_with_indexes,
};
use self::record::apply_rule_to_record;
use self::record_trace::apply_rule_to_record_traced;
use self::types::Namespace;
pub(crate) use self::types::{EvalItem, EvalLocals, EvalValue};
use self::v1_expr::{
    canonical_ref_path, eval_chain, eval_expr, eval_record_when, eval_record_when_traced, eval_ref,
    eval_when, eval_when_expr, eval_when_expr_traced, eval_when_traced, resolve_source,
};
use self::v1_trace::eval_expr_traced;
use self::v2_trace::{eval_v2_condition_traced, eval_v2_pipe_traced, sort_key_to_json};
pub use api::*;
pub use stream::*;

fn eval_mapping(
    mapping: &crate::model::Mapping,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    mapping_path: &str,
    version: u8,
) -> Result<Option<JsonValue>, TransformError> {
    let value = if let Some(source) = &mapping.source {
        resolve_source(source, record, context, out, mapping_path)?
    } else if let Some(literal) = &mapping.value {
        EvalValue::Value(literal.clone())
    } else if let Some(expr) = &mapping.expr {
        // Check if this is a v2 expression (version 2)
        if version >= 2 {
            let expr_path = format!("{}.expr", mapping_path);
            // Try to interpret as v2 pipe
            let v2_json = expr_to_json_for_v2_pipe(expr);
            if let Some(json_val) = v2_json {
                let v2_pipe = parse_v2_pipe_from_value(&json_val).map_err(|e| {
                    TransformError::new(TransformErrorKind::ExprError, e.to_string())
                        .with_path(&expr_path)
                })?;
                let v2_ctx = V2EvalContext::new();
                let v2_result = eval_v2_pipe(&v2_pipe, record, context, out, &expr_path, &v2_ctx)?;
                // Convert v2 EvalValue to v1 EvalValue
                match v2_result {
                    V2EvalValue::Missing => EvalValue::Missing,
                    V2EvalValue::Value(v) => EvalValue::Value(v),
                }
            } else {
                // v2 but not a v2 pipe - use v1 eval
                eval_expr(expr, record, context, out, &expr_path, None)?
            }
        } else {
            // v1 rule - use v1 eval
            eval_expr(
                expr,
                record,
                context,
                out,
                &format!("{}.expr", mapping_path),
                None,
            )?
        }
    } else {
        return Err(TransformError::new(
            TransformErrorKind::InvalidInput,
            "mapping must define source, value, or expr",
        )
        .with_path(mapping_path));
    };

    let mut value = match value {
        EvalValue::Missing => {
            if let Some(default) = &mapping.default {
                default.clone()
            } else if mapping.required {
                return Err(TransformError::new(
                    TransformErrorKind::MissingRequired,
                    "required value is missing",
                )
                .with_path(mapping_path));
            } else {
                return Ok(None);
            }
        }
        EvalValue::Value(value) => value,
    };

    if value.is_null() {
        if mapping.required {
            return Err(TransformError::new(
                TransformErrorKind::MissingRequired,
                "required value is null",
            )
            .with_path(mapping_path));
        }
        return Ok(Some(value));
    }

    if let Some(type_name) = &mapping.value_type {
        value = cast_value(&value, type_name, &format!("{}.type", mapping_path))?;
    }

    Ok(Some(value))
}

fn eval_mapping_traced(
    mapping: &crate::model::Mapping,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    mapping_path: &str,
    version: u8,
    collector: &mut TraceCollector,
) -> Result<Option<JsonValue>, TransformError> {
    let value = if let Some(source) = &mapping.source {
        let value = resolve_source(source, record, context, out, mapping_path)?;
        collector
            .emit(TraceEventKind::SourceRead, TracePhase::Instant)
            .rule_path(format!("{}.source", mapping_path))
            .input_path(canonical_source_path(source))
            .finish_with_eval_output(collector, &value, Some(source));
        value
    } else if let Some(literal) = &mapping.value {
        collector
            .emit(TraceEventKind::LiteralEval, TracePhase::Instant)
            .rule_path(format!("{}.value", mapping_path))
            .finish_with_output(collector, literal, None);
        EvalValue::Value(literal.clone())
    } else if let Some(expr) = &mapping.expr {
        if version >= 2 {
            let expr_path = format!("{}.expr", mapping_path);
            let v2_json = expr_to_json_for_v2_pipe(expr);
            if let Some(json_val) = v2_json {
                let v2_pipe = parse_v2_pipe_from_value(&json_val).map_err(|e| {
                    TransformError::new(TransformErrorKind::ExprError, e.to_string())
                        .with_path(&expr_path)
                })?;
                let v2_ctx = V2EvalContext::new();
                let v2_result = eval_v2_pipe_traced(
                    &v2_pipe, record, context, out, &expr_path, &v2_ctx, collector,
                )?;
                match v2_result {
                    V2EvalValue::Missing => EvalValue::Missing,
                    V2EvalValue::Value(v) => EvalValue::Value(v),
                }
            } else {
                eval_expr_traced(expr, record, context, out, &expr_path, None, collector)?
            }
        } else {
            eval_expr_traced(
                expr,
                record,
                context,
                out,
                &format!("{}.expr", mapping_path),
                None,
                collector,
            )?
        }
    } else {
        return Err(TransformError::new(
            TransformErrorKind::InvalidInput,
            "mapping must define source, value, or expr",
        )
        .with_path(mapping_path));
    };

    let mut value = match value {
        EvalValue::Missing => {
            if let Some(default) = &mapping.default {
                collector
                    .emit(TraceEventKind::DefaultApplied, TracePhase::Instant)
                    .rule_path(format!("{}.default", mapping_path))
                    .finish_with_output(collector, default, None);
                default.clone()
            } else if mapping.required {
                return Err(TransformError::new(
                    TransformErrorKind::MissingRequired,
                    "required value is missing",
                )
                .with_path(mapping_path));
            } else {
                return Ok(None);
            }
        }
        EvalValue::Value(value) => value,
    };

    if value.is_null() {
        if mapping.required {
            return Err(TransformError::new(
                TransformErrorKind::MissingRequired,
                "required value is null",
            )
            .with_path(mapping_path));
        }
        return Ok(Some(value));
    }

    if let Some(type_name) = &mapping.value_type {
        value = cast_value(&value, type_name, &format!("{}.type", mapping_path))?;
        collector
            .emit(TraceEventKind::TypeCast, TracePhase::Instant)
            .rule_path(format!("{}.type", mapping_path))
            .finish_with_output(collector, &value, None);
    }

    Ok(Some(value))
}

fn canonical_source_path(source: &str) -> String {
    match parse_source(source) {
        Ok((Namespace::Input, path)) => canonical_input_path(path),
        Ok((Namespace::Context, path)) => canonical_context_path(path),
        Ok((Namespace::Out, path)) => canonical_out_path(path),
        _ => canonical_input_path(source),
    }
}
