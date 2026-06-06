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
    eval_v2_expr, eval_v2_let_step, eval_v2_op_step, eval_v2_ref, eval_v2_start,
};
use crate::v2_model::{V2ComparisonOp, V2Condition, V2Pipe, V2Ref, V2Start, V2Step};
use crate::v2_parser::{parse_v2_condition, parse_v2_expr};

const REGEX_CACHE_CAPACITY: usize = 128;
pub(crate) const BRANCH_MAX_DEPTH: usize = 64;

fn regex_cache() -> &'static Mutex<LruCache<String, Regex>> {
    static REGEX_CACHE: OnceLock<Mutex<LruCache<String, Regex>>> = OnceLock::new();
    REGEX_CACHE.get_or_init(|| Mutex::new(LruCache::new(REGEX_CACHE_CAPACITY)))
}

fn cached_regex(pattern: &str, path: &str) -> Result<Regex, TransformError> {
    if let Some(regex) = {
        let mut cache = regex_cache().lock().unwrap_or_else(|err| err.into_inner());
        cache.get_cloned_by(pattern)
    } {
        return Ok(regex);
    }

    let regex = Regex::new(pattern).map_err(|_| {
        TransformError::new(TransformErrorKind::ExprError, "regex pattern is invalid")
            .with_path(path)
    })?;
    {
        let mut cache = regex_cache().lock().unwrap_or_else(|err| err.into_inner());
        cache.insert(pattern.to_string(), regex.clone());
    }
    Ok(regex)
}

mod api;
mod branch;
mod compiled;
mod custom_ops;
mod finalize;
mod mapping;
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
pub(crate) use self::compiled::{CompiledLookup, CompiledMapping, CompiledRule, LookupMatches};
pub(crate) use self::custom_ops::{
    eval_custom_call_step, eval_custom_call_step_traced, eval_custom_op_step,
    eval_custom_op_step_traced, parse_known_custom_call_literal_start,
};
use self::finalize::{apply_finalize, apply_finalize_traced, sort_key_from_value};
use self::mapping::{
    eval_mapping_traced, eval_mapping_traced_with_source_redaction_hint,
    eval_mapping_with_v2_context,
};
pub(crate) use self::operators::eval_op;
use self::operators::{
    SortKey, arg_expr_at, args_len, cast_value, compare_sort_keys, eval_lookup, locals_with_item,
    locals_with_precomputed_args, value_as_bool, value_to_string, value_to_string_optional,
};
use self::path_ops::{
    flatten_object, has_duplicate_path, has_path_conflict, merge_object, parse_path_tokens,
    parse_ref, parse_source, remove_path, set_path, set_path_object_only, set_path_tokens,
    set_path_with_indexes,
};
use self::record::apply_rule_to_record;
use self::record_trace::apply_rule_to_record_traced;
use self::types::Namespace;
pub(crate) use self::types::{
    EvalItem, EvalLimits, EvalLocals, EvalValue, GeneratedObjectBudget,
    extend_generated_array_items, push_generated_array_item,
};
use self::v1_expr::{
    canonical_ref_path, eval_chain, eval_expr, eval_record_when, eval_record_when_traced, eval_ref,
    eval_when, eval_when_expr_traced_with_v2_context, eval_when_expr_with_v2_context,
    eval_when_traced, resolve_source,
};
use self::v1_trace::eval_expr_traced;
use self::v2_trace::{
    eval_v2_condition_traced, eval_v2_expr_traced, eval_v2_pipe_traced, sort_key_to_json,
};
use crate::expr_json::{expr_to_json_for_v2_condition, expr_to_json_for_v2_pipe, literal_string};
pub use api::*;
pub use stream::*;
