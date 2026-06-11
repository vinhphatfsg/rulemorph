use serde_json::{Map as JsonMap, Value as JsonValue};
use std::collections::BTreeSet;

use crate::error::ErrorCode;
use crate::model::{Expr, Mapping, RuleFile, V2RuleStep};
use crate::v2_model::{
    V2CallArg, V2Condition, V2Expr, V2ObjectFieldValue, V2OpStep, V2Pipe, V2Start, V2Step,
    object_field_rule_path,
};
use crate::v2_parser::{parse_v2_condition, parse_v2_expr};

use super::ValidationCtx;
use super::v2_expr::expr_to_json_value;

mod hints;
mod options;
mod path;
mod profiles;
mod refs;

use options::validate_codec_binding;
use refs::{
    validate_condition_expr_codec_refs, validate_expr_codec_refs,
    validate_finalize_wrap_codec_refs, validate_mapping_codec_refs, validate_step_codec_refs,
};

const MAX_TYPED_VALUE_HINTS: usize = 1024;
const MAX_TYPED_VALUE_HINT_PATH_BYTES: usize = 4096;
const MAX_TYPED_VALUE_HINT_PATH_TOKENS: usize = 256;
const MAX_TYPED_VALUE_CODECS: usize = 1024;
const MAX_TYPED_VALUE_CODEC_NAME_BYTES: usize = 256;

pub(super) fn validate_codecs(rule: &RuleFile, ctx: &mut ValidationCtx<'_>) {
    if !rule.codecs.is_empty() && rule.version != 2 {
        ctx.push(
            ErrorCode::InvalidStep,
            "codecs is only supported in version 2",
            "codecs",
        );
        return;
    }

    if rule.codecs.len() > MAX_TYPED_VALUE_CODECS {
        ctx.push(
            ErrorCode::InvalidExprShape,
            "typed value codec count exceeds configured limit",
            "codecs",
        );
    }
    for (name, codec) in &rule.codecs {
        if name.len() > MAX_TYPED_VALUE_CODEC_NAME_BYTES {
            ctx.push(
                ErrorCode::InvalidExprShape,
                "typed value codec name bytes exceed configured limit",
                format!("codecs.{}", name),
            );
        }
        validate_codec_binding(codec, &format!("codecs.{}", name), ctx);
    }
    for (name, def) in &rule.defs {
        let base = format!("defs.{}", name);
        if let Some(expr) = &def.expr {
            validate_expr_codec_refs(expr, &format!("{}.expr", base), rule.version, ctx);
        }
        if let Some(mappings) = &def.mappings {
            validate_mapping_codec_refs(mappings, &format!("{}.mappings", base), rule.version, ctx);
        }
    }
    validate_mapping_codec_refs(&rule.mappings, "mappings", rule.version, ctx);
    if let Some(steps) = &rule.steps {
        validate_step_codec_refs(steps, "steps", rule.version, ctx);
    }
    if let Some(expr) = &rule.record_when {
        validate_condition_expr_codec_refs(expr, "record_when", rule.version, ctx);
    }
    if let Some(finalize) = &rule.finalize
        && let Some(filter) = &finalize.filter
    {
        validate_condition_expr_codec_refs(filter, "finalize.filter", rule.version, ctx);
    }
    if let Some(finalize) = &rule.finalize
        && let Some(wrap) = &finalize.wrap
    {
        validate_finalize_wrap_codec_refs(wrap, "finalize.wrap", rule.version, ctx);
    }
}
