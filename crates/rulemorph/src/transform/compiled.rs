use std::cell::OnceCell;
use std::collections::HashMap;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use crate::error::{TransformError, TransformErrorKind};
use crate::expr_json::expr_to_json_for_v2_pipe;
use crate::model::{Expr, Mapping, RuleFile};
use crate::path::{PathToken, get_path, parse_path};
use crate::v2_model::V2Pipe;
use crate::v2_parser::parse_v2_pipe_from_value;

use super::value_to_string_optional;

const LOOKUP_INDEX_MAX_ITEMS: usize = 50_000;
const LOOKUP_INDEX_MAX_CACHED_PER_RULE: usize = 8;
const LOOKUP_INDEX_MAX_KEY_BYTES: usize = 256 * 1024;
const LOOKUP_INDEX_MAX_CLONED_VALUE_BYTES: usize = 256 * 1024;
const LOOKUP_INDEX_MAX_RETAINED_BYTES_PER_RULE: usize = 2 * 1024 * 1024;

pub(crate) struct CompiledRule {
    mappings: Vec<CompiledMapping>,
}

impl CompiledRule {
    pub(super) fn new(rule: &RuleFile) -> Self {
        let lookup_index_budget = Arc::new(LookupIndexBudget::new());
        Self {
            mappings: rule
                .mappings
                .iter()
                .enumerate()
                .map(|(index, _)| CompiledMapping::new(index, Arc::clone(&lookup_index_budget)))
                .collect(),
        }
    }

    pub(super) fn mapping(&self, index: usize) -> Option<&CompiledMapping> {
        self.mappings.get(index)
    }
}

pub(crate) struct CompiledMapping {
    mapping_path: String,
    expr_path: String,
    target_path: String,
    lookup_index_budget: Arc<LookupIndexBudget>,
    target_tokens: OnceCell<Result<Vec<PathToken>, TransformError>>,
    v2_pipe: OnceCell<Option<Result<V2Pipe, TransformError>>>,
    v1_lookup: OnceCell<Option<CompiledLookup>>,
}

impl CompiledMapping {
    fn new(index: usize, lookup_index_budget: Arc<LookupIndexBudget>) -> Self {
        let mapping_path = format!("mappings[{index}]");
        Self {
            expr_path: format!("{mapping_path}.expr"),
            target_path: format!("{mapping_path}.target"),
            mapping_path,
            lookup_index_budget,
            target_tokens: OnceCell::new(),
            v2_pipe: OnceCell::new(),
            v1_lookup: OnceCell::new(),
        }
    }

    pub(super) fn mapping_path(&self) -> &str {
        &self.mapping_path
    }

    pub(super) fn expr_path(&self) -> &str {
        &self.expr_path
    }

    pub(super) fn target_tokens(&self, mapping: &Mapping) -> Result<&[PathToken], TransformError> {
        self.target_tokens
            .get_or_init(|| compile_target_tokens(mapping, &self.target_path))
            .as_deref()
            .map_err(Clone::clone)
    }

    pub(super) fn v2_pipe(
        &self,
        mapping: &Mapping,
        version: u8,
    ) -> Option<Result<&V2Pipe, TransformError>> {
        self.v2_pipe
            .get_or_init(|| compile_v2_pipe(mapping, &self.expr_path, version))
            .as_ref()
            .map(|result| result.as_ref().map_err(Clone::clone))
    }

    pub(super) fn v1_lookup(&self, mapping: &Mapping) -> Option<&CompiledLookup> {
        self.v1_lookup
            .get_or_init(|| {
                compile_v1_lookup(
                    mapping,
                    &self.expr_path,
                    Arc::clone(&self.lookup_index_budget),
                )
            })
            .as_ref()
    }
}

pub(crate) struct CompiledLookup {
    key_arg_path: String,
    output_arg_path: String,
    key_tokens: OnceCell<Result<Vec<PathToken>, TransformError>>,
    output_tokens: OnceCell<Option<Result<Vec<PathToken>, TransformError>>>,
    context_collection_tokens: OnceCell<Option<Vec<PathToken>>>,
    index: OnceCell<Option<LookupIndex>>,
    lookup_index_budget: Arc<LookupIndexBudget>,
}

impl CompiledLookup {
    pub(super) fn key_tokens(&self, args: &[Expr]) -> Result<&[PathToken], TransformError> {
        self.key_tokens
            .get_or_init(|| compile_lookup_key_tokens(args, &self.key_arg_path))
            .as_deref()
            .map_err(Clone::clone)
    }

    pub(super) fn output_tokens(
        &self,
        args: &[Expr],
    ) -> Option<Result<&[PathToken], TransformError>> {
        self.output_tokens
            .get_or_init(|| compile_lookup_output_tokens(args, &self.output_arg_path))
            .as_ref()
            .map(|result| result.as_deref().map_err(Clone::clone))
    }

    pub(super) fn index(
        &self,
        args: &[Expr],
        collection_array: &[serde_json::Value],
        key_tokens: &[PathToken],
        output_tokens: Option<&[PathToken]>,
    ) -> Option<&LookupIndex> {
        if collection_array.len() > LOOKUP_INDEX_MAX_ITEMS {
            return None;
        }
        if !self.lookup_index_budget.can_build_index() {
            return None;
        }
        self.context_collection_tokens
            .get_or_init(|| compile_lookup_context_collection_tokens(args))
            .as_ref()?;
        self.index
            .get_or_init(|| {
                if !self.lookup_index_budget.try_reserve_build_slot() {
                    return None;
                }
                let build = LookupIndex::build(collection_array, key_tokens, output_tokens)?;
                if self
                    .lookup_index_budget
                    .try_reserve_bytes(build.retained_bytes)
                {
                    Some(build.index)
                } else {
                    None
                }
            })
            .as_ref()
    }

    pub(super) fn context_collection_tokens(&self, args: &[Expr]) -> Option<&[PathToken]> {
        self.context_collection_tokens
            .get_or_init(|| compile_lookup_context_collection_tokens(args))
            .as_deref()
    }
}

struct LookupIndexBudget {
    build_slots: AtomicUsize,
    retained_bytes: AtomicUsize,
}

impl LookupIndexBudget {
    fn new() -> Self {
        Self {
            build_slots: AtomicUsize::new(0),
            retained_bytes: AtomicUsize::new(0),
        }
    }

    fn can_build_index(&self) -> bool {
        self.build_slots.load(Ordering::Relaxed) < LOOKUP_INDEX_MAX_CACHED_PER_RULE
            && self.retained_bytes.load(Ordering::Relaxed)
                < LOOKUP_INDEX_MAX_RETAINED_BYTES_PER_RULE
    }

    fn try_reserve_build_slot(&self) -> bool {
        let mut build_slots = self.build_slots.load(Ordering::Relaxed);
        loop {
            if build_slots >= LOOKUP_INDEX_MAX_CACHED_PER_RULE {
                return false;
            }
            match self.build_slots.compare_exchange_weak(
                build_slots,
                build_slots + 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(current) => build_slots = current,
            }
        }
    }

    fn try_reserve_bytes(&self, retained_bytes: usize) -> bool {
        let mut current_bytes = self.retained_bytes.load(Ordering::Relaxed);
        loop {
            let Some(next_bytes) = current_bytes.checked_add(retained_bytes) else {
                return false;
            };
            if next_bytes > LOOKUP_INDEX_MAX_RETAINED_BYTES_PER_RULE {
                return false;
            }
            match self.retained_bytes.compare_exchange_weak(
                current_bytes,
                next_bytes,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(current) => current_bytes = current,
            }
        }
    }
}

pub(crate) enum LookupIndex {
    ClonedValues {
        values_by_key: HashMap<String, Vec<serde_json::Value>>,
    },
    ItemIndices {
        indices_by_key: HashMap<String, Vec<usize>>,
    },
}

pub(crate) enum LookupMatches<'a> {
    ClonedValues(&'a [serde_json::Value]),
    ItemIndices(&'a [usize]),
}

struct LookupIndexBuild {
    index: LookupIndex,
    retained_bytes: usize,
}

impl LookupIndex {
    fn build(
        collection_array: &[serde_json::Value],
        key_tokens: &[PathToken],
        output_tokens: Option<&[PathToken]>,
    ) -> Option<LookupIndexBuild> {
        let mut values_by_key: HashMap<String, Vec<serde_json::Value>> = HashMap::new();
        let mut key_bytes = 0usize;
        let mut cloned_value_bytes = 0usize;
        let mut entries = 0usize;
        for item in collection_array {
            let Some((key_value, value)) = lookup_index_item(item, key_tokens, output_tokens)
            else {
                continue;
            };
            let Some(item_key) = lookup_index_key(key_value) else {
                continue;
            };
            let item_key_bytes = item_key.len();
            if key_bytes.saturating_add(item_key_bytes) > LOOKUP_INDEX_MAX_KEY_BYTES {
                return None;
            }
            let value_bytes = lookup_index_value_size_bytes(value);
            if cloned_value_bytes.saturating_add(value_bytes) > LOOKUP_INDEX_MAX_CLONED_VALUE_BYTES
            {
                return Self::build_item_indices(collection_array, key_tokens, output_tokens);
            }
            key_bytes = key_bytes.saturating_add(item_key_bytes);
            cloned_value_bytes = cloned_value_bytes.saturating_add(value_bytes);
            entries = entries.saturating_add(1);
            values_by_key
                .entry(item_key.into_owned())
                .or_default()
                .push(value.clone());
        }
        let retained_bytes = lookup_index_retained_bytes(
            key_bytes,
            cloned_value_bytes,
            entries,
            values_by_key.len(),
        );
        Some(LookupIndexBuild {
            index: Self::ClonedValues { values_by_key },
            retained_bytes,
        })
    }

    fn build_item_indices(
        collection_array: &[serde_json::Value],
        key_tokens: &[PathToken],
        output_tokens: Option<&[PathToken]>,
    ) -> Option<LookupIndexBuild> {
        let mut indices_by_key: HashMap<String, Vec<usize>> = HashMap::new();
        let mut key_bytes = 0usize;
        let mut entries = 0usize;
        for (index, item) in collection_array.iter().enumerate() {
            let Some((key_value, _)) = lookup_index_item(item, key_tokens, output_tokens) else {
                continue;
            };
            let Some(item_key) = lookup_index_key(key_value) else {
                continue;
            };
            let item_key_bytes = item_key.len();
            if key_bytes.saturating_add(item_key_bytes) > LOOKUP_INDEX_MAX_KEY_BYTES {
                return None;
            }
            key_bytes = key_bytes.saturating_add(item_key_bytes);
            entries = entries.saturating_add(1);
            indices_by_key
                .entry(item_key.into_owned())
                .or_default()
                .push(index);
        }
        let retained_bytes =
            lookup_index_retained_bytes(key_bytes, 0, entries, indices_by_key.len());
        Some(LookupIndexBuild {
            index: Self::ItemIndices { indices_by_key },
            retained_bytes,
        })
    }

    pub(super) fn get(&self, key: &str) -> Option<LookupMatches<'_>> {
        match self {
            Self::ClonedValues { values_by_key } => values_by_key
                .get(key)
                .map(Vec::as_slice)
                .map(LookupMatches::ClonedValues),
            Self::ItemIndices { indices_by_key } => indices_by_key
                .get(key)
                .map(Vec::as_slice)
                .map(LookupMatches::ItemIndices),
        }
    }
}

fn lookup_index_retained_bytes(
    key_bytes: usize,
    cloned_value_bytes: usize,
    entries: usize,
    unique_keys: usize,
) -> usize {
    key_bytes
        .saturating_add(cloned_value_bytes)
        .saturating_add(entries.saturating_mul(std::mem::size_of::<usize>().saturating_mul(2)))
        .saturating_add(unique_keys.saturating_mul(64))
}

fn lookup_index_item<'a>(
    item: &'a serde_json::Value,
    key_tokens: &[PathToken],
    output_tokens: Option<&[PathToken]>,
) -> Option<(&'a serde_json::Value, &'a serde_json::Value)> {
    let key_value = get_path(item, key_tokens)?;
    let value = match output_tokens {
        Some(tokens) => get_path(item, tokens)?,
        None => item,
    };
    Some((key_value, value))
}

enum LookupIndexKey<'a> {
    Borrowed(&'a str),
    Owned(String),
}

impl LookupIndexKey<'_> {
    fn len(&self) -> usize {
        match self {
            Self::Borrowed(value) => value.len(),
            Self::Owned(value) => value.len(),
        }
    }

    fn into_owned(self) -> String {
        match self {
            Self::Borrowed(value) => value.to_string(),
            Self::Owned(value) => value,
        }
    }
}

fn lookup_index_key(value: &serde_json::Value) -> Option<LookupIndexKey<'_>> {
    match value {
        serde_json::Value::String(value) => Some(LookupIndexKey::Borrowed(value.as_str())),
        serde_json::Value::Number(_) => value_to_string_optional(value).map(LookupIndexKey::Owned),
        serde_json::Value::Bool(false) => Some(LookupIndexKey::Borrowed("false")),
        serde_json::Value::Bool(true) => Some(LookupIndexKey::Borrowed("true")),
        _ => None,
    }
}

fn lookup_index_value_size_bytes(value: &serde_json::Value) -> usize {
    match value {
        serde_json::Value::Null => 4,
        serde_json::Value::Bool(false) => 5,
        serde_json::Value::Bool(true) => 4,
        serde_json::Value::Number(number) => number.to_string().len(),
        serde_json::Value::String(value) => value.len().saturating_add(2),
        serde_json::Value::Array(items) => items.iter().fold(2usize, |size, item| {
            size.saturating_add(lookup_index_value_size_bytes(item))
                .saturating_add(1)
        }),
        serde_json::Value::Object(object) => object.iter().fold(2usize, |size, (key, value)| {
            size.saturating_add(key.len())
                .saturating_add(3)
                .saturating_add(lookup_index_value_size_bytes(value))
        }),
    }
}

fn compile_target_tokens(
    mapping: &Mapping,
    target_path: &str,
) -> Result<Vec<PathToken>, TransformError> {
    let tokens = parse_path(&mapping.target).map_err(|err| {
        TransformError::new(TransformErrorKind::InvalidTarget, err.message()).with_path(target_path)
    })?;
    if tokens.is_empty() {
        return Err(TransformError::new(
            TransformErrorKind::InvalidTarget,
            "target path is invalid",
        )
        .with_path(target_path));
    }
    if tokens
        .iter()
        .any(|token| matches!(token, PathToken::Index(_)))
    {
        return Err(TransformError::new(
            TransformErrorKind::InvalidTarget,
            "target path must not include indexes",
        )
        .with_path(target_path));
    }
    Ok(tokens)
}

fn compile_v2_pipe(
    mapping: &Mapping,
    expr_path: &str,
    version: u8,
) -> Option<Result<V2Pipe, TransformError>> {
    if version < 2 {
        return None;
    }
    let expr = mapping.expr.as_ref()?;
    let value = expr_to_json_for_v2_pipe(expr)?;
    Some(parse_v2_pipe_from_value(&value).map_err(|err| {
        TransformError::new(TransformErrorKind::ExprError, err.to_string()).with_path(expr_path)
    }))
}

fn compile_v1_lookup(
    mapping: &Mapping,
    expr_path: &str,
    lookup_index_budget: Arc<LookupIndexBudget>,
) -> Option<CompiledLookup> {
    let Some(Expr::Op(expr_op)) = mapping.expr.as_ref() else {
        return None;
    };
    if !matches!(expr_op.op.as_str(), "lookup" | "lookup_first") {
        return None;
    }
    Some(CompiledLookup {
        key_arg_path: format!("{expr_path}.args[1]"),
        output_arg_path: format!("{expr_path}.args[3]"),
        key_tokens: OnceCell::new(),
        output_tokens: OnceCell::new(),
        context_collection_tokens: OnceCell::new(),
        index: OnceCell::new(),
        lookup_index_budget,
    })
}

fn compile_lookup_context_collection_tokens(args: &[Expr]) -> Option<Vec<PathToken>> {
    let Expr::Ref(expr_ref) = args.first()? else {
        return None;
    };
    let path = expr_ref.ref_path.strip_prefix("context.")?;
    parse_path(path).ok()
}

fn compile_lookup_key_tokens(
    args: &[Expr],
    key_arg_path: &str,
) -> Result<Vec<PathToken>, TransformError> {
    let key_expr = args.get(1).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "lookup key_path must be a non-empty string literal",
        )
        .with_path(key_arg_path)
    })?;
    let key_path = lookup_literal_string(key_expr).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "lookup key_path must be a non-empty string literal",
        )
        .with_path(key_arg_path)
    })?;
    if key_path.is_empty() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "lookup key_path must be a non-empty string literal",
        )
        .with_path(key_arg_path));
    }
    parse_path(key_path).map_err(|_| {
        TransformError::new(TransformErrorKind::ExprError, "lookup key_path is invalid")
            .with_path(key_arg_path)
    })
}

fn compile_lookup_output_tokens(
    args: &[Expr],
    output_arg_path: &str,
) -> Option<Result<Vec<PathToken>, TransformError>> {
    let output_expr = args.get(3)?;
    Some(compile_lookup_output_tokens_inner(
        output_expr,
        output_arg_path,
    ))
}

fn compile_lookup_output_tokens_inner(
    output_expr: &Expr,
    output_arg_path: &str,
) -> Result<Vec<PathToken>, TransformError> {
    let value = lookup_literal_string(output_expr).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "lookup output_path must be a non-empty string literal",
        )
        .with_path(output_arg_path)
    })?;
    if value.is_empty() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "lookup output_path must be a non-empty string literal",
        )
        .with_path(output_arg_path));
    }
    parse_path(value).map_err(|_| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "lookup output_path is invalid",
        )
        .with_path(output_arg_path)
    })
}

fn lookup_literal_string(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Literal(value) => value.as_str(),
        _ => None,
    }
}
