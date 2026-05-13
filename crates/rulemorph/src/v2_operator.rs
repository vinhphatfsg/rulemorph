#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct V2OperatorMetadata {
    pub(crate) name: &'static str,
    pub(crate) validates: bool,
    pub(crate) arg_range: V2OperatorArgRange,
    pub(crate) trace: V2OperatorTrace,
    pub(crate) skips_args_when_pipe_is_missing: bool,
    pub(crate) stops_after_missing_arg: bool,
    pub(crate) arg_scopes: &'static [V2OperatorArgScopeRule],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct V2OperatorArgRange {
    pub(crate) min: usize,
    pub(crate) max: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum V2OperatorTrace {
    EagerArgs,
    LazyShortCircuit,
    ItemLevelCollection,
    Delegated,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct V2OperatorArgScopeRule {
    selector: V2OperatorArgSelector,
    pub(crate) scope: V2OperatorArgScope,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum V2OperatorArgSelector {
    Exact(usize),
    Last,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum V2OperatorArgScope {
    Item,
    ItemAndAcc,
}

const fn range(min: usize, max: Option<usize>) -> V2OperatorArgRange {
    V2OperatorArgRange { min, max }
}

const NO_SCOPE: &[V2OperatorArgScopeRule] = &[];
const ITEM_ARG_0: &[V2OperatorArgScopeRule] = &[V2OperatorArgScopeRule {
    selector: V2OperatorArgSelector::Exact(0),
    scope: V2OperatorArgScope::Item,
}];
const ITEM_ACC_ARG_0: &[V2OperatorArgScopeRule] = &[V2OperatorArgScopeRule {
    selector: V2OperatorArgSelector::Exact(0),
    scope: V2OperatorArgScope::ItemAndAcc,
}];
const ITEM_ACC_ARG_1: &[V2OperatorArgScopeRule] = &[V2OperatorArgScopeRule {
    selector: V2OperatorArgSelector::Exact(1),
    scope: V2OperatorArgScope::ItemAndAcc,
}];
const ITEM_LAST_ARG: &[V2OperatorArgScopeRule] = &[V2OperatorArgScopeRule {
    selector: V2OperatorArgSelector::Last,
    scope: V2OperatorArgScope::Item,
}];

macro_rules! op {
    ($name:literal, $range:expr, $trace:ident, $skip_missing_pipe:expr, $stop_after_missing_arg:expr, $scopes:ident) => {
        V2OperatorMetadata {
            name: $name,
            validates: true,
            arg_range: $range,
            trace: V2OperatorTrace::$trace,
            skips_args_when_pipe_is_missing: $skip_missing_pipe,
            stops_after_missing_arg: $stop_after_missing_arg,
            arg_scopes: $scopes,
        }
    };
}

pub(crate) const V2_OPERATORS: &[V2OperatorMetadata] = &[
    // String operations
    op!("concat", range(1, None), EagerArgs, true, true, NO_SCOPE),
    op!(
        "to_string",
        range(0, Some(0)),
        EagerArgs,
        true,
        false,
        NO_SCOPE
    ),
    op!("trim", range(0, Some(0)), EagerArgs, true, false, NO_SCOPE),
    op!(
        "lowercase",
        range(0, Some(0)),
        EagerArgs,
        true,
        false,
        NO_SCOPE
    ),
    op!(
        "uppercase",
        range(0, Some(0)),
        EagerArgs,
        true,
        false,
        NO_SCOPE
    ),
    op!(
        "replace",
        range(2, Some(3)),
        EagerArgs,
        true,
        true,
        NO_SCOPE
    ),
    op!("split", range(1, Some(1)), EagerArgs, true, true, NO_SCOPE),
    op!(
        "pad_start",
        range(1, Some(2)),
        EagerArgs,
        true,
        true,
        NO_SCOPE
    ),
    op!(
        "pad_end",
        range(1, Some(2)),
        EagerArgs,
        true,
        true,
        NO_SCOPE
    ),
    // Null handling
    op!(
        "coalesce",
        range(1, None),
        LazyShortCircuit,
        false,
        false,
        NO_SCOPE
    ),
    // Lookup
    op!(
        "lookup",
        range(2, Some(4)),
        Delegated,
        false,
        false,
        NO_SCOPE
    ),
    op!(
        "lookup_first",
        range(2, Some(4)),
        Delegated,
        false,
        false,
        NO_SCOPE
    ),
    // Arithmetic
    op!("+", range(1, None), EagerArgs, true, true, NO_SCOPE),
    op!("-", range(1, None), EagerArgs, true, true, NO_SCOPE),
    op!("*", range(1, None), EagerArgs, true, true, NO_SCOPE),
    op!("/", range(1, None), EagerArgs, true, true, NO_SCOPE),
    op!("multiply", range(1, None), EagerArgs, true, true, NO_SCOPE),
    op!("add", range(1, None), EagerArgs, true, true, NO_SCOPE),
    op!("subtract", range(1, None), EagerArgs, true, true, NO_SCOPE),
    op!("divide", range(1, None), EagerArgs, true, true, NO_SCOPE),
    op!("round", range(0, Some(1)), EagerArgs, true, true, NO_SCOPE),
    op!(
        "to_base",
        range(1, Some(1)),
        EagerArgs,
        true,
        true,
        NO_SCOPE
    ),
    // Date
    op!(
        "date_format",
        range(1, Some(3)),
        EagerArgs,
        true,
        true,
        NO_SCOPE
    ),
    op!(
        "to_unixtime",
        range(0, Some(2)),
        EagerArgs,
        true,
        true,
        NO_SCOPE
    ),
    // Logical
    op!(
        "and",
        range(1, None),
        LazyShortCircuit,
        false,
        false,
        NO_SCOPE
    ),
    op!(
        "or",
        range(1, None),
        LazyShortCircuit,
        false,
        false,
        NO_SCOPE
    ),
    op!("not", range(0, Some(0)), EagerArgs, true, false, NO_SCOPE),
    // Comparison
    op!("==", range(1, Some(1)), EagerArgs, false, false, NO_SCOPE),
    op!("!=", range(1, Some(1)), EagerArgs, false, false, NO_SCOPE),
    op!("<", range(1, Some(1)), EagerArgs, false, false, NO_SCOPE),
    op!("<=", range(1, Some(1)), EagerArgs, false, false, NO_SCOPE),
    op!(">", range(1, Some(1)), EagerArgs, false, false, NO_SCOPE),
    op!(">=", range(1, Some(1)), EagerArgs, false, false, NO_SCOPE),
    op!("~=", range(1, Some(1)), EagerArgs, false, false, NO_SCOPE),
    op!("eq", range(1, Some(1)), EagerArgs, false, false, NO_SCOPE),
    op!("ne", range(1, Some(1)), EagerArgs, false, false, NO_SCOPE),
    op!("lt", range(1, Some(1)), EagerArgs, false, false, NO_SCOPE),
    op!("lte", range(1, Some(1)), EagerArgs, false, false, NO_SCOPE),
    op!("gt", range(1, Some(1)), EagerArgs, false, false, NO_SCOPE),
    op!("gte", range(1, Some(1)), EagerArgs, false, false, NO_SCOPE),
    op!(
        "match",
        range(1, Some(1)),
        EagerArgs,
        false,
        false,
        NO_SCOPE
    ),
    // JSON
    op!("merge", range(1, None), EagerArgs, true, true, NO_SCOPE),
    op!(
        "deep_merge",
        range(1, None),
        EagerArgs,
        true,
        true,
        NO_SCOPE
    ),
    op!("get", range(1, Some(1)), EagerArgs, true, true, NO_SCOPE),
    op!("pick", range(1, None), EagerArgs, false, true, NO_SCOPE),
    op!("omit", range(1, None), EagerArgs, false, true, NO_SCOPE),
    op!("keys", range(0, Some(0)), EagerArgs, true, false, NO_SCOPE),
    op!(
        "values",
        range(0, Some(0)),
        EagerArgs,
        true,
        false,
        NO_SCOPE
    ),
    op!(
        "entries",
        range(0, Some(0)),
        EagerArgs,
        true,
        false,
        NO_SCOPE
    ),
    op!("len", range(0, Some(0)), EagerArgs, true, false, NO_SCOPE),
    op!(
        "from_entries",
        range(1, None),
        EagerArgs,
        true,
        false,
        NO_SCOPE
    ),
    op!(
        "object_flatten",
        range(1, Some(1)),
        EagerArgs,
        true,
        false,
        NO_SCOPE
    ),
    op!(
        "object_unflatten",
        range(1, Some(1)),
        EagerArgs,
        true,
        false,
        NO_SCOPE
    ),
    // Array
    op!(
        "map",
        range(1, Some(1)),
        ItemLevelCollection,
        false,
        false,
        ITEM_ARG_0
    ),
    op!(
        "filter",
        range(1, Some(1)),
        ItemLevelCollection,
        false,
        false,
        ITEM_ARG_0
    ),
    op!(
        "flat_map",
        range(1, Some(1)),
        ItemLevelCollection,
        false,
        false,
        ITEM_ARG_0
    ),
    op!(
        "flatten",
        range(0, Some(1)),
        EagerArgs,
        true,
        true,
        NO_SCOPE
    ),
    op!("take", range(1, Some(1)), EagerArgs, true, true, NO_SCOPE),
    op!("drop", range(1, Some(1)), EagerArgs, true, true, NO_SCOPE),
    op!("slice", range(1, Some(2)), EagerArgs, true, true, NO_SCOPE),
    op!("chunk", range(1, Some(1)), EagerArgs, true, true, NO_SCOPE),
    op!("zip", range(1, None), EagerArgs, true, true, NO_SCOPE),
    op!(
        "zip_with",
        range(2, None),
        Delegated,
        false,
        false,
        ITEM_LAST_ARG
    ),
    op!("unzip", range(0, Some(0)), EagerArgs, true, false, NO_SCOPE),
    op!(
        "group_by",
        range(1, Some(1)),
        ItemLevelCollection,
        false,
        false,
        ITEM_ARG_0
    ),
    op!(
        "key_by",
        range(1, Some(1)),
        ItemLevelCollection,
        false,
        false,
        ITEM_ARG_0
    ),
    op!(
        "partition",
        range(1, Some(1)),
        ItemLevelCollection,
        false,
        false,
        ITEM_ARG_0
    ),
    op!(
        "unique",
        range(0, Some(0)),
        EagerArgs,
        true,
        false,
        NO_SCOPE
    ),
    op!(
        "distinct_by",
        range(1, Some(1)),
        ItemLevelCollection,
        false,
        false,
        ITEM_ARG_0
    ),
    op!(
        "sort_by",
        range(1, Some(2)),
        ItemLevelCollection,
        false,
        false,
        ITEM_ARG_0
    ),
    op!(
        "find",
        range(1, Some(1)),
        ItemLevelCollection,
        false,
        false,
        ITEM_ARG_0
    ),
    op!(
        "find_index",
        range(1, Some(1)),
        ItemLevelCollection,
        false,
        false,
        ITEM_ARG_0
    ),
    op!(
        "index_of",
        range(1, Some(1)),
        EagerArgs,
        true,
        true,
        NO_SCOPE
    ),
    op!(
        "contains",
        range(1, Some(1)),
        EagerArgs,
        true,
        true,
        NO_SCOPE
    ),
    op!("sum", range(0, Some(0)), EagerArgs, true, false, NO_SCOPE),
    op!("avg", range(0, Some(0)), EagerArgs, true, false, NO_SCOPE),
    op!("min", range(0, Some(0)), EagerArgs, true, false, NO_SCOPE),
    op!("max", range(0, Some(0)), EagerArgs, true, false, NO_SCOPE),
    op!(
        "reduce",
        range(1, Some(1)),
        ItemLevelCollection,
        false,
        false,
        ITEM_ACC_ARG_0
    ),
    op!(
        "fold",
        range(2, Some(2)),
        ItemLevelCollection,
        false,
        false,
        ITEM_ACC_ARG_1
    ),
    op!("first", range(0, Some(0)), EagerArgs, true, false, NO_SCOPE),
    op!("last", range(0, Some(0)), EagerArgs, true, false, NO_SCOPE),
    // Type casts
    op!(
        "string",
        range(0, Some(0)),
        EagerArgs,
        true,
        false,
        NO_SCOPE
    ),
    op!("int", range(0, Some(0)), EagerArgs, true, false, NO_SCOPE),
    op!("float", range(0, Some(0)), EagerArgs, true, false, NO_SCOPE),
    op!("bool", range(0, Some(0)), EagerArgs, true, false, NO_SCOPE),
];

pub(crate) fn operator(name: &str) -> Option<&'static V2OperatorMetadata> {
    V2_OPERATORS.iter().find(|metadata| metadata.name == name)
}

pub(crate) fn is_valid_operator(name: &str) -> bool {
    operator(name).is_some_and(|metadata| metadata.validates)
}

pub(crate) fn operator_arg_range(name: &str) -> (usize, Option<usize>) {
    operator(name)
        .map(|metadata| (metadata.arg_range.min, metadata.arg_range.max))
        .unwrap_or((0, None))
}

pub(crate) fn operator_arg_scope(
    name: &str,
    arg_index: usize,
    arg_count: usize,
) -> Option<V2OperatorArgScope> {
    operator(name).and_then(|metadata| {
        metadata
            .arg_scopes
            .iter()
            .find(|rule| match rule.selector {
                V2OperatorArgSelector::Exact(index) => arg_index == index,
                V2OperatorArgSelector::Last => arg_index + 1 == arg_count,
            })
            .map(|rule| rule.scope)
    })
}

#[cfg(test)]
pub(crate) fn operator_has_eager_args(name: &str) -> bool {
    operator(name)
        .map(|metadata| metadata.trace == V2OperatorTrace::EagerArgs)
        .unwrap_or(true)
}

#[cfg(test)]
pub(crate) fn operator_has_item_level_trace(name: &str) -> bool {
    operator(name).is_some_and(|metadata| metadata.trace == V2OperatorTrace::ItemLevelCollection)
}

#[cfg(test)]
pub(crate) fn operator_has_lazy_arg_trace(name: &str) -> bool {
    operator(name).is_some_and(|metadata| metadata.trace == V2OperatorTrace::LazyShortCircuit)
}
