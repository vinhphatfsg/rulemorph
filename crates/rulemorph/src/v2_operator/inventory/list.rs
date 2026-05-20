use super::definitions::{
    ITEM_ACC_ARG_0, ITEM_ACC_ARG_1, ITEM_ARG_0, ITEM_LAST_ARG, NO_SCOPE, range,
};
use crate::v2_operator::types::{V2OperatorMetadata, V2OperatorTrace};

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
