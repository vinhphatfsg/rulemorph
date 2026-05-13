use super::scope::LocalScope;

pub(super) fn element_expr_scope(
    op: &str,
    injected: bool,
    args_len: usize,
    parent_scope: LocalScope,
) -> Option<(usize, LocalScope)> {
    let item_scope = if parent_scope.allows_acc() {
        LocalScope::ItemAcc
    } else {
        LocalScope::Item
    };
    match op {
        "map" | "filter" | "flat_map" | "group_by" | "key_by" | "partition" | "distinct_by"
        | "sort_by" | "find" | "find_index" => {
            let index = if injected { 0 } else { 1 };
            Some((index, item_scope))
        }
        "zip_with" => args_len.checked_sub(1).map(|index| (index, item_scope)),
        "reduce" => {
            let index = if injected { 0 } else { 1 };
            Some((index, LocalScope::ItemAcc))
        }
        "fold" => {
            let index = if injected { 1 } else { 2 };
            Some((index, LocalScope::ItemAcc))
        }
        _ => None,
    }
}

pub(super) fn is_valid_op(value: &str) -> bool {
    matches!(
        value,
        "concat"
            | "coalesce"
            | "to_string"
            | "trim"
            | "lowercase"
            | "uppercase"
            | "replace"
            | "split"
            | "pad_start"
            | "pad_end"
            | "lookup"
            | "lookup_first"
            | "merge"
            | "deep_merge"
            | "get"
            | "pick"
            | "omit"
            | "keys"
            | "values"
            | "entries"
            | "len"
            | "from_entries"
            | "object_flatten"
            | "object_unflatten"
            | "map"
            | "filter"
            | "flat_map"
            | "flatten"
            | "take"
            | "drop"
            | "slice"
            | "chunk"
            | "zip"
            | "zip_with"
            | "unzip"
            | "group_by"
            | "key_by"
            | "partition"
            | "unique"
            | "distinct_by"
            | "sort_by"
            | "find"
            | "find_index"
            | "index_of"
            | "contains"
            | "sum"
            | "avg"
            | "min"
            | "max"
            | "reduce"
            | "fold"
            | "+"
            | "-"
            | "*"
            | "/"
            | "round"
            | "to_base"
            | "date_format"
            | "to_unixtime"
            | "and"
            | "or"
            | "not"
            | "=="
            | "!="
            | "<"
            | "<="
            | ">"
            | ">="
            | "~="
    )
}
