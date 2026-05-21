use super::super::super::definitions::{ITEM_ARG_0, op, range};
use crate::v2_operator::types::{V2OperatorMetadata, V2OperatorTrace};

pub(super) const fn map() -> V2OperatorMetadata {
    item_level("map", 1, Some(1))
}

pub(super) const fn filter() -> V2OperatorMetadata {
    item_level("filter", 1, Some(1))
}

pub(super) const fn flat_map() -> V2OperatorMetadata {
    item_level("flat_map", 1, Some(1))
}

pub(super) const fn group_by() -> V2OperatorMetadata {
    item_level("group_by", 1, Some(1))
}

pub(super) const fn key_by() -> V2OperatorMetadata {
    item_level("key_by", 1, Some(1))
}

pub(super) const fn partition() -> V2OperatorMetadata {
    item_level("partition", 1, Some(1))
}

pub(super) const fn distinct_by() -> V2OperatorMetadata {
    item_level("distinct_by", 1, Some(1))
}

pub(super) const fn sort_by() -> V2OperatorMetadata {
    item_level("sort_by", 1, Some(2))
}

pub(super) const fn find() -> V2OperatorMetadata {
    item_level("find", 1, Some(1))
}

pub(super) const fn find_index() -> V2OperatorMetadata {
    item_level("find_index", 1, Some(1))
}

const fn item_level(name: &'static str, min: usize, max: Option<usize>) -> V2OperatorMetadata {
    op(
        name,
        range(min, max),
        V2OperatorTrace::ItemLevelCollection,
        false,
        false,
        ITEM_ARG_0,
    )
}
