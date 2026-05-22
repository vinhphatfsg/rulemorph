use super::super::super::definitions::{ITEM_LAST_ARG, NO_SCOPE, op, range};
use crate::v2_operator::types::{V2OperatorMetadata, V2OperatorTrace};

pub(super) const fn flatten() -> V2OperatorMetadata {
    eager_stop_after_missing("flatten", 0, Some(1))
}

pub(super) const fn take() -> V2OperatorMetadata {
    eager_stop_after_missing("take", 1, Some(1))
}

pub(super) const fn drop() -> V2OperatorMetadata {
    eager_stop_after_missing("drop", 1, Some(1))
}

pub(super) const fn slice() -> V2OperatorMetadata {
    eager_stop_after_missing("slice", 1, Some(2))
}

pub(super) const fn chunk() -> V2OperatorMetadata {
    eager_stop_after_missing("chunk", 1, Some(1))
}

pub(super) const fn zip() -> V2OperatorMetadata {
    eager_stop_after_missing("zip", 1, None)
}

pub(super) const fn zip_with() -> V2OperatorMetadata {
    op(
        "zip_with",
        range(2, None),
        V2OperatorTrace::Delegated,
        false,
        false,
        ITEM_LAST_ARG,
    )
}

pub(super) const fn unzip() -> V2OperatorMetadata {
    eager_no_stop_after_missing("unzip", 0, Some(0))
}

pub(super) const fn unique() -> V2OperatorMetadata {
    eager_no_stop_after_missing("unique", 0, Some(0))
}

pub(super) const fn index_of() -> V2OperatorMetadata {
    eager_stop_after_missing("index_of", 1, Some(1))
}

pub(super) const fn contains() -> V2OperatorMetadata {
    eager_stop_after_missing("contains", 1, Some(1))
}

const fn eager_stop_after_missing(
    name: &'static str,
    min: usize,
    max: Option<usize>,
) -> V2OperatorMetadata {
    op(
        name,
        range(min, max),
        V2OperatorTrace::EagerArgs,
        true,
        true,
        NO_SCOPE,
    )
}

const fn eager_no_stop_after_missing(
    name: &'static str,
    min: usize,
    max: Option<usize>,
) -> V2OperatorMetadata {
    op(
        name,
        range(min, max),
        V2OperatorTrace::EagerArgs,
        true,
        false,
        NO_SCOPE,
    )
}
