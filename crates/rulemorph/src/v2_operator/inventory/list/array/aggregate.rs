use super::super::super::definitions::{ITEM_ACC_ARG_0, ITEM_ACC_ARG_1, NO_SCOPE, op, range};
use crate::v2_operator::types::{V2OperatorMetadata, V2OperatorTrace};

pub(super) const fn sum() -> V2OperatorMetadata {
    eager_no_stop_after_missing("sum")
}

pub(super) const fn avg() -> V2OperatorMetadata {
    eager_no_stop_after_missing("avg")
}

pub(super) const fn min() -> V2OperatorMetadata {
    eager_no_stop_after_missing("min")
}

pub(super) const fn max() -> V2OperatorMetadata {
    eager_no_stop_after_missing("max")
}

pub(super) const fn reduce() -> V2OperatorMetadata {
    op(
        "reduce",
        range(1, Some(1)),
        V2OperatorTrace::ItemLevelCollection,
        false,
        false,
        ITEM_ACC_ARG_0,
    )
}

pub(super) const fn fold() -> V2OperatorMetadata {
    op(
        "fold",
        range(2, Some(2)),
        V2OperatorTrace::ItemLevelCollection,
        false,
        false,
        ITEM_ACC_ARG_1,
    )
}

pub(super) const fn first() -> V2OperatorMetadata {
    eager_no_stop_after_missing("first")
}

pub(super) const fn last() -> V2OperatorMetadata {
    eager_no_stop_after_missing("last")
}

const fn eager_no_stop_after_missing(name: &'static str) -> V2OperatorMetadata {
    op(
        name,
        range(0, Some(0)),
        V2OperatorTrace::EagerArgs,
        true,
        false,
        NO_SCOPE,
    )
}
