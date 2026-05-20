use super::super::definitions::{NO_SCOPE, op, range};
use crate::v2_operator::types::{V2OperatorMetadata, V2OperatorTrace};

pub(super) const DATE_OPERATORS: &[V2OperatorMetadata] = &[
    op(
        "date_format",
        range(1, Some(3)),
        V2OperatorTrace::EagerArgs,
        true,
        true,
        NO_SCOPE,
    ),
    op(
        "to_unixtime",
        range(0, Some(2)),
        V2OperatorTrace::EagerArgs,
        true,
        true,
        NO_SCOPE,
    ),
];
