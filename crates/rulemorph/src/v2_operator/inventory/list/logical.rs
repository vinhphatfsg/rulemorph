use super::super::definitions::{NO_SCOPE, op, range};
use crate::v2_operator::types::{V2OperatorMetadata, V2OperatorTrace};

pub(super) const LOGICAL_OPERATORS: &[V2OperatorMetadata] = &[
    op(
        "and",
        range(1, None),
        V2OperatorTrace::LazyShortCircuit,
        false,
        false,
        NO_SCOPE,
    ),
    op(
        "or",
        range(1, None),
        V2OperatorTrace::LazyShortCircuit,
        false,
        false,
        NO_SCOPE,
    ),
    op(
        "not",
        range(0, Some(0)),
        V2OperatorTrace::EagerArgs,
        true,
        false,
        NO_SCOPE,
    ),
];
