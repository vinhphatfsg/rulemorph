use super::super::definitions::{NO_SCOPE, op, range};
use crate::v2_operator::types::{V2OperatorMetadata, V2OperatorTrace};

pub(super) const NULL_OPERATORS: &[V2OperatorMetadata] = &[op(
    "coalesce",
    range(1, None),
    V2OperatorTrace::LazyShortCircuit,
    false,
    false,
    NO_SCOPE,
)];
