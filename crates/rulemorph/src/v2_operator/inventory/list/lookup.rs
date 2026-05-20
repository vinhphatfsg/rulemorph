use super::super::definitions::{NO_SCOPE, op, range};
use crate::v2_operator::types::{V2OperatorMetadata, V2OperatorTrace};

pub(super) const LOOKUP_OPERATORS: &[V2OperatorMetadata] = &[
    op(
        "lookup",
        range(2, Some(4)),
        V2OperatorTrace::Delegated,
        false,
        false,
        NO_SCOPE,
    ),
    op(
        "lookup_first",
        range(2, Some(4)),
        V2OperatorTrace::Delegated,
        false,
        false,
        NO_SCOPE,
    ),
];
