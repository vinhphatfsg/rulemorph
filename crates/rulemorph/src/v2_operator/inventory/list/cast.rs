use super::super::definitions::{NO_SCOPE, op, range};
use crate::v2_operator::types::{V2OperatorMetadata, V2OperatorTrace};

pub(super) const CAST_OPERATORS: &[V2OperatorMetadata] = &[
    op(
        "string",
        range(0, Some(0)),
        V2OperatorTrace::EagerArgs,
        true,
        false,
        NO_SCOPE,
    ),
    op(
        "int",
        range(0, Some(0)),
        V2OperatorTrace::EagerArgs,
        true,
        false,
        NO_SCOPE,
    ),
    op(
        "float",
        range(0, Some(0)),
        V2OperatorTrace::EagerArgs,
        true,
        false,
        NO_SCOPE,
    ),
    op(
        "bool",
        range(0, Some(0)),
        V2OperatorTrace::EagerArgs,
        true,
        false,
        NO_SCOPE,
    ),
];
