use super::super::super::definitions::{NO_SCOPE, op, range};
use crate::v2_operator::types::{V2OperatorMetadata, V2OperatorTrace};

pub(super) const fn eq() -> V2OperatorMetadata {
    comparison("==")
}

pub(super) const fn ne() -> V2OperatorMetadata {
    comparison("!=")
}

pub(super) const fn lt() -> V2OperatorMetadata {
    comparison("<")
}

pub(super) const fn lte() -> V2OperatorMetadata {
    comparison("<=")
}

pub(super) const fn gt() -> V2OperatorMetadata {
    comparison(">")
}

pub(super) const fn gte() -> V2OperatorMetadata {
    comparison(">=")
}

pub(super) const fn pattern() -> V2OperatorMetadata {
    comparison("~=")
}

const fn comparison(name: &'static str) -> V2OperatorMetadata {
    op(
        name,
        range(1, Some(1)),
        V2OperatorTrace::EagerArgs,
        false,
        false,
        NO_SCOPE,
    )
}
