use crate::v2_operator::types::V2OperatorMetadata;

mod aliases;
mod symbols;

pub(super) const COMPARISON_OPERATORS: &[V2OperatorMetadata] = &[
    symbols::eq(),
    symbols::ne(),
    symbols::lt(),
    symbols::lte(),
    symbols::gt(),
    symbols::gte(),
    symbols::pattern(),
    aliases::eq(),
    aliases::ne(),
    aliases::lt(),
    aliases::lte(),
    aliases::gt(),
    aliases::gte(),
    aliases::pattern(),
];
