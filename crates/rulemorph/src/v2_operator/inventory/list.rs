mod arithmetic;
mod array;
mod cast;
mod comparison;
mod date;
mod json;
mod logical;
mod lookup;
mod null;
mod string;

use crate::v2_operator::types::V2OperatorMetadata;

pub(crate) const V2_OPERATOR_GROUPS: &[&[V2OperatorMetadata]] = &[
    string::STRING_OPERATORS,
    null::NULL_OPERATORS,
    lookup::LOOKUP_OPERATORS,
    arithmetic::ARITHMETIC_OPERATORS,
    date::DATE_OPERATORS,
    logical::LOGICAL_OPERATORS,
    comparison::COMPARISON_OPERATORS,
    json::JSON_OPERATORS,
    array::ARRAY_OPERATORS,
    cast::CAST_OPERATORS,
];

pub(crate) fn operators() -> impl Iterator<Item = &'static V2OperatorMetadata> {
    V2_OPERATOR_GROUPS
        .iter()
        .flat_map(|operators| operators.iter())
}
