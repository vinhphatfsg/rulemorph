use super::inventory::V2_OPERATORS;
#[cfg(test)]
use super::types::V2OperatorTrace;
use super::types::{V2OperatorArgScope, V2OperatorArgSelector, V2OperatorMetadata};

pub(crate) fn operator(name: &str) -> Option<&'static V2OperatorMetadata> {
    V2_OPERATORS.iter().find(|metadata| metadata.name == name)
}

pub(crate) fn is_valid_operator(name: &str) -> bool {
    operator(name).is_some_and(|metadata| metadata.validates)
}

pub(crate) fn operator_arg_range(name: &str) -> (usize, Option<usize>) {
    operator(name)
        .map(|metadata| (metadata.arg_range.min, metadata.arg_range.max))
        .unwrap_or((0, None))
}

pub(crate) fn operator_arg_scope(
    name: &str,
    arg_index: usize,
    arg_count: usize,
) -> Option<V2OperatorArgScope> {
    operator(name).and_then(|metadata| {
        metadata
            .arg_scopes
            .iter()
            .find(|rule| match rule.selector {
                V2OperatorArgSelector::Exact(index) => arg_index == index,
                V2OperatorArgSelector::Last => arg_index + 1 == arg_count,
            })
            .map(|rule| rule.scope)
    })
}

#[cfg(test)]
pub(crate) fn operator_has_eager_args(name: &str) -> bool {
    operator(name)
        .map(|metadata| metadata.trace == V2OperatorTrace::EagerArgs)
        .unwrap_or(true)
}

#[cfg(test)]
pub(crate) fn operator_has_item_level_trace(name: &str) -> bool {
    operator(name).is_some_and(|metadata| metadata.trace == V2OperatorTrace::ItemLevelCollection)
}

#[cfg(test)]
pub(crate) fn operator_has_lazy_arg_trace(name: &str) -> bool {
    operator(name).is_some_and(|metadata| metadata.trace == V2OperatorTrace::LazyShortCircuit)
}
