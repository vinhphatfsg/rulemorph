use super::super::types::{
    V2OperatorArgRange, V2OperatorArgScope, V2OperatorArgScopeRule, V2OperatorArgSelector,
    V2OperatorMetadata, V2OperatorTrace,
};

pub(super) const fn op(
    name: &'static str,
    arg_range: V2OperatorArgRange,
    trace: V2OperatorTrace,
    skips_args_when_pipe_is_missing: bool,
    stops_after_missing_arg: bool,
    arg_scopes: &'static [V2OperatorArgScopeRule],
) -> V2OperatorMetadata {
    V2OperatorMetadata {
        name,
        validates: true,
        arg_range,
        trace,
        skips_args_when_pipe_is_missing,
        stops_after_missing_arg,
        arg_scopes,
    }
}

pub(super) const fn range(min: usize, max: Option<usize>) -> V2OperatorArgRange {
    V2OperatorArgRange { min, max }
}

pub(super) const NO_SCOPE: &[V2OperatorArgScopeRule] = &[];
pub(super) const ITEM_ARG_0: &[V2OperatorArgScopeRule] = &[V2OperatorArgScopeRule {
    selector: V2OperatorArgSelector::Exact(0),
    scope: V2OperatorArgScope::Item,
}];
pub(super) const ITEM_ACC_ARG_0: &[V2OperatorArgScopeRule] = &[V2OperatorArgScopeRule {
    selector: V2OperatorArgSelector::Exact(0),
    scope: V2OperatorArgScope::ItemAndAcc,
}];
pub(super) const ITEM_ACC_ARG_1: &[V2OperatorArgScopeRule] = &[V2OperatorArgScopeRule {
    selector: V2OperatorArgSelector::Exact(1),
    scope: V2OperatorArgScope::ItemAndAcc,
}];
pub(super) const ITEM_LAST_ARG: &[V2OperatorArgScopeRule] = &[V2OperatorArgScopeRule {
    selector: V2OperatorArgSelector::Last,
    scope: V2OperatorArgScope::Item,
}];
