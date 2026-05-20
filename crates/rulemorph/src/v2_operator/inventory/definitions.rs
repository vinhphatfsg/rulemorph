use super::super::types::{
    V2OperatorArgRange, V2OperatorArgScope, V2OperatorArgScopeRule, V2OperatorArgSelector,
};

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
