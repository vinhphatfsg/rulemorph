#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct V2OperatorMetadata {
    pub(crate) name: &'static str,
    pub(crate) validates: bool,
    pub(crate) arg_range: V2OperatorArgRange,
    pub(crate) trace: V2OperatorTrace,
    pub(crate) skips_args_when_pipe_is_missing: bool,
    pub(crate) stops_after_missing_arg: bool,
    pub(crate) arg_scopes: &'static [V2OperatorArgScopeRule],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct V2OperatorArgRange {
    pub(crate) min: usize,
    pub(crate) max: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum V2OperatorTrace {
    EagerArgs,
    LazyShortCircuit,
    ItemLevelCollection,
    Delegated,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct V2OperatorArgScopeRule {
    pub(in crate::v2_operator) selector: V2OperatorArgSelector,
    pub(crate) scope: V2OperatorArgScope,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::v2_operator) enum V2OperatorArgSelector {
    Exact(usize),
    Last,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum V2OperatorArgScope {
    Item,
    ItemAndAcc,
}
