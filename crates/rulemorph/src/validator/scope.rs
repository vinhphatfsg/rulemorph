#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum LocalScope {
    None,
    Item,
    ItemAcc,
}

impl LocalScope {
    pub(super) fn allows_item(self) -> bool {
        matches!(self, LocalScope::Item | LocalScope::ItemAcc)
    }

    pub(super) fn allows_acc(self) -> bool {
        matches!(self, LocalScope::ItemAcc)
    }
}
