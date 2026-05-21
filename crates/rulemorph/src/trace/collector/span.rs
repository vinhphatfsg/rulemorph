#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::trace) enum SpanAction {
    Push(u64),
    Pop(Option<u64>),
}

#[derive(Debug, Default)]
pub(super) struct SpanStack {
    ids: Vec<u64>,
}

impl SpanStack {
    pub(super) fn new() -> Self {
        Self { ids: Vec::new() }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }

    pub(super) fn current_parent(&self) -> Option<u64> {
        self.ids.last().copied()
    }

    pub(super) fn apply(&mut self, action: Option<SpanAction>) {
        match action {
            Some(SpanAction::Push(id)) => self.ids.push(id),
            Some(SpanAction::Pop(Some(expected))) => {
                if self.ids.last().copied() == Some(expected) {
                    self.ids.pop();
                }
            }
            Some(SpanAction::Pop(None)) | None => {}
        }
    }
}
