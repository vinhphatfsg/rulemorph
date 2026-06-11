#[derive(Clone, Copy)]
pub(super) struct Fence {
    pub(super) marker: u8,
    pub(super) len: usize,
}

#[derive(Clone, Copy)]
pub(super) struct ActiveFence {
    pub(super) fence: Fence,
    pub(super) quote_depth: usize,
}

#[derive(Clone, Copy)]
pub(super) struct ActiveHtmlBlock {
    pub(super) end: HtmlBlockEnd,
    pub(super) quote_depth: usize,
    pub(super) can_interrupt_paragraph: bool,
}

#[derive(Clone, Copy)]
pub(super) enum HtmlBlockEnd {
    ClosingTag(&'static str),
    Contains(&'static str),
    BlankLine,
}

#[derive(Clone, Copy)]
pub(super) struct TableHeaderCandidate {
    pub(super) cells: usize,
    pub(super) quote_depth: usize,
    pub(super) fallback_nodes: usize,
}

#[derive(Clone, Copy)]
pub(super) struct TableState {
    pub(super) columns: usize,
    pub(super) quote_depth: usize,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) struct ListState {
    pub(super) quote_depth: usize,
    pub(super) kind: ListKind,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum ListKind {
    Unordered,
    Ordered,
}
