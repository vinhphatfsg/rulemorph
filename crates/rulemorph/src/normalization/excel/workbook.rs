mod rewrite;
mod selection;

pub(super) use rewrite::rewrite_workbook_for_calamine;
pub(super) use selection::selected_worksheet_path;

const OFFICE_RELATIONSHIPS_NS: &[u8] =
    b"http://schemas.openxmlformats.org/officeDocument/2006/relationships";
