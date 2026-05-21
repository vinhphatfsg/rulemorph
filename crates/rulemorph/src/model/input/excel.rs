use serde::Deserialize;

fn default_true() -> bool {
    true
}

fn default_header_row() -> usize {
    1
}

fn default_excel_empty_cell() -> ExcelEmptyCellPolicy {
    ExcelEmptyCellPolicy::Missing
}

fn default_excel_formula() -> ExcelFormulaPolicy {
    ExcelFormulaPolicy::Cached
}

fn default_excel_date() -> ExcelDatePolicy {
    ExcelDatePolicy::Iso8601
}

fn default_excel_cell_error() -> ExcelCellErrorPolicy {
    ExcelCellErrorPolicy::Error
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct ExcelInput {
    #[serde(default)]
    pub sheet: Option<ExcelSheetRef>,
    #[serde(default = "default_true")]
    pub has_header: bool,
    #[serde(default = "default_header_row")]
    pub header_row: usize,
    #[serde(default)]
    pub data_start_row: Option<usize>,
    #[serde(default)]
    pub range: Option<String>,
    #[serde(default)]
    pub columns: Option<Vec<ExcelColumn>>,
    #[serde(default = "default_excel_empty_cell")]
    pub empty_cell: ExcelEmptyCellPolicy,
    #[serde(default = "default_excel_formula")]
    pub formula: ExcelFormulaPolicy,
    #[serde(default = "default_excel_date")]
    pub date: ExcelDatePolicy,
    #[serde(default = "default_excel_cell_error")]
    pub cell_error: ExcelCellErrorPolicy,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
#[serde(untagged)]
pub enum ExcelSheetRef {
    Name(String),
    Index(usize),
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct ExcelColumn {
    pub name: String,
    pub column: String,
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ExcelEmptyCellPolicy {
    Missing,
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ExcelFormulaPolicy {
    Cached,
    Formula,
    Error,
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ExcelDatePolicy {
    Iso8601,
    Serial,
    String,
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ExcelCellErrorPolicy {
    Error,
}
