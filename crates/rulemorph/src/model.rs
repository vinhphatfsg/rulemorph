use std::collections::BTreeMap;

use serde::Deserialize;
use serde_json::Value as JsonValue;

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct RuleFile {
    pub version: u8,
    pub input: InputSpec,
    #[serde(default)]
    pub output: Option<OutputSpec>,
    #[serde(default)]
    pub record_when: Option<Expr>,
    #[serde(default)]
    pub mappings: Vec<Mapping>,
    #[serde(default)]
    pub steps: Option<Vec<V2RuleStep>>,
    #[serde(default)]
    pub finalize: Option<FinalizeSpec>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct OutputSpec {
    pub name: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct InputSpec {
    pub format: InputFormat,
    #[serde(default)]
    pub csv: Option<CsvInput>,
    #[serde(default)]
    pub json: Option<JsonInput>,
    #[serde(default)]
    pub yaml: Option<YamlInput>,
    #[serde(default)]
    pub toml: Option<TomlInput>,
    #[serde(default)]
    pub xml: Option<XmlInput>,
    #[serde(default)]
    pub html: Option<HtmlInput>,
    #[serde(default)]
    pub excel: Option<ExcelInput>,
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum InputFormat {
    Csv,
    Json,
    Yaml,
    Toml,
    Xml,
    Html,
    Excel,
}

fn default_true() -> bool {
    true
}

fn default_delimiter() -> String {
    ",".to_string()
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct CsvInput {
    #[serde(default = "default_true")]
    pub has_header: bool,
    #[serde(default = "default_delimiter")]
    pub delimiter: String,
    pub columns: Option<Vec<Column>>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct Column {
    pub name: String,
    #[serde(rename = "type")]
    pub value_type: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct JsonInput {
    #[serde(default)]
    pub records_path: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct YamlInput {
    #[serde(default)]
    pub records_path: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct TomlInput {
    #[serde(default)]
    pub records_path: Option<String>,
}

fn default_attr_prefix() -> String {
    "@".to_string()
}

fn default_text_key() -> String {
    "#text".to_string()
}

fn default_xml_child_policy() -> XmlChildPolicy {
    XmlChildPolicy::Array
}

fn default_xml_namespaces() -> XmlNamespacePolicy {
    XmlNamespacePolicy::Qualified
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct XmlInput {
    pub records_path: String,
    #[serde(default = "default_attr_prefix")]
    pub attr_prefix: String,
    #[serde(default = "default_text_key")]
    pub text_key: String,
    #[serde(default = "default_xml_child_policy")]
    pub child_policy: XmlChildPolicy,
    #[serde(default = "default_true")]
    pub trim_text: bool,
    #[serde(default = "default_true")]
    pub collapse_whitespace: bool,
    #[serde(default = "default_xml_namespaces")]
    pub namespaces: XmlNamespacePolicy,
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum XmlChildPolicy {
    Array,
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum XmlNamespacePolicy {
    Qualified,
    Strip,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct HtmlInput {
    pub records_selector: String,
    pub fields: BTreeMap<String, HtmlField>,
    #[serde(default = "default_true")]
    pub trim_text: bool,
    #[serde(default = "default_true")]
    pub collapse_whitespace: bool,
}

fn default_html_value() -> HtmlValueKind {
    HtmlValueKind::Text
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct HtmlField {
    #[serde(default)]
    pub selector: Option<String>,
    #[serde(default = "default_html_value")]
    pub value: HtmlValueKind,
    #[serde(default)]
    pub attr: Option<String>,
    #[serde(default)]
    pub multiple: bool,
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum HtmlValueKind {
    Text,
    Html,
    Attr,
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

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct Mapping {
    pub target: String,
    pub source: Option<String>,
    pub value: Option<JsonValue>,
    pub expr: Option<Expr>,
    pub when: Option<Expr>,
    #[serde(rename = "type")]
    pub value_type: Option<String>,
    #[serde(default)]
    pub required: bool,
    pub default: Option<JsonValue>,
}

// =============================================================================
// v2 Rule Steps / Finalize
// =============================================================================

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct V2RuleStep {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub mappings: Option<Vec<Mapping>>,
    #[serde(default)]
    pub record_when: Option<Expr>,
    #[serde(default)]
    pub asserts: Option<Vec<V2Assert>>,
    #[serde(default)]
    pub branch: Option<V2Branch>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct V2Assert {
    pub when: Expr,
    pub error: V2AssertError,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct V2AssertError {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct V2Branch {
    pub when: Expr,
    pub then: String,
    #[serde(default)]
    pub r#else: Option<String>,
    #[serde(rename = "return", default)]
    pub return_: bool,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct FinalizeSpec {
    #[serde(default)]
    pub filter: Option<Expr>,
    #[serde(default)]
    pub sort: Option<FinalizeSort>,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub offset: Option<usize>,
    #[serde(default)]
    pub wrap: Option<JsonValue>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct FinalizeSort {
    pub by: String,
    #[serde(default = "default_sort_order")]
    pub order: String,
}

fn default_sort_order() -> String {
    "asc".to_string()
}

#[derive(Debug, Deserialize, Clone, PartialEq)]
#[serde(untagged)]
pub enum Expr {
    Ref(ExprRef),
    Op(ExprOp),
    Chain(ExprChain),
    Literal(JsonValue),
}

#[derive(Debug, Deserialize, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ExprRef {
    #[serde(rename = "ref")]
    pub ref_path: String,
}

#[derive(Debug, Deserialize, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ExprOp {
    pub op: String,
    #[serde(default)]
    pub args: Vec<Expr>,
}

#[derive(Debug, Deserialize, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ExprChain {
    pub chain: Vec<Expr>,
}
