use std::collections::BTreeMap;

use serde::Deserialize;

mod excel;

pub use excel::{
    ExcelCellErrorPolicy, ExcelColumn, ExcelDatePolicy, ExcelEmptyCellPolicy, ExcelFormulaPolicy,
    ExcelInput, ExcelSheetRef,
};

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
