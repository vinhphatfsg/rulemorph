use serde::Deserialize;

fn default_true() -> bool {
    true
}

fn default_flavor() -> MarkdownFlavor {
    MarkdownFlavor::Gfm
}

fn default_frontmatter() -> MarkdownFrontmatter {
    MarkdownFrontmatter::Auto
}

fn default_records() -> MarkdownRecordsMode {
    MarkdownRecordsMode::Document
}

fn default_table_header_policy() -> MarkdownTableHeaderPolicy {
    MarkdownTableHeaderPolicy::Strict
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct MarkdownInput {
    #[serde(default = "default_flavor")]
    pub flavor: MarkdownFlavor,
    #[serde(default = "default_frontmatter")]
    pub frontmatter: MarkdownFrontmatter,
    #[serde(default = "default_records")]
    pub records: MarkdownRecordsMode,
    #[serde(default)]
    pub section_levels: Option<Vec<u8>>,
    #[serde(default = "default_table_header_policy")]
    pub table_header_policy: MarkdownTableHeaderPolicy,
    #[serde(default)]
    pub include: MarkdownInclude,
    #[serde(default = "default_true")]
    pub trim_text: bool,
    #[serde(default = "default_true")]
    pub collapse_whitespace: bool,
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum MarkdownFlavor {
    Commonmark,
    Gfm,
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum MarkdownFrontmatter {
    None,
    Yaml,
    Toml,
    Auto,
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MarkdownRecordsMode {
    Document,
    Sections,
    TableRows,
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum MarkdownTableHeaderPolicy {
    Strict,
    Index,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct MarkdownInclude {
    #[serde(default = "default_true")]
    pub body_text: bool,
    #[serde(default)]
    pub body_markdown: bool,
    #[serde(default = "default_true")]
    pub blocks: bool,
    #[serde(default = "default_true")]
    pub links: bool,
    #[serde(default = "default_true")]
    pub images: bool,
    #[serde(default = "default_true")]
    pub code_blocks: bool,
    #[serde(default = "default_true")]
    pub tables: bool,
    #[serde(default = "default_true")]
    pub raw_html: bool,
    #[serde(default)]
    pub sourcepos: bool,
}

impl Default for MarkdownInclude {
    fn default() -> Self {
        Self {
            body_text: true,
            body_markdown: false,
            blocks: true,
            links: true,
            images: true,
            code_blocks: true,
            tables: true,
            raw_html: true,
            sourcepos: false,
        }
    }
}
