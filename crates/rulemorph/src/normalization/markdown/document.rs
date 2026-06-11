use super::text::normalize_text;
use super::*;

#[derive(Clone)]
pub(super) struct MarkdownDocument {
    pub(super) frontmatter: Map<String, JsonValue>,
    pub(super) title: String,
    pub(super) body_text: String,
    pub(super) sections: Vec<Section>,
    pub(super) section_index: Vec<SectionSummary>,
    pub(super) blocks: Vec<Map<String, JsonValue>>,
    pub(super) links: Vec<JsonValue>,
    pub(super) images: Vec<JsonValue>,
    pub(super) code_blocks: Vec<JsonValue>,
    pub(super) tables: Vec<JsonValue>,
    pub(super) raw_html: Vec<JsonValue>,
}

#[derive(Clone)]
pub(super) struct Section {
    pub(super) id: String,
    pub(super) level: u8,
    pub(super) heading: String,
    pub(super) heading_block_id: Option<String>,
    pub(super) path: Vec<String>,
    pub(super) ordinal_path: Vec<usize>,
    pub(super) content_block_ids: Vec<String>,
    pub(super) child_ids: Vec<String>,
    pub(super) children: Vec<Section>,
}

#[derive(Clone)]
pub(super) struct SectionSummary {
    pub(super) id: String,
    pub(super) level: u8,
    pub(super) heading: String,
    pub(super) path: Vec<String>,
    pub(super) ordinal_path: Vec<usize>,
}

pub(super) fn document_record(document: MarkdownDocument, markdown: &MarkdownInput) -> JsonValue {
    let mut record = Map::new();
    record.insert(
        "record_type".to_string(),
        JsonValue::String("document".to_string()),
    );
    record.insert(
        "frontmatter".to_string(),
        JsonValue::Object(document.frontmatter.clone()),
    );
    record.insert("title".to_string(), JsonValue::String(document.title));
    if markdown.include.body_text {
        record.insert(
            "body_text".to_string(),
            JsonValue::String(normalize_text(&document.body_text, markdown)),
        );
    }
    record.insert(
        "sections".to_string(),
        JsonValue::Array(document.sections.iter().map(section_to_json).collect()),
    );
    record.insert(
        "section_index".to_string(),
        JsonValue::Array(
            document
                .section_index
                .iter()
                .map(section_summary_to_json)
                .collect(),
        ),
    );
    if markdown.include.blocks {
        record.insert(
            "blocks".to_string(),
            JsonValue::Array(document.blocks.into_iter().map(JsonValue::Object).collect()),
        );
    }
    if markdown.include.links {
        record.insert("links".to_string(), JsonValue::Array(document.links));
    }
    if markdown.include.images {
        record.insert("images".to_string(), JsonValue::Array(document.images));
    }
    if markdown.include.code_blocks {
        record.insert(
            "code_blocks".to_string(),
            JsonValue::Array(document.code_blocks),
        );
    }
    if markdown.include.tables {
        record.insert("tables".to_string(), JsonValue::Array(document.tables));
    }
    if markdown.include.raw_html {
        record.insert("raw_html".to_string(), JsonValue::Array(document.raw_html));
    }
    JsonValue::Object(record)
}

pub(super) fn section_to_json(section: &Section) -> JsonValue {
    json!({
        "id": section.id,
        "level": section.level,
        "heading": section.heading,
        "heading_block_id": section.heading_block_id,
        "path": section.path,
        "ordinal_path": section.ordinal_path,
        "content_block_ids": section.content_block_ids,
        "child_ids": section.child_ids,
        "children": section.children.iter().map(section_to_json).collect::<Vec<_>>(),
    })
}

pub(super) fn block_section_to_json(section: &Section) -> JsonValue {
    json!({
        "id": section.id,
        "heading": section.heading,
        "path": section.path,
    })
}

fn section_summary_to_json(section: &SectionSummary) -> JsonValue {
    json!({
        "id": section.id,
        "level": section.level,
        "heading": section.heading,
        "path": section.path,
        "ordinal_path": section.ordinal_path,
    })
}

pub(super) fn table_alignments(node: Node<'_>) -> Vec<String> {
    let data = node.data.borrow();
    match &data.value {
        NodeValue::Table(table) => table
            .alignments
            .iter()
            .map(|alignment| match alignment {
                TableAlignment::Left => "left",
                TableAlignment::Center => "center",
                TableAlignment::Right => "right",
                TableAlignment::None => "none",
            })
            .map(ToOwned::to_owned)
            .collect(),
        _ => Vec::new(),
    }
}

pub(super) fn strings_to_value(values: &[String]) -> JsonValue {
    JsonValue::Array(values.iter().cloned().map(JsonValue::String).collect())
}
