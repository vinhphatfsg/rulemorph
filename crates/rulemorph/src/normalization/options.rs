#[derive(Debug, Clone)]
pub struct NormalizationOptions {
    pub max_input_bytes: usize,
    pub max_records: usize,
    pub max_depth: usize,
    pub max_array_len: usize,
    pub max_text_bytes: usize,
    pub max_yaml_aliases: usize,
    pub max_yaml_expanded_nodes: usize,
    pub max_xml_nodes: usize,
    pub max_html_nodes: usize,
    pub max_markdown_nodes: usize,
    pub max_markdown_table_cells: usize,
    pub max_excel_zip_entries: usize,
    pub max_excel_uncompressed_bytes: usize,
    pub max_excel_entry_uncompressed_bytes: usize,
    pub max_excel_sheets: usize,
    pub max_excel_rows: usize,
    pub max_excel_cells: usize,
    pub max_excel_shared_strings: usize,
    pub max_excel_shared_string_bytes: usize,
    pub max_excel_styles: usize,
    pub max_range_items: Option<usize>,
    pub max_object_fields: usize,
    pub max_object_key_bytes: usize,
    pub max_object_depth: usize,
    pub max_generated_json_nodes: usize,
    pub max_generated_json_bytes: usize,
}

impl Default for NormalizationOptions {
    fn default() -> Self {
        Self {
            max_input_bytes: 64 * 1024 * 1024,
            max_records: 100_000,
            max_depth: 256,
            max_array_len: 1_000_000,
            max_text_bytes: 8 * 1024 * 1024,
            max_yaml_aliases: 10_000,
            max_yaml_expanded_nodes: 1_000_000,
            max_xml_nodes: 1_000_000,
            max_html_nodes: 1_000_000,
            max_markdown_nodes: 1_000_000,
            max_markdown_table_cells: 1_000_000,
            max_excel_zip_entries: 10_000,
            max_excel_uncompressed_bytes: 256 * 1024 * 1024,
            max_excel_entry_uncompressed_bytes: 64 * 1024 * 1024,
            max_excel_sheets: 128,
            max_excel_rows: 100_000,
            max_excel_cells: 1_000_000,
            max_excel_shared_strings: 1_000_000,
            max_excel_shared_string_bytes: 64 * 1024 * 1024,
            max_excel_styles: 65_536,
            max_range_items: Some(10_000),
            max_object_fields: 10_000,
            max_object_key_bytes: 4 * 1024,
            max_object_depth: 64,
            max_generated_json_nodes: 100_000,
            max_generated_json_bytes: 10 * 1024 * 1024,
        }
    }
}

impl NormalizationOptions {
    pub fn large() -> Self {
        Self {
            max_input_bytes: 512 * 1024 * 1024,
            max_records: 1_000_000,
            max_array_len: 10_000_000,
            max_excel_uncompressed_bytes: 1024 * 1024 * 1024,
            max_excel_rows: 1_000_000,
            max_excel_cells: 10_000_000,
            max_markdown_nodes: 10_000_000,
            max_markdown_table_cells: 10_000_000,
            max_object_fields: 100_000,
            max_object_key_bytes: 16 * 1024,
            max_object_depth: 128,
            max_generated_json_nodes: 1_000_000,
            max_generated_json_bytes: 128 * 1024 * 1024,
            ..Self::default()
        }
    }
}
