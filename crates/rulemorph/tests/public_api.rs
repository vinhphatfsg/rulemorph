#[test]
fn markdown_input_can_be_constructed_from_public_api_types() {
    use std::collections::BTreeMap;

    use rulemorph::{
        InputFormat, InputSpec, MarkdownFlavor, MarkdownFrontmatter, MarkdownInclude,
        MarkdownInput, MarkdownRecordsMode, MarkdownTableHeaderPolicy, RuleFile,
    };

    let rule = RuleFile {
        version: 1,
        input: InputSpec {
            format: InputFormat::Markdown,
            csv: None,
            json: None,
            yaml: None,
            toml: None,
            xml: None,
            html: None,
            excel: None,
            markdown: Some(MarkdownInput {
                flavor: MarkdownFlavor::Gfm,
                frontmatter: MarkdownFrontmatter::Auto,
                records: MarkdownRecordsMode::Document,
                section_levels: None,
                table_header_policy: MarkdownTableHeaderPolicy::Strict,
                include: MarkdownInclude::default(),
                trim_text: true,
                collapse_whitespace: true,
            }),
        },
        output: None,
        defs: BTreeMap::new(),
        codecs: BTreeMap::new(),
        record_when: None,
        mappings: Vec::new(),
        steps: None,
        finalize: None,
    };

    assert_eq!(rule.input.format, InputFormat::Markdown);
}
