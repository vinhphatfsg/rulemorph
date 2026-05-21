#[test]
fn xlsform_survey_sheet_transforms_to_question_schema() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: excel
  excel:
    sheet: survey
mappings:
  - target: "name"
    source: "name"
  - target: "kind"
    source: "type"
  - target: "label"
    source: "label"
  - target: "required"
    source: "required"
    type: "bool"
  - target: "relevance"
    source: "relevant"
"#,
    )
    .expect("parse rule");
    let input = build_string_table_xlsx(
        "survey",
        &["type", "name", "label", "required", "relevant"],
        &[
            vec!["text", "respondent_name", "Respondent name", "true", ""],
            vec!["integer", "age", "Age", "false", "${respondent_name} != ''"],
        ],
    );
    let output =
        transform_input(&rule, InputData::Bytes(&input), None).expect("transform excel input");
    assert_eq!(
        output,
        serde_json::json!([
            {
                "name": "respondent_name",
                "kind": "text",
                "label": "Respondent name",
                "required": true,
                "relevance": ""
            },
            {
                "name": "age",
                "kind": "integer",
                "label": "Age",
                "required": false,
                "relevance": "${respondent_name} != ''"
            }
        ])
    );
}
