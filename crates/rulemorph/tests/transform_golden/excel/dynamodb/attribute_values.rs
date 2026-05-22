#[test]
fn excel_rows_transform_to_dynamodb_attribute_values() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: excel
  excel:
    sheet: Users
mappings:
  - target: "Item.PK.S"
    expr:
      op: "concat"
      args: ["USER#", { ref: "input.user_id" }]
  - target: "Item.SK.S"
    value: "PROFILE"
  - target: "Item.email.S"
    source: "email"
    type: "string"
  - target: "Item.age.N"
    source: "age"
    type: "string"
  - target: "Item.active.BOOL"
    source: "active"
    type: "bool"
"#,
    )
    .expect("parse rule");
    let input = build_dynamodb_users_xlsx();
    let output =
        transform_input(&rule, InputData::Bytes(&input), None).expect("transform excel input");
    assert_eq!(
        output,
        serde_json::json!([
            {
                "Item": {
                    "PK": { "S": "USER#u001" },
                    "SK": { "S": "PROFILE" },
                    "email": { "S": "alice@example.com" },
                    "age": { "N": "31" },
                    "active": { "BOOL": true }
                }
            },
            {
                "Item": {
                    "PK": { "S": "USER#u002" },
                    "SK": { "S": "PROFILE" },
                    "email": { "S": "bob@example.com" },
                    "age": { "N": "28" },
                    "active": { "BOOL": false }
                }
            }
        ])
    );
}
