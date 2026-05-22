#[test]
fn excel_rows_transform_to_dynamodb_batch_write_item_payload() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: excel
  excel:
    sheet: Users
mappings:
  - target: "PutRequest.Item.PK.S"
    expr:
      op: "concat"
      args: ["USER#", { ref: "input.user_id" }]
  - target: "PutRequest.Item.SK.S"
    value: "PROFILE"
  - target: "PutRequest.Item.email.S"
    source: "email"
    type: "string"
  - target: "PutRequest.Item.age.N"
    source: "age"
    type: "string"
  - target: "PutRequest.Item.active.BOOL"
    source: "active"
    type: "bool"
finalize:
  wrap:
    RequestItems:
      UsersTable: "@out"
"#,
    )
    .expect("parse rule");
    let input = build_dynamodb_users_xlsx();
    let output =
        transform_input(&rule, InputData::Bytes(&input), None).expect("transform excel input");
    assert_eq!(
        output,
        serde_json::json!({
            "RequestItems": {
                "UsersTable": [
                    {
                        "PutRequest": {
                            "Item": {
                                "PK": { "S": "USER#u001" },
                                "SK": { "S": "PROFILE" },
                                "email": { "S": "alice@example.com" },
                                "age": { "N": "31" },
                                "active": { "BOOL": true }
                            }
                        }
                    },
                    {
                        "PutRequest": {
                            "Item": {
                                "PK": { "S": "USER#u002" },
                                "SK": { "S": "PROFILE" },
                                "email": { "S": "bob@example.com" },
                                "age": { "N": "28" },
                                "active": { "BOOL": false }
                            }
                        }
                    }
                ]
            }
        })
    );
}
