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

#[test]
fn excel_rows_transform_to_dynamodb_extended_attribute_values() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: excel
  excel:
    sheet: Products
mappings:
  - target: "Item.PK.S"
    expr:
      - "PRODUCT#"
      - concat: ["@input.sku"]
  - target: "Item.SK.S"
    value: "METADATA"
  - target: "Item.name.S"
    source: "name"
    type: "string"
  - target: "Item.price.N"
    source: "price"
    type: "string"
  - target: "Item.active.BOOL"
    source: "active"
    type: "bool"
  - target: "Item.tags.SS"
    expr:
      - "@input.tags"
      - split: [","]
  - target: "Item.dimensions.M.width.N"
    source: 'input.["dimensions.width"]'
    type: "string"
  - target: "Item.dimensions.M.height.N"
    source: 'input.["dimensions.height"]'
    type: "string"
  - target: "Item.archived.NULL"
    value: false
"#,
    )
    .expect("parse rule");
    let input = build_string_table_xlsx(
        "Products",
        &[
            "sku",
            "name",
            "price",
            "active",
            "tags",
            "dimensions.width",
            "dimensions.height",
        ],
        &[
            vec![
                "p001",
                "Notebook",
                "1299",
                "true",
                "stationery,paper",
                "148",
                "210",
            ],
            vec!["p002", "Pen", "199", "false", "stationery,ink", "10", "140"],
        ],
    );
    let output =
        transform_input(&rule, InputData::Bytes(&input), None).expect("transform excel input");
    assert_eq!(
        output,
        serde_json::json!([
            {
                "Item": {
                    "PK": { "S": "PRODUCT#p001" },
                    "SK": { "S": "METADATA" },
                    "name": { "S": "Notebook" },
                    "price": { "N": "1299" },
                    "active": { "BOOL": true },
                    "tags": { "SS": ["stationery", "paper"] },
                    "dimensions": { "M": { "width": { "N": "148" }, "height": { "N": "210" } } },
                    "archived": { "NULL": false }
                }
            },
            {
                "Item": {
                    "PK": { "S": "PRODUCT#p002" },
                    "SK": { "S": "METADATA" },
                    "name": { "S": "Pen" },
                    "price": { "N": "199" },
                    "active": { "BOOL": false },
                    "tags": { "SS": ["stationery", "ink"] },
                    "dimensions": { "M": { "width": { "N": "10" }, "height": { "N": "140" } } },
                    "archived": { "NULL": false }
                }
            }
        ])
    );
}
