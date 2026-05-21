#[test]
fn excel_flat_spreadsheet_columns_transform_to_nested_json_document() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: excel
  excel:
    sheet: Orders
mappings:
  - target: "document"
    expr:
      - "@input"
      - object_unflatten: []
"#,
    )
    .expect("parse rule");
    let input = build_string_table_xlsx(
        "Orders",
        &[
            "order.id",
            "buyer.name",
            "buyer.email",
            "shipping.address.city",
            "items.primary.sku",
            "items.primary.qty",
            "items.secondary.sku",
            "items.secondary.qty",
        ],
        &[
            vec![
                "o001",
                "Alice",
                "alice@example.com",
                "Tokyo",
                "p001",
                "2",
                "p002",
                "1",
            ],
            vec![
                "o002",
                "Bob",
                "bob@example.com",
                "Osaka",
                "p003",
                "4",
                "p004",
                "3",
            ],
        ],
    );
    let output =
        transform_input(&rule, InputData::Bytes(&input), None).expect("transform excel input");
    assert_eq!(
        output,
        serde_json::json!([
            {
                "document": {
                    "order": { "id": "o001" },
                    "buyer": { "name": "Alice", "email": "alice@example.com" },
                    "shipping": { "address": { "city": "Tokyo" } },
                    "items": {
                        "primary": { "sku": "p001", "qty": "2" },
                        "secondary": { "sku": "p002", "qty": "1" }
                    }
                }
            },
            {
                "document": {
                    "order": { "id": "o002" },
                    "buyer": { "name": "Bob", "email": "bob@example.com" },
                    "shipping": { "address": { "city": "Osaka" } },
                    "items": {
                        "primary": { "sku": "p003", "qty": "4" },
                        "secondary": { "sku": "p004", "qty": "3" }
                    }
                }
            }
        ])
    );
}
