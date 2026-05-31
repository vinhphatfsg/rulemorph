#![allow(dead_code)]

use criterion::Throughput;
use rulemorph::{RuleFile, TransformStream, parse_rule_file};
use serde_json::{Value as JsonValue, json};

pub const SIMPLE_RULES: &str = r#"
version: 1
input:
  format: json
  json: {}
mappings:
  - target: "id"
    source: "input.id"
  - target: "name"
    source: "input.name"
  - target: "price"
    source: "input.price"
    type: "float"
"#;

pub const LOOKUP_RULES: &str = r#"
version: 1
input:
  format: json
  json: {}
mappings:
  - target: "id"
    source: "input.id"
  - target: "user_name"
    expr:
      op: "lookup_first"
      args:
        - { ref: "context.users" }
        - "id"
        - { ref: "input.user_id" }
        - "name"
  - target: "tags"
    expr:
      op: "lookup"
      args:
        - { ref: "context.tags" }
        - "id"
        - { ref: "input.tag_id" }
        - "value"
"#;

pub const V2_COLLECTION_RULES: &str = r#"
version: 2
input:
  format: json
mappings:
  - target: "id"
    source: "input.id"
  - target: "active_total"
    expr:
      - "@input.items"
      - filter: ["@item.active"]
      - map:
          - "@item.amount"
      - sum
"#;

pub fn parse_rule(source: &str) -> RuleFile {
    parse_rule_file(source).expect("benchmark rule should parse")
}

pub fn simple_input(record_count: usize) -> String {
    let records: Vec<JsonValue> = (0..record_count)
        .map(|i| {
            json!({
                "id": i as i64,
                "name": format!("item-{}", i),
                "price": (i % 100) as f64 + 0.5,
            })
        })
        .collect();
    serde_json::to_string(&records).expect("failed to serialize simple input")
}

pub fn lookup_input(record_count: usize, user_count: usize, tag_count: usize) -> String {
    let records: Vec<JsonValue> = (0..record_count)
        .map(|i| {
            json!({
                "id": i as i64,
                "user_id": (i % user_count) as i64,
                "tag_id": format!("t{}", i % tag_count),
            })
        })
        .collect();
    serde_json::to_string(&records).expect("failed to serialize lookup input")
}

pub fn lookup_context(user_count: usize, tag_count: usize) -> JsonValue {
    let users: Vec<JsonValue> = (0..user_count)
        .map(|i| json!({ "id": i as i64, "name": format!("user-{}", i) }))
        .collect();
    let tags: Vec<JsonValue> = (0..tag_count)
        .map(|i| json!({ "id": format!("t{}", i), "value": format!("tag-{}", i) }))
        .collect();
    json!({ "users": users, "tags": tags })
}

pub fn v2_collection_input(record_count: usize, item_count: usize) -> String {
    let records: Vec<JsonValue> = (0..record_count)
        .map(|i| {
            let items: Vec<JsonValue> = (0..item_count)
                .map(|j| json!({ "active": j % 2 == 0, "amount": ((i + j) % 10) as i64 }))
                .collect();
            json!({ "id": i as i64, "items": items })
        })
        .collect();
    serde_json::to_string(&records).expect("failed to serialize v2 input")
}

pub fn extended_input(record_count: usize) -> String {
    let records: Vec<JsonValue> = (0..record_count)
        .map(|_| {
            json!({
                "text": "abc-123-abc",
                "regex_text": "a1b2c3",
                "csv": "a,b,c",
                "pad": "7",
                "num_a": 80.6,
                "num_b": "2.5",
                "num_c": 3,
                "base_value": 255,
                "date_simple": "2024-01-02 03:04:05",
                "date_tz": "2024-01-02T03:04:05+09:00",
                "unix_s": "1970-01-01T00:00:01Z",
                "unix_ms": "1970-01-01T00:00:00.123Z"
            })
        })
        .collect();
    serde_json::to_string(&records).expect("failed to serialize extended input")
}

pub fn csv_input(record_count: usize) -> String {
    let mut input = String::from("id,name,price\n");
    for i in 0..record_count {
        input.push_str(&format!("{},item-{},{:.1}\n", i, i, (i % 100) as f64 + 0.5));
    }
    input
}

pub fn drain_stream(stream: TransformStream<'_>) -> usize {
    let mut count = 0usize;
    for item in stream {
        let item = item.expect("stream item should transform");
        if item.output.is_some() {
            count += 1;
        }
        std::hint::black_box(item.warnings);
    }
    count
}

pub fn elements(records: usize) -> Throughput {
    Throughput::Elements(records as u64)
}

pub fn bytes(input: &str) -> Throughput {
    Throughput::Bytes(input.len() as u64)
}
