use rulemorph::{parse_rule_file, transform};
use serde_json::json;

const PERF_RULES: &str = r#"
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

#[test]
fn performance_workload_smoke_still_transforms_lookup_records() {
    let record_count = 1_000usize;
    let user_count = 100usize;
    let tag_count = 100usize;

    let rule = parse_rule_file(PERF_RULES).expect("failed to parse perf rules");
    let input = build_input(record_count, user_count, tag_count);
    let context = build_context(user_count, tag_count);

    let output = transform(&rule, &input, Some(&context)).expect("transform failed");
    let records = output.as_array().expect("output should be an array");

    assert_eq!(records.len(), record_count);
    assert_eq!(records[0]["user_name"], "user-0");
    assert_eq!(records[0]["tags"], json!(["tag-0"]));
    assert_eq!(records[999]["user_name"], "user-99");
    assert_eq!(records[999]["tags"], json!(["tag-99"]));
}

#[test]
fn indexed_lookup_preserves_duplicate_and_missing_output_semantics() {
    let rule = parse_rule_file(
        r#"
version: 1
input:
  format: json
  json: {}
mappings:
  - target: "first"
    expr:
      op: "lookup_first"
      args:
        - { ref: "context.items" }
        - "id"
        - { ref: "input.id" }
        - "value"
  - target: "all"
    expr:
      op: "lookup"
      args:
        - { ref: "context.items" }
        - "id"
        - { ref: "input.id" }
        - "value"
"#,
    )
    .expect("failed to parse lookup rules");
    let input = serde_json::to_string(&vec![
        json!({ "id": "a" }),
        json!({ "id": "b" }),
        json!({ "id": "c" }),
    ])
    .expect("failed to serialize input");
    let context = json!({
        "items": [
            { "id": "a", "value": "first-a" },
            { "id": "a" },
            { "id": "a", "value": "second-a" },
            { "id": "b" },
            { "id": "b", "value": "only-b" },
            { "id": "c" }
        ]
    });

    let output = transform(&rule, &input, Some(&context)).expect("transform failed");

    assert_eq!(
        output,
        json!([
            { "first": "first-a", "all": ["first-a", "second-a"] },
            { "first": "only-b", "all": ["only-b"] },
            {}
        ])
    );
}

fn build_context(user_count: usize, tag_count: usize) -> serde_json::Value {
    let mut users = Vec::with_capacity(user_count);
    for i in 0..user_count {
        users.push(json!({
            "id": i as i64,
            "name": format!("user-{}", i),
            "role": "member"
        }));
    }

    let mut tags = Vec::with_capacity(tag_count);
    for i in 0..tag_count {
        tags.push(json!({
            "id": format!("t{}", i),
            "value": format!("tag-{}", i)
        }));
    }

    json!({
        "users": users,
        "tags": tags
    })
}

fn build_input(record_count: usize, user_count: usize, tag_count: usize) -> String {
    let mut records = Vec::with_capacity(record_count);
    for i in 0..record_count {
        records.push(json!({
            "id": i as i64,
            "user_id": (i % user_count) as i64,
            "tag_id": format!("t{}", i % tag_count),
        }));
    }

    serde_json::to_string(&records).expect("failed to serialize input")
}
