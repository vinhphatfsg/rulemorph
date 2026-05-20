use serde_json::json;

use crate::run_ok;

#[test]
fn v2_collection_missing_pipe_semantics_are_characterized() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "map_default"
    expr:
      - "@input.missing"
      - map: ["@item.value"]
    default: ["default"]
  - target: "filter_empty"
    expr:
      - "@input.missing"
      - filter: ["@item.active"]
  - target: "flat_map_empty"
    expr:
      - "@input.missing"
      - flat_map: ["@item.tags"]
  - target: "group_by_empty"
    expr:
      - "@input.missing"
      - group_by: ["@item.role"]
  - target: "key_by_empty"
    expr:
      - "@input.missing"
      - key_by: ["@item.id"]
  - target: "partition_empty"
    expr:
      - "@input.missing"
      - partition: ["@item.active"]
  - target: "distinct_by_empty"
    expr:
      - "@input.missing"
      - distinct_by: ["@item.role"]
  - target: "sort_by_empty"
    expr:
      - "@input.missing"
      - sort_by: ["@item.score"]
  - target: "find_null"
    expr:
      - "@input.missing"
      - find: ["@item.active"]
  - target: "find_index_minus_one"
    expr:
      - "@input.missing"
      - find_index: ["@item.active"]
  - target: "reduce_null"
    expr:
      - "@input.missing"
      - reduce: [["@acc", { "+": "@item" }]]
  - target: "fold_initial"
    expr:
      - "@input.missing"
      - fold:
          - 10
          - ["@acc", { "+": "@item" }]
  - target: "zip_with_empty"
    expr:
      - "@input.missing"
      - zip_with:
          - "@input.other"
          - "@item"
"#;
    assert_eq!(
        run_ok(yaml, r#"[{"other":[1,2]}]"#),
        json!([{
            "map_default": ["default"],
            "filter_empty": [],
            "flat_map_empty": [],
            "group_by_empty": {},
            "key_by_empty": {},
            "partition_empty": [[], []],
            "distinct_by_empty": [],
            "sort_by_empty": [],
            "find_null": null,
            "find_index_minus_one": -1,
            "reduce_null": null,
            "fold_initial": 10,
            "zip_with_empty": []
        }])
    );
}
