use rulemorph::{TransformErrorKind, parse_rule_file, transform, transform_with_warnings};
use serde_json::json;

fn run_ok(yaml: &str, input: &str) -> serde_json::Value {
    let rule = parse_rule_file(yaml).expect("parse rule");
    let (output, warnings) =
        transform_with_warnings(&rule, input, None).expect("transform should succeed");
    assert!(warnings.is_empty(), "unexpected warnings: {warnings:?}");
    output
}

fn run_err(yaml: &str, input: &str) -> rulemorph::TransformError {
    let rule = parse_rule_file(yaml).expect("parse rule");
    transform(&rule, input, None).expect_err("transform should fail")
}

#[test]
fn v2_zip_with_uses_all_arrays_and_last_arg_as_expression() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "rows"
    expr:
      - "@input.left"
      - zip_with:
          - "@input.right"
          - "@input.extra"
          - "@item"
  - target: "indexes"
    expr:
      - "@input.left"
      - zip_with:
          - "@input.right"
          - "@item.index"
"#;
    let input = r#"[{
        "left": [1, 2, 3],
        "right": ["a", "b"],
        "extra": [true, false, true]
    }]"#;

    assert_eq!(
        run_ok(yaml, input),
        json!([{
            "rows": [[1, "a", true], [2, "b", false]],
            "indexes": [0, 1]
        }])
    );
}

#[test]
fn v2_zip_with_rejects_non_array_argument_with_arg_path() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "rows"
    expr:
      - "@input.left"
      - zip_with:
          - "@input.not_array"
          - "@item"
"#;
    let err = run_err(yaml, r#"[{"left":[1], "not_array": "x"}]"#);

    assert_eq!(err.kind, TransformErrorKind::ExprError);
    assert_eq!(err.path.as_deref(), Some("mappings[0].expr[1].args[0]"));
    assert_eq!(err.message, "expr arg must be an array");
}

#[test]
fn v2_collection_representative_errors_keep_paths() {
    let arity_yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "out"
    expr:
      - "@input.items"
      - filter: []
"#;
    let err = run_err(arity_yaml, r#"[{"items":[]}]"#);
    assert_eq!(err.kind, TransformErrorKind::ExprError);
    assert_eq!(err.path.as_deref(), Some("mappings[0].expr[1]"));
    assert_eq!(err.message, "filter requires exactly one argument");

    let non_array_yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "out"
    expr:
      - "@input.items"
      - filter: ["@item.active"]
"#;
    let err = run_err(non_array_yaml, r#"[{"items":"not an array"}]"#);
    assert_eq!(err.kind, TransformErrorKind::ExprError);
    assert_eq!(err.path.as_deref(), Some("mappings[0].expr[1]"));
    assert_eq!(err.message, "expr arg must be an array");

    let predicate_yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "out"
    expr:
      - "@input.items"
      - filter: ["@item.name"]
"#;
    let err = run_err(predicate_yaml, r#"[{"items":[{"name":"Alice"}]}]"#);
    assert_eq!(err.kind, TransformErrorKind::ExprError);
    assert_eq!(err.path.as_deref(), Some("mappings[0].expr[1].args[0]"));
    assert_eq!(err.message, "value must be a boolean");
}

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

#[test]
fn v2_sort_by_order_type_and_stable_ties_are_characterized() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "score_desc"
    expr:
      - "@input.users"
      - sort_by:
          - "@item.score"
          - desc
  - target: "name_asc"
    expr:
      - "@input.users"
      - sort_by: ["@item.name"]
  - target: "flag_asc"
    expr:
      - "@input.users"
      - sort_by: ["@item.flag"]
"#;
    let input = r#"[{
        "users": [
            {"id":"a","score":2,"name":"Bob","flag":true},
            {"id":"b","score":1,"name":"Alice","flag":false},
            {"id":"c","score":2,"name":"Carol","flag":true}
        ]
    }]"#;

    assert_eq!(
        run_ok(yaml, input),
        json!([{
            "score_desc": [
                {"id":"a","score":2,"name":"Bob","flag":true},
                {"id":"c","score":2,"name":"Carol","flag":true},
                {"id":"b","score":1,"name":"Alice","flag":false}
            ],
            "name_asc": [
                {"id":"b","score":1,"name":"Alice","flag":false},
                {"id":"a","score":2,"name":"Bob","flag":true},
                {"id":"c","score":2,"name":"Carol","flag":true}
            ],
            "flag_asc": [
                {"id":"b","score":1,"name":"Alice","flag":false},
                {"id":"a","score":2,"name":"Bob","flag":true},
                {"id":"c","score":2,"name":"Carol","flag":true}
            ]
        }])
    );
}

#[test]
fn v2_sort_by_invalid_order_and_mixed_key_errors_keep_paths() {
    let invalid_order_yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "out"
    expr:
      - "@input.users"
      - sort_by:
          - "@item.score"
          - sideways
"#;
    let err = run_err(invalid_order_yaml, r#"[{"users":[{"score":1}]}]"#);
    assert_eq!(err.kind, TransformErrorKind::ExprError);
    assert_eq!(err.path.as_deref(), Some("mappings[0].expr[1].args[1]"));
    assert_eq!(err.message, "order must be asc or desc");

    let mixed_key_yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "out"
    expr:
      - "@input.users"
      - sort_by: ["@item.key"]
"#;
    let err = run_err(mixed_key_yaml, r#"[{"users":[{"key":1},{"key":"two"}]}]"#);
    assert_eq!(err.kind, TransformErrorKind::ExprError);
    assert_eq!(err.path.as_deref(), Some("mappings[0].expr[1].args[0]"));
    assert_eq!(err.message, "sort_by keys must be all the same type");
}

#[test]
fn v2_reduce_and_fold_preserve_acc_and_item_scope() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "reduce_indexes"
    expr:
      - "@input.numbers"
      - reduce:
          - ["@acc", { "+": "@item.index" }]
  - target: "fold_values"
    expr:
      - "@input.objects"
      - fold:
          - 100
          - ["@acc", { "+": "@item.value.amount" }]
"#;
    let input = r#"[{"numbers":[1,2,3], "objects":[{"amount":5},{"amount":7},{"amount":9}]}]"#;

    assert_eq!(
        run_ok(yaml, input),
        json!([{ "reduce_indexes": 4.0, "fold_values": 121.0 }])
    );
}
