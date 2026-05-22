use serde_json::json;

use crate::{TransformErrorKind, run_err, run_ok};

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
