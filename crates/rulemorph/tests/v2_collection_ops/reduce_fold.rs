use serde_json::json;

use crate::run_ok;

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
