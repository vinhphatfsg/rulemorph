use serde_json::json;

use crate::{TransformErrorKind, run_err, run_ok};

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
