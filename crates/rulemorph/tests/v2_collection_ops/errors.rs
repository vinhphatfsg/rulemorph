use crate::{TransformErrorKind, run_err};

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
