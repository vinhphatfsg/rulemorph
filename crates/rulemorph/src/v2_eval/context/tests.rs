use super::*;
use crate::transform::EvalLimits;
use crate::v2_eval::eval_v2_pipe;
use crate::v2_parser::parse_v2_pipe_from_value;
use serde_json::json;

#[test]
fn test_context_new() {
    let ctx = V2EvalContext::new();
    assert!(ctx.get_pipe_value().is_none());
    assert!(ctx.resolve_local("x").is_none());
    assert!(ctx.get_item().is_none());
    assert!(ctx.get_acc().is_none());
}

#[test]
fn test_context_with_pipe_value() {
    let ctx = V2EvalContext::new().with_pipe_value(EvalValue::Value(json!(42)));
    assert!(ctx.get_pipe_value().is_some());
    assert_eq!(ctx.get_pipe_value(), Some(&EvalValue::Value(json!(42))));
}

#[test]
fn test_context_with_let_binding() {
    let ctx =
        V2EvalContext::new().with_let_binding("x".to_string(), EvalValue::Value(json!(100)));
    assert!(ctx.resolve_local("x").is_some());
    assert_eq!(ctx.resolve_local("x"), Some(&EvalValue::Value(json!(100))));
    assert!(ctx.resolve_local("y").is_none());
}

#[test]
fn test_context_with_multiple_let_bindings() {
    let ctx = V2EvalContext::new().with_let_bindings(vec![
        ("a".to_string(), EvalValue::Value(json!(1))),
        ("b".to_string(), EvalValue::Value(json!(2))),
    ]);
    assert!(ctx.resolve_local("a").is_some());
    assert!(ctx.resolve_local("b").is_some());
    assert!(ctx.resolve_local("c").is_none());
}

#[test]
fn test_context_scope_chain() {
    let ctx = V2EvalContext::new().with_let_binding("x".to_string(), EvalValue::Value(json!(1)));
    let inner_ctx = ctx
        .clone()
        .with_let_binding("y".to_string(), EvalValue::Value(json!(2)));

    assert!(inner_ctx.resolve_local("x").is_some());
    assert!(inner_ctx.resolve_local("y").is_some());

    assert!(ctx.resolve_local("x").is_some());
    assert!(ctx.resolve_local("y").is_none());
}

#[test]
fn test_context_with_item() {
    let item_value = json!({"name": "test"});
    let ctx = V2EvalContext::new().with_item(EvalItem {
        value: &item_value,
        index: 0,
    });
    assert!(ctx.has_item_scope());
    assert!(ctx.get_item().is_some());
    let item = ctx.get_item().unwrap();
    assert_eq!(item.value, &json!({"name": "test"}));
    assert_eq!(item.index, 0);
}

#[test]
fn test_context_with_acc() {
    let acc_value = json!(0);
    let ctx = V2EvalContext::new().with_acc(&acc_value);
    assert!(ctx.has_acc_scope());
    assert!(ctx.get_acc().is_some());
    assert_eq!(ctx.get_acc(), Some(&json!(0)));
}

#[test]
fn test_eval_value_is_missing() {
    assert!(EvalValue::Missing.is_missing());
    assert!(!EvalValue::Value(json!(null)).is_missing());
}

#[test]
fn test_eval_value_into_value() {
    assert_eq!(EvalValue::Missing.into_value(), None);
    assert_eq!(
        EvalValue::Value(json!("hello")).into_value(),
        Some(json!("hello"))
    );
}

#[test]
fn test_eval_value_as_value() {
    let missing = EvalValue::Missing;
    let val = EvalValue::Value(json!(42));
    assert!(missing.as_value().is_none());
    assert_eq!(val.as_value(), Some(&json!(42)));
}

#[test]
fn test_context_preserves_pipe_value_after_let() {
    let ctx = V2EvalContext::new()
        .with_pipe_value(EvalValue::Value(json!(100)))
        .with_let_binding("x".to_string(), EvalValue::Value(json!(50)));

    assert_eq!(ctx.get_pipe_value(), Some(&EvalValue::Value(json!(100))));
    assert_eq!(ctx.resolve_local("x"), Some(&EvalValue::Value(json!(50))));
}

#[test]
fn test_custom_op_counter_resets_for_public_eval_calls() {
    let rule = crate::parse_rule_file(
        r#"
version: 2
input:
  format: json
  json: {}
defs:
  id:
    input: int
    returns: int
    expr: "$"
mappings: []
"#,
    )
    .expect("rule parses");
    let pipe = parse_v2_pipe_from_value(&json!(["@input.n", "id"])).expect("pipe parses");
    let ctx = V2EvalContext::new()
        .with_limits(EvalLimits {
            max_custom_op_calls_per_record: 1,
            ..EvalLimits::default()
        })
        .with_rule(&rule);
    let out = json!({});

    for n in [1, 2] {
        let record = json!({ "n": n });
        let result =
            eval_v2_pipe(&pipe, &record, None, &out, "expr", &ctx).expect("eval stays in limit");
        assert_eq!(result, EvalValue::Value(json!(n)));
    }
}
