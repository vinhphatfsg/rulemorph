use super::*;
use serde_json::json;

#[test]
fn test_eval_let_single_binding() {
    let let_step = V2LetStep {
        bindings: vec![(
            "x".to_string(),
            V2Expr::Pipe(V2Pipe {
                start: V2Start::Literal(json!(42)),
                steps: vec![],
            }),
        )],
    };
    let record = json!({});
    let out = json!({});
    let ctx = V2EvalContext::new();
    let result = eval_v2_let_step(
        &let_step,
        EvalValue::Value(json!("pipe_value")),
        &record,
        None,
        &out,
        "test",
        &ctx,
    );
    assert!(result.is_ok());
    let new_ctx = result.unwrap();
    assert_eq!(
        new_ctx.resolve_local("x"),
        Some(&EvalValue::Value(json!(42)))
    );
}

#[test]
fn test_eval_let_multiple_bindings() {
    let let_step = V2LetStep {
        bindings: vec![
            (
                "a".to_string(),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(1)),
                    steps: vec![],
                }),
            ),
            (
                "b".to_string(),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(2)),
                    steps: vec![],
                }),
            ),
        ],
    };
    let record = json!({});
    let out = json!({});
    let ctx = V2EvalContext::new();
    let result = eval_v2_let_step(
        &let_step,
        EvalValue::Value(json!("pipe")),
        &record,
        None,
        &out,
        "test",
        &ctx,
    );
    assert!(result.is_ok());
    let new_ctx = result.unwrap();
    assert_eq!(
        new_ctx.resolve_local("a"),
        Some(&EvalValue::Value(json!(1)))
    );
    assert_eq!(
        new_ctx.resolve_local("b"),
        Some(&EvalValue::Value(json!(2)))
    );
}

#[test]
fn test_eval_let_binding_uses_pipe_value() {
    // let: { x: $ } should bind x to current pipe value
    let let_step = V2LetStep {
        bindings: vec![(
            "x".to_string(),
            V2Expr::Pipe(V2Pipe {
                start: V2Start::PipeValue,
                steps: vec![],
            }),
        )],
    };
    let record = json!({});
    let out = json!({});
    let ctx = V2EvalContext::new();
    let result = eval_v2_let_step(
        &let_step,
        EvalValue::Value(json!(100)),
        &record,
        None,
        &out,
        "test",
        &ctx,
    );
    assert!(result.is_ok());
    let new_ctx = result.unwrap();
    assert_eq!(
        new_ctx.resolve_local("x"),
        Some(&EvalValue::Value(json!(100)))
    );
}

#[test]
fn test_eval_let_binding_from_input() {
    let let_step = V2LetStep {
        bindings: vec![(
            "name".to_string(),
            V2Expr::Pipe(V2Pipe {
                start: V2Start::Ref(V2Ref::Input("user.name".to_string())),
                steps: vec![],
            }),
        )],
    };
    let record = json!({"user": {"name": "Alice"}});
    let out = json!({});
    let ctx = V2EvalContext::new();
    let result = eval_v2_let_step(
        &let_step,
        EvalValue::Value(json!("ignored")),
        &record,
        None,
        &out,
        "test",
        &ctx,
    );
    assert!(result.is_ok());
    let new_ctx = result.unwrap();
    assert_eq!(
        new_ctx.resolve_local("name"),
        Some(&EvalValue::Value(json!("Alice")))
    );
}

#[test]
fn test_eval_let_binding_chain() {
    // let: { x: 10, y: @x } - y should be able to reference x
    let let_step = V2LetStep {
        bindings: vec![
            (
                "x".to_string(),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(10)),
                    steps: vec![],
                }),
            ),
            (
                "y".to_string(),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Ref(V2Ref::Local("x".to_string())),
                    steps: vec![],
                }),
            ),
        ],
    };
    let record = json!({});
    let out = json!({});
    let ctx = V2EvalContext::new();
    let result = eval_v2_let_step(
        &let_step,
        EvalValue::Value(json!("pipe")),
        &record,
        None,
        &out,
        "test",
        &ctx,
    );
    assert!(result.is_ok());
    let new_ctx = result.unwrap();
    assert_eq!(
        new_ctx.resolve_local("x"),
        Some(&EvalValue::Value(json!(10)))
    );
    assert_eq!(
        new_ctx.resolve_local("y"),
        Some(&EvalValue::Value(json!(10)))
    );
}

#[test]
fn test_eval_pipe_with_let() {
    // [100, { let: { x: $ } }, @x] -> 100
    let pipe = V2Pipe {
        start: V2Start::Literal(json!(100)),
        steps: vec![V2Step::Let(V2LetStep {
            bindings: vec![(
                "x".to_string(),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::PipeValue,
                    steps: vec![],
                }),
            )],
        })],
    };
    let record = json!({});
    let out = json!({});
    let ctx = V2EvalContext::new();
    let result = eval_v2_pipe(&pipe, &record, None, &out, "test", &ctx);
    // Let step doesn't change pipe value
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(100)));
}

#[test]
fn test_eval_pipe_let_then_op() {
    // [100, { let: { factor: 2 } }, { op: "multiply", args: [@factor] }] -> 200
    let pipe = V2Pipe {
        start: V2Start::Literal(json!(100)),
        steps: vec![
            V2Step::Let(V2LetStep {
                bindings: vec![(
                    "factor".to_string(),
                    V2Expr::Pipe(V2Pipe {
                        start: V2Start::Literal(json!(2)),
                        steps: vec![],
                    }),
                )],
            }),
            V2Step::Op(V2OpStep {
                op: "multiply".to_string(),
                args: vec![V2Expr::Pipe(V2Pipe {
                    start: V2Start::Ref(V2Ref::Local("factor".to_string())),
                    steps: vec![],
                })],
            }),
        ],
    };
    let record = json!({});
    let out = json!({});
    let ctx = V2EvalContext::new();
    let result = eval_v2_pipe(&pipe, &record, None, &out, "test", &ctx);
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(200.0)));
}
