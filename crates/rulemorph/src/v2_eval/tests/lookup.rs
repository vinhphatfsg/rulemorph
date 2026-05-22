use super::*;
use serde_json::{Value as JsonValue, json};

fn make_departments() -> JsonValue {
    json!([
        {"id": 1, "name": "Engineering", "budget": 100000},
        {"id": 2, "name": "Sales", "budget": 50000},
        {"id": 3, "name": "HR", "budget": 30000}
    ])
}

include!("lookup/first.rs");

#[test]
fn test_lookup_all_matches() {
    // lookup (not lookup_first) returns all matches
    let employees = json!([
        {"name": "Alice", "dept": "Engineering"},
        {"name": "Bob", "dept": "Sales"},
        {"name": "Charlie", "dept": "Engineering"},
        {"name": "Diana", "dept": "HR"}
    ]);
    let op = V2OpStep {
        op: "lookup".to_string(),
        args: vec![
            V2Expr::Pipe(V2Pipe {
                start: V2Start::Ref(V2Ref::Context("employees".to_string())),
                steps: vec![],
            }),
            V2Expr::Pipe(V2Pipe {
                start: V2Start::Literal(json!("dept")),
                steps: vec![],
            }),
            V2Expr::Pipe(V2Pipe {
                start: V2Start::Literal(json!("Engineering")),
                steps: vec![],
            }),
            V2Expr::Pipe(V2Pipe {
                start: V2Start::Literal(json!("name")),
                steps: vec![],
            }),
        ],
    };
    let record = json!({});
    let context = json!({"employees": employees});
    let out = json!({});
    let ctx = V2EvalContext::new();
    let result = eval_v2_op_step(
        &op,
        EvalValue::Value(json!(null)),
        &record,
        Some(&context),
        &out,
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(["Alice", "Charlie"])));
}

#[test]
fn test_lookup_skips_matches_missing_get_field() {
    let employees = json!([
        {"name": "Alice", "dept": "Engineering"},
        {"dept": "Engineering"},
        {"name": "Charlie", "dept": "Engineering"}
    ]);
    let op = V2OpStep {
        op: "lookup".to_string(),
        args: vec![
            V2Expr::Pipe(V2Pipe {
                start: V2Start::Ref(V2Ref::Context("employees".to_string())),
                steps: vec![],
            }),
            V2Expr::Pipe(V2Pipe {
                start: V2Start::Literal(json!("dept")),
                steps: vec![],
            }),
            V2Expr::Pipe(V2Pipe {
                start: V2Start::Literal(json!("Engineering")),
                steps: vec![],
            }),
            V2Expr::Pipe(V2Pipe {
                start: V2Start::Literal(json!("name")),
                steps: vec![],
            }),
        ],
    };
    let record = json!({});
    let context = json!({"employees": employees});
    let out = json!({});
    let ctx = V2EvalContext::new();
    let result = eval_v2_op_step(
        &op,
        EvalValue::Value(json!(null)),
        &record,
        Some(&context),
        &out,
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(["Alice", "Charlie"])));
}

#[test]
fn test_lookup_no_matches() {
    let op = V2OpStep {
        op: "lookup".to_string(),
        args: vec![
            V2Expr::Pipe(V2Pipe {
                start: V2Start::Ref(V2Ref::Context("departments".to_string())),
                steps: vec![],
            }),
            V2Expr::Pipe(V2Pipe {
                start: V2Start::Literal(json!("id")),
                steps: vec![],
            }),
            V2Expr::Pipe(V2Pipe {
                start: V2Start::Literal(json!(999)),
                steps: vec![],
            }),
        ],
    };
    let record = json!({});
    let context = json!({"departments": make_departments()});
    let out = json!({});
    let ctx = V2EvalContext::new();
    let result = eval_v2_op_step(
        &op,
        EvalValue::Value(json!(null)),
        &record,
        Some(&context),
        &out,
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!([])));
}

#[test]
fn test_lookup_missing_match_value_does_not_match_null() {
    let users = json!([
        {"id": null, "name": "MissingUser"},
        {"id": 1, "name": "Alice"}
    ]);
    let op = V2OpStep {
        op: "lookup".to_string(),
        args: vec![
            V2Expr::Pipe(V2Pipe {
                start: V2Start::Ref(V2Ref::Context("users".to_string())),
                steps: vec![],
            }),
            V2Expr::Pipe(V2Pipe {
                start: V2Start::Literal(json!("id")),
                steps: vec![],
            }),
            V2Expr::Pipe(V2Pipe {
                start: V2Start::Ref(V2Ref::Input("user_id".to_string())),
                steps: vec![],
            }),
            V2Expr::Pipe(V2Pipe {
                start: V2Start::Literal(json!("name")),
                steps: vec![],
            }),
        ],
    };
    let record = json!({});
    let context = json!({"users": users});
    let out = json!({});
    let ctx = V2EvalContext::new();
    let result = eval_v2_op_step(
        &op,
        EvalValue::Value(json!(null)),
        &record,
        Some(&context),
        &out,
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Missing)));
}
