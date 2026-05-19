use super::*;
use serde_json::{Value as JsonValue, json};

fn lit(value: JsonValue) -> V2Expr {
    V2Expr::Pipe(V2Pipe {
        start: V2Start::Literal(value),
        steps: vec![],
    })
}

#[test]
fn test_eval_op_trim() {
    let op = V2OpStep {
        op: "trim".to_string(),
        args: vec![],
    };
    let ctx = V2EvalContext::new();
    let result = eval_v2_op_step(
        &op,
        EvalValue::Value(json!("  hello  ")),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("hello")));
}

#[test]
fn test_eval_op_lowercase() {
    let op = V2OpStep {
        op: "lowercase".to_string(),
        args: vec![],
    };
    let ctx = V2EvalContext::new();
    let result = eval_v2_op_step(
        &op,
        EvalValue::Value(json!("HELLO")),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("hello")));
}

#[test]
fn test_eval_op_uppercase() {
    let op = V2OpStep {
        op: "uppercase".to_string(),
        args: vec![],
    };
    let ctx = V2EvalContext::new();
    let result = eval_v2_op_step(
        &op,
        EvalValue::Value(json!("hello")),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("HELLO")));
}

#[test]
fn test_eval_op_to_string() {
    let op = V2OpStep {
        op: "to_string".to_string(),
        args: vec![],
    };
    let ctx = V2EvalContext::new();

    // Number to string
    let result = eval_v2_op_step(
        &op,
        EvalValue::Value(json!(42)),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("42")));

    // Bool to string
    let result = eval_v2_op_step(
        &op,
        EvalValue::Value(json!(true)),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("true")));
}

#[test]
fn test_eval_op_replace() {
    let op = V2OpStep {
        op: "replace".to_string(),
        args: vec![lit(json!("world")), lit(json!("there"))],
    };
    let ctx = V2EvalContext::new();
    let result = eval_v2_op_step(
        &op,
        EvalValue::Value(json!("hello world")),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("hello there")));
}

#[test]
fn test_eval_op_split_and_pad() {
    let split = V2OpStep {
        op: "split".to_string(),
        args: vec![lit(json!(","))],
    };
    let pad_start = V2OpStep {
        op: "pad_start".to_string(),
        args: vec![lit(json!(3)), lit(json!("0"))],
    };
    let pad_end = V2OpStep {
        op: "pad_end".to_string(),
        args: vec![lit(json!(3)), lit(json!("0"))],
    };
    let ctx = V2EvalContext::new();

    let split_result = eval_v2_op_step(
        &split,
        EvalValue::Value(json!("a,b,c")),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(
        split_result,
        Ok(EvalValue::Value(v)) if v == json!(["a", "b", "c"])
    ));

    let pad_start_result = eval_v2_op_step(
        &pad_start,
        EvalValue::Value(json!("7")),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(pad_start_result, Ok(EvalValue::Value(v)) if v == json!("007")));

    let pad_end_result = eval_v2_op_step(
        &pad_end,
        EvalValue::Value(json!("7")),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(pad_end_result, Ok(EvalValue::Value(v)) if v == json!("700")));
}

#[test]
fn test_eval_op_round_and_to_base() {
    let round = V2OpStep {
        op: "round".to_string(),
        args: vec![lit(json!(2))],
    };
    let to_base = V2OpStep {
        op: "to_base".to_string(),
        args: vec![lit(json!(2))],
    };
    let ctx = V2EvalContext::new();

    let rounded = eval_v2_op_step(
        &round,
        EvalValue::Value(json!(1.2345)),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    )
    .unwrap();
    if let EvalValue::Value(v) = rounded {
        let value = v.as_f64().unwrap();
        assert!((value - 1.23).abs() < 1e-9);
    } else {
        panic!("expected rounded value");
    }

    let base = eval_v2_op_step(
        &to_base,
        EvalValue::Value(json!(10)),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(base, Ok(EvalValue::Value(v)) if v == json!("1010")));
}

#[test]
fn test_eval_op_json_merge() {
    let op = V2OpStep {
        op: "merge".to_string(),
        args: vec![lit(json!({"b": 2}))],
    };
    let ctx = V2EvalContext::new();
    let result = eval_v2_op_step(
        &op,
        EvalValue::Value(json!({"a": 1})),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!({"a": 1, "b": 2})));
}

#[test]
fn test_eval_op_array_map_and_reduce() {
    let map_expr = V2Expr::Pipe(V2Pipe {
        start: V2Start::Ref(V2Ref::Item(String::new())),
        steps: vec![V2Step::Op(V2OpStep {
            op: "add".to_string(),
            args: vec![lit(json!(1))],
        })],
    });
    let map = V2OpStep {
        op: "map".to_string(),
        args: vec![map_expr],
    };
    let reduce_expr = V2Expr::Pipe(V2Pipe {
        start: V2Start::Ref(V2Ref::Acc(String::new())),
        steps: vec![V2Step::Op(V2OpStep {
            op: "add".to_string(),
            args: vec![V2Expr::Pipe(V2Pipe {
                start: V2Start::Ref(V2Ref::Item(String::new())),
                steps: vec![],
            })],
        })],
    });
    let reduce = V2OpStep {
        op: "reduce".to_string(),
        args: vec![reduce_expr],
    };
    let ctx = V2EvalContext::new();

    let map_result = eval_v2_op_step(
        &map,
        EvalValue::Value(json!([1, 2, 3])),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(map_result, Ok(EvalValue::Value(v)) if v == json!([2.0, 3.0, 4.0])));

    let reduce_result = eval_v2_op_step(
        &reduce,
        EvalValue::Value(json!([1, 2, 3])),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(reduce_result, Ok(EvalValue::Value(v)) if v == json!(6.0)));
}

#[test]
fn test_eval_op_first_last() {
    let first = V2OpStep {
        op: "first".to_string(),
        args: vec![],
    };
    let last = V2OpStep {
        op: "last".to_string(),
        args: vec![],
    };
    let ctx = V2EvalContext::new();

    let first_result = eval_v2_op_step(
        &first,
        EvalValue::Value(json!([1, 2])),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(first_result, Ok(EvalValue::Value(v)) if v == json!(1)));

    let last_result = eval_v2_op_step(
        &last,
        EvalValue::Value(json!([1, 2])),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(last_result, Ok(EvalValue::Value(v)) if v == json!(2)));
}

#[test]
fn test_eval_op_type_casts() {
    let op_int = V2OpStep {
        op: "int".to_string(),
        args: vec![],
    };
    let op_float = V2OpStep {
        op: "float".to_string(),
        args: vec![],
    };
    let op_bool = V2OpStep {
        op: "bool".to_string(),
        args: vec![],
    };
    let op_string = V2OpStep {
        op: "string".to_string(),
        args: vec![],
    };
    let ctx = V2EvalContext::new();

    let int_result = eval_v2_op_step(
        &op_int,
        EvalValue::Value(json!("42")),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(int_result, Ok(EvalValue::Value(v)) if v == json!(42)));

    let float_result = eval_v2_op_step(
        &op_float,
        EvalValue::Value(json!("3.14")),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    if let Ok(EvalValue::Value(v)) = float_result {
        let value = v.as_f64().unwrap();
        assert!((value - 3.14).abs() < 1e-9);
    } else {
        panic!("expected float cast");
    }

    let bool_result = eval_v2_op_step(
        &op_bool,
        EvalValue::Value(json!("true")),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(bool_result, Ok(EvalValue::Value(v)) if v == json!(true)));

    let string_result = eval_v2_op_step(
        &op_string,
        EvalValue::Value(json!(12)),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(string_result, Ok(EvalValue::Value(v)) if v == json!("12")));
}

#[test]
fn test_eval_op_and_or_short_circuit() {
    let or_op = V2OpStep {
        op: "or".to_string(),
        args: vec![V2Expr::Pipe(V2Pipe {
            start: V2Start::Literal(json!(1)),
            steps: vec![V2Step::Op(V2OpStep {
                op: "divide".to_string(),
                args: vec![V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(0)),
                    steps: vec![],
                })],
            })],
        })],
    };
    let and_op = V2OpStep {
        op: "and".to_string(),
        args: vec![V2Expr::Pipe(V2Pipe {
            start: V2Start::Literal(json!(1)),
            steps: vec![V2Step::Op(V2OpStep {
                op: "divide".to_string(),
                args: vec![V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(0)),
                    steps: vec![],
                })],
            })],
        })],
    };
    let ctx = V2EvalContext::new();

    let or_result = eval_v2_op_step(
        &or_op,
        EvalValue::Value(json!(true)),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(or_result, Ok(EvalValue::Value(v)) if v == json!(true)));

    let and_result = eval_v2_op_step(
        &and_op,
        EvalValue::Value(json!(false)),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(and_result, Ok(EvalValue::Value(v)) if v == json!(false)));
}

#[test]
fn test_eval_op_add() {
    let op = V2OpStep {
        op: "add".to_string(),
        args: vec![V2Expr::Pipe(V2Pipe {
            start: V2Start::Literal(json!(10)),
            steps: vec![],
        })],
    };
    let ctx = V2EvalContext::new();
    let result = eval_v2_op_step(
        &op,
        EvalValue::Value(json!(5)),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(15.0)));
}

#[test]
fn test_eval_op_subtract() {
    let op = V2OpStep {
        op: "subtract".to_string(),
        args: vec![V2Expr::Pipe(V2Pipe {
            start: V2Start::Literal(json!(3)),
            steps: vec![],
        })],
    };
    let ctx = V2EvalContext::new();
    let result = eval_v2_op_step(
        &op,
        EvalValue::Value(json!(10)),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(7.0)));
}

include!("op_step/comparison.rs");

#[test]
fn test_eval_op_pick_multiple_paths() {
    let op = V2OpStep {
        op: "pick".to_string(),
        args: vec![lit(json!("name")), lit(json!("price"))],
    };
    let ctx = V2EvalContext::new();
    let result = eval_v2_op_step(
        &op,
        EvalValue::Value(json!({"name": "apple", "price": 100, "category": "fruit"})),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(
        result,
        Ok(EvalValue::Value(v)) if v == json!({"name": "apple", "price": 100})
    ));
}

#[test]
fn test_eval_op_omit_multiple_paths() {
    let op = V2OpStep {
        op: "omit".to_string(),
        args: vec![lit(json!("category")), lit(json!("price"))],
    };
    let ctx = V2EvalContext::new();
    let result = eval_v2_op_step(
        &op,
        EvalValue::Value(json!({"name": "apple", "price": 100, "category": "fruit"})),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(
        result,
        Ok(EvalValue::Value(v)) if v == json!({"name": "apple"})
    ));
}

#[test]
fn test_eval_op_pick_paths_array_arg() {
    let op = V2OpStep {
        op: "pick".to_string(),
        args: vec![lit(json!(["name", "price"]))],
    };
    let ctx = V2EvalContext::new();
    let result = eval_v2_op_step(
        &op,
        EvalValue::Value(json!({"name": "apple", "price": 100, "category": "fruit"})),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(
        result,
        Ok(EvalValue::Value(v)) if v == json!({"name": "apple", "price": 100})
    ));
}

#[test]
fn test_eval_op_multiply() {
    let op = V2OpStep {
        op: "multiply".to_string(),
        args: vec![V2Expr::Pipe(V2Pipe {
            start: V2Start::Literal(json!(0.9)),
            steps: vec![],
        })],
    };
    let ctx = V2EvalContext::new();
    let result = eval_v2_op_step(
        &op,
        EvalValue::Value(json!(100)),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(90.0)));
}

#[test]
fn test_eval_op_divide() {
    let op = V2OpStep {
        op: "divide".to_string(),
        args: vec![V2Expr::Pipe(V2Pipe {
            start: V2Start::Literal(json!(2)),
            steps: vec![],
        })],
    };
    let ctx = V2EvalContext::new();
    let result = eval_v2_op_step(
        &op,
        EvalValue::Value(json!(10)),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(5.0)));
}

#[test]
fn test_eval_op_divide_by_zero() {
    let op = V2OpStep {
        op: "divide".to_string(),
        args: vec![V2Expr::Pipe(V2Pipe {
            start: V2Start::Literal(json!(0)),
            steps: vec![],
        })],
    };
    let ctx = V2EvalContext::new();
    let result = eval_v2_op_step(
        &op,
        EvalValue::Value(json!(10)),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(result.is_err());
}

#[test]
fn test_eval_op_coalesce() {
    let op = V2OpStep {
        op: "coalesce".to_string(),
        args: vec![V2Expr::Pipe(V2Pipe {
            start: V2Start::Literal(json!("default")),
            steps: vec![],
        })],
    };
    let ctx = V2EvalContext::new();

    // When pipe value is present, use it
    let result = eval_v2_op_step(
        &op,
        EvalValue::Value(json!("value")),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("value")));

    // When pipe value is null, use first non-null arg
    let result = eval_v2_op_step(
        &op,
        EvalValue::Value(json!(null)),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("default")));

    // When pipe value is missing, use first non-null arg
    let result = eval_v2_op_step(
        &op,
        EvalValue::Missing,
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("default")));
}

#[test]
fn test_eval_op_unknown() {
    let op = V2OpStep {
        op: "unknown_op".to_string(),
        args: vec![],
    };
    let ctx = V2EvalContext::new();
    let result = eval_v2_op_step(
        &op,
        EvalValue::Value(json!("test")),
        &json!({}),
        None,
        &json!({}),
        "test",
        &ctx,
    );
    assert!(result.is_err());
}
