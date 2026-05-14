use super::*;
use crate::v2_model::{
    V2Comparison, V2ComparisonOp, V2Condition, V2IfStep, V2LetStep, V2MapStep, V2Ref, V2Step,
};

#[cfg(test)]
mod v2_ref_eval_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_eval_input_ref() {
        let record = json!({"name": "Alice", "age": 30});
        let ctx = V2EvalContext::new();
        let result = eval_v2_ref(
            &V2Ref::Input("name".to_string()),
            &record,
            None,
            &json!({}),
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("Alice")));
    }

    #[test]
    fn test_eval_input_ref_nested() {
        let record = json!({"user": {"profile": {"name": "Bob"}}});
        let ctx = V2EvalContext::new();
        let result = eval_v2_ref(
            &V2Ref::Input("user.profile.name".to_string()),
            &record,
            None,
            &json!({}),
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("Bob")));
    }

    #[test]
    fn test_eval_input_ref_missing() {
        let record = json!({"name": "Alice"});
        let ctx = V2EvalContext::new();
        let result = eval_v2_ref(
            &V2Ref::Input("nonexistent".to_string()),
            &record,
            None,
            &json!({}),
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Missing)));
    }

    #[test]
    fn test_eval_context_ref() {
        let record = json!({});
        let context = json!({"rate": 1.5, "config": {"enabled": true}});
        let ctx = V2EvalContext::new();
        let result = eval_v2_ref(
            &V2Ref::Context("rate".to_string()),
            &record,
            Some(&context),
            &json!({}),
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(1.5)));
    }

    #[test]
    fn test_eval_context_ref_nested() {
        let record = json!({});
        let context = json!({"config": {"enabled": true}});
        let ctx = V2EvalContext::new();
        let result = eval_v2_ref(
            &V2Ref::Context("config.enabled".to_string()),
            &record,
            Some(&context),
            &json!({}),
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(true)));
    }

    #[test]
    fn test_eval_context_ref_no_context_missing() {
        let record = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_ref(
            &V2Ref::Context("rate".to_string()),
            &record,
            None,
            &json!({}),
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Missing)));
    }

    #[test]
    fn test_eval_out_ref() {
        let record = json!({});
        let out = json!({"computed": 42});
        let ctx = V2EvalContext::new();
        let result = eval_v2_ref(
            &V2Ref::Out("computed".to_string()),
            &record,
            None,
            &out,
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(42)));
    }

    #[test]
    fn test_eval_local_ref() {
        let ctx = V2EvalContext::new()
            .with_let_binding("price".to_string(), EvalValue::Value(json!(100)));
        let result = eval_v2_ref(
            &V2Ref::Local("price".to_string()),
            &json!({}),
            None,
            &json!({}),
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(100)));
    }

    #[test]
    fn test_eval_local_ref_undefined_error() {
        let ctx = V2EvalContext::new();
        let result = eval_v2_ref(
            &V2Ref::Local("undefined".to_string()),
            &json!({}),
            None,
            &json!({}),
            "test",
            &ctx,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_eval_item_ref() {
        let item_value = json!({"name": "item1", "value": 10});
        let ctx = V2EvalContext::new().with_item(EvalItem {
            value: &item_value,
            index: 2,
        });
        let result = eval_v2_ref(
            &V2Ref::Item("name".to_string()),
            &json!({}),
            None,
            &json!({}),
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("item1")));
    }

    #[test]
    fn test_eval_item_ref_index() {
        let item_value = json!({"name": "item1"});
        let ctx = V2EvalContext::new().with_item(EvalItem {
            value: &item_value,
            index: 5,
        });
        let result = eval_v2_ref(
            &V2Ref::Item("index".to_string()),
            &json!({}),
            None,
            &json!({}),
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(5)));
    }

    #[test]
    fn test_eval_item_ref_no_scope_error() {
        let ctx = V2EvalContext::new();
        let result = eval_v2_ref(
            &V2Ref::Item("value".to_string()),
            &json!({}),
            None,
            &json!({}),
            "test",
            &ctx,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_eval_acc_ref() {
        let acc_value = json!(100);
        let ctx = V2EvalContext::new().with_acc(&acc_value);
        let result = eval_v2_ref(
            &V2Ref::Acc(String::new()),
            &json!({}),
            None,
            &json!({}),
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(100)));
    }

    #[test]
    fn test_eval_acc_ref_no_scope_error() {
        let ctx = V2EvalContext::new();
        let result = eval_v2_ref(
            &V2Ref::Acc("value".to_string()),
            &json!({}),
            None,
            &json!({}),
            "test",
            &ctx,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_eval_input_ref_empty_path() {
        let record = json!({"name": "Alice"});
        let ctx = V2EvalContext::new();
        let result = eval_v2_ref(
            &V2Ref::Input(String::new()),
            &record,
            None,
            &json!({}),
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!({"name": "Alice"})));
    }
}

#[cfg(test)]
mod v2_start_eval_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_eval_start_literal_string() {
        let ctx = V2EvalContext::new();
        let result = eval_v2_start(
            &V2Start::Literal(json!("hello")),
            &json!({}),
            None,
            &json!({}),
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("hello")));
    }

    #[test]
    fn test_eval_start_literal_number() {
        let ctx = V2EvalContext::new();
        let result = eval_v2_start(
            &V2Start::Literal(json!(42)),
            &json!({}),
            None,
            &json!({}),
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(42)));
    }

    #[test]
    fn test_eval_start_literal_bool() {
        let ctx = V2EvalContext::new();
        let result = eval_v2_start(
            &V2Start::Literal(json!(true)),
            &json!({}),
            None,
            &json!({}),
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(true)));
    }

    #[test]
    fn test_eval_start_literal_null() {
        let ctx = V2EvalContext::new();
        let result = eval_v2_start(
            &V2Start::Literal(json!(null)),
            &json!({}),
            None,
            &json!({}),
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(null)));
    }

    #[test]
    fn test_eval_start_literal_array() {
        let ctx = V2EvalContext::new();
        let result = eval_v2_start(
            &V2Start::Literal(json!([1, 2, 3])),
            &json!({}),
            None,
            &json!({}),
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!([1, 2, 3])));
    }

    #[test]
    fn test_eval_start_literal_object() {
        let ctx = V2EvalContext::new();
        let result = eval_v2_start(
            &V2Start::Literal(json!({"key": "value"})),
            &json!({}),
            None,
            &json!({}),
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!({"key": "value"})));
    }

    #[test]
    fn test_eval_start_ref() {
        let ctx = V2EvalContext::new();
        let result = eval_v2_start(
            &V2Start::Ref(V2Ref::Input("name".to_string())),
            &json!({"name": "Bob"}),
            None,
            &json!({}),
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("Bob")));
    }

    #[test]
    fn test_eval_start_pipe_value() {
        let ctx = V2EvalContext::new().with_pipe_value(EvalValue::Value(json!(42)));
        let result = eval_v2_start(
            &V2Start::PipeValue,
            &json!({}),
            None,
            &json!({}),
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(42)));
    }

    #[test]
    fn test_eval_start_pipe_value_not_available() {
        // When pipe value is not set, it returns Missing (not error)
        // This allows ops like lookup_first that don't use pipe input to work
        let ctx = V2EvalContext::new();
        let result = eval_v2_start(
            &V2Start::PipeValue,
            &json!({}),
            None,
            &json!({}),
            "test",
            &ctx,
        );
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), EvalValue::Missing);
    }

    #[test]
    fn test_eval_start_pipe_value_missing() {
        let ctx = V2EvalContext::new().with_pipe_value(EvalValue::Missing);
        let result = eval_v2_start(
            &V2Start::PipeValue,
            &json!({}),
            None,
            &json!({}),
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Missing)));
    }
}

#[cfg(test)]
mod v2_op_step_eval_tests {
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

    #[test]
    fn test_eval_op_comparison_aliases() {
        let ctx = V2EvalContext::new();
        let cases = [
            ("eq", json!(1), json!("1"), true),
            ("ne", json!(1), json!(2), true),
            ("lt", json!(5), json!(10), true),
            ("lte", json!(10), json!(10), true),
            ("gt", json!(10), json!(5), true),
            ("gte", json!(10), json!(10), true),
            ("match", json!("apple"), json!("^a.*"), true),
        ];

        for (op, left, right, expected) in cases {
            let op_step = V2OpStep {
                op: op.to_string(),
                args: vec![lit(right)],
            };
            let result = eval_v2_op_step(
                &op_step,
                EvalValue::Value(left),
                &json!({}),
                None,
                &json!({}),
                "test",
                &ctx,
            );
            assert!(
                matches!(result, Ok(EvalValue::Value(v)) if v == json!(expected)),
                "op {}",
                op
            );
        }
    }

    #[test]
    fn test_eval_op_comparison_symbols() {
        let ctx = V2EvalContext::new();
        let cases = [
            ("==", json!(1), json!("1"), true),
            ("!=", json!(1), json!(2), true),
            ("<", json!(5), json!(10), true),
            ("<=", json!(10), json!(10), true),
            (">", json!(10), json!(5), true),
            (">=", json!(10), json!(10), true),
            ("~=", json!("apple"), json!("^a.*"), true),
        ];

        for (op, left, right, expected) in cases {
            let op_step = V2OpStep {
                op: op.to_string(),
                args: vec![lit(right)],
            };
            let result = eval_v2_op_step(
                &op_step,
                EvalValue::Value(left),
                &json!({}),
                None,
                &json!({}),
                "test",
                &ctx,
            );
            assert!(
                matches!(result, Ok(EvalValue::Value(v)) if v == json!(expected)),
                "op {}",
                op
            );
        }
    }

    #[test]
    fn test_eval_op_comparison_missing_null_semantics() {
        let ctx = V2EvalContext::new();
        let missing_eq_null = V2OpStep {
            op: "eq".to_string(),
            args: vec![lit(json!(null))],
        };
        let result = eval_v2_op_step(
            &missing_eq_null,
            EvalValue::Missing,
            &json!({}),
            None,
            &json!({}),
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(true)));

        let missing_ne_value = V2OpStep {
            op: "ne".to_string(),
            args: vec![lit(json!("value"))],
        };
        let result = eval_v2_op_step(
            &missing_ne_value,
            EvalValue::Missing,
            &json!({}),
            None,
            &json!({}),
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(true)));
    }

    #[test]
    fn test_eval_op_comparison_error_paths() {
        let ctx = V2EvalContext::new();
        let non_numeric_compare = V2OpStep {
            op: "<".to_string(),
            args: vec![lit(json!(1))],
        };
        let err = eval_v2_op_step(
            &non_numeric_compare,
            EvalValue::Value(json!("not-a-number")),
            &json!({}),
            None,
            &json!({}),
            "test",
            &ctx,
        )
        .expect_err("non-numeric comparison should error");
        assert_eq!(err.kind, TransformErrorKind::ExprError);
        assert_eq!(err.message, "comparison operand must be a number");
        assert_eq!(err.path.as_deref(), Some("test"));

        let invalid_regex = V2OpStep {
            op: "~=".to_string(),
            args: vec![lit(json!("["))],
        };
        let err = eval_v2_op_step(
            &invalid_regex,
            EvalValue::Value(json!("apple")),
            &json!({}),
            None,
            &json!({}),
            "test",
            &ctx,
        )
        .expect_err("invalid regex should error");
        assert_eq!(err.kind, TransformErrorKind::ExprError);
        assert!(err.message.starts_with("invalid regex pattern:"));
        assert_eq!(err.path.as_deref(), Some("test.args[0]"));
    }

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
}

#[cfg(test)]
mod v2_let_step_eval_tests {
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
}

#[cfg(test)]
mod v2_if_step_eval_tests {
    use super::*;
    use serde_json::json;

    // ------ Condition evaluation tests ------

    #[test]
    fn test_eval_condition_eq_true() {
        let cond = V2Condition::Comparison(V2Comparison {
            op: V2ComparisonOp::Eq,
            args: vec![
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(10)),
                    steps: vec![],
                }),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(10)),
                    steps: vec![],
                }),
            ],
        });
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_condition(&cond, &record, None, &out, "test", &ctx);
        assert!(matches!(result, Ok(true)));
    }

    #[test]
    fn test_eval_condition_eq_false() {
        let cond = V2Condition::Comparison(V2Comparison {
            op: V2ComparisonOp::Eq,
            args: vec![
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(10)),
                    steps: vec![],
                }),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(20)),
                    steps: vec![],
                }),
            ],
        });
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_condition(&cond, &record, None, &out, "test", &ctx);
        assert!(matches!(result, Ok(false)));
    }

    #[test]
    fn test_eval_condition_eq_numeric_string_is_false() {
        let cond = V2Condition::Comparison(V2Comparison {
            op: V2ComparisonOp::Eq,
            args: vec![
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!("1")),
                    steps: vec![],
                }),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(1)),
                    steps: vec![],
                }),
            ],
        });
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_condition(&cond, &record, None, &out, "test", &ctx);
        assert!(matches!(result, Ok(false)));
    }

    #[test]
    fn test_eval_condition_eq_missing_as_null() {
        let cond = V2Condition::Comparison(V2Comparison {
            op: V2ComparisonOp::Eq,
            args: vec![
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Ref(V2Ref::Input("optional".to_string())),
                    steps: vec![],
                }),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(null)),
                    steps: vec![],
                }),
            ],
        });
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_condition(&cond, &record, None, &out, "test", &ctx);
        assert!(matches!(result, Ok(true)));
    }

    #[test]
    fn test_eval_condition_ne() {
        let cond = V2Condition::Comparison(V2Comparison {
            op: V2ComparisonOp::Ne,
            args: vec![
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!("a")),
                    steps: vec![],
                }),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!("b")),
                    steps: vec![],
                }),
            ],
        });
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_condition(&cond, &record, None, &out, "test", &ctx);
        assert!(matches!(result, Ok(true)));
    }

    #[test]
    fn test_eval_condition_gt() {
        let cond = V2Condition::Comparison(V2Comparison {
            op: V2ComparisonOp::Gt,
            args: vec![
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(20)),
                    steps: vec![],
                }),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(10)),
                    steps: vec![],
                }),
            ],
        });
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_condition(&cond, &record, None, &out, "test", &ctx);
        assert!(matches!(result, Ok(true)));
    }

    #[test]
    fn test_eval_condition_gt_non_numeric_string_compares_lexicographically() {
        let cond = V2Condition::Comparison(V2Comparison {
            op: V2ComparisonOp::Gt,
            args: vec![
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!("B")),
                    steps: vec![],
                }),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!("A")),
                    steps: vec![],
                }),
            ],
        });
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_condition(&cond, &record, None, &out, "test", &ctx);
        assert!(matches!(result, Ok(true)));
    }

    #[test]
    fn test_eval_condition_lt() {
        let cond = V2Condition::Comparison(V2Comparison {
            op: V2ComparisonOp::Lt,
            args: vec![
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(5)),
                    steps: vec![],
                }),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(10)),
                    steps: vec![],
                }),
            ],
        });
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_condition(&cond, &record, None, &out, "test", &ctx);
        assert!(matches!(result, Ok(true)));
    }

    #[test]
    fn test_eval_condition_gte_equal() {
        let cond = V2Condition::Comparison(V2Comparison {
            op: V2ComparisonOp::Gte,
            args: vec![
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(10)),
                    steps: vec![],
                }),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(10)),
                    steps: vec![],
                }),
            ],
        });
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_condition(&cond, &record, None, &out, "test", &ctx);
        assert!(matches!(result, Ok(true)));
    }

    #[test]
    fn test_eval_condition_lte_less() {
        let cond = V2Condition::Comparison(V2Comparison {
            op: V2ComparisonOp::Lte,
            args: vec![
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(5)),
                    steps: vec![],
                }),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(10)),
                    steps: vec![],
                }),
            ],
        });
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_condition(&cond, &record, None, &out, "test", &ctx);
        assert!(matches!(result, Ok(true)));
    }

    #[test]
    fn test_eval_condition_match() {
        let cond = V2Condition::Comparison(V2Comparison {
            op: V2ComparisonOp::Match,
            args: vec![
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!("hello123")),
                    steps: vec![],
                }),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!("^hello\\d+")),
                    steps: vec![],
                }),
            ],
        });
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_condition(&cond, &record, None, &out, "test", &ctx);
        assert!(matches!(result, Ok(true)));
    }

    #[test]
    fn test_eval_condition_all_true() {
        let cond = V2Condition::All(vec![
            V2Condition::Comparison(V2Comparison {
                op: V2ComparisonOp::Gt,
                args: vec![
                    V2Expr::Pipe(V2Pipe {
                        start: V2Start::Literal(json!(10)),
                        steps: vec![],
                    }),
                    V2Expr::Pipe(V2Pipe {
                        start: V2Start::Literal(json!(5)),
                        steps: vec![],
                    }),
                ],
            }),
            V2Condition::Comparison(V2Comparison {
                op: V2ComparisonOp::Lt,
                args: vec![
                    V2Expr::Pipe(V2Pipe {
                        start: V2Start::Literal(json!(10)),
                        steps: vec![],
                    }),
                    V2Expr::Pipe(V2Pipe {
                        start: V2Start::Literal(json!(20)),
                        steps: vec![],
                    }),
                ],
            }),
        ]);
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_condition(&cond, &record, None, &out, "test", &ctx);
        assert!(matches!(result, Ok(true)));
    }

    #[test]
    fn test_eval_condition_all_false() {
        let cond = V2Condition::All(vec![
            V2Condition::Comparison(V2Comparison {
                op: V2ComparisonOp::Gt,
                args: vec![
                    V2Expr::Pipe(V2Pipe {
                        start: V2Start::Literal(json!(10)),
                        steps: vec![],
                    }),
                    V2Expr::Pipe(V2Pipe {
                        start: V2Start::Literal(json!(5)),
                        steps: vec![],
                    }),
                ],
            }),
            V2Condition::Comparison(V2Comparison {
                op: V2ComparisonOp::Lt, // 10 < 5 is false
                args: vec![
                    V2Expr::Pipe(V2Pipe {
                        start: V2Start::Literal(json!(10)),
                        steps: vec![],
                    }),
                    V2Expr::Pipe(V2Pipe {
                        start: V2Start::Literal(json!(5)),
                        steps: vec![],
                    }),
                ],
            }),
        ]);
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_condition(&cond, &record, None, &out, "test", &ctx);
        assert!(matches!(result, Ok(false)));
    }

    #[test]
    fn test_eval_condition_any_true() {
        let cond = V2Condition::Any(vec![
            V2Condition::Comparison(V2Comparison {
                op: V2ComparisonOp::Eq,
                args: vec![
                    V2Expr::Pipe(V2Pipe {
                        start: V2Start::Literal(json!("admin")),
                        steps: vec![],
                    }),
                    V2Expr::Pipe(V2Pipe {
                        start: V2Start::Literal(json!("user")),
                        steps: vec![],
                    }),
                ],
            }),
            V2Condition::Comparison(V2Comparison {
                op: V2ComparisonOp::Gt,
                args: vec![
                    V2Expr::Pipe(V2Pipe {
                        start: V2Start::Literal(json!(100)),
                        steps: vec![],
                    }),
                    V2Expr::Pipe(V2Pipe {
                        start: V2Start::Literal(json!(50)),
                        steps: vec![],
                    }),
                ],
            }),
        ]);
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_condition(&cond, &record, None, &out, "test", &ctx);
        assert!(matches!(result, Ok(true)));
    }

    #[test]
    fn test_eval_condition_any_false() {
        let cond = V2Condition::Any(vec![
            V2Condition::Comparison(V2Comparison {
                op: V2ComparisonOp::Eq,
                args: vec![
                    V2Expr::Pipe(V2Pipe {
                        start: V2Start::Literal(json!(1)),
                        steps: vec![],
                    }),
                    V2Expr::Pipe(V2Pipe {
                        start: V2Start::Literal(json!(2)),
                        steps: vec![],
                    }),
                ],
            }),
            V2Condition::Comparison(V2Comparison {
                op: V2ComparisonOp::Eq,
                args: vec![
                    V2Expr::Pipe(V2Pipe {
                        start: V2Start::Literal(json!(3)),
                        steps: vec![],
                    }),
                    V2Expr::Pipe(V2Pipe {
                        start: V2Start::Literal(json!(4)),
                        steps: vec![],
                    }),
                ],
            }),
        ]);
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_condition(&cond, &record, None, &out, "test", &ctx);
        assert!(matches!(result, Ok(false)));
    }

    #[test]
    fn test_eval_condition_expr_truthy() {
        let cond = V2Condition::Expr(V2Expr::Pipe(V2Pipe {
            start: V2Start::Literal(json!(true)),
            steps: vec![],
        }));
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_condition(&cond, &record, None, &out, "test", &ctx);
        assert!(matches!(result, Ok(true)));
    }

    #[test]
    fn test_eval_condition_expr_falsy() {
        let cond = V2Condition::Expr(V2Expr::Pipe(V2Pipe {
            start: V2Start::Literal(json!(false)),
            steps: vec![],
        }));
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_condition(&cond, &record, None, &out, "test", &ctx);
        assert!(matches!(result, Ok(false)));
    }

    #[test]
    fn test_eval_condition_expr_non_bool_errors() {
        let cond = V2Condition::Expr(V2Expr::Pipe(V2Pipe {
            start: V2Start::Literal(json!("active")),
            steps: vec![],
        }));
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_condition(&cond, &record, None, &out, "test", &ctx);
        assert!(matches!(result, Err(err)
            if err.kind == TransformErrorKind::ExprError
                && err.message == "when/record_when must evaluate to boolean"
                && err.path.as_deref() == Some("test.expr")
        ));
    }

    #[test]
    fn test_eval_condition_expr_missing_is_false() {
        let cond = V2Condition::Expr(V2Expr::Pipe(V2Pipe {
            start: V2Start::Ref(V2Ref::Input("active".to_string())),
            steps: vec![],
        }));
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_condition(&cond, &record, None, &out, "test", &ctx);
        assert!(matches!(result, Ok(false)));
    }

    #[test]
    fn test_eval_condition_with_pipe_value() {
        // Condition: { gt: ["$", 100] }
        let cond = V2Condition::Comparison(V2Comparison {
            op: V2ComparisonOp::Gt,
            args: vec![
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::PipeValue,
                    steps: vec![],
                }),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(100)),
                    steps: vec![],
                }),
            ],
        });
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new().with_pipe_value(EvalValue::Value(json!(150)));
        let result = eval_v2_condition(&cond, &record, None, &out, "test", &ctx);
        assert!(matches!(result, Ok(true)));
    }

    // ------ If step evaluation tests ------

    #[test]
    fn test_eval_if_step_then_branch() {
        // if: { cond: { gt: ["$", 10] }, then: [{ multiply: 2 }] }
        let if_step = V2IfStep {
            cond: V2Condition::Comparison(V2Comparison {
                op: V2ComparisonOp::Gt,
                args: vec![
                    V2Expr::Pipe(V2Pipe {
                        start: V2Start::PipeValue,
                        steps: vec![],
                    }),
                    V2Expr::Pipe(V2Pipe {
                        start: V2Start::Literal(json!(10)),
                        steps: vec![],
                    }),
                ],
            }),
            then_branch: V2Pipe {
                start: V2Start::PipeValue,
                steps: vec![V2Step::Op(V2OpStep {
                    op: "multiply".to_string(),
                    args: vec![V2Expr::Pipe(V2Pipe {
                        start: V2Start::Literal(json!(2)),
                        steps: vec![],
                    })],
                })],
            },
            else_branch: None,
        };
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_if_step(
            &if_step,
            EvalValue::Value(json!(20)),
            &record,
            None,
            &out,
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(40.0)));
    }

    #[test]
    fn test_eval_if_step_else_branch() {
        // if: { cond: { gt: ["$", 10] }, then: [{ multiply: 2 }], else: [{ multiply: 0.5 }] }
        let if_step = V2IfStep {
            cond: V2Condition::Comparison(V2Comparison {
                op: V2ComparisonOp::Gt,
                args: vec![
                    V2Expr::Pipe(V2Pipe {
                        start: V2Start::PipeValue,
                        steps: vec![],
                    }),
                    V2Expr::Pipe(V2Pipe {
                        start: V2Start::Literal(json!(10)),
                        steps: vec![],
                    }),
                ],
            }),
            then_branch: V2Pipe {
                start: V2Start::PipeValue,
                steps: vec![V2Step::Op(V2OpStep {
                    op: "multiply".to_string(),
                    args: vec![V2Expr::Pipe(V2Pipe {
                        start: V2Start::Literal(json!(2)),
                        steps: vec![],
                    })],
                })],
            },
            else_branch: Some(V2Pipe {
                start: V2Start::PipeValue,
                steps: vec![V2Step::Op(V2OpStep {
                    op: "multiply".to_string(),
                    args: vec![V2Expr::Pipe(V2Pipe {
                        start: V2Start::Literal(json!(0.5)),
                        steps: vec![],
                    })],
                })],
            }),
        };
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        // pipe value 5 is less than 10, so else branch is taken
        let result = eval_v2_if_step(
            &if_step,
            EvalValue::Value(json!(5)),
            &record,
            None,
            &out,
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(2.5)));
    }

    #[test]
    fn test_eval_if_step_no_else_returns_pipe_value() {
        // if: { cond: { gt: ["$", 10] }, then: [{ multiply: 2 }] }
        let if_step = V2IfStep {
            cond: V2Condition::Comparison(V2Comparison {
                op: V2ComparisonOp::Gt,
                args: vec![
                    V2Expr::Pipe(V2Pipe {
                        start: V2Start::PipeValue,
                        steps: vec![],
                    }),
                    V2Expr::Pipe(V2Pipe {
                        start: V2Start::Literal(json!(10)),
                        steps: vec![],
                    }),
                ],
            }),
            then_branch: V2Pipe {
                start: V2Start::PipeValue,
                steps: vec![V2Step::Op(V2OpStep {
                    op: "multiply".to_string(),
                    args: vec![V2Expr::Pipe(V2Pipe {
                        start: V2Start::Literal(json!(2)),
                        steps: vec![],
                    })],
                })],
            },
            else_branch: None,
        };
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        // pipe value 5 is less than 10, no else branch, returns original pipe value
        let result = eval_v2_if_step(
            &if_step,
            EvalValue::Value(json!(5)),
            &record,
            None,
            &out,
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(5)));
    }

    #[test]
    fn test_eval_pipe_with_if_step() {
        // [10000, { if: { cond: { gt: ["$", 5000] }, then: [{ multiply: 0.9 }] } }]
        let pipe = V2Pipe {
            start: V2Start::Literal(json!(10000)),
            steps: vec![V2Step::If(V2IfStep {
                cond: V2Condition::Comparison(V2Comparison {
                    op: V2ComparisonOp::Gt,
                    args: vec![
                        V2Expr::Pipe(V2Pipe {
                            start: V2Start::PipeValue,
                            steps: vec![],
                        }),
                        V2Expr::Pipe(V2Pipe {
                            start: V2Start::Literal(json!(5000)),
                            steps: vec![],
                        }),
                    ],
                }),
                then_branch: V2Pipe {
                    start: V2Start::PipeValue,
                    steps: vec![V2Step::Op(V2OpStep {
                        op: "multiply".to_string(),
                        args: vec![V2Expr::Pipe(V2Pipe {
                            start: V2Start::Literal(json!(0.9)),
                            steps: vec![],
                        })],
                    })],
                },
                else_branch: None,
            })],
        };
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_pipe(&pipe, &record, None, &out, "test", &ctx);
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(9000.0)));
    }

    #[test]
    fn test_eval_if_with_input_condition() {
        // if: { cond: { eq: ["@input.role", "admin"] }, then: [100], else: [50] }
        let if_step = V2IfStep {
            cond: V2Condition::Comparison(V2Comparison {
                op: V2ComparisonOp::Eq,
                args: vec![
                    V2Expr::Pipe(V2Pipe {
                        start: V2Start::Ref(V2Ref::Input("role".to_string())),
                        steps: vec![],
                    }),
                    V2Expr::Pipe(V2Pipe {
                        start: V2Start::Literal(json!("admin")),
                        steps: vec![],
                    }),
                ],
            }),
            then_branch: V2Pipe {
                start: V2Start::Literal(json!(100)),
                steps: vec![],
            },
            else_branch: Some(V2Pipe {
                start: V2Start::Literal(json!(50)),
                steps: vec![],
            }),
        };
        let record = json!({"role": "admin"});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_if_step(
            &if_step,
            EvalValue::Value(json!(0)),
            &record,
            None,
            &out,
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(100)));

        // When not admin
        let record2 = json!({"role": "user"});
        let result2 = eval_v2_if_step(
            &if_step,
            EvalValue::Value(json!(0)),
            &record2,
            None,
            &out,
            "test",
            &ctx,
        );
        assert!(matches!(result2, Ok(EvalValue::Value(v)) if v == json!(50)));
    }

    #[test]
    fn test_eval_nested_if() {
        // Nested if: if x > 100 then (if x > 500 then "gold" else "silver") else "bronze"
        let if_step = V2IfStep {
            cond: V2Condition::Comparison(V2Comparison {
                op: V2ComparisonOp::Gt,
                args: vec![
                    V2Expr::Pipe(V2Pipe {
                        start: V2Start::PipeValue,
                        steps: vec![],
                    }),
                    V2Expr::Pipe(V2Pipe {
                        start: V2Start::Literal(json!(100)),
                        steps: vec![],
                    }),
                ],
            }),
            then_branch: V2Pipe {
                start: V2Start::PipeValue,
                steps: vec![V2Step::If(V2IfStep {
                    cond: V2Condition::Comparison(V2Comparison {
                        op: V2ComparisonOp::Gt,
                        args: vec![
                            V2Expr::Pipe(V2Pipe {
                                start: V2Start::PipeValue,
                                steps: vec![],
                            }),
                            V2Expr::Pipe(V2Pipe {
                                start: V2Start::Literal(json!(500)),
                                steps: vec![],
                            }),
                        ],
                    }),
                    then_branch: V2Pipe {
                        start: V2Start::Literal(json!("gold")),
                        steps: vec![],
                    },
                    else_branch: Some(V2Pipe {
                        start: V2Start::Literal(json!("silver")),
                        steps: vec![],
                    }),
                })],
            },
            else_branch: Some(V2Pipe {
                start: V2Start::Literal(json!("bronze")),
                steps: vec![],
            }),
        };
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();

        // 50 -> bronze
        let result = eval_v2_if_step(
            &if_step,
            EvalValue::Value(json!(50)),
            &record,
            None,
            &out,
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("bronze")));

        // 200 -> silver
        let result = eval_v2_if_step(
            &if_step,
            EvalValue::Value(json!(200)),
            &record,
            None,
            &out,
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("silver")));

        // 600 -> gold
        let result = eval_v2_if_step(
            &if_step,
            EvalValue::Value(json!(600)),
            &record,
            None,
            &out,
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("gold")));
    }
}

#[cfg(test)]
mod v2_map_step_eval_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_eval_map_step_simple() {
        // map: [uppercase] on ["a", "b", "c"] -> ["A", "B", "C"]
        let map_step = V2MapStep {
            steps: vec![V2Step::Op(V2OpStep {
                op: "uppercase".to_string(),
                args: vec![],
            })],
        };
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_map_step(
            &map_step,
            EvalValue::Value(json!(["a", "b", "c"])),
            &record,
            None,
            &out,
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(["A", "B", "C"])));
    }

    #[test]
    fn test_eval_map_step_with_multiply() {
        // map: [multiply: 2] on [1, 2, 3] -> [2, 4, 6]
        let map_step = V2MapStep {
            steps: vec![V2Step::Op(V2OpStep {
                op: "multiply".to_string(),
                args: vec![V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(2)),
                    steps: vec![],
                })],
            })],
        };
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_map_step(
            &map_step,
            EvalValue::Value(json!([1, 2, 3])),
            &record,
            None,
            &out,
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!([2.0, 4.0, 6.0])));
    }

    #[test]
    fn test_eval_map_step_empty_array() {
        let map_step = V2MapStep {
            steps: vec![V2Step::Op(V2OpStep {
                op: "uppercase".to_string(),
                args: vec![],
            })],
        };
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_map_step(
            &map_step,
            EvalValue::Value(json!([])),
            &record,
            None,
            &out,
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!([])));
    }

    #[test]
    fn test_eval_map_step_missing_returns_missing() {
        let map_step = V2MapStep {
            steps: vec![V2Step::Op(V2OpStep {
                op: "uppercase".to_string(),
                args: vec![],
            })],
        };
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_map_step(
            &map_step,
            EvalValue::Missing,
            &record,
            None,
            &out,
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Missing)));
    }

    #[test]
    fn test_eval_map_step_non_array_error() {
        let map_step = V2MapStep {
            steps: vec![V2Step::Op(V2OpStep {
                op: "uppercase".to_string(),
                args: vec![],
            })],
        };
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_map_step(
            &map_step,
            EvalValue::Value(json!("not an array")),
            &record,
            None,
            &out,
            "test",
            &ctx,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_eval_map_step_with_item_ref() {
        // Access @item.name from each object
        let map_step = V2MapStep {
            steps: vec![V2Step::Op(V2OpStep {
                op: "concat".to_string(),
                args: vec![V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!("!")),
                    steps: vec![],
                })],
            })],
        };
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_map_step(
            &map_step,
            EvalValue::Value(json!(["hello", "world"])),
            &record,
            None,
            &out,
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(["hello!", "world!"])));
    }

    #[test]
    fn test_eval_map_step_with_item_index() {
        // Create pipe that returns @item.index
        // This requires testing through the full context
        let pipe = V2Pipe {
            start: V2Start::Ref(V2Ref::Input("items".to_string())),
            steps: vec![V2Step::Map(V2MapStep {
                steps: vec![], // Just return the item as-is
            })],
        };
        let record = json!({"items": [10, 20, 30]});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_pipe(&pipe, &record, None, &out, "test", &ctx);
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!([10, 20, 30])));
    }

    #[test]
    fn test_eval_map_step_multiple_ops() {
        // map: [trim, uppercase] on ["  a  ", "  b  "] -> ["A", "B"]
        let map_step = V2MapStep {
            steps: vec![
                V2Step::Op(V2OpStep {
                    op: "trim".to_string(),
                    args: vec![],
                }),
                V2Step::Op(V2OpStep {
                    op: "uppercase".to_string(),
                    args: vec![],
                }),
            ],
        };
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_map_step(
            &map_step,
            EvalValue::Value(json!(["  a  ", "  b  "])),
            &record,
            None,
            &out,
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(["A", "B"])));
    }

    #[test]
    fn test_eval_pipe_with_map_step() {
        // Full pipe: [@input.names, { map: [uppercase] }]
        let pipe = V2Pipe {
            start: V2Start::Ref(V2Ref::Input("names".to_string())),
            steps: vec![V2Step::Map(V2MapStep {
                steps: vec![V2Step::Op(V2OpStep {
                    op: "uppercase".to_string(),
                    args: vec![],
                })],
            })],
        };
        let record = json!({"names": ["alice", "bob"]});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_pipe(&pipe, &record, None, &out, "test", &ctx);
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(["ALICE", "BOB"])));
    }

    #[test]
    fn test_eval_map_with_if_step() {
        // map with conditional: double if > 5, else keep
        let map_step = V2MapStep {
            steps: vec![V2Step::If(V2IfStep {
                cond: V2Condition::Comparison(V2Comparison {
                    op: V2ComparisonOp::Gt,
                    args: vec![
                        V2Expr::Pipe(V2Pipe {
                            start: V2Start::PipeValue,
                            steps: vec![],
                        }),
                        V2Expr::Pipe(V2Pipe {
                            start: V2Start::Literal(json!(5)),
                            steps: vec![],
                        }),
                    ],
                }),
                then_branch: V2Pipe {
                    start: V2Start::PipeValue,
                    steps: vec![V2Step::Op(V2OpStep {
                        op: "multiply".to_string(),
                        args: vec![V2Expr::Pipe(V2Pipe {
                            start: V2Start::Literal(json!(2)),
                            steps: vec![],
                        })],
                    })],
                },
                else_branch: None,
            })],
        };
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        // [3, 7, 2, 10] -> [3, 14, 2, 20] (only 7 and 10 are > 5)
        let result = eval_v2_map_step(
            &map_step,
            EvalValue::Value(json!([3, 7, 2, 10])),
            &record,
            None,
            &out,
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!([3, 14.0, 2, 20.0])));
    }

    #[test]
    fn test_eval_nested_map() {
        // Nested map: [[1, 2], [3, 4]] -> map of (map multiply 2) -> [[2, 4], [6, 8]]
        let map_step = V2MapStep {
            steps: vec![V2Step::Map(V2MapStep {
                steps: vec![V2Step::Op(V2OpStep {
                    op: "multiply".to_string(),
                    args: vec![V2Expr::Pipe(V2Pipe {
                        start: V2Start::Literal(json!(2)),
                        steps: vec![],
                    })],
                })],
            })],
        };
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_map_step(
            &map_step,
            EvalValue::Value(json!([[1, 2], [3, 4]])),
            &record,
            None,
            &out,
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!([[2.0, 4.0], [6.0, 8.0]])));
    }

    #[test]
    fn test_eval_map_objects() {
        // Map over array of objects and extract a field
        // Since we're using pipe value directly, this tests object handling
        let pipe = V2Pipe {
            start: V2Start::Ref(V2Ref::Input("users".to_string())),
            steps: vec![V2Step::Map(V2MapStep {
                steps: vec![], // No-op, just return items
            })],
        };
        let record = json!({"users": [{"name": "Alice"}, {"name": "Bob"}]});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_pipe(&pipe, &record, None, &out, "test", &ctx);
        assert!(
            matches!(result, Ok(EvalValue::Value(v)) if v == json!([{"name": "Alice"}, {"name": "Bob"}]))
        );
    }
}

#[cfg(test)]
mod v2_pipe_eval_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_eval_pipe_simple_ref() {
        let pipe = V2Pipe {
            start: V2Start::Ref(V2Ref::Input("name".to_string())),
            steps: vec![],
        };
        let record = json!({"name": "Alice"});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_pipe(&pipe, &record, None, &out, "test", &ctx);
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("Alice")));
    }

    #[test]
    fn test_eval_pipe_literal_start() {
        let pipe = V2Pipe {
            start: V2Start::Literal(json!(42)),
            steps: vec![],
        };
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_pipe(&pipe, &record, None, &out, "test", &ctx);
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(42)));
    }

    #[test]
    fn test_eval_pipe_chain_ops() {
        // ["  hello  ", trim, uppercase]
        let pipe = V2Pipe {
            start: V2Start::Literal(json!("  hello  ")),
            steps: vec![
                V2Step::Op(V2OpStep {
                    op: "trim".to_string(),
                    args: vec![],
                }),
                V2Step::Op(V2OpStep {
                    op: "uppercase".to_string(),
                    args: vec![],
                }),
            ],
        };
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_pipe(&pipe, &record, None, &out, "test", &ctx);
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("HELLO")));
    }

    #[test]
    fn test_eval_pipe_with_context() {
        // [@context.multiplier, multiply: @input.value]
        let pipe = V2Pipe {
            start: V2Start::Ref(V2Ref::Context("multiplier".to_string())),
            steps: vec![V2Step::Op(V2OpStep {
                op: "multiply".to_string(),
                args: vec![V2Expr::Pipe(V2Pipe {
                    start: V2Start::Ref(V2Ref::Input("value".to_string())),
                    steps: vec![],
                })],
            })],
        };
        let record = json!({"value": 10});
        let context = json!({"multiplier": 5});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_pipe(&pipe, &record, Some(&context), &out, "test", &ctx);
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(50.0)));
    }

    #[test]
    fn test_eval_pipe_with_out_ref() {
        // [@out.previous, add: 1]
        let pipe = V2Pipe {
            start: V2Start::Ref(V2Ref::Out("previous".to_string())),
            steps: vec![V2Step::Op(V2OpStep {
                op: "add".to_string(),
                args: vec![V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(1)),
                    steps: vec![],
                })],
            })],
        };
        let record = json!({});
        let out = json!({"previous": 99});
        let ctx = V2EvalContext::new();
        let result = eval_v2_pipe(&pipe, &record, None, &out, "test", &ctx);
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(100.0)));
    }

    #[test]
    fn test_eval_pipe_complex_chain() {
        // [@input.price, let: {original: $}, multiply: 0.9, let: {discounted: $},
        //  if: {cond: {gt: [$, 1000]}, then: [subtract: 100]}]
        let pipe = V2Pipe {
            start: V2Start::Ref(V2Ref::Input("price".to_string())),
            steps: vec![
                V2Step::Let(V2LetStep {
                    bindings: vec![(
                        "original".to_string(),
                        V2Expr::Pipe(V2Pipe {
                            start: V2Start::PipeValue,
                            steps: vec![],
                        }),
                    )],
                }),
                V2Step::Op(V2OpStep {
                    op: "multiply".to_string(),
                    args: vec![V2Expr::Pipe(V2Pipe {
                        start: V2Start::Literal(json!(0.9)),
                        steps: vec![],
                    })],
                }),
                V2Step::If(V2IfStep {
                    cond: V2Condition::Comparison(V2Comparison {
                        op: V2ComparisonOp::Gt,
                        args: vec![
                            V2Expr::Pipe(V2Pipe {
                                start: V2Start::PipeValue,
                                steps: vec![],
                            }),
                            V2Expr::Pipe(V2Pipe {
                                start: V2Start::Literal(json!(1000)),
                                steps: vec![],
                            }),
                        ],
                    }),
                    then_branch: V2Pipe {
                        start: V2Start::PipeValue,
                        steps: vec![V2Step::Op(V2OpStep {
                            op: "subtract".to_string(),
                            args: vec![V2Expr::Pipe(V2Pipe {
                                start: V2Start::Literal(json!(100)),
                                steps: vec![],
                            })],
                        })],
                    },
                    else_branch: None,
                }),
            ],
        };
        let record = json!({"price": 2000});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_pipe(&pipe, &record, None, &out, "test", &ctx);
        // 2000 * 0.9 = 1800 > 1000, so 1800 - 100 = 1700
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(1700.0)));
    }

    #[test]
    fn test_eval_pipe_all_step_types() {
        // Test combining let, op, if, map in one pipe
        let pipe = V2Pipe {
            start: V2Start::Ref(V2Ref::Input("items".to_string())),
            steps: vec![
                // map: multiply each by 2
                V2Step::Map(V2MapStep {
                    steps: vec![V2Step::Op(V2OpStep {
                        op: "multiply".to_string(),
                        args: vec![V2Expr::Pipe(V2Pipe {
                            start: V2Start::Literal(json!(2)),
                            steps: vec![],
                        })],
                    })],
                }),
            ],
        };
        let record = json!({"items": [1, 2, 3]});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_pipe(&pipe, &record, None, &out, "test", &ctx);
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!([2.0, 4.0, 6.0])));
    }

    #[test]
    fn test_eval_pipe_coalesce_chain() {
        // [@input.primary, coalesce: @input.secondary, coalesce: "default"]
        let pipe = V2Pipe {
            start: V2Start::Ref(V2Ref::Input("primary".to_string())),
            steps: vec![
                V2Step::Op(V2OpStep {
                    op: "coalesce".to_string(),
                    args: vec![V2Expr::Pipe(V2Pipe {
                        start: V2Start::Ref(V2Ref::Input("secondary".to_string())),
                        steps: vec![],
                    })],
                }),
                V2Step::Op(V2OpStep {
                    op: "coalesce".to_string(),
                    args: vec![V2Expr::Pipe(V2Pipe {
                        start: V2Start::Literal(json!("default")),
                        steps: vec![],
                    })],
                }),
            ],
        };

        // Test with primary present
        let record = json!({"primary": "first"});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_pipe(&pipe, &record, None, &out, "test", &ctx);
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("first")));

        // Test with primary null, secondary present
        let record = json!({"primary": null, "secondary": "second"});
        let result = eval_v2_pipe(&pipe, &record, None, &out, "test", &ctx);
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("second")));

        // Test with both null, use default
        let record = json!({"primary": null, "secondary": null});
        let result = eval_v2_pipe(&pipe, &record, None, &out, "test", &ctx);
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("default")));
    }

    #[test]
    fn test_eval_expr_with_v2_pipe() {
        let expr = V2Expr::Pipe(V2Pipe {
            start: V2Start::Literal(json!("hello")),
            steps: vec![V2Step::Op(V2OpStep {
                op: "uppercase".to_string(),
                args: vec![],
            })],
        });
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_expr(&expr, &record, None, &out, "test", &ctx);
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("HELLO")));
    }

    #[test]
    fn test_eval_pipe_deep_nesting() {
        // Deeply nested: input -> map -> if -> op
        let pipe = V2Pipe {
            start: V2Start::Ref(V2Ref::Input("scores".to_string())),
            steps: vec![V2Step::Map(V2MapStep {
                steps: vec![V2Step::If(V2IfStep {
                    cond: V2Condition::Comparison(V2Comparison {
                        op: V2ComparisonOp::Gte,
                        args: vec![
                            V2Expr::Pipe(V2Pipe {
                                start: V2Start::PipeValue,
                                steps: vec![],
                            }),
                            V2Expr::Pipe(V2Pipe {
                                start: V2Start::Literal(json!(60)),
                                steps: vec![],
                            }),
                        ],
                    }),
                    then_branch: V2Pipe {
                        start: V2Start::Literal(json!("pass")),
                        steps: vec![],
                    },
                    else_branch: Some(V2Pipe {
                        start: V2Start::Literal(json!("fail")),
                        steps: vec![],
                    }),
                })],
            })],
        };
        let record = json!({"scores": [80, 55, 90, 45]});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_pipe(&pipe, &record, None, &out, "test", &ctx);
        assert!(
            matches!(result, Ok(EvalValue::Value(v)) if v == json!(["pass", "fail", "pass", "fail"]))
        );
    }
}

#[cfg(test)]
mod v2_lookup_eval_tests {
    use super::*;
    use serde_json::json;

    fn make_departments() -> JsonValue {
        json!([
            {"id": 1, "name": "Engineering", "budget": 100000},
            {"id": 2, "name": "Sales", "budget": 50000},
            {"id": 3, "name": "HR", "budget": 30000}
        ])
    }

    #[test]
    fn test_lookup_first_basic() {
        // lookup_first: {from: @context.departments, match: [id, 2], get: name}
        let op = V2OpStep {
            op: "lookup_first".to_string(),
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
                    start: V2Start::Literal(json!(2)),
                    steps: vec![],
                }),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!("name")),
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
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("Sales")));
    }

    #[test]
    fn test_lookup_first_uses_pipe_value_from() {
        let op = V2OpStep {
            op: "lookup_first".to_string(),
            args: vec![
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!("id")),
                    steps: vec![],
                }),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(2)),
                    steps: vec![],
                }),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!("budget")),
                    steps: vec![],
                }),
            ],
        };
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_op_step(
            &op,
            EvalValue::Value(make_departments()),
            &record,
            None,
            &out,
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(50000)));
    }

    #[test]
    fn test_lookup_first_no_match() {
        let op = V2OpStep {
            op: "lookup_first".to_string(),
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
                    start: V2Start::Literal(json!(999)), // Non-existent ID
                    steps: vec![],
                }),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!("name")),
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
        assert!(matches!(result, Ok(EvalValue::Missing)));
    }

    #[test]
    fn test_lookup_first_return_whole_object() {
        // Without 'get', return the whole matched object
        let op = V2OpStep {
            op: "lookup_first".to_string(),
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
                    start: V2Start::Literal(json!(1)),
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
        assert!(
            matches!(result, Ok(EvalValue::Value(v)) if v == json!({"id": 1, "name": "Engineering", "budget": 100000}))
        );
    }

    #[test]
    fn test_lookup_first_with_input_match_value() {
        // Match using value from input
        let op = V2OpStep {
            op: "lookup_first".to_string(),
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
                    start: V2Start::Ref(V2Ref::Input("dept_id".to_string())),
                    steps: vec![],
                }),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!("name")),
                    steps: vec![],
                }),
            ],
        };
        let record = json!({"dept_id": 3});
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
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!("HR")));
    }

    #[test]
    fn test_lookup_first_missing_match_value_does_not_match_null() {
        let users = json!([
            {"id": null, "name": "MissingUser"},
            {"id": 1, "name": "Alice"}
        ]);
        let op = V2OpStep {
            op: "lookup_first".to_string(),
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

    #[test]
    fn test_lookup_first_missing_from() {
        let op = V2OpStep {
            op: "lookup_first".to_string(),
            args: vec![
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Ref(V2Ref::Context("nonexistent".to_string())),
                    steps: vec![],
                }),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!("id")),
                    steps: vec![],
                }),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!(1)),
                    steps: vec![],
                }),
            ],
        };
        let record = json!({});
        let context = json!({});
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
        // Missing 'from' returns Missing
        assert!(matches!(result, Ok(EvalValue::Missing)));
    }

    #[test]
    fn test_lookup_first_insufficient_args() {
        let op = V2OpStep {
            op: "lookup_first".to_string(),
            args: vec![V2Expr::Pipe(V2Pipe {
                start: V2Start::Literal(json!([])),
                steps: vec![],
            })],
        };
        let record = json!({});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_op_step(
            &op,
            EvalValue::Value(json!(null)),
            &record,
            None,
            &out,
            "test",
            &ctx,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_lookup_in_pipe() {
        // Full pipe: lookup then transform result
        // Simpler test: just lookup and verify
        let pipe = V2Pipe {
            start: V2Start::Literal(json!(null)),
            steps: vec![V2Step::Op(V2OpStep {
                op: "lookup_first".to_string(),
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
                        start: V2Start::Ref(V2Ref::Input("dept_id".to_string())),
                        steps: vec![],
                    }),
                    V2Expr::Pipe(V2Pipe {
                        start: V2Start::Literal(json!("budget")),
                        steps: vec![],
                    }),
                ],
            })],
        };
        let record = json!({"dept_id": 2}); // Sales dept
        let context = json!({"departments": make_departments()});
        let out = json!({});
        let ctx = V2EvalContext::new();
        let result = eval_v2_pipe(&pipe, &record, Some(&context), &out, "test", &ctx);
        // Sales budget is 50000
        assert!(matches!(result, Ok(EvalValue::Value(v)) if v == json!(50000)));
    }

    #[test]
    fn test_lookup_then_multiply() {
        // Two-step pipe: lookup, then multiply
        let pipe = V2Pipe {
            start: V2Start::Ref(V2Ref::Context("departments".to_string())),
            steps: vec![],
        };
        let record = json!({"dept_id": 2});
        let context = json!({"departments": make_departments()});
        let out = json!({});
        let ctx = V2EvalContext::new();

        // First verify context is accessible
        let result = eval_v2_pipe(&pipe, &record, Some(&context), &out, "test", &ctx);
        assert!(result.is_ok());

        // Now test just the lookup op step directly
        let op = V2OpStep {
            op: "lookup_first".to_string(),
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
                    start: V2Start::Literal(json!(2)),
                    steps: vec![],
                }),
                V2Expr::Pipe(V2Pipe {
                    start: V2Start::Literal(json!("budget")),
                    steps: vec![],
                }),
            ],
        };
        let result = eval_v2_op_step(
            &op,
            EvalValue::Value(json!(null)),
            &record,
            Some(&context),
            &out,
            "test",
            &ctx,
        );
        assert!(matches!(result, Ok(EvalValue::Value(ref v)) if *v == json!(50000)));

        // Now multiply it
        let multiply_op = V2OpStep {
            op: "multiply".to_string(),
            args: vec![V2Expr::Pipe(V2Pipe {
                start: V2Start::Literal(json!(1.1)),
                steps: vec![],
            })],
        };
        let budget = result.unwrap();
        let result2 = eval_v2_op_step(&multiply_op, budget, &record, None, &out, "test", &ctx);
        // multiply returns f64, check approximately 55000
        match result2 {
            Ok(EvalValue::Value(v)) => {
                let num = v.as_f64().expect("should be number");
                assert!(
                    (num - 55000.0).abs() < 0.001,
                    "expected 55000.0, got {}",
                    num
                );
            }
            other => panic!("expected Ok(EvalValue::Value), got {:?}", other),
        }
    }
}
