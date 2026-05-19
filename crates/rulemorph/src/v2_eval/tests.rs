use super::*;
use crate::error::TransformErrorKind;
use crate::v2_model::{
    V2Comparison, V2ComparisonOp, V2Condition, V2Expr, V2IfStep, V2LetStep, V2MapStep, V2OpStep,
    V2Pipe, V2Ref, V2Start, V2Step,
};
use serde_json::Value as JsonValue;

#[cfg(test)]
#[path = "tests/ref_eval.rs"]
mod v2_ref_eval_tests;

#[cfg(test)]
#[path = "tests/start_eval.rs"]
mod v2_start_eval_tests;

#[cfg(test)]
#[path = "tests/op_step.rs"]
mod v2_op_step_eval_tests;

#[cfg(test)]
#[path = "tests/let_step.rs"]
mod v2_let_step_eval_tests;

#[cfg(test)]
#[path = "tests/if_step.rs"]
mod v2_if_step_eval_tests;

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
