use super::*;

mod v2_pipe_parser_tests {
    use super::*;

    #[test]
    fn test_parse_simple_pipe() {
        // ["@input.name", "trim"]
        let arr = vec![json!("@input.name"), json!("trim")];
        let pipe = parse_v2_pipe(&arr).unwrap();

        assert_eq!(pipe.start, V2Start::Ref(V2Ref::Input("name".to_string())));
        assert_eq!(pipe.steps.len(), 1);
        if let V2Step::Op(op) = &pipe.steps[0] {
            assert_eq!(op.op, "trim");
            assert!(op.args.is_empty());
        } else {
            panic!("Expected Op step");
        }
    }

    #[test]
    fn test_parse_pipe_with_multiple_steps() {
        // ["@input.name", "trim", "uppercase"]
        let arr = vec![json!("@input.name"), json!("trim"), json!("uppercase")];
        let pipe = parse_v2_pipe(&arr).unwrap();

        assert_eq!(pipe.steps.len(), 2);
    }

    #[test]
    fn test_parse_pipe_with_op_object() {
        // ["@input.value", { "op": "add", "args": [10] }]
        let arr = vec![json!("@input.value"), json!({ "op": "add", "args": [10] })];
        let pipe = parse_v2_pipe(&arr).unwrap();

        if let V2Step::Op(op) = &pipe.steps[0] {
            assert_eq!(op.op, "add");
            assert_eq!(op.args.len(), 1);
        } else {
            panic!("Expected Op step");
        }
    }

    #[test]
    fn test_parse_pipe_with_pipe_value_start() {
        // ["$", "trim"]
        let arr = vec![json!("$"), json!("trim")];
        let pipe = parse_v2_pipe(&arr).unwrap();

        assert_eq!(pipe.start, V2Start::PipeValue);
    }

    #[test]
    fn test_parse_pipe_with_literal_start() {
        // [42, { "op": "multiply", "args": [2] }]
        let arr = vec![json!(42), json!({ "op": "multiply", "args": [2] })];
        let pipe = parse_v2_pipe(&arr).unwrap();

        assert_eq!(pipe.start, V2Start::Literal(json!(42)));
    }

    #[test]
    fn test_parse_pipe_with_literal_escape() {
        // ["lit:@input.name", "trim"]
        let arr = vec![json!("lit:@input.name"), json!("trim")];
        let pipe = parse_v2_pipe(&arr).unwrap();

        assert_eq!(pipe.start, V2Start::Literal(json!("@input.name")));
    }

    #[test]
    fn test_parse_empty_pipe_error() {
        let arr: Vec<JsonValue> = vec![];
        let result = parse_v2_pipe(&arr);
        assert_eq!(result, Err(V2ParseError::EmptyPipe));
    }

    #[test]
    fn test_parse_v2_start_ref() {
        let result = parse_v2_start(&json!("@input.name")).unwrap();
        assert_eq!(result, V2Start::Ref(V2Ref::Input("name".to_string())));
    }

    #[test]
    fn test_parse_v2_start_pipe_value() {
        let result = parse_v2_start(&json!("$")).unwrap();
        assert_eq!(result, V2Start::PipeValue);
    }

    #[test]
    fn test_parse_v2_start_literal() {
        let result = parse_v2_start(&json!(123)).unwrap();
        assert_eq!(result, V2Start::Literal(json!(123)));

        let result = parse_v2_start(&json!(true)).unwrap();
        assert_eq!(result, V2Start::Literal(json!(true)));

        let result = parse_v2_start(&json!(null)).unwrap();
        assert_eq!(result, V2Start::Literal(json!(null)));
    }

    #[test]
    fn test_parse_v2_start_invalid_at_ref_error() {
        let invalid_refs = [json!("@"), json!("@foo-bar"), json!("@123invalid")];
        for value in invalid_refs {
            let err = parse_v2_start(&value).unwrap_err();
            assert!(matches!(err, V2ParseError::InvalidStart(_)));
        }
    }
}
