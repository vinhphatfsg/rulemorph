#[test]
fn test_is_v2_expr_pipe_array() {
        // Helper function to detect v2 syntax
        assert!(is_v2_expr(&json!(["@input.name", "trim"])));
        assert!(is_v2_expr(&json!([])));
        assert!(is_v2_expr(&json!(["hello", "trim"])));
        assert!(is_v2_expr(&json!([{"lookup_first": []}, "trim"])));
        assert!(is_v2_expr(&json!("@input.name")));
        assert!(is_v2_expr(&json!("lit:@input.name")));
        assert!(!is_v2_expr(&json!({ "ref": "input.name" })));
        assert!(!is_v2_expr(&json!({ "op": "uppercase", "args": [] })));
}
