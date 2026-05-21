mod v2_ref_parser_tests {
    use super::*;

    #[test]
    fn test_parse_input_ref() {
        assert_eq!(
            parse_v2_ref("@input.name"),
            Some(V2Ref::Input("name".to_string()))
        );
        assert_eq!(
            parse_v2_ref("@input.user.profile.name"),
            Some(V2Ref::Input("user.profile.name".to_string()))
        );
        assert_eq!(
            parse_v2_ref("@input.items[0].id"),
            Some(V2Ref::Input("items[0].id".to_string()))
        );
        assert_eq!(parse_v2_ref("@input"), Some(V2Ref::Input(String::new())));
    }

    #[test]
    fn test_parse_context_ref() {
        assert_eq!(
            parse_v2_ref("@context.config"),
            Some(V2Ref::Context("config".to_string()))
        );
        assert_eq!(
            parse_v2_ref("@context.users[0].id"),
            Some(V2Ref::Context("users[0].id".to_string()))
        );
        assert_eq!(
            parse_v2_ref("@context"),
            Some(V2Ref::Context(String::new()))
        );
    }

    #[test]
    fn test_parse_out_ref() {
        assert_eq!(
            parse_v2_ref("@out.user_id"),
            Some(V2Ref::Out("user_id".to_string()))
        );
        assert_eq!(
            parse_v2_ref("@out.computed_field"),
            Some(V2Ref::Out("computed_field".to_string()))
        );
        assert_eq!(parse_v2_ref("@out"), Some(V2Ref::Out(String::new())));
    }

    #[test]
    fn test_parse_item_ref() {
        assert_eq!(
            parse_v2_ref("@item.value"),
            Some(V2Ref::Item("value".to_string()))
        );
        assert_eq!(parse_v2_ref("@item"), Some(V2Ref::Item(String::new())));
    }

    #[test]
    fn test_parse_acc_ref() {
        assert_eq!(
            parse_v2_ref("@acc.total"),
            Some(V2Ref::Acc("total".to_string()))
        );
        assert_eq!(parse_v2_ref("@acc"), Some(V2Ref::Acc(String::new())));
    }

    #[test]
    fn test_parse_local_ref() {
        assert_eq!(
            parse_v2_ref("@myVar"),
            Some(V2Ref::Local("myVar".to_string()))
        );
        assert_eq!(
            parse_v2_ref("@price"),
            Some(V2Ref::Local("price".to_string()))
        );
        assert_eq!(
            parse_v2_ref("@_temp"),
            Some(V2Ref::Local("_temp".to_string()))
        );
        assert_eq!(
            parse_v2_ref("@var123"),
            Some(V2Ref::Local("var123".to_string()))
        );
    }

    #[test]
    fn test_invalid_refs() {
        // No @ prefix
        assert_eq!(parse_v2_ref("input.name"), None);
        // Empty after @
        assert_eq!(parse_v2_ref("@"), None);
        // Trailing dot for root namespaces should be invalid
        assert_eq!(parse_v2_ref("@input."), None);
        assert_eq!(parse_v2_ref("@context."), None);
        assert_eq!(parse_v2_ref("@out."), None);
        assert_eq!(parse_v2_ref("@item."), None);
        assert_eq!(parse_v2_ref("@acc."), None);
        // Invalid identifier
        assert_eq!(parse_v2_ref("@123invalid"), None);
    }

    #[test]
    fn test_is_pipe_value() {
        assert!(is_pipe_value("$"));
        assert!(!is_pipe_value("$$"));
        assert!(!is_pipe_value("@input.name"));
        assert!(!is_pipe_value(""));
    }

    #[test]
    fn test_is_literal_escape() {
        assert!(is_literal_escape("lit:@input.name"));
        assert!(is_literal_escape("lit:$"));
        assert!(is_literal_escape("lit:"));
        assert!(!is_literal_escape("@input.name"));
        assert!(!is_literal_escape("literal:"));
    }

    #[test]
    fn test_extract_literal() {
        assert_eq!(extract_literal("lit:@input.name"), Some("@input.name"));
        assert_eq!(extract_literal("lit:$"), Some("$"));
        assert_eq!(extract_literal("lit:"), Some(""));
        assert_eq!(extract_literal("@input.name"), None);
    }

    #[test]
    fn test_is_v2_ref() {
        assert!(is_v2_ref("@input.name"));
        assert!(is_v2_ref("@myVar"));
        assert!(!is_v2_ref("input.name"));
        assert!(!is_v2_ref("$"));
        assert!(!is_v2_ref("lit:@input"));
    }
}
