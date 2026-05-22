use serde_json::json;

use super::{PathError, PathToken, get_path, parse_path};

#[test]
fn parse_path_keeps_dot_index_and_quoted_segments() {
    assert_eq!(
        parse_path(r#"items[12]["full.name"]['quote\'key']"#),
        Ok(vec![
            PathToken::Key("items".to_string()),
            PathToken::Index(12),
            PathToken::Key("full.name".to_string()),
            PathToken::Key("quote'key".to_string()),
        ])
    );
}

#[test]
fn parse_path_keeps_error_kinds_for_invalid_segments() {
    assert_eq!(parse_path(""), Err(PathError::Empty));
    assert_eq!(parse_path(".name"), Err(PathError::EmptyKey));
    assert_eq!(parse_path("name."), Err(PathError::InvalidSyntax));
    assert_eq!(parse_path(r#"name[""]"#), Err(PathError::EmptyKey));
    assert_eq!(
        parse_path(r#"name["bad\q"]"#),
        Err(PathError::InvalidEscape)
    );
    assert_eq!(parse_path("name[abc]"), Err(PathError::InvalidSyntax));
}

#[test]
fn parse_path_saturates_oversized_indexes() {
    assert_eq!(
        parse_path("items[999999999999999999999999999999999999999999999]"),
        Ok(vec![
            PathToken::Key("items".to_string()),
            PathToken::Index(usize::MAX),
        ])
    );
}

#[test]
fn get_path_traverses_objects_and_arrays_without_coercion() {
    let value = json!({
        "items": [
            {"name": "first"},
            {"name": "second"}
        ]
    });
    let tokens = parse_path("items[1].name").expect("path parses");

    assert_eq!(get_path(&value, &tokens), Some(&json!("second")));
    assert_eq!(
        get_path(
            &value,
            &[PathToken::Key("items".to_string()), PathToken::Index(3)]
        ),
        None
    );
    assert_eq!(
        get_path(
            &value,
            &[
                PathToken::Key("items".to_string()),
                PathToken::Key("0".to_string())
            ]
        ),
        None
    );
}
