use std::path::Path;

use super::{RuleFormat, RuleParseError};
use crate::error::YamlLocation;

#[test]
fn rule_format_from_path_keeps_json_extension_detection() {
    assert_eq!(
        RuleFormat::from_path(Path::new("rules.json")),
        RuleFormat::Json
    );
    assert_eq!(
        RuleFormat::from_path(Path::new("rules.JSON")),
        RuleFormat::Json
    );
    assert_eq!(
        RuleFormat::from_path(Path::new("rules.yaml")),
        RuleFormat::Yaml
    );
    assert_eq!(
        RuleFormat::from_path(Path::new("rules.yml")),
        RuleFormat::Yaml
    );
    assert_eq!(RuleFormat::from_path(Path::new("rules")), RuleFormat::Yaml);
}

#[test]
fn rule_parse_error_display_and_location_are_stable() {
    let err = RuleParseError {
        format: RuleFormat::Json,
        message: "duplicate key".to_string(),
        location: Some(YamlLocation { line: 2, column: 4 }),
    };

    assert_eq!(err.line_column(), Some((2, 4)));
    assert_eq!(err.to_string(), "failed to parse json rules: duplicate key");
}
