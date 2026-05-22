mod json;
mod yaml;

pub use self::json::parse_json_value_strict;
pub use self::yaml::{
    StrictYamlError, parse_yaml_value_strict, parse_yaml_value_strict_with_limits,
};
