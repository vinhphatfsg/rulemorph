use std::fs;

use rulemorph::{
    ErrorCode, RuleFormat, parse_rule_file, parse_rule_file_with_format, validate_rule_file,
    validate_rule_file_with_source,
};

mod common;

use common::validation::{fixtures_dir, load_expected_errors, load_rule, normalize_errors};

include!("validation/core.rs");

include!("validation/input_format.rs");

include!("validation/rule_format.rs");

// =============================================================================
// v2 Validation Tests
// =============================================================================

include!("validation/v2.rs");
