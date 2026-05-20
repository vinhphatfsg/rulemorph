use std::fs;

use assert_cmd::cargo::cargo_bin_cmd;

#[path = "common/cli.rs"]
mod cli_common;

#[cfg(feature = "server")]
use cli_common::stdout_json;
use cli_common::{
    assert_json_stdout_eq, fixtures_dir, read_json, rulemorph_output, stderr_json, stderr_string,
    stdout_string,
};

include!("cli/input_formats.rs");

include!("cli/preflight.rs");

include!("cli/transform_command.rs");

include!("cli/validate.rs");

include!("cli/limits.rs");

include!("cli/generate.rs");

include!("cli/api_keys.rs");
