use std::fs;

use serde_json::{Value, json};
use tempfile::tempdir;

mod common;

use common::stdio::{
    McpServer, assert_tool_schema_enum, assert_tool_schema_required, call_tool, call_tool_rule,
    content_json, content_text, core_fixtures_dir, initialize, list_tools, mapping_by_target,
    tool_by_name, tool_call_request, tool_schema_property, tools_array,
};

include!("stdio/catalog.rs");

include!("stdio/transform.rs");

include!("stdio/transform_paths.rs");

include!("stdio/validate.rs");

include!("stdio/dto.rs");

include!("stdio/analyze_generate.rs");
