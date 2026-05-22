// Tests are kept in a sibling module so endpoint_engine.rs can stay focused on runtime code.
use super::*;
use futures_util::stream;
use rulemorph::parse_rule_file;
use serde_json::json;
use std::fs::File;
use std::io::Write;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

mod support;
use support::*;

include!("tests/security.rs");
include!("tests/trace_graph.rs");
include!("tests/internal_auth.rs");

include!("tests/path_zip.rs");
include!("tests/network_body.rs");
include!("tests/reply.rs");

include!("tests/payload_limits.rs");

include!("tests/catch.rs");

include!("tests/multipart_import.rs");
