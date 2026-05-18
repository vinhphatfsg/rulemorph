use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};

use rulemorph::{Mapping, RuleFile, parse_rule_file};
use serde_json::{Value, json};

pub struct McpServer {
    child: Child,
    stdin: Option<ChildStdin>,
    stdout: BufReader<std::process::ChildStdout>,
}

impl McpServer {
    pub fn start() -> Self {
        let bin = env!("CARGO_BIN_EXE_rulemorph-mcp");
        let mut child = Command::new(bin)
            .env("RULEMORPH_MCP_ALLOW_ANY_PATH", "1")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn mcp server");

        let stdin = child.stdin.take().expect("take stdin");
        let stdout = child.stdout.take().expect("take stdout");

        Self {
            child,
            stdin: Some(stdin),
            stdout: BufReader::new(stdout),
        }
    }

    pub fn start_with_allowed_root(root: &Path) -> Self {
        let bin = env!("CARGO_BIN_EXE_rulemorph-mcp");
        let mut child = Command::new(bin)
            .env("RULEMORPH_MCP_ALLOWED_ROOTS", root)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn mcp server");

        let stdin = child.stdin.take().expect("take stdin");
        let stdout = child.stdout.take().expect("take stdout");

        Self {
            child,
            stdin: Some(stdin),
            stdout: BufReader::new(stdout),
        }
    }

    pub fn send(&mut self, message: &Value) -> Value {
        let text = serde_json::to_string(message).expect("serialize request");
        let stdin = self.stdin.as_mut().expect("stdin available");
        writeln!(stdin, "{}", text).expect("write request");
        stdin.flush().expect("flush request");

        let mut line = String::new();
        self.stdout.read_line(&mut line).expect("read response");
        assert!(!line.trim().is_empty(), "empty response");
        serde_json::from_str(&line).expect("parse response")
    }

    pub fn shutdown(mut self) {
        self.stdin.take();
        let _ = self.child.wait();
    }
}

pub fn initialize(server: &mut McpServer) {
    let request = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {
                "name": "tests",
                "version": "0.0"
            }
        }
    });
    let response = server.send(&request);
    assert_eq!(response["result"]["protocolVersion"], "2024-11-05");
}

pub fn tool_call_request(id: u64, name: &str, arguments: Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "tools/call",
        "params": {
            "name": name,
            "arguments": arguments
        }
    })
}

pub fn content_text(response: &Value) -> &str {
    response["result"]["content"][0]["text"]
        .as_str()
        .expect("content text")
}

pub fn content_json(response: &Value) -> Value {
    serde_json::from_str(content_text(response)).expect("content json")
}

pub fn call_tool(server: &mut McpServer, id: u64, name: &str, arguments: Value) -> Value {
    let request = tool_call_request(id, name, arguments);
    server.send(&request)
}

pub fn call_tool_text(server: &mut McpServer, id: u64, name: &str, arguments: Value) -> String {
    content_text(&call_tool(server, id, name, arguments)).to_string()
}

pub fn call_tool_rule(server: &mut McpServer, id: u64, name: &str, arguments: Value) -> RuleFile {
    parse_generated_rule(&call_tool_text(server, id, name, arguments))
}

pub fn parse_generated_rule(output_text: &str) -> RuleFile {
    parse_rule_file(output_text).expect("parse output rules")
}

pub fn mapping_by_target<'a>(rule: &'a RuleFile, target: &str) -> &'a Mapping {
    rule.mappings
        .iter()
        .find(|mapping| mapping.target == target)
        .unwrap_or_else(|| panic!("{target} mapping"))
}

pub fn core_fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace crates dir")
        .join("rulemorph")
        .join("tests")
        .join("fixtures")
}
