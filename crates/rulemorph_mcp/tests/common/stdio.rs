use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};

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

pub fn core_fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace crates dir")
        .join("rulemorph")
        .join("tests")
        .join("fixtures")
}
