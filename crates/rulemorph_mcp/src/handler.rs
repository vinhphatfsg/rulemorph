use serde_json::{Value, json};

use crate::errors::{CallError, tool_error_result};
use crate::prompts::{prompts_get_result, prompts_list_result};
use crate::resources::{resources_list_result, resources_read_result};
use crate::schemas::{
    analyze_input_input_schema, generate_dto_input_schema, generate_rules_from_base_input_schema,
    generate_rules_from_dto_input_schema, list_ops_input_schema, transform_input_schema,
    validate_rules_input_schema,
};
use crate::tools::analyze_input::run_analyze_input_tool;
use crate::tools::generate_dto::run_generate_dto_tool;
use crate::tools::generate_rules_from_base::run_generate_rules_from_base_tool;
use crate::tools::generate_rules_from_dto::run_generate_rules_from_dto_tool;
use crate::tools::list_ops::run_list_ops_tool;
use crate::tools::transform::run_transform_tool;
use crate::tools::validate::run_validate_rules_tool;

const PROTOCOL_VERSION: &str = "2024-11-05";

pub(crate) fn handle_message(message: Value) -> Option<Value> {
    let obj = message.as_object()?;
    let id = obj.get("id").cloned();
    let method = obj.get("method").and_then(|value| value.as_str());

    let Some(method) = method else {
        return id.map(|id| error_response(id, -32600, "Invalid Request"));
    };

    match method {
        "initialize" => id.map(|id| ok_response(id, initialize_result())),
        "tools/list" => id.map(|id| ok_response(id, tools_list_result())),
        "tools/call" => {
            let id = id?;
            let params = obj.get("params").cloned().unwrap_or(Value::Null);
            match handle_tools_call(&params) {
                Ok(result) => Some(ok_response(id, result)),
                Err(CallError::InvalidParams(message)) => {
                    Some(error_response(id, -32602, &message))
                }
                Err(CallError::Tool { message, errors }) => {
                    Some(ok_response(id, tool_error_result(&message, errors)))
                }
            }
        }
        "resources/list" => id.map(|id| ok_response(id, resources_list_result())),
        "resources/read" => {
            let id = id?;
            let params = obj.get("params").cloned().unwrap_or(Value::Null);
            match resources_read_result(&params) {
                Ok(result) => Some(ok_response(id, result)),
                Err(message) => Some(error_response(id, -32602, &message)),
            }
        }
        "prompts/list" => id.map(|id| ok_response(id, prompts_list_result())),
        "prompts/get" => {
            let id = id?;
            let params = obj.get("params").cloned().unwrap_or(Value::Null);
            match prompts_get_result(&params) {
                Ok(result) => Some(ok_response(id, result)),
                Err(message) => Some(error_response(id, -32602, &message)),
            }
        }
        "ping" => id.map(|id| ok_response(id, json!({}))),
        "shutdown" => id.map(|id| ok_response(id, Value::Null)),
        "initialized" => None,
        _ => id.map(|id| error_response(id, -32601, "Method not found")),
    }
}

fn ok_response(id: Value, result: Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": result,
    })
}

fn error_response(id: Value, code: i32, message: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {
            "code": code,
            "message": message,
        }
    })
}

fn initialize_result() -> Value {
    json!({
        "protocolVersion": PROTOCOL_VERSION,
        "capabilities": {
            "tools": {
                "listChanged": false
            },
            "resources": {
                "listChanged": false
            },
            "prompts": {
                "listChanged": false
            }
        },
        "serverInfo": {
            "name": "rulemorph-mcp",
            "version": env!("CARGO_PKG_VERSION")
        }
    })
}

fn tools_list_result() -> Value {
    json!({
        "tools": [
            {
                "name": "transform",
                "description": "Transform CSV/JSON input with a YAML rule file.",
                "inputSchema": transform_input_schema()
            },
            {
                "name": "validate_rules",
                "description": "Validate a YAML rule file.",
                "inputSchema": validate_rules_input_schema()
            },
            {
                "name": "generate_dto",
                "description": "Generate DTO definitions from a YAML rule file.",
                "inputSchema": generate_dto_input_schema()
            },
            {
                "name": "list_ops",
                "description": "List supported expression ops, comparisons, and type casts.",
                "inputSchema": list_ops_input_schema()
            },
            {
                "name": "analyze_input",
                "description": "Analyze input data and summarize field paths and types.",
                "inputSchema": analyze_input_input_schema()
            },
            {
                "name": "generate_rules_from_base",
                "description": "Generate rules by mapping input data to existing rule targets.",
                "inputSchema": generate_rules_from_base_input_schema()
            },
            {
                "name": "generate_rules_from_dto",
                "description": "Generate rules by mapping input data to a DTO schema.",
                "inputSchema": generate_rules_from_dto_input_schema()
            }
        ]
    })
}

fn handle_tools_call(params: &Value) -> Result<Value, CallError> {
    let obj = params
        .as_object()
        .ok_or_else(|| CallError::InvalidParams("params must be an object".to_string()))?;
    let name = obj
        .get("name")
        .and_then(|value| value.as_str())
        .ok_or_else(|| CallError::InvalidParams("params.name is required".to_string()))?;
    let args = obj
        .get("arguments")
        .and_then(|value| value.as_object())
        .ok_or_else(|| {
            CallError::InvalidParams("params.arguments must be an object".to_string())
        })?;

    match name {
        "transform" => run_transform_tool(args),
        "validate_rules" => run_validate_rules_tool(args),
        "generate_dto" => run_generate_dto_tool(args),
        "list_ops" => Ok(run_list_ops_tool()),
        "analyze_input" => run_analyze_input_tool(args),
        "generate_rules_from_base" => run_generate_rules_from_base_tool(args),
        "generate_rules_from_dto" => run_generate_rules_from_dto_tool(args),
        _ => Ok(tool_error_result(&format!("unknown tool: {}", name), None)),
    }
}
