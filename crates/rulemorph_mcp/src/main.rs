use std::io::{self, BufReader};

use rulemorph::{
    generate_dto, transform_input_with_warnings, transform_input_with_warnings_with_base_dir,
    validate_rule_file_with_source,
};
use serde_json::{Map, Value, json};

mod args;
mod diagnostics;
mod dto_language;
mod dto_normalize;
mod dto_parse;
mod dto_schema;
mod errors;
mod input_analysis;
mod input_records;
mod path_expr;
mod prompts;
mod protocol;
mod resources;
mod rule_source;
mod rules_yaml;
mod sandbox;
mod schemas;
mod tools;

use self::args::{
    get_optional_bool, get_optional_json_value, get_optional_object, get_optional_string,
    get_optional_usize,
};
use self::diagnostics::{
    collect_rule_warnings, parse_error_json, preview_ndjson, rule_warnings_to_json,
    transform_error_json, transform_error_to_text, transform_to_ndjson, truncate_to_bytes,
    validation_errors_to_text, validation_errors_to_values, warnings_to_json,
};
use self::dto_language::{dto_error_json, dto_language_to_str, parse_dto_language};
use self::errors::{CallError, io_error_json, tool_error_result};
use self::prompts::{prompts_get_result, prompts_list_result};
use self::protocol::{OutputMode, read_message, write_message};
use self::resources::{resources_list_result, resources_read_result};
use self::rule_source::{
    OwnedInput, load_rule_from_source, rule_has_file_branch, validate_transform_format,
};
use self::rules_yaml::apply_format_override;
use self::sandbox::{read_allowed_bytes, read_allowed_to_string, write_allowed_output};
use self::schemas::{
    analyze_input_input_schema, generate_dto_input_schema, generate_rules_from_base_input_schema,
    generate_rules_from_dto_input_schema, list_ops_input_schema, transform_input_schema,
    validate_rules_input_schema,
};
use self::tools::analyze_input::run_analyze_input_tool;
use self::tools::generate_rules_from_base::run_generate_rules_from_base_tool;
use self::tools::generate_rules_from_dto::run_generate_rules_from_dto_tool;
use self::tools::list_ops::run_list_ops_tool;

const PROTOCOL_VERSION: &str = "2024-11-05";

fn main() {
    if let Err(err) = run() {
        eprintln!("fatal: {}", err);
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let stdin = io::stdin();
    let stdout = io::stdout();
    let mut reader = BufReader::new(stdin.lock());
    let mut writer = io::BufWriter::new(stdout.lock());
    let mut output_mode = OutputMode::Line;

    loop {
        let message = match read_message(&mut reader, &mut output_mode) {
            Ok(Some(message)) => message,
            Ok(None) => break,
            Err(err) => return Err(err.to_string()),
        };

        let value: Value = match serde_json::from_str(&message) {
            Ok(value) => value,
            Err(err) => {
                eprintln!("invalid json: {}", err);
                continue;
            }
        };

        if let Some(response) = handle_message(value) {
            write_message(&mut writer, output_mode, &response).map_err(|err| err.to_string())?;
        }
    }

    Ok(())
}

fn handle_message(message: Value) -> Option<Value> {
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

fn run_transform_tool(args: &Map<String, Value>) -> Result<Value, CallError> {
    let rules_path = get_optional_string(args, "rules_path").map_err(CallError::InvalidParams)?;
    let rules_text = get_optional_string(args, "rules_text").map_err(CallError::InvalidParams)?;
    let rules_format =
        get_optional_string(args, "rules_format").map_err(CallError::InvalidParams)?;
    let input_path = get_optional_string(args, "input_path").map_err(CallError::InvalidParams)?;
    let input_text = get_optional_string(args, "input_text").map_err(CallError::InvalidParams)?;
    let input_json =
        get_optional_json_value(args, "input_json").map_err(CallError::InvalidParams)?;
    let context_path =
        get_optional_string(args, "context_path").map_err(CallError::InvalidParams)?;
    let context_json =
        get_optional_object(args, "context_json").map_err(CallError::InvalidParams)?;
    let format = get_optional_string(args, "format").map_err(CallError::InvalidParams)?;
    let ndjson = get_optional_bool(args, "ndjson")
        .map_err(CallError::InvalidParams)?
        .unwrap_or(false);
    let validate = get_optional_bool(args, "validate")
        .map_err(CallError::InvalidParams)?
        .unwrap_or(false);
    let output_path = get_optional_string(args, "output_path").map_err(CallError::InvalidParams)?;
    let max_output_bytes =
        get_optional_usize(args, "max_output_bytes").map_err(CallError::InvalidParams)?;
    let preview_rows =
        get_optional_usize(args, "preview_rows").map_err(CallError::InvalidParams)?;
    let return_output_json = get_optional_bool(args, "return_output_json")
        .map_err(CallError::InvalidParams)?
        .unwrap_or(false);

    let rule_source_count = rules_path.is_some() as u8 + rules_text.is_some() as u8;
    if rule_source_count == 0 {
        return Err(CallError::InvalidParams(
            "rules_path or rules_text is required".to_string(),
        ));
    }
    if rule_source_count > 1 {
        return Err(CallError::InvalidParams(
            "rules_path and rules_text are mutually exclusive".to_string(),
        ));
    }

    let input_source_count =
        input_path.is_some() as u8 + input_text.is_some() as u8 + input_json.is_some() as u8;
    if input_source_count == 0 {
        return Err(CallError::InvalidParams(
            "input_path, input_text, or input_json is required".to_string(),
        ));
    }
    if input_source_count > 1 {
        return Err(CallError::InvalidParams(
            "input_path, input_text, and input_json are mutually exclusive".to_string(),
        ));
    }

    if context_path.is_some() && context_json.is_some() {
        return Err(CallError::InvalidParams(
            "context_path and context_json are mutually exclusive".to_string(),
        ));
    }

    if input_json.is_some()
        && format
            .as_deref()
            .is_some_and(|value| !value.eq_ignore_ascii_case("json"))
    {
        return Err(CallError::InvalidParams(
            "format must be json when input_json is provided".to_string(),
        ));
    }
    validate_transform_format(format.as_deref())?;

    let (mut rule, yaml, base_dir) = load_rule_from_source(
        rules_path.as_deref(),
        rules_text.as_deref(),
        rules_format.as_deref(),
    )?;
    if rules_text.is_some() && rule_has_file_branch(&rule) {
        return Err(CallError::InvalidParams(
            "rules_text cannot use branch file references; use rules_path under an allowed root"
                .to_string(),
        ));
    }

    let input = match (
        input_path.as_deref(),
        input_text.as_deref(),
        input_json.as_ref(),
    ) {
        (Some(path), None, None) => OwnedInput::Bytes(read_allowed_bytes(path, "input")?),
        (None, Some(text), None) => OwnedInput::Text(text.to_string()),
        (None, None, Some(value)) => serde_json::to_string(value)
            .map_err(|err| {
                let message = format!("failed to serialize input JSON: {}", err);
                CallError::Tool {
                    message: message.clone(),
                    errors: Some(vec![parse_error_json(&message, None)]),
                }
            })
            .map(OwnedInput::Text)?,
        _ => {
            return Err(CallError::InvalidParams(
                "input_path, input_text, or input_json is required".to_string(),
            ));
        }
    };

    let context_value = match (context_path.as_deref(), context_json.as_ref()) {
        (Some(path), None) => {
            let data = read_allowed_to_string(path, "context")?;
            Some(serde_json::from_str(&data).map_err(|err| {
                let message = format!("failed to parse context JSON: {}", err);
                CallError::Tool {
                    message: message.clone(),
                    errors: Some(vec![parse_error_json(&message, Some(path))]),
                }
            })?)
        }
        (None, Some(value)) => Some(value.clone()),
        (None, None) => None,
        _ => None,
    };

    let format_override = if input_json.is_some() {
        Some("json".to_string())
    } else {
        format
    };
    apply_format_override(&mut rule, format_override.as_deref())
        .map_err(CallError::InvalidParams)?;

    if validate {
        if let Err(errors) = validate_rule_file_with_source(&rule, &yaml) {
            let error_text = validation_errors_to_text(&errors);
            let error_values = validation_errors_to_values(&errors);
            return Err(CallError::Tool {
                message: error_text,
                errors: Some(error_values),
            });
        }
    }

    let (output_value, output_text, warnings) = if ndjson {
        let (output_text, warnings) = transform_to_ndjson(
            &rule,
            input.as_input_data(),
            context_value.as_ref(),
            base_dir.as_deref(),
        )?;
        (None, output_text, warnings)
    } else {
        let (output, warnings) = match base_dir.as_deref() {
            Some(base_dir) => transform_input_with_warnings_with_base_dir(
                &rule,
                input.as_input_data(),
                context_value.as_ref(),
                base_dir,
            ),
            None => {
                transform_input_with_warnings(&rule, input.as_input_data(), context_value.as_ref())
            }
        }
        .map_err(|err| CallError::Tool {
            message: transform_error_to_text(&err),
            errors: Some(vec![transform_error_json(&err)]),
        })?;
        let output_text = serde_json::to_string(&output).map_err(|err| {
            let message = format!("failed to serialize output JSON: {}", err);
            CallError::Tool {
                message: message.clone(),
                errors: Some(vec![parse_error_json(&message, None)]),
            }
        })?;
        (Some(output), output_text, warnings)
    };

    if let Some(path) = output_path.as_deref() {
        write_allowed_output(path, &output_text).map_err(|err| {
            let message = err;
            CallError::Tool {
                message: message.clone(),
                errors: Some(vec![io_error_json(&message, Some(path))]),
            }
        })?;
    }

    let output_bytes = output_text.as_bytes().len();
    let mut response_text = output_text.clone();
    let mut truncated = false;

    if ndjson {
        if let Some(limit) = preview_rows {
            let preview = preview_ndjson(&output_text, limit);
            if preview.len() != output_text.len() {
                truncated = true;
            }
            response_text = preview;
        }
    }

    if let Some(max_bytes) = max_output_bytes {
        if output_bytes > max_bytes {
            truncated = true;
        }
        if response_text.as_bytes().len() > max_bytes {
            response_text = truncate_to_bytes(&response_text, max_bytes).to_string();
            truncated = true;
        }
    }

    let mut result = json!({
        "content": [
            {
                "type": "text",
                "text": response_text
            }
        ]
    });

    let exceeds_max = max_output_bytes.map_or(false, |max| output_bytes > max);
    let mut meta = serde_json::Map::new();
    if !warnings.is_empty() {
        meta.insert("warnings".to_string(), warnings_to_json(&warnings));
    }
    if let Some(path) = output_path {
        meta.insert("output_path".to_string(), json!(path));
    }
    if truncated {
        meta.insert("output_bytes".to_string(), json!(output_bytes));
        meta.insert("truncated".to_string(), json!(true));
    }
    if return_output_json && !ndjson && !exceeds_max {
        if let Some(output) = output_value {
            meta.insert("output".to_string(), output);
        }
    }
    if !meta.is_empty() {
        result["meta"] = Value::Object(meta);
    }

    Ok(result)
}

fn run_validate_rules_tool(args: &Map<String, Value>) -> Result<Value, CallError> {
    let rules_path = get_optional_string(args, "rules_path").map_err(CallError::InvalidParams)?;
    let rules_text = get_optional_string(args, "rules_text").map_err(CallError::InvalidParams)?;
    let rules_format =
        get_optional_string(args, "rules_format").map_err(CallError::InvalidParams)?;

    let rule_source_count = rules_path.is_some() as u8 + rules_text.is_some() as u8;
    if rule_source_count == 0 {
        return Err(CallError::InvalidParams(
            "rules_path or rules_text is required".to_string(),
        ));
    }
    if rule_source_count > 1 {
        return Err(CallError::InvalidParams(
            "rules_path and rules_text are mutually exclusive".to_string(),
        ));
    }

    let (rule, yaml, _) = load_rule_from_source(
        rules_path.as_deref(),
        rules_text.as_deref(),
        rules_format.as_deref(),
    )?;
    match validate_rule_file_with_source(&rule, &yaml) {
        Ok(_) => {
            let warnings = collect_rule_warnings(&rule);
            let mut result = json!({
                "content": [
                    {
                        "type": "text",
                        "text": "ok"
                    }
                ]
            });
            if !warnings.is_empty() {
                result["meta"] = json!({
                    "warnings": rule_warnings_to_json(&warnings)
                });
            }
            Ok(result)
        }
        Err(errors) => {
            let error_values = validation_errors_to_values(&errors);
            Ok(json!({
                "content": [
                    {
                        "type": "text",
                        "text": "validation failed"
                    }
                ],
                "isError": true,
                "meta": {
                    "errors": error_values
                }
            }))
        }
    }
}

fn run_generate_dto_tool(args: &Map<String, Value>) -> Result<Value, CallError> {
    let rules_path = get_optional_string(args, "rules_path").map_err(CallError::InvalidParams)?;
    let rules_text = get_optional_string(args, "rules_text").map_err(CallError::InvalidParams)?;
    let rules_format =
        get_optional_string(args, "rules_format").map_err(CallError::InvalidParams)?;
    let language = get_optional_string(args, "language").map_err(CallError::InvalidParams)?;
    let name = get_optional_string(args, "name").map_err(CallError::InvalidParams)?;

    let rule_source_count = rules_path.is_some() as u8 + rules_text.is_some() as u8;
    if rule_source_count == 0 {
        return Err(CallError::InvalidParams(
            "rules_path or rules_text is required".to_string(),
        ));
    }
    if rule_source_count > 1 {
        return Err(CallError::InvalidParams(
            "rules_path and rules_text are mutually exclusive".to_string(),
        ));
    }

    let language =
        language.ok_or_else(|| CallError::InvalidParams("language is required".to_string()))?;
    let language = parse_dto_language(&language).map_err(CallError::InvalidParams)?;

    let (rule, _, _) = load_rule_from_source(
        rules_path.as_deref(),
        rules_text.as_deref(),
        rules_format.as_deref(),
    )?;
    let dto = generate_dto(&rule, language, name.as_deref()).map_err(|err| {
        let message = format!("failed to generate dto: {}", err);
        CallError::Tool {
            message: message.clone(),
            errors: Some(vec![dto_error_json(&message)]),
        }
    })?;

    let mut meta = serde_json::Map::new();
    meta.insert("language".to_string(), json!(dto_language_to_str(language)));
    if let Some(name) = name {
        meta.insert("name".to_string(), json!(name));
    }

    Ok(json!({
        "content": [
            {
                "type": "text",
                "text": dto
            }
        ],
        "meta": meta
    }))
}
