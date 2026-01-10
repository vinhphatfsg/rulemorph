use std::fs;
use std::io::{self, BufRead, BufReader, Write};

use serde_json::{json, Map, Value};
use transform_rules::{
    parse_rule_file, transform_stream, transform_with_warnings, validate_rule_file_with_source,
    InputFormat, RuleError, RuleFile, TransformError, TransformErrorKind, TransformWarning,
};

const PROTOCOL_VERSION: &str = "2024-11-05";

fn main() {
    if let Err(err) = run() {
        eprintln!("fatal: {}", err);
        std::process::exit(1);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OutputMode {
    Line,
    ContentLength,
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
            write_message(&mut writer, output_mode, &response)
                .map_err(|err| err.to_string())?;
        }
    }

    Ok(())
}

fn read_message(
    reader: &mut impl BufRead,
    output_mode: &mut OutputMode,
) -> io::Result<Option<String>> {
    let mut line = String::new();
    loop {
        line.clear();
        let bytes = reader.read_line(&mut line)?;
        if bytes == 0 {
            return Ok(None);
        }

        if let Some(length) = line.strip_prefix("Content-Length:") {
            let length = length.trim().parse::<usize>().map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "invalid Content-Length")
            })?;

            loop {
                line.clear();
                let bytes = reader.read_line(&mut line)?;
                if bytes == 0 {
                    return Ok(None);
                }
                if line == "\r\n" || line == "\n" {
                    break;
                }
            }

            let mut buffer = vec![0u8; length];
            reader.read_exact(&mut buffer)?;
            *output_mode = OutputMode::ContentLength;
            return Ok(Some(String::from_utf8_lossy(&buffer).to_string()));
        }

        let trimmed = line.trim_end_matches(['\r', '\n']);
        if trimmed.is_empty() {
            continue;
        }
        *output_mode = OutputMode::Line;
        return Ok(Some(trimmed.to_string()));
    }
}

fn write_message(writer: &mut impl Write, output_mode: OutputMode, message: &Value) -> io::Result<()> {
    let text = serde_json::to_string(message)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;

    match output_mode {
        OutputMode::Line => {
            writeln!(writer, "{}", text)?;
        }
        OutputMode::ContentLength => {
            write!(writer, "Content-Length: {}\r\n\r\n{}", text.len(), text)?;
        }
    }

    writer.flush()
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
            }
        },
        "serverInfo": {
            "name": "transform-rules-mcp",
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
                "inputSchema": tool_input_schema()
            }
        ]
    })
}

fn tool_input_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "rules_path": {
                "type": "string",
                "description": "Path to the YAML rules file. Mutually exclusive with rules_text.",
                "examples": ["rules.yaml"]
            },
            "rules_text": {
                "type": "string",
                "description": "Inline YAML rules content. Mutually exclusive with rules_path.",
                "examples": ["version: 1\ninput:\n  format: json\n  json: {}\nmappings:\n  - target: \"id\"\n    source: \"id\""]
            },
            "input_path": {
                "type": "string",
                "description": "Path to the input CSV/JSON file. Mutually exclusive with input_text and input_json.",
                "examples": ["input.json"]
            },
            "input_text": {
                "type": "string",
                "description": "Inline input text (CSV or JSON). Mutually exclusive with input_path and input_json.",
                "examples": ["{\"items\":[{\"id\":1}]}"]
            },
            "input_json": {
                "type": ["object", "array"],
                "description": "Inline input JSON value. Mutually exclusive with input_path and input_text.",
                "examples": [[{"id": 1}]]
            },
            "context_path": {
                "type": "string",
                "description": "Optional path to a JSON context file. Mutually exclusive with context_json.",
                "examples": ["context.json"]
            },
            "context_json": {
                "type": "object",
                "description": "Optional inline JSON context value. Mutually exclusive with context_path.",
                "examples": [{"tenant_id": "t-001"}]
            },
            "format": {
                "type": "string",
                "enum": ["csv", "json"],
                "description": "Override input format from the rule file.",
                "examples": ["json"]
            },
            "ndjson": {
                "type": "boolean",
                "description": "Emit NDJSON output (one JSON object per line).",
                "examples": [false]
            },
            "validate": {
                "type": "boolean",
                "description": "Validate the rule file before transforming.",
                "examples": [true]
            },
            "output_path": {
                "type": "string",
                "description": "Optional path to write the output.",
                "examples": ["out.json"]
            },
            "max_output_bytes": {
                "type": "integer",
                "minimum": 1,
                "description": "Maximum output size in bytes before truncation.",
                "examples": [1000000]
            },
            "preview_rows": {
                "type": "integer",
                "minimum": 1,
                "description": "Maximum rows to return when ndjson=true.",
                "examples": [100]
            },
            "return_output_json": {
                "type": "boolean",
                "description": "Include parsed output JSON in meta.output when ndjson=false and within size limits.",
                "examples": [false]
            }
        },
        "allOf": [
            {
                "oneOf": [
                    {
                        "required": ["rules_path"],
                        "not": { "required": ["rules_text"] }
                    },
                    {
                        "required": ["rules_text"],
                        "not": { "required": ["rules_path"] }
                    }
                ]
            },
            {
                "oneOf": [
                    {
                        "required": ["input_path"],
                        "not": {
                            "anyOf": [
                                { "required": ["input_text"] },
                                { "required": ["input_json"] }
                            ]
                        }
                    },
                    {
                        "required": ["input_text"],
                        "not": {
                            "anyOf": [
                                { "required": ["input_path"] },
                                { "required": ["input_json"] }
                            ]
                        }
                    },
                    {
                        "required": ["input_json"],
                        "not": {
                            "anyOf": [
                                { "required": ["input_path"] },
                                { "required": ["input_text"] }
                            ]
                        }
                    }
                ]
            },
            {
                "not": { "required": ["context_path", "context_json"] }
            }
        ]
    })
}

enum CallError {
    InvalidParams(String),
    Tool {
        message: String,
        errors: Option<Vec<Value>>,
    },
}

fn handle_tools_call(params: &Value) -> Result<Value, CallError> {
    let obj = params.as_object().ok_or_else(|| {
        CallError::InvalidParams("params must be an object".to_string())
    })?;
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
        _ => Ok(tool_error_result(&format!("unknown tool: {}", name), None)),
    }
}

fn run_transform_tool(args: &Map<String, Value>) -> Result<Value, CallError> {
    let rules_path = get_optional_string(args, "rules_path").map_err(CallError::InvalidParams)?;
    let rules_text = get_optional_string(args, "rules_text").map_err(CallError::InvalidParams)?;
    let input_path = get_optional_string(args, "input_path").map_err(CallError::InvalidParams)?;
    let input_text = get_optional_string(args, "input_text").map_err(CallError::InvalidParams)?;
    let input_json = get_optional_json_value(args, "input_json").map_err(CallError::InvalidParams)?;
    let context_path = get_optional_string(args, "context_path").map_err(CallError::InvalidParams)?;
    let context_json = get_optional_object(args, "context_json").map_err(CallError::InvalidParams)?;
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
    let preview_rows = get_optional_usize(args, "preview_rows").map_err(CallError::InvalidParams)?;
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
            .is_some_and(|value| value.eq_ignore_ascii_case("csv"))
    {
        return Err(CallError::InvalidParams(
            "format must be json when input_json is provided".to_string(),
        ));
    }

    let (mut rule, yaml) = match (rules_path.as_deref(), rules_text.as_deref()) {
        (Some(path), None) => {
            let yaml = fs::read_to_string(path).map_err(|err| {
                let message = format!("failed to read rules: {}", err);
                CallError::Tool {
                    message: message.clone(),
                    errors: Some(vec![io_error_json(&message, Some(path))]),
                }
            })?;
            let rule = parse_rule_file(&yaml).map_err(|err| {
                let message = format!("failed to parse rules: {}", err);
                CallError::Tool {
                    message: message.clone(),
                    errors: Some(vec![parse_error_json(&message, Some(path))]),
                }
            })?;
            (rule, yaml)
        }
        (None, Some(text)) => {
            let rule = parse_rule_file(text).map_err(|err| {
                let message = format!("failed to parse rules: {}", err);
                CallError::Tool {
                    message: message.clone(),
                    errors: Some(vec![parse_error_json(&message, None)]),
                }
            })?;
            (rule, text.to_string())
        }
        _ => {
            return Err(CallError::InvalidParams(
                "rules_path or rules_text is required".to_string(),
            ))
        }
    };

    let input = match (input_path.as_deref(), input_text.as_deref(), input_json.as_ref()) {
        (Some(path), None, None) => fs::read_to_string(path).map_err(|err| {
            let message = format!("failed to read input: {}", err);
            CallError::Tool {
                message: message.clone(),
                errors: Some(vec![io_error_json(&message, Some(path))]),
            }
        })?,
        (None, Some(text), None) => text.to_string(),
        (None, None, Some(value)) => serde_json::to_string(value).map_err(|err| {
            let message = format!("failed to serialize input JSON: {}", err);
            CallError::Tool {
                message: message.clone(),
                errors: Some(vec![parse_error_json(&message, None)]),
            }
        })?,
        _ => {
            return Err(CallError::InvalidParams(
                "input_path, input_text, or input_json is required".to_string(),
            ))
        }
    };

    let context_value = match (context_path.as_deref(), context_json.as_ref()) {
        (Some(path), None) => {
            let data = fs::read_to_string(path).map_err(|err| {
                let message = format!("failed to read context: {}", err);
                CallError::Tool {
                    message: message.clone(),
                    errors: Some(vec![io_error_json(&message, Some(path))]),
                }
            })?;
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
        let (output_text, warnings) = transform_to_ndjson(&rule, &input, context_value.as_ref())?;
        (None, output_text, warnings)
    } else {
        let (output, warnings) =
            transform_with_warnings(&rule, &input, context_value.as_ref()).map_err(|err| {
                CallError::Tool {
                    message: transform_error_to_text(&err),
                    errors: Some(vec![transform_error_json(&err)]),
                }
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
        write_output(path, &output_text).map_err(|err| {
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

fn tool_error_result(message: &str, errors: Option<Vec<Value>>) -> Value {
    let mut result = json!({
        "content": [
            {
                "type": "text",
                "text": message
            }
        ],
        "isError": true
    });

    if let Some(errors) = errors {
        result["meta"] = json!({ "errors": errors });
    }

    result
}

fn get_optional_string(args: &Map<String, Value>, key: &str) -> Result<Option<String>, String> {
    match args.get(key) {
        Some(Value::String(value)) => Ok(Some(value.clone())),
        Some(Value::Null) => Ok(None),
        Some(_) => Err(format!("{} must be a string", key)),
        None => Ok(None),
    }
}

fn get_optional_bool(args: &Map<String, Value>, key: &str) -> Result<Option<bool>, String> {
    match args.get(key) {
        Some(Value::Bool(value)) => Ok(Some(*value)),
        Some(Value::Null) => Ok(None),
        Some(_) => Err(format!("{} must be a boolean", key)),
        None => Ok(None),
    }
}

fn get_optional_usize(args: &Map<String, Value>, key: &str) -> Result<Option<usize>, String> {
    match args.get(key) {
        Some(Value::Number(value)) => value
            .as_u64()
            .and_then(|value| {
                if value > 0 {
                    Some(value as usize)
                } else {
                    None
                }
            })
            .ok_or_else(|| format!("{} must be a positive integer", key))
            .map(Some),
        Some(Value::Null) => Ok(None),
        Some(_) => Err(format!("{} must be a positive integer", key)),
        None => Ok(None),
    }
}

fn get_optional_json_value(args: &Map<String, Value>, key: &str) -> Result<Option<Value>, String> {
    match args.get(key) {
        Some(Value::Array(_)) | Some(Value::Object(_)) => Ok(args.get(key).cloned()),
        Some(Value::Null) => Ok(None),
        Some(_) => Err(format!("{} must be an object or array", key)),
        None => Ok(None),
    }
}

fn get_optional_object(args: &Map<String, Value>, key: &str) -> Result<Option<Value>, String> {
    match args.get(key) {
        Some(Value::Object(_)) => Ok(args.get(key).cloned()),
        Some(Value::Null) => Ok(None),
        Some(_) => Err(format!("{} must be an object", key)),
        None => Ok(None),
    }
}

fn apply_format_override(rule: &mut RuleFile, format: Option<&str>) -> Result<(), String> {
    let Some(format) = format else { return Ok(()); };
    let normalized = format.to_lowercase();
    rule.input.format = match normalized.as_str() {
        "csv" => InputFormat::Csv,
        "json" => InputFormat::Json,
        _ => return Err(format!("unknown format: {}", format)),
    };
    Ok(())
}

fn write_output(path: &str, output: &str) -> Result<(), String> {
    let path = std::path::Path::new(path);
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)
                .map_err(|err| format!("failed to create output directory: {}", err))?;
        }
    }
    fs::write(path, output.as_bytes()).map_err(|err| format!("failed to write output: {}", err))
}

fn transform_to_ndjson(
    rule: &RuleFile,
    input: &str,
    context: Option<&serde_json::Value>,
) -> Result<(String, Vec<TransformWarning>), CallError> {
    let stream = transform_stream(rule, input, context).map_err(|err| CallError::Tool {
        message: transform_error_to_text(&err),
        errors: Some(vec![transform_error_json(&err)]),
    })?;
    let mut output = String::new();
    let mut warnings = Vec::new();

    for item in stream {
        let item = item.map_err(|err| CallError::Tool {
            message: transform_error_to_text(&err),
            errors: Some(vec![transform_error_json(&err)]),
        })?;
        warnings.extend(item.warnings);
        let line = serde_json::to_string(&item.output).map_err(|err| {
            let message = format!("failed to serialize output JSON: {}", err);
            CallError::Tool {
                message: message.clone(),
                errors: Some(vec![parse_error_json(&message, None)]),
            }
        })?;
        output.push_str(&line);
        output.push('\n');
    }

    Ok((output, warnings))
}

fn validation_errors_to_text(errors: &[RuleError]) -> String {
    let values = validation_errors_to_values(errors);
    serde_json::to_string(&values).unwrap_or_else(|_| "validation error".to_string())
}

fn validation_errors_to_values(errors: &[RuleError]) -> Vec<Value> {
    errors.iter().map(validation_error_json).collect()
}

fn validation_error_json(err: &RuleError) -> Value {
    let mut value = json!({
        "type": "validation",
        "code": err.code.as_str(),
        "message": err.message,
    });

    if let Some(path) = &err.path {
        value["path"] = json!(path);
    }
    if let Some(location) = &err.location {
        value["line"] = json!(location.line);
        value["column"] = json!(location.column);
    }

    value
}

fn parse_error_json(message: &str, path: Option<&str>) -> Value {
    let mut value = json!({
        "type": "parse",
        "message": message,
    });
    if let Some(path) = path {
        value["path"] = json!(path);
    }
    value
}

fn io_error_json(message: &str, path: Option<&str>) -> Value {
    let mut value = json!({
        "type": "io",
        "message": message,
    });
    if let Some(path) = path {
        value["path"] = json!(path);
    }
    value
}

fn truncate_to_bytes(text: &str, max_bytes: usize) -> &str {
    if text.len() <= max_bytes {
        return text;
    }
    let mut end = max_bytes;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

fn preview_ndjson(text: &str, max_rows: usize) -> String {
    let mut preview = String::new();
    for (index, line) in text.split_terminator('\n').enumerate() {
        if index >= max_rows {
            break;
        }
        preview.push_str(line);
        preview.push('\n');
    }
    preview
}

fn transform_error_to_text(err: &TransformError) -> String {
    let value = transform_error_json(err);
    serde_json::to_string(&vec![value]).unwrap_or_else(|_| err.message.clone())
}

fn transform_error_json(err: &TransformError) -> Value {
    let mut value = json!({
        "type": "transform",
        "kind": transform_kind_to_str(&err.kind),
        "message": err.message,
    });
    if let Some(path) = &err.path {
        value["path"] = json!(path);
    }
    value
}

fn warnings_to_json(warnings: &[TransformWarning]) -> Value {
    let values: Vec<_> = warnings.iter().map(transform_warning_json).collect();
    Value::Array(values)
}

fn transform_warning_json(warning: &TransformWarning) -> Value {
    let mut value = json!({
        "type": "warning",
        "kind": transform_kind_to_str(&warning.kind),
        "message": warning.message,
    });
    if let Some(path) = &warning.path {
        value["path"] = json!(path);
    }
    value
}

fn transform_kind_to_str(kind: &TransformErrorKind) -> &'static str {
    match kind {
        TransformErrorKind::InvalidInput => "InvalidInput",
        TransformErrorKind::InvalidRecordsPath => "InvalidRecordsPath",
        TransformErrorKind::InvalidRef => "InvalidRef",
        TransformErrorKind::InvalidTarget => "InvalidTarget",
        TransformErrorKind::MissingRequired => "MissingRequired",
        TransformErrorKind::TypeCastFailed => "TypeCastFailed",
        TransformErrorKind::ExprError => "ExprError",
    }
}
