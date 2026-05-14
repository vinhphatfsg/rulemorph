use std::collections::{HashMap, HashSet};
use std::io::{self, BufReader};
use std::path::{Path, PathBuf};

use rulemorph::{
    Expr, ExprChain, ExprOp, InputData, InputFormat, RuleError, RuleFile, RuleFormat,
    TransformError, TransformErrorKind, TransformWarning, generate_dto,
    parse_rule_file_with_format, transform_input_with_warnings,
    transform_input_with_warnings_with_base_dir, transform_stream_input,
    transform_stream_input_with_base_dir, validate_rule_file_with_source,
};
use serde_json::{Map, Value, json};
use serde_yaml::{Mapping as YamlMapping, Value as YamlValue};

mod args;
mod dto_language;
mod dto_normalize;
mod dto_schema;
mod errors;
mod input_analysis;
mod input_records;
mod path_expr;
mod prompts;
mod protocol;
mod resources;
mod sandbox;
mod schemas;
mod tools;

use self::args::{
    get_optional_bool, get_optional_json_value, get_optional_object, get_optional_string,
    get_optional_usize,
};
use self::dto_language::{
    DtoSourceLanguage, dto_error_json, dto_language_to_str, parse_dto_language,
    parse_dto_source_language,
};
use self::dto_normalize::{
    normalize_java_text, normalize_kotlin_text, normalize_python_text, normalize_rust_text,
    normalize_swift_text, normalize_typescript_text,
};
use self::dto_schema::{
    DtoField, DtoFieldType, DtoSchema, DtoType, PrimitiveKind, generate_mappings_from_schema,
};
use self::errors::{CallError, io_error_json, tool_error_result};
use self::input_analysis::{analyze_records, build_input_paths, select_candidates, stats_to_json};
use self::input_records::{
    InputDataFormat, json_records_from_value, normalize_format, parse_csv_records,
    parse_json_records_strict,
};
use self::path_expr::leaf_from_path;
use self::prompts::{prompts_get_result, prompts_list_result};
use self::protocol::{OutputMode, read_message, write_message};
use self::resources::{resources_list_result, resources_read_result};
use self::sandbox::{
    read_allowed_bytes, read_allowed_file, read_allowed_to_string, write_allowed_output,
};
use self::schemas::{
    analyze_input_input_schema, generate_dto_input_schema, generate_rules_from_base_input_schema,
    generate_rules_from_dto_input_schema, list_ops_input_schema, transform_input_schema,
    validate_rules_input_schema,
};
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

fn run_analyze_input_tool(args: &Map<String, Value>) -> Result<Value, CallError> {
    let input_path = get_optional_string(args, "input_path").map_err(CallError::InvalidParams)?;
    let input_text = get_optional_string(args, "input_text").map_err(CallError::InvalidParams)?;
    let input_json =
        get_optional_json_value(args, "input_json").map_err(CallError::InvalidParams)?;
    let format = get_optional_string(args, "format").map_err(CallError::InvalidParams)?;
    let records_path =
        get_optional_string(args, "records_path").map_err(CallError::InvalidParams)?;
    let max_paths = get_optional_usize(args, "max_paths").map_err(CallError::InvalidParams)?;

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

    if input_json.is_some()
        && format
            .as_deref()
            .is_some_and(|value| value.eq_ignore_ascii_case("csv"))
    {
        return Err(CallError::InvalidParams(
            "format must be json when input_json is provided".to_string(),
        ));
    }

    let input_text = match (input_path.as_deref(), input_text.as_deref()) {
        (Some(path), None) => read_allowed_to_string(path, "input")?,
        (None, Some(text)) => text.to_string(),
        (None, None) => String::new(),
        _ => {
            return Err(CallError::InvalidParams(
                "input_path, input_text, or input_json is required".to_string(),
            ));
        }
    };

    let records = if let Some(value) = input_json {
        json_records_from_value(&value, records_path.as_deref())?
    } else {
        match normalize_format(format.as_deref(), &input_text) {
            InputDataFormat::Json => parse_json_records_strict(
                &input_text,
                records_path.as_deref(),
                input_path.as_deref(),
            )?,
            InputDataFormat::Csv => parse_csv_records(&input_text).map_err(|err| {
                let message = format!("failed to parse input CSV: {}", err);
                CallError::Tool {
                    message: message.clone(),
                    errors: Some(vec![parse_error_json(&message, input_path.as_deref())]),
                }
            })?,
        }
    };

    let stats = analyze_records(&records, max_paths);
    let paths_json = stats_to_json(&stats);

    let summary = json!({
        "records": records.len(),
        "paths": stats.len()
    });

    let meta = json!({
        "summary": summary,
        "paths": paths_json
    });
    let text = serde_json::to_string_pretty(&meta)
        .unwrap_or_else(|_| "{\"error\":\"failed to serialize analysis\"}".to_string());

    Ok(json!({
        "content": [
            {
                "type": "text",
                "text": text
            }
        ],
        "meta": meta
    }))
}

fn run_generate_rules_from_base_tool(args: &Map<String, Value>) -> Result<Value, CallError> {
    let rules_path = get_optional_string(args, "rules_path").map_err(CallError::InvalidParams)?;
    let rules_text = get_optional_string(args, "rules_text").map_err(CallError::InvalidParams)?;
    let rules_format =
        get_optional_string(args, "rules_format").map_err(CallError::InvalidParams)?;
    let input_path = get_optional_string(args, "input_path").map_err(CallError::InvalidParams)?;
    let input_text = get_optional_string(args, "input_text").map_err(CallError::InvalidParams)?;
    let input_json =
        get_optional_json_value(args, "input_json").map_err(CallError::InvalidParams)?;
    let format = get_optional_string(args, "format").map_err(CallError::InvalidParams)?;
    let records_path =
        get_optional_string(args, "records_path").map_err(CallError::InvalidParams)?;
    let max_candidates =
        get_optional_usize(args, "max_candidates").map_err(CallError::InvalidParams)?;

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

    if input_json.is_some()
        && format
            .as_deref()
            .is_some_and(|value| value.eq_ignore_ascii_case("csv"))
    {
        return Err(CallError::InvalidParams(
            "format must be json when input_json is provided".to_string(),
        ));
    }
    if format.as_deref().is_some_and(|value| {
        !value.eq_ignore_ascii_case("csv") && !value.eq_ignore_ascii_case("json")
    }) {
        return Err(CallError::InvalidParams(
            "format must be csv or json".to_string(),
        ));
    }

    let (rule, yaml, _) = load_rule_from_source(
        rules_path.as_deref(),
        rules_text.as_deref(),
        rules_format.as_deref(),
    )?;
    let mut yaml_value: YamlValue = serde_yaml::from_str(&yaml).map_err(|err| {
        let message = format!("failed to parse rules yaml: {}", err);
        CallError::Tool {
            message: message.clone(),
            errors: Some(vec![parse_error_json(&message, None)]),
        }
    })?;

    let input_text = match (input_path.as_deref(), input_text.as_deref()) {
        (Some(path), None) => read_allowed_to_string(path, "input")?,
        (None, Some(text)) => text.to_string(),
        (None, None) => String::new(),
        _ => {
            return Err(CallError::InvalidParams(
                "input_path, input_text, or input_json is required".to_string(),
            ));
        }
    };

    let records_path = records_path.or_else(|| {
        rule.input
            .json
            .as_ref()
            .and_then(|json| json.records_path.clone())
    });

    let parse_format = if input_json.is_some() {
        InputDataFormat::Json
    } else if let Some(format) = format.as_deref() {
        if format.eq_ignore_ascii_case("csv") {
            InputDataFormat::Csv
        } else {
            InputDataFormat::Json
        }
    } else {
        match rule.input.format {
            InputFormat::Csv => InputDataFormat::Csv,
            InputFormat::Json => InputDataFormat::Json,
            InputFormat::Yaml
            | InputFormat::Toml
            | InputFormat::Xml
            | InputFormat::Html
            | InputFormat::Excel => InputDataFormat::Json,
        }
    };

    let has_input_json = input_json.is_some();
    let records = match (parse_format, input_json) {
        (InputDataFormat::Json, Some(value)) => {
            json_records_from_value(&value, records_path.as_deref())?
        }
        (InputDataFormat::Json, None) => {
            parse_json_records_strict(&input_text, records_path.as_deref(), input_path.as_deref())?
        }
        (InputDataFormat::Csv, _) => parse_csv_records(&input_text).map_err(|err| {
            let message = format!("failed to parse input CSV: {}", err);
            CallError::Tool {
                message: message.clone(),
                errors: Some(vec![parse_error_json(&message, input_path.as_deref())]),
            }
        })?,
    };

    let format_override = if has_input_json {
        Some("json".to_string())
    } else {
        format
    };
    let format_for_yaml = if format_override.is_some() {
        format_override.as_deref()
    } else if records_path.is_some() {
        Some("json")
    } else {
        None
    };
    update_yaml_input_spec(&mut yaml_value, format_for_yaml, records_path.as_deref());

    let stats = analyze_records(&records, None);
    let input_paths = build_input_paths(&stats);
    let input_path_set: HashSet<String> =
        input_paths.iter().map(|info| info.path.clone()).collect();

    let max_candidates = max_candidates.unwrap_or(3);
    let mut candidates_meta = Vec::new();
    let mut unmapped = Vec::new();
    let mut missing_refs = Vec::new();
    let mut missing_ref_set = HashSet::new();
    let mut mapped = 0usize;
    let mut with_expr = 0usize;
    let mut with_value = 0usize;

    let mappings = yaml_mappings_sequence_mut(&mut yaml_value)?;

    for (index, mapping) in rule.mappings.iter().enumerate() {
        collect_missing_refs(
            &mapping.target,
            mapping.expr.as_ref(),
            mapping.when.as_ref(),
            &input_path_set,
            &mut missing_refs,
            &mut missing_ref_set,
        );
        if mapping.expr.is_some() {
            with_expr += 1;
            continue;
        }
        if mapping.value.is_some() {
            with_value += 1;
            continue;
        }

        let target_leaf = leaf_from_path(&mapping.target).unwrap_or_default();
        let candidates = select_candidates(
            &target_leaf,
            mapping.source.as_deref(),
            mapping.value_type.as_deref(),
            &input_paths,
            max_candidates,
        );
        let selected = candidates.first().cloned();

        if let Some(selected) = selected.as_ref() {
            mapped += 1;
            update_yaml_mapping(mappings, index, Some(&selected.source))?;
        } else {
            unmapped.push(mapping.target.clone());
            update_yaml_mapping(mappings, index, None)?;
        }

        let candidates_json: Vec<Value> = candidates
            .iter()
            .map(|candidate| {
                json!({
                    "source": candidate.source,
                    "score": candidate.score,
                    "reason": candidate.reason,
                    "confidence": candidate.confidence
                })
            })
            .collect();
        let mut entry = json!({
            "target": mapping.target,
            "candidates": candidates_json
        });
        if let Some(selected) = selected {
            entry["selected"] = json!(selected.source);
            entry["confidence"] = json!(selected.confidence);
        }
        candidates_meta.push(entry);
    }

    let output_text = serde_yaml::to_string(&yaml_value).map_err(|err| {
        let message = format!("failed to serialize rules yaml: {}", err);
        CallError::Tool {
            message: message.clone(),
            errors: Some(vec![parse_error_json(&message, None)]),
        }
    })?;

    let mut meta = serde_json::Map::new();
    meta.insert(
        "summary".to_string(),
        json!({
            "total": rule.mappings.len(),
            "mapped": mapped,
            "unmapped": unmapped.len(),
            "with_expr": with_expr,
            "with_value": with_value
        }),
    );
    meta.insert("candidates".to_string(), Value::Array(candidates_meta));
    if !unmapped.is_empty() {
        meta.insert("unmapped".to_string(), json!(unmapped));
    }
    if !missing_refs.is_empty() {
        meta.insert("missing_refs".to_string(), Value::Array(missing_refs));
    }

    Ok(json!({
        "content": [
            {
                "type": "text",
                "text": output_text
            }
        ],
        "meta": meta
    }))
}

fn run_generate_rules_from_dto_tool(args: &Map<String, Value>) -> Result<Value, CallError> {
    let dto_text = get_optional_string(args, "dto_text").map_err(CallError::InvalidParams)?;
    let dto_language =
        get_optional_string(args, "dto_language").map_err(CallError::InvalidParams)?;
    let input_path = get_optional_string(args, "input_path").map_err(CallError::InvalidParams)?;
    let input_text = get_optional_string(args, "input_text").map_err(CallError::InvalidParams)?;
    let input_json =
        get_optional_json_value(args, "input_json").map_err(CallError::InvalidParams)?;
    let format = get_optional_string(args, "format").map_err(CallError::InvalidParams)?;
    let records_path =
        get_optional_string(args, "records_path").map_err(CallError::InvalidParams)?;
    let max_candidates =
        get_optional_usize(args, "max_candidates").map_err(CallError::InvalidParams)?;

    let dto_text =
        dto_text.ok_or_else(|| CallError::InvalidParams("dto_text is required".to_string()))?;
    let dto_language = dto_language
        .ok_or_else(|| CallError::InvalidParams("dto_language is required".to_string()))?;
    let dto_language =
        parse_dto_source_language(&dto_language).map_err(CallError::InvalidParams)?;

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

    if input_json.is_some()
        && format
            .as_deref()
            .is_some_and(|value| value.eq_ignore_ascii_case("csv"))
    {
        return Err(CallError::InvalidParams(
            "format must be json when input_json is provided".to_string(),
        ));
    }
    if format.as_deref().is_some_and(|value| {
        !value.eq_ignore_ascii_case("csv") && !value.eq_ignore_ascii_case("json")
    }) {
        return Err(CallError::InvalidParams(
            "format must be csv or json".to_string(),
        ));
    }

    let input_text = match (input_path.as_deref(), input_text.as_deref()) {
        (Some(path), None) => read_allowed_to_string(path, "input")?,
        (None, Some(text)) => text.to_string(),
        (None, None) => String::new(),
        _ => {
            return Err(CallError::InvalidParams(
                "input_path, input_text, or input_json is required".to_string(),
            ));
        }
    };

    let has_input_json = input_json.is_some();
    let parse_format = if has_input_json {
        InputDataFormat::Json
    } else if let Some(format) = format.as_deref() {
        if format.eq_ignore_ascii_case("csv") {
            InputDataFormat::Csv
        } else {
            InputDataFormat::Json
        }
    } else {
        normalize_format(None, &input_text)
    };

    let records = match (parse_format, input_json) {
        (InputDataFormat::Json, Some(value)) => {
            json_records_from_value(&value, records_path.as_deref())?
        }
        (InputDataFormat::Json, None) => {
            parse_json_records_strict(&input_text, records_path.as_deref(), input_path.as_deref())?
        }
        (InputDataFormat::Csv, _) => parse_csv_records(&input_text).map_err(|err| {
            let message = format!("failed to parse input CSV: {}", err);
            CallError::Tool {
                message: message.clone(),
                errors: Some(vec![parse_error_json(&message, input_path.as_deref())]),
            }
        })?,
    };

    let schema = parse_dto_schema(&dto_text, dto_language).map_err(|message| CallError::Tool {
        message: message.clone(),
        errors: Some(vec![dto_error_json(&message)]),
    })?;
    let generated = generate_mappings_from_schema(&schema).map_err(|message| CallError::Tool {
        message: message.clone(),
        errors: Some(vec![dto_error_json(&message)]),
    })?;

    let stats = analyze_records(&records, None);
    let input_paths = build_input_paths(&stats);
    let max_candidates = max_candidates.unwrap_or(3);

    let mut candidates_meta = Vec::new();
    let mut unmapped = Vec::new();
    let mut mapped = 0usize;

    let mut mappings_yaml = Vec::new();
    for mapping in &generated {
        let target_leaf = leaf_from_path(&mapping.target).unwrap_or_default();
        let candidates = select_candidates(
            &target_leaf,
            None,
            mapping.value_type.as_deref(),
            &input_paths,
            max_candidates,
        );
        let selected = candidates.first().cloned();

        let mut mapping_map = YamlMapping::new();
        mapping_map.insert(
            yaml_key("target"),
            YamlValue::String(mapping.target.clone()),
        );
        if let Some(value_type) = mapping.value_type.as_deref() {
            mapping_map.insert(yaml_key("type"), YamlValue::String(value_type.to_string()));
        }
        if let Some(selected) = selected.as_ref() {
            mapped += 1;
            mapping_map.insert(
                yaml_key("source"),
                YamlValue::String(selected.source.clone()),
            );
            if mapping.required {
                mapping_map.insert(yaml_key("required"), YamlValue::Bool(true));
            }
        } else {
            unmapped.push(mapping.target.clone());
            mapping_map.insert(yaml_key("value"), YamlValue::Null);
            mapping_map.insert(yaml_key("required"), YamlValue::Bool(false));
        }
        mappings_yaml.push(YamlValue::Mapping(mapping_map));

        let candidates_json: Vec<Value> = candidates
            .iter()
            .map(|candidate| {
                json!({
                    "source": candidate.source,
                    "score": candidate.score,
                    "reason": candidate.reason,
                    "confidence": candidate.confidence
                })
            })
            .collect();
        let mut entry = json!({
            "target": mapping.target,
            "candidates": candidates_json
        });
        if let Some(selected) = selected {
            entry["selected"] = json!(selected.source);
            entry["confidence"] = json!(selected.confidence);
        }
        candidates_meta.push(entry);
    }

    let format_str = if has_input_json {
        "json".to_string()
    } else if let Some(format) = format.as_deref() {
        if format.eq_ignore_ascii_case("csv") {
            "csv".to_string()
        } else {
            "json".to_string()
        }
    } else {
        match parse_format {
            InputDataFormat::Csv => "csv".to_string(),
            InputDataFormat::Json => "json".to_string(),
        }
    };

    let input_yaml = build_input_yaml(&format_str, records_path.as_deref());
    let mut root = YamlMapping::new();
    root.insert(yaml_key("version"), YamlValue::Number(1.into()));
    root.insert(yaml_key("input"), input_yaml);
    root.insert(yaml_key("mappings"), YamlValue::Sequence(mappings_yaml));
    let yaml_value = YamlValue::Mapping(root);
    let output_text = serde_yaml::to_string(&yaml_value).map_err(|err| {
        let message = format!("failed to serialize rules yaml: {}", err);
        CallError::Tool {
            message: message.clone(),
            errors: Some(vec![parse_error_json(&message, None)]),
        }
    })?;

    let mut meta = serde_json::Map::new();
    meta.insert(
        "summary".to_string(),
        json!({
            "total": generated.len(),
            "mapped": mapped,
            "unmapped": unmapped.len()
        }),
    );
    meta.insert("candidates".to_string(), Value::Array(candidates_meta));
    if !unmapped.is_empty() {
        meta.insert("unmapped".to_string(), json!(unmapped));
    }

    Ok(json!({
        "content": [
            {
                "type": "text",
                "text": output_text
            }
        ],
        "meta": meta
    }))
}

fn load_rule_from_source(
    rules_path: Option<&str>,
    rules_text: Option<&str>,
    rules_format: Option<&str>,
) -> Result<(RuleFile, String, Option<PathBuf>), CallError> {
    let format_override = parse_rules_format(rules_format)?;
    match (rules_path, rules_text) {
        (Some(path), None) => {
            let (resolved_path, yaml) = read_allowed_file(path, "rules")?;
            let format = format_override.unwrap_or_else(|| RuleFormat::from_path(&resolved_path));
            let rule = parse_rule_file_with_format(&yaml, format).map_err(|err| {
                let message = format!("failed to parse rules: {}", err);
                CallError::Tool {
                    message: message.clone(),
                    errors: Some(vec![parse_error_json(&message, Some(path))]),
                }
            })?;
            let base_dir = resolved_path.parent().map(Path::to_path_buf);
            Ok((rule, yaml, base_dir))
        }
        (None, Some(text)) => {
            let format = format_override.unwrap_or(RuleFormat::Yaml);
            let rule = parse_rule_file_with_format(text, format).map_err(|err| {
                let message = format!("failed to parse rules: {}", err);
                CallError::Tool {
                    message: message.clone(),
                    errors: Some(vec![parse_error_json(&message, None)]),
                }
            })?;
            Ok((rule, text.to_string(), None))
        }
        _ => Err(CallError::InvalidParams(
            "rules_path or rules_text is required".to_string(),
        )),
    }
}

fn parse_rules_format(value: Option<&str>) -> Result<Option<RuleFormat>, CallError> {
    match value {
        None => Ok(None),
        Some(value) if value.eq_ignore_ascii_case("yaml") => Ok(Some(RuleFormat::Yaml)),
        Some(value) if value.eq_ignore_ascii_case("json") => Ok(Some(RuleFormat::Json)),
        Some(_) => Err(CallError::InvalidParams(
            "rules_format must be yaml or json".to_string(),
        )),
    }
}

fn validate_transform_format(value: Option<&str>) -> Result<(), CallError> {
    let Some(value) = value else {
        return Ok(());
    };
    if matches!(
        value.to_ascii_lowercase().as_str(),
        "csv" | "json" | "yaml" | "toml" | "xml" | "html" | "excel"
    ) {
        return Ok(());
    }
    Err(CallError::InvalidParams(
        "format must be csv, json, yaml, toml, xml, html, or excel".to_string(),
    ))
}

enum OwnedInput {
    Text(String),
    Bytes(Vec<u8>),
}

impl OwnedInput {
    fn as_input_data(&self) -> InputData<'_> {
        match self {
            OwnedInput::Text(value) => InputData::Text(value),
            OwnedInput::Bytes(value) => InputData::Bytes(value),
        }
    }
}

fn rule_has_file_branch(rule: &RuleFile) -> bool {
    rule.steps
        .as_ref()
        .is_some_and(|steps| steps.iter().any(|step| step.branch.is_some()))
}

fn parse_dto_schema(text: &str, language: DtoSourceLanguage) -> Result<DtoSchema, String> {
    let (types, order) = match language {
        DtoSourceLanguage::TypeScript => parse_typescript_types(text)?,
        DtoSourceLanguage::Rust => parse_rust_types(text)?,
        DtoSourceLanguage::Python => parse_python_types(text)?,
        DtoSourceLanguage::Go => parse_go_types(text)?,
        DtoSourceLanguage::Java => parse_java_types(text)?,
        DtoSourceLanguage::Kotlin => parse_kotlin_types(text)?,
        DtoSourceLanguage::Swift => parse_swift_types(text)?,
    };

    let root = if types.contains_key("Record") {
        "Record".to_string()
    } else {
        order
            .first()
            .cloned()
            .ok_or_else(|| "no dto types found".to_string())?
    };

    Ok(DtoSchema { root, types })
}

fn parse_typescript_types(text: &str) -> Result<(HashMap<String, DtoType>, Vec<String>), String> {
    let mut types: HashMap<String, DtoType> = HashMap::new();
    let mut order = Vec::new();
    let mut current: Option<String> = None;
    let mut pending_json_key: Option<String> = None;

    let normalized = normalize_typescript_text(text);
    for raw_line in normalized.lines() {
        let mut line = raw_line.trim();
        if line.is_empty() {
            continue;
        }

        if line.starts_with("export interface ") || line.starts_with("interface ") {
            let line = line.strip_prefix("export ").unwrap_or(line);
            let name_part = line.strip_prefix("interface ").unwrap_or(line).trim();
            let name = name_part
                .split(|ch: char| ch.is_whitespace() || ch == '{')
                .next()
                .unwrap_or("")
                .trim();
            if name.is_empty() {
                continue;
            }
            current = Some(name.to_string());
            pending_json_key = None;
            types
                .entry(name.to_string())
                .or_insert_with(|| DtoType { fields: Vec::new() });
            order.push(name.to_string());
            continue;
        }

        let Some(current_name) = current.clone() else {
            continue;
        };
        if line.starts_with('}') {
            current = None;
            pending_json_key = None;
            continue;
        }

        if let Some((json_key, rest)) = parse_json_comment(line) {
            pending_json_key = Some(json_key);
            line = rest.trim();
            if line.is_empty() {
                continue;
            }
        }

        if !line.contains(':') {
            continue;
        }

        let line = line.trim_end_matches(';').trim();
        let mut parts = line.splitn(2, ':');
        let name_part = parts.next().unwrap_or("").trim();
        let type_part = parts.next().unwrap_or("").trim();
        if name_part.is_empty() || type_part.is_empty() {
            continue;
        }
        let optional = name_part.ends_with('?');
        let field_name = name_part.trim_end_matches('?').trim().to_string();

        let type_token = type_part
            .split(|ch| ch == '|' || ch == '&')
            .next()
            .unwrap_or("")
            .trim()
            .trim_end_matches(';');
        let field_type = if type_token.contains('[') {
            DtoFieldType::Unknown
        } else {
            match type_token {
                "string" => DtoFieldType::Primitive(PrimitiveKind::String),
                "number" => DtoFieldType::Primitive(PrimitiveKind::Float),
                "boolean" => DtoFieldType::Primitive(PrimitiveKind::Bool),
                "unknown" | "any" => DtoFieldType::Unknown,
                "" => DtoFieldType::Unknown,
                other => DtoFieldType::Object(other.to_string()),
            }
        };

        let json_key = pending_json_key
            .take()
            .unwrap_or_else(|| field_name.clone());
        if let Some(dto_type) = types.get_mut(&current_name) {
            dto_type.fields.push(DtoField {
                json_key,
                field_type,
                optional,
            });
        }
    }

    Ok((types, order))
}

fn parse_rust_types(text: &str) -> Result<(HashMap<String, DtoType>, Vec<String>), String> {
    let mut types: HashMap<String, DtoType> = HashMap::new();
    let mut order = Vec::new();
    let mut current: Option<String> = None;
    let mut pending_json_key: Option<String> = None;

    let normalized = normalize_rust_text(text);
    for raw_line in normalized.lines() {
        let mut line = raw_line.trim();
        if line.is_empty() {
            continue;
        }

        if line.starts_with("pub struct ") {
            let name_part = line.strip_prefix("pub struct ").unwrap_or(line).trim();
            let name = name_part
                .split(|ch: char| ch.is_whitespace() || ch == '{')
                .next()
                .unwrap_or("")
                .trim();
            if name.is_empty() {
                continue;
            }
            current = Some(name.to_string());
            pending_json_key = None;
            types
                .entry(name.to_string())
                .or_insert_with(|| DtoType { fields: Vec::new() });
            order.push(name.to_string());
            continue;
        }

        let Some(current_name) = current.clone() else {
            continue;
        };
        if line.starts_with('}') {
            current = None;
            pending_json_key = None;
            continue;
        }

        if line.starts_with("#[serde") {
            if let Some(rename) = parse_serde_rename(line) {
                pending_json_key = Some(rename);
            }
            if let Some(end) = line.find(']') {
                let rest = line[end + 1..].trim();
                if rest.is_empty() {
                    continue;
                }
                line = rest;
            } else {
                continue;
            }
        }

        if !line.starts_with("pub ") {
            continue;
        }

        let line = line.trim_end_matches(',');
        let rest = line.strip_prefix("pub ").unwrap_or(line).trim();
        let mut parts = rest.splitn(2, ':');
        let field_name = parts.next().unwrap_or("").trim();
        let type_part = parts.next().unwrap_or("").trim();
        if field_name.is_empty() || type_part.is_empty() {
            continue;
        }

        let compact = type_part.replace(' ', "");
        let (type_name, optional) = if compact.starts_with("Option<") && compact.ends_with('>') {
            (compact[7..compact.len() - 1].to_string(), true)
        } else {
            (compact, false)
        };

        let type_key = type_name
            .rsplit("::")
            .next()
            .unwrap_or(&type_name)
            .to_string();
        let field_type = match type_key.as_str() {
            "String" => DtoFieldType::Primitive(PrimitiveKind::String),
            "bool" => DtoFieldType::Primitive(PrimitiveKind::Bool),
            "i8" | "i16" | "i32" | "i64" | "isize" | "u8" | "u16" | "u32" | "u64" | "usize" => {
                DtoFieldType::Primitive(PrimitiveKind::Int)
            }
            "f32" | "f64" => DtoFieldType::Primitive(PrimitiveKind::Float),
            _ if type_key.ends_with("Value") => DtoFieldType::Unknown,
            _ => DtoFieldType::Object(type_key),
        };

        let json_key = pending_json_key
            .take()
            .unwrap_or_else(|| field_name.to_string());
        if let Some(dto_type) = types.get_mut(&current_name) {
            dto_type.fields.push(DtoField {
                json_key,
                field_type,
                optional,
            });
        }
    }

    Ok((types, order))
}

fn parse_first_quoted_value(text: &str) -> Option<String> {
    let mut best: Option<(usize, char)> = None;
    for quote in ['"', '\''] {
        if let Some(pos) = text.find(quote) {
            if best.map_or(true, |(best_pos, _)| pos < best_pos) {
                best = Some((pos, quote));
            }
        }
    }

    let (pos, quote) = best?;
    let after = &text[pos + 1..];
    let end = after.find(quote)?;
    Some(after[..end].to_string())
}

fn parse_quoted_value_after(line: &str, marker: &str) -> Option<String> {
    let start = line.find(marker)?;
    let after = &line[start + marker.len()..];
    parse_first_quoted_value(after)
}

fn parse_named_argument(line: &str, key: &str) -> Option<String> {
    let start = line.find(key)?;
    let after = &line[start + key.len()..];
    let eq_pos = after.find('=')?;
    let after_eq = after[eq_pos + 1..].trim_start();
    parse_first_quoted_value(after_eq)
}

fn parse_common_rename_annotation(line: &str) -> Option<String> {
    parse_quoted_value_after(line, "@JsonProperty")
        .or_else(|| parse_quoted_value_after(line, "@SerializedName"))
        .or_else(|| parse_quoted_value_after(line, "@SerialName"))
        .or_else(|| parse_quoted_value_after(line, "@Json"))
}

fn strip_leading_annotations(
    line: &str,
    pending_json_key: &mut Option<String>,
    pending_optional: &mut bool,
) -> String {
    let mut rest = line.trim();
    loop {
        if !rest.starts_with('@') {
            break;
        }
        if let Some(rename) = parse_common_rename_annotation(rest) {
            *pending_json_key = Some(rename);
        }
        if rest.starts_with("@Nullable") {
            *pending_optional = true;
        }
        if let Some(end) = rest.find(')') {
            rest = rest[end + 1..].trim();
            if rest.is_empty() {
                return String::new();
            }
        } else if let Some(space) = rest.find(' ') {
            rest = rest[space + 1..].trim();
            if rest.is_empty() {
                return String::new();
            }
        } else {
            return String::new();
        }
    }

    rest.to_string()
}

fn parse_python_alias(line: &str) -> Option<String> {
    parse_named_argument(line, "alias")
}

fn parse_python_types(text: &str) -> Result<(HashMap<String, DtoType>, Vec<String>), String> {
    let mut types: HashMap<String, DtoType> = HashMap::new();
    let mut order = Vec::new();
    let mut current: Option<String> = None;
    let mut current_indent: Option<usize> = None;
    let normalized = normalize_python_text(text);

    for raw_line in normalized.lines() {
        let indent = raw_line.chars().take_while(|ch| ch.is_whitespace()).count();
        let mut line = raw_line.trim();
        let mut class_line = false;
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        if line.starts_with("class ") {
            class_line = true;
            let name_part = line.strip_prefix("class ").unwrap_or(line).trim();
            let name = name_part
                .split(|ch: char| ch.is_whitespace() || ch == '(' || ch == ':')
                .next()
                .unwrap_or("")
                .trim();
            if name.is_empty() {
                continue;
            }
            current = Some(name.to_string());
            current_indent = Some(indent);
            types
                .entry(name.to_string())
                .or_insert_with(|| DtoType { fields: Vec::new() });
            order.push(name.to_string());
            if let Some(colon_pos) = line.find(':') {
                line = line[colon_pos + 1..].trim();
                if line.is_empty() {
                    continue;
                }
            } else {
                continue;
            }
        }

        if let Some(indent_level) = current_indent {
            if !class_line && indent <= indent_level && !line.is_empty() {
                current = None;
                current_indent = None;
            }
        }

        let Some(current_name) = current.clone() else {
            continue;
        };

        if line.starts_with('@') {
            continue;
        }

        if let Some(comment_pos) = line.find('#') {
            line = line[..comment_pos].trim();
        }
        if line.is_empty() || !line.contains(':') {
            continue;
        }

        let mut parts = line.splitn(2, ':');
        let field_name = parts.next().unwrap_or("").trim();
        let mut rest = parts.next().unwrap_or("").trim();
        rest = rest.trim_end_matches(';').trim();
        if field_name.is_empty() || rest.is_empty() {
            continue;
        }

        let mut optional = false;
        if let Some(eq_pos) = rest.find('=') {
            let (type_part, value_part) = rest.split_at(eq_pos);
            rest = type_part.trim();
            if value_part.contains("None") {
                optional = true;
            }
        }

        if rest.contains("Optional[")
            || rest.contains("None")
            || rest.contains("| None")
            || rest.contains("None |")
        {
            optional = true;
        }

        let mut type_token = rest.trim();
        if let Some(start) = type_token.find("Optional[") {
            let after = &type_token[start + "Optional[".len()..];
            if let Some(end) = after.find(']') {
                type_token = after[..end].trim();
            }
        } else if let Some(start) = type_token.find("Union[") {
            let after = &type_token[start + "Union[".len()..];
            if let Some(end) = after.find(']') {
                let inner = &after[..end];
                if let Some(first) = inner
                    .split(',')
                    .map(|item| item.trim())
                    .find(|item| !item.contains("None"))
                {
                    type_token = first;
                }
            }
        } else if type_token.contains('|') {
            if let Some(first) = type_token
                .split('|')
                .map(|item| item.trim())
                .find(|item| !item.contains("None"))
            {
                type_token = first;
            }
        }

        let type_token = type_token.trim_start_matches("typing.");
        let field_type = if type_token.contains('[')
            || type_token.contains("List")
            || type_token.contains("Dict")
            || type_token.contains("list")
            || type_token.contains("dict")
        {
            DtoFieldType::Unknown
        } else {
            match type_token {
                "str" | "string" => DtoFieldType::Primitive(PrimitiveKind::String),
                "int" => DtoFieldType::Primitive(PrimitiveKind::Int),
                "float" => DtoFieldType::Primitive(PrimitiveKind::Float),
                "bool" | "boolean" => DtoFieldType::Primitive(PrimitiveKind::Bool),
                "Any" | "any" => DtoFieldType::Unknown,
                "" => DtoFieldType::Unknown,
                other => DtoFieldType::Object(other.to_string()),
            }
        };

        let json_key = parse_python_alias(line).unwrap_or_else(|| field_name.to_string());
        if let Some(dto_type) = types.get_mut(&current_name) {
            dto_type.fields.push(DtoField {
                json_key,
                field_type,
                optional,
            });
        }
    }

    Ok((types, order))
}

fn parse_go_types(text: &str) -> Result<(HashMap<String, DtoType>, Vec<String>), String> {
    let mut types: HashMap<String, DtoType> = HashMap::new();
    let mut order = Vec::new();
    let mut index = 0usize;
    let bytes = text.as_bytes();

    while index < bytes.len() {
        let slice = &text[index..];
        let Some(pos) = slice.find("type ") else {
            break;
        };
        index += pos + 5;
        let rest = &text[index..];
        let name = rest.trim_start().split_whitespace().next().unwrap_or("");
        if name.is_empty() {
            index = index.saturating_add(1);
            continue;
        }
        let name_start = rest.find(name).unwrap_or(0);
        index += name_start + name.len();
        let after_name = &text[index..];
        let Some(struct_pos) = after_name.find("struct") else {
            index = index.saturating_add(1);
            continue;
        };
        index += struct_pos + "struct".len();
        let after_struct = &text[index..];
        let Some(brace_pos) = after_struct.find('{') else {
            index = index.saturating_add(1);
            continue;
        };
        index += brace_pos + 1;

        let mut brace_depth = 1usize;
        let mut body_end = index;
        while body_end < bytes.len() {
            match bytes[body_end] as char {
                '{' => brace_depth += 1,
                '}' => {
                    brace_depth -= 1;
                    if brace_depth == 0 {
                        break;
                    }
                }
                _ => {}
            }
            body_end += 1;
        }
        if brace_depth != 0 {
            break;
        }

        let body = &text[index..body_end];
        let dto_type = types
            .entry(name.to_string())
            .or_insert_with(|| DtoType { fields: Vec::new() });
        parse_go_struct_fields(body, dto_type);
        order.push(name.to_string());
        index = body_end + 1;
    }

    Ok((types, order))
}

fn parse_go_struct_fields(body: &str, dto_type: &mut DtoType) {
    let mut chars = body.chars().peekable();
    while let Some(ch) = chars.peek().copied() {
        if ch.is_whitespace() {
            chars.next();
            continue;
        }
        if ch == '/' {
            chars.next();
            if matches!(chars.peek(), Some('/')) {
                while let Some(next) = chars.next() {
                    if next == '\n' {
                        break;
                    }
                }
                continue;
            }
            if matches!(chars.peek(), Some('*')) {
                chars.next();
                while let Some(next) = chars.next() {
                    if next == '*' && matches!(chars.peek(), Some('/')) {
                        chars.next();
                        break;
                    }
                }
                continue;
            }
            continue;
        }

        let field_name = read_go_token(&mut chars);
        if field_name.is_empty() {
            chars.next();
            continue;
        }
        skip_go_whitespace(&mut chars);
        let field_type = read_go_token(&mut chars);
        if field_type.is_empty() {
            continue;
        }

        skip_go_whitespace(&mut chars);
        let tag = if matches!(chars.peek(), Some('`')) {
            chars.next();
            let mut tag_value = String::new();
            while let Some(next) = chars.next() {
                if next == '`' {
                    break;
                }
                tag_value.push(next);
            }
            Some(tag_value)
        } else {
            None
        };

        let (json_key, tag_optional, skip_field) = parse_go_json_tag(tag.as_deref());
        if skip_field {
            continue;
        }

        let mut optional = tag_optional;
        let mut type_token = field_type.trim().to_string();
        if let Some(stripped) = type_token.strip_prefix('*') {
            optional = true;
            type_token = stripped.to_string();
        }

        let field_type = if type_token.contains('[') || type_token.contains("map[") {
            DtoFieldType::Unknown
        } else {
            match type_token.as_str() {
                "string" => DtoFieldType::Primitive(PrimitiveKind::String),
                "bool" => DtoFieldType::Primitive(PrimitiveKind::Bool),
                "int" | "int8" | "int16" | "int32" | "int64" | "uint" | "uint8" | "uint16"
                | "uint32" | "uint64" | "uintptr" => DtoFieldType::Primitive(PrimitiveKind::Int),
                "float32" | "float64" => DtoFieldType::Primitive(PrimitiveKind::Float),
                "" => DtoFieldType::Unknown,
                other => DtoFieldType::Object(other.to_string()),
            }
        };

        let json_key = json_key.unwrap_or_else(|| field_name.clone());
        dto_type.fields.push(DtoField {
            json_key,
            field_type,
            optional,
        });
    }
}

fn read_go_token(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) -> String {
    let mut token = String::new();
    while let Some(&ch) = chars.peek() {
        if ch.is_whitespace() || ch == '`' || ch == '{' || ch == '}' {
            break;
        }
        token.push(ch);
        chars.next();
    }
    token
}

fn skip_go_whitespace(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) {
    while let Some(&ch) = chars.peek() {
        if !ch.is_whitespace() {
            break;
        }
        chars.next();
    }
}

fn parse_go_json_tag(tag: Option<&str>) -> (Option<String>, bool, bool) {
    let Some(tag) = tag else {
        return (None, false, false);
    };
    let Some(start) = tag.find("json:\"") else {
        return (None, false, false);
    };
    let after = &tag[start + 6..];
    let Some(end) = after.find('"') else {
        return (None, false, false);
    };
    let content = &after[..end];
    if content == "-" {
        return (None, false, true);
    }
    let mut parts = content.split(',');
    let name_part = parts.next().unwrap_or("");
    let omitempty = parts.any(|part| part.trim() == "omitempty");
    let name = if name_part.is_empty() {
        None
    } else {
        Some(name_part.to_string())
    };
    (name, omitempty, false)
}

fn parse_java_types(text: &str) -> Result<(HashMap<String, DtoType>, Vec<String>), String> {
    let mut types: HashMap<String, DtoType> = HashMap::new();
    let mut order = Vec::new();
    let mut current: Option<String> = None;
    let mut pending_json_key: Option<String> = None;
    let mut pending_optional = false;
    let mut record_param_depth = 0i32;
    let normalized = normalize_java_text(text);

    for raw_line in normalized.lines() {
        let mut line = raw_line.trim();
        if line.is_empty() {
            continue;
        }

        if line.contains(" class ") || line.starts_with("class ") {
            let name_part = if let Some(idx) = line.find("class ") {
                &line[idx + 6..]
            } else {
                line
            };
            let name = name_part
                .split(|ch: char| ch.is_whitespace() || ch == '{' || ch == '(')
                .next()
                .unwrap_or("")
                .trim();
            if !name.is_empty() {
                current = Some(name.to_string());
                types
                    .entry(name.to_string())
                    .or_insert_with(|| DtoType { fields: Vec::new() });
                order.push(name.to_string());
            }
            record_param_depth = 0;
            pending_json_key = None;
            pending_optional = false;
            continue;
        }

        if line.contains(" record ") || line.starts_with("record ") {
            let name_part = if let Some(idx) = line.find("record ") {
                &line[idx + 7..]
            } else {
                line
            };
            let name = name_part
                .split(|ch: char| ch.is_whitespace() || ch == '{' || ch == '(')
                .next()
                .unwrap_or("")
                .trim();
            if !name.is_empty() {
                current = Some(name.to_string());
                types
                    .entry(name.to_string())
                    .or_insert_with(|| DtoType { fields: Vec::new() });
                order.push(name.to_string());
                if let Some(paren_pos) = line.find('(') {
                    record_param_depth = 1;
                    line = line[paren_pos + 1..].trim();
                } else {
                    record_param_depth = 0;
                    continue;
                }
                pending_json_key = None;
                pending_optional = false;
            }
        }

        let Some(current_name) = current.clone() else {
            continue;
        };
        if line.starts_with('}') {
            current = None;
            record_param_depth = 0;
            pending_json_key = None;
            pending_optional = false;
            continue;
        }

        if record_param_depth > 0 {
            let open_parens = line.matches('(').count() as i32;
            let close_parens = line.matches(')').count() as i32;
            let next_depth = record_param_depth + open_parens - close_parens;
            if next_depth <= 0 {
                if let Some(end) = line.rfind(')') {
                    line = line[..end].trim();
                }
                record_param_depth = 0;
            } else {
                record_param_depth = next_depth;
            }
            if line.is_empty() {
                continue;
            }
            let stripped =
                strip_leading_annotations(line, &mut pending_json_key, &mut pending_optional);
            line = stripped.trim();
            if line.is_empty() {
                continue;
            }
            parse_java_field_line(
                line,
                &current_name,
                &mut types,
                &mut pending_json_key,
                &mut pending_optional,
            );
            continue;
        }

        let stripped =
            strip_leading_annotations(line, &mut pending_json_key, &mut pending_optional);
        line = stripped.trim();
        if line.is_empty() || !line.contains(';') {
            continue;
        }
        parse_java_field_line(
            line,
            &current_name,
            &mut types,
            &mut pending_json_key,
            &mut pending_optional,
        );
    }

    Ok((types, order))
}

fn parse_java_field_line(
    line: &str,
    current_name: &str,
    types: &mut HashMap<String, DtoType>,
    pending_json_key: &mut Option<String>,
    pending_optional: &mut bool,
) {
    let mut cleaned = line;
    if let Some(comment_pos) = cleaned.find("//") {
        cleaned = cleaned[..comment_pos].trim();
    }
    cleaned = cleaned.split('=').next().unwrap_or(cleaned).trim();
    cleaned = cleaned.trim_end_matches(';').trim();
    cleaned = cleaned.trim_end_matches(',').trim();
    if cleaned.is_empty() {
        return;
    }

    let modifiers = [
        "public",
        "private",
        "protected",
        "static",
        "final",
        "transient",
        "volatile",
    ];
    let mut rest = cleaned;
    loop {
        let mut stripped = None;
        for modifier in modifiers {
            if rest.starts_with(modifier) {
                let after = rest[modifier.len()..].trim_start();
                if after.len() != rest.len() {
                    stripped = Some(after);
                    break;
                }
            }
        }
        if let Some(value) = stripped {
            rest = value;
            continue;
        }
        break;
    }

    let Some(split_pos) = rest.rfind(|ch: char| ch.is_whitespace()) else {
        return;
    };
    let type_part = rest[..split_pos].trim();
    let field_name = rest[split_pos..].trim();
    if field_name.is_empty() || type_part.is_empty() {
        return;
    }

    let optional = *pending_optional || type_part.replace(' ', "").contains("Optional<");
    *pending_optional = false;

    let type_key = type_part
        .rsplit('.')
        .next()
        .unwrap_or(type_part)
        .trim()
        .trim_end_matches('>');
    let type_key = type_key.rsplit('<').next().unwrap_or(type_key).trim();
    let field_type = match type_key {
        "String" => DtoFieldType::Primitive(PrimitiveKind::String),
        "boolean" | "Boolean" => DtoFieldType::Primitive(PrimitiveKind::Bool),
        "byte" | "short" | "int" | "long" | "Byte" | "Short" | "Integer" | "Long" => {
            DtoFieldType::Primitive(PrimitiveKind::Int)
        }
        "float" | "double" | "Float" | "Double" => DtoFieldType::Primitive(PrimitiveKind::Float),
        "" => DtoFieldType::Unknown,
        other => DtoFieldType::Object(other.to_string()),
    };

    let json_key = pending_json_key
        .take()
        .unwrap_or_else(|| field_name.to_string());
    if let Some(dto_type) = types.get_mut(current_name) {
        dto_type.fields.push(DtoField {
            json_key,
            field_type,
            optional,
        });
    }
}

fn parse_kotlin_types(text: &str) -> Result<(HashMap<String, DtoType>, Vec<String>), String> {
    let mut types: HashMap<String, DtoType> = HashMap::new();
    let mut order = Vec::new();
    let mut current: Option<String> = None;
    let mut pending_json_key: Option<String> = None;
    let mut pending_optional = false;
    let mut param_depth = 0i32;
    let normalized = normalize_kotlin_text(text);

    for raw_line in normalized.lines() {
        let mut line = raw_line.trim();
        if line.is_empty() {
            continue;
        }

        if line.contains(" class ") || line.starts_with("class ") || line.starts_with("data class ")
        {
            let name_part = if let Some(idx) = line.find("class ") {
                &line[idx + 6..]
            } else {
                line
            };
            let name = name_part
                .split(|ch: char| ch.is_whitespace() || ch == '(' || ch == '{')
                .next()
                .unwrap_or("")
                .trim();
            if !name.is_empty() {
                current = Some(name.to_string());
                types
                    .entry(name.to_string())
                    .or_insert_with(|| DtoType { fields: Vec::new() });
                order.push(name.to_string());
                pending_json_key = None;
                pending_optional = false;
                param_depth = 0;
                if let Some(paren_pos) = line.find('(') {
                    param_depth += 1;
                    line = line[paren_pos + 1..].trim();
                } else {
                    continue;
                }
            }
        }

        let Some(current_name) = current.clone() else {
            continue;
        };
        if line.starts_with('}') {
            current = None;
            param_depth = 0;
            pending_json_key = None;
            pending_optional = false;
            continue;
        }

        if param_depth <= 0 {
            continue;
        }

        let open_parens = line.matches('(').count() as i32;
        let close_parens = line.matches(')').count() as i32;
        let next_depth = param_depth + open_parens - close_parens;
        let mut slice = line;
        if next_depth <= 0 {
            if let Some(end) = slice.rfind(')') {
                slice = slice[..end].trim();
            }
        }

        if param_depth <= 0 && slice.is_empty() {
            param_depth = next_depth.max(0);
            continue;
        }

        let stripped =
            strip_leading_annotations(slice, &mut pending_json_key, &mut pending_optional);
        line = stripped.trim();
        if line.is_empty() {
            param_depth = next_depth.max(0);
            continue;
        }

        let line = line.trim_end_matches(',').trim();
        let rest = if let Some(stripped) = line.strip_prefix("val ") {
            stripped
        } else if let Some(stripped) = line.strip_prefix("var ") {
            stripped
        } else {
            line
        };

        let mut parts = rest.splitn(2, ':');
        let field_name = parts.next().unwrap_or("").trim();
        let type_part = parts.next().unwrap_or("").trim();
        if field_name.is_empty() || type_part.is_empty() {
            continue;
        }

        let mut optional = pending_optional;
        pending_optional = false;
        if type_part.contains('?') || type_part.contains("= null") {
            optional = true;
        }

        let type_token = type_part
            .split('=')
            .next()
            .unwrap_or(type_part)
            .trim()
            .trim_end_matches('?');
        let field_type = if type_token.contains('<') {
            DtoFieldType::Unknown
        } else {
            match type_token {
                "String" => DtoFieldType::Primitive(PrimitiveKind::String),
                "Boolean" => DtoFieldType::Primitive(PrimitiveKind::Bool),
                "Int" | "Long" | "Short" | "Byte" => DtoFieldType::Primitive(PrimitiveKind::Int),
                "Float" | "Double" => DtoFieldType::Primitive(PrimitiveKind::Float),
                "" => DtoFieldType::Unknown,
                other => DtoFieldType::Object(other.to_string()),
            }
        };

        let json_key = pending_json_key
            .take()
            .unwrap_or_else(|| field_name.to_string());
        if let Some(dto_type) = types.get_mut(&current_name) {
            dto_type.fields.push(DtoField {
                json_key,
                field_type,
                optional,
            });
        }

        param_depth = next_depth.max(0);
    }

    Ok((types, order))
}

fn parse_swift_types(text: &str) -> Result<(HashMap<String, DtoType>, Vec<String>), String> {
    let mut types: HashMap<String, DtoType> = HashMap::new();
    let mut order = Vec::new();
    let mut current: Option<String> = None;
    let mut coding_keys: HashMap<String, String> = HashMap::new();
    let mut in_coding_keys = false;
    let mut coding_depth = 0i32;
    let mut type_depth = 0i32;
    let normalized = normalize_swift_text(text);

    for raw_line in normalized.lines() {
        let line = raw_line.trim();
        if line.is_empty() {
            continue;
        }

        if line.contains(" struct ")
            || line.starts_with("struct ")
            || line.contains(" class ")
            || line.starts_with("class ")
        {
            let keyword_pos = if let Some(pos) = line.find("struct ") {
                pos + 7
            } else if let Some(pos) = line.find("class ") {
                pos + 6
            } else {
                0
            };
            let name_part = line[keyword_pos..].split_whitespace().next().unwrap_or("");
            let name = name_part
                .split(|ch: char| ch == ':' || ch == '{')
                .next()
                .unwrap_or("")
                .trim();
            if !name.is_empty() {
                current = Some(name.to_string());
                types
                    .entry(name.to_string())
                    .or_insert_with(|| DtoType { fields: Vec::new() });
                order.push(name.to_string());
                coding_keys.clear();
                in_coding_keys = false;
                coding_depth = 0;
                type_depth = 0;
            }
        }

        let open_braces = line.matches('{').count() as i32;
        let close_braces = line.matches('}').count() as i32;
        if current.is_some() {
            type_depth += open_braces - close_braces;
            if type_depth < 0 {
                type_depth = 0;
            }
        }

        let Some(current_name) = current.clone() else {
            continue;
        };

        if line.starts_with("enum CodingKeys") {
            in_coding_keys = true;
            coding_depth = open_braces - close_braces;
            continue;
        }

        if in_coding_keys {
            coding_depth += open_braces - close_braces;
            if line.starts_with("case ") {
                let cases = parse_swift_cases(line);
                if let Some(dto_type) = types.get_mut(&current_name) {
                    for (field, rename) in cases {
                        coding_keys.insert(field.clone(), rename.clone());
                        for existing in &mut dto_type.fields {
                            if existing.json_key == field {
                                existing.json_key = rename.clone();
                            }
                        }
                    }
                }
            }
            if coding_depth <= 0 {
                in_coding_keys = false;
                coding_depth = 0;
            }
            continue;
        }

        if type_depth == 0 && line.starts_with('}') {
            current = None;
            continue;
        }

        if !(line.starts_with("let ") || line.starts_with("var ")) {
            continue;
        }

        let rest = line.trim_end_matches(';').trim_end_matches(',').trim();
        let rest = rest
            .strip_prefix("let ")
            .or_else(|| rest.strip_prefix("var "));
        let Some(rest) = rest else { continue };
        let mut parts = rest.splitn(2, ':');
        let field_name = parts.next().unwrap_or("").trim();
        let mut type_part = parts.next().unwrap_or("").trim();
        if field_name.is_empty() || type_part.is_empty() {
            continue;
        }
        if let Some(eq_pos) = type_part.find('=') {
            type_part = type_part[..eq_pos].trim();
        }

        let mut optional = type_part.contains('?');
        let type_token = type_part.trim_end_matches('?');
        let field_type = if type_token.contains('<') {
            DtoFieldType::Unknown
        } else {
            match type_token {
                "String" => DtoFieldType::Primitive(PrimitiveKind::String),
                "Bool" => DtoFieldType::Primitive(PrimitiveKind::Bool),
                "Int" | "Int8" | "Int16" | "Int32" | "Int64" | "UInt" | "UInt8" | "UInt16"
                | "UInt32" | "UInt64" => DtoFieldType::Primitive(PrimitiveKind::Int),
                "Float" | "Double" => DtoFieldType::Primitive(PrimitiveKind::Float),
                "" => DtoFieldType::Unknown,
                other => DtoFieldType::Object(other.to_string()),
            }
        };

        if type_part.contains("Optional<") {
            optional = true;
        }

        let json_key = coding_keys
            .get(field_name)
            .cloned()
            .unwrap_or_else(|| field_name.to_string());
        if let Some(dto_type) = types.get_mut(&current_name) {
            dto_type.fields.push(DtoField {
                json_key,
                field_type,
                optional,
            });
        }
    }

    Ok((types, order))
}

fn parse_swift_cases(line: &str) -> Vec<(String, String)> {
    let mut cases = Vec::new();
    let rest = line.strip_prefix("case ").unwrap_or(line).trim();
    let mut current = String::new();
    let mut in_string = false;
    let mut escape = false;

    for ch in rest.chars() {
        if in_string {
            current.push(ch);
            if escape {
                escape = false;
                continue;
            }
            if ch == '\\' {
                escape = true;
                continue;
            }
            if ch == '"' {
                in_string = false;
            }
            continue;
        }

        if ch == '"' {
            in_string = true;
            current.push(ch);
            continue;
        }

        if ch == ',' {
            push_swift_case(&mut cases, &current);
            current.clear();
            continue;
        }

        current.push(ch);
    }
    push_swift_case(&mut cases, &current);
    cases
}

fn push_swift_case(cases: &mut Vec<(String, String)>, text: &str) {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return;
    }
    let mut parts = trimmed.splitn(2, '=');
    let name = parts.next().unwrap_or("").trim();
    if name.is_empty() {
        return;
    }
    let rename = parts
        .next()
        .and_then(|value| parse_first_quoted_value(value))
        .unwrap_or_else(|| name.to_string());
    cases.push((name.to_string(), rename));
}

fn parse_json_comment(line: &str) -> Option<(String, &str)> {
    let marker = line.find("json:")?;
    let after_marker = &line[marker + 5..];
    let quote_start = after_marker.find('"')?;
    let after_quote = &after_marker[quote_start + 1..];
    let quote_end = after_quote.find('"')?;
    let json_key = after_quote[..quote_end].to_string();
    let rest = if let Some(end) = line.find("*/") {
        &line[end + 2..]
    } else {
        ""
    };
    Some((json_key, rest))
}

fn parse_serde_rename(line: &str) -> Option<String> {
    let marker = line.find("rename")?;
    let after_marker = &line[marker..];
    let quote_start = after_marker.find('"')?;
    let after_quote = &after_marker[quote_start + 1..];
    let quote_end = after_quote.find('"')?;
    Some(after_quote[..quote_end].to_string())
}

fn build_input_yaml(format: &str, records_path: Option<&str>) -> YamlValue {
    let mut input_map = YamlMapping::new();
    input_map.insert(yaml_key("format"), YamlValue::String(format.to_string()));
    if format.eq_ignore_ascii_case("json") {
        let mut json_map = YamlMapping::new();
        if let Some(records_path) = records_path {
            json_map.insert(
                yaml_key("records_path"),
                YamlValue::String(records_path.to_string()),
            );
        }
        input_map.insert(yaml_key("json"), YamlValue::Mapping(json_map));
    } else {
        input_map.insert(yaml_key("csv"), YamlValue::Mapping(YamlMapping::new()));
    }
    YamlValue::Mapping(input_map)
}

fn update_yaml_input_spec(root: &mut YamlValue, format: Option<&str>, records_path: Option<&str>) {
    if format.is_none() && records_path.is_none() {
        return;
    }
    let Some(root_map) = root.as_mapping_mut() else {
        return;
    };
    let input_value = root_map
        .entry(yaml_key("input"))
        .or_insert_with(|| YamlValue::Mapping(YamlMapping::new()));
    let Some(input_map) = input_value.as_mapping_mut() else {
        return;
    };

    if let Some(format) = format {
        input_map.insert(yaml_key("format"), YamlValue::String(format.to_string()));
    }
    if let Some(records_path) = records_path {
        let json_value = input_map
            .entry(yaml_key("json"))
            .or_insert_with(|| YamlValue::Mapping(YamlMapping::new()));
        if let Some(json_map) = json_value.as_mapping_mut() {
            json_map.insert(
                yaml_key("records_path"),
                YamlValue::String(records_path.to_string()),
            );
        }
    }
}

fn yaml_mappings_sequence_mut(root: &mut YamlValue) -> Result<&mut Vec<YamlValue>, CallError> {
    let Some(root_map) = root.as_mapping_mut() else {
        let message = "rules yaml must be a mapping".to_string();
        return Err(CallError::Tool {
            message: message.clone(),
            errors: Some(vec![parse_error_json(&message, None)]),
        });
    };
    let Some(mappings_value) = root_map.get_mut(&yaml_key("mappings")) else {
        let message = "rules yaml is missing mappings".to_string();
        return Err(CallError::Tool {
            message: message.clone(),
            errors: Some(vec![parse_error_json(&message, None)]),
        });
    };
    mappings_value.as_sequence_mut().ok_or_else(|| {
        let message = "rules yaml mappings must be a sequence".to_string();
        CallError::Tool {
            message: message.clone(),
            errors: Some(vec![parse_error_json(&message, None)]),
        }
    })
}

fn update_yaml_mapping(
    mappings: &mut Vec<YamlValue>,
    index: usize,
    source: Option<&str>,
) -> Result<(), CallError> {
    let Some(mapping_value) = mappings.get_mut(index) else {
        let message = "mapping index out of range".to_string();
        return Err(CallError::Tool {
            message: message.clone(),
            errors: Some(vec![parse_error_json(&message, None)]),
        });
    };
    let Some(mapping_map) = mapping_value.as_mapping_mut() else {
        let message = "mapping entry must be a mapping".to_string();
        return Err(CallError::Tool {
            message: message.clone(),
            errors: Some(vec![parse_error_json(&message, None)]),
        });
    };

    if let Some(source) = source {
        mapping_map.insert(yaml_key("source"), YamlValue::String(source.to_string()));
        mapping_map.remove(&yaml_key("value"));
        mapping_map.remove(&yaml_key("expr"));
    } else {
        mapping_map.remove(&yaml_key("source"));
        mapping_map.remove(&yaml_key("expr"));
        mapping_map.insert(yaml_key("value"), YamlValue::Null);
        mapping_map.insert(yaml_key("required"), YamlValue::Bool(false));
    }
    Ok(())
}

fn yaml_key(key: &str) -> YamlValue {
    YamlValue::String(key.to_string())
}

fn collect_missing_refs(
    target: &str,
    expr: Option<&Expr>,
    when: Option<&Expr>,
    input_paths: &HashSet<String>,
    out: &mut Vec<Value>,
    seen: &mut HashSet<String>,
) {
    for expr in [expr, when] {
        let Some(expr) = expr else { continue };
        let mut refs = Vec::new();
        collect_expr_refs(expr, &mut refs);
        for reference in refs {
            let Some(path) = input_ref_path(&reference) else {
                continue;
            };
            if input_paths.contains(&path) {
                continue;
            }
            let key = format!("{}|{}", target, reference);
            if seen.insert(key) {
                out.push(json!({
                    "target": target,
                    "ref": reference,
                    "path": path
                }));
            }
        }
    }
}

fn collect_expr_refs(expr: &Expr, out: &mut Vec<String>) {
    match expr {
        Expr::Ref(reference) => out.push(reference.ref_path.clone()),
        Expr::Op(op) => {
            for arg in &op.args {
                collect_expr_refs(arg, out);
            }
        }
        Expr::Chain(chain) => {
            for item in &chain.chain {
                collect_expr_refs(item, out);
            }
        }
        Expr::Literal(_) => {}
    }
}

fn input_ref_path(reference: &str) -> Option<String> {
    let trimmed = reference.trim();
    if let Some(rest) = trimmed.strip_prefix("input.") {
        if rest.is_empty() {
            None
        } else {
            Some(rest.to_string())
        }
    } else {
        None
    }
}

fn apply_format_override(rule: &mut RuleFile, format: Option<&str>) -> Result<(), String> {
    let Some(format) = format else {
        return Ok(());
    };
    let normalized = format.to_lowercase();
    rule.input.format = match normalized.as_str() {
        "csv" => InputFormat::Csv,
        "json" => InputFormat::Json,
        "yaml" => InputFormat::Yaml,
        "toml" => InputFormat::Toml,
        "xml" => InputFormat::Xml,
        "html" => InputFormat::Html,
        "excel" => InputFormat::Excel,
        _ => return Err(format!("unknown format: {}", format)),
    };
    Ok(())
}

fn transform_to_ndjson(
    rule: &RuleFile,
    input: InputData<'_>,
    context: Option<&serde_json::Value>,
    base_dir: Option<&Path>,
) -> Result<(String, Vec<TransformWarning>), CallError> {
    let stream = match base_dir {
        Some(base_dir) => transform_stream_input_with_base_dir(rule, input, context, base_dir),
        None => transform_stream_input(rule, input, context),
    }
    .map_err(|err| CallError::Tool {
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
        let output_value = match item.output {
            Some(output_value) => output_value,
            None => continue,
        };
        let line = serde_json::to_string(&output_value).map_err(|err| {
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

struct RuleWarning {
    code: &'static str,
    message: String,
    path: Option<String>,
}

fn collect_rule_warnings(rule: &RuleFile) -> Vec<RuleWarning> {
    let mut warnings = Vec::new();
    if let Some(expr) = &rule.record_when {
        collect_expr_warnings(expr, "record_when", &mut warnings);
    }
    for (index, mapping) in rule.mappings.iter().enumerate() {
        let base_path = format!("mappings[{}]", index);
        if let Some(expr) = &mapping.expr {
            collect_expr_warnings(expr, &format!("{}.expr", base_path), &mut warnings);
        }
        if let Some(expr) = &mapping.when {
            collect_expr_warnings(expr, &format!("{}.when", base_path), &mut warnings);
        }
    }
    warnings
}

fn collect_expr_warnings(expr: &Expr, path: &str, warnings: &mut Vec<RuleWarning>) {
    match expr {
        Expr::Ref(_) | Expr::Literal(_) => {}
        Expr::Op(expr_op) => collect_op_warnings(expr_op, path, false, warnings),
        Expr::Chain(chain) => collect_chain_warnings(chain, path, warnings),
    }
}

fn collect_chain_warnings(chain: &ExprChain, path: &str, warnings: &mut Vec<RuleWarning>) {
    for (index, step) in chain.chain.iter().enumerate() {
        let step_path = format!("{}.chain[{}]", path, index);
        if index == 0 {
            collect_expr_warnings(step, &step_path, warnings);
            continue;
        }

        match step {
            Expr::Op(expr_op) => collect_op_warnings(expr_op, &step_path, true, warnings),
            _ => collect_expr_warnings(step, &step_path, warnings),
        }
    }
}

fn collect_op_warnings(
    expr_op: &ExprOp,
    path: &str,
    chain_step: bool,
    warnings: &mut Vec<RuleWarning>,
) {
    if expr_op.op == "date_format" {
        warn_date_format_missing_input_format(expr_op, path, chain_step, warnings);
    } else if expr_op.op == "to_unixtime" {
        warnings.push(RuleWarning {
            code: "to_unixtime_auto_parse",
            message: "to_unixtime relies on heuristic date parsing; consider normalizing with date_format + input_format.".to_string(),
            path: Some(path.to_string()),
        });
    }

    for (index, arg) in expr_op.args.iter().enumerate() {
        let arg_path = format!("{}.args[{}]", path, index);
        collect_expr_warnings(arg, &arg_path, warnings);
    }
}

fn warn_date_format_missing_input_format(
    expr_op: &ExprOp,
    path: &str,
    chain_step: bool,
    warnings: &mut Vec<RuleWarning>,
) {
    let input_index = if chain_step { 1 } else { 2 };
    if expr_op.args.len() <= input_index {
        warnings.push(RuleWarning {
            code: "date_format_missing_input_format",
            message: "date_format without input_format relies on heuristic parsing; consider providing input_format.".to_string(),
            path: Some(format!("{}.args", path)),
        });
        return;
    }

    if expr_looks_like_timezone(&expr_op.args[input_index]) {
        warnings.push(RuleWarning {
            code: "date_format_missing_input_format",
            message: "date_format without input_format relies on heuristic parsing; consider providing input_format.".to_string(),
            path: Some(format!("{}.args[{}]", path, input_index)),
        });
    }
}

fn expr_looks_like_timezone(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(Value::String(value)) => looks_like_timezone(value),
        _ => false,
    }
}

fn looks_like_timezone(value: &str) -> bool {
    if value.eq_ignore_ascii_case("utc") || value == "Z" {
        return true;
    }
    matches!(value.chars().next(), Some('+') | Some('-'))
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

fn rule_warnings_to_json(warnings: &[RuleWarning]) -> Value {
    let values: Vec<_> = warnings.iter().map(rule_warning_json).collect();
    Value::Array(values)
}

fn rule_warning_json(warning: &RuleWarning) -> Value {
    let mut value = json!({
        "type": "warning",
        "code": warning.code,
        "message": warning.message,
    });
    if let Some(path) = &warning.path {
        value["path"] = json!(path);
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
        TransformErrorKind::AssertionFailed => "AssertionFailed",
    }
}
