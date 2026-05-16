use std::fs;
use std::io::Read;
use std::path::PathBuf;

use rulemorph::{
    InputFormat, NormalizationOptions, RuleFile, RuleFormat, parse_rule_file_with_format,
};

use super::{FormatOverride, LimitsProfileArg, RulesFormatArg};

pub(super) fn load_rule(
    path: &PathBuf,
    override_format: Option<RulesFormatArg>,
) -> Result<(RuleFile, String), i32> {
    let yaml = match fs::read_to_string(path) {
        Ok(data) => data,
        Err(err) => {
            eprintln!("failed to read rules: {}", err);
            return Err(1);
        }
    };

    let format = detect_rule_format(path, override_format);
    let rule = match parse_rule_file_with_format(&yaml, format) {
        Ok(rule) => rule,
        Err(err) => {
            eprintln!("failed to parse rules: {}", err);
            return Err(1);
        }
    };

    Ok((rule, yaml))
}

pub(super) fn detect_rule_format(
    path: &PathBuf,
    override_format: Option<RulesFormatArg>,
) -> RuleFormat {
    match override_format {
        Some(RulesFormatArg::Yaml) => RuleFormat::Yaml,
        Some(RulesFormatArg::Json) => RuleFormat::Json,
        None => RuleFormat::from_path(path),
    }
}

pub(super) fn rule_base_dir(path: &PathBuf) -> PathBuf {
    path.parent()
        .unwrap_or_else(|| std::path::Path::new("."))
        .to_path_buf()
}

pub(super) fn apply_format_override(rule: &mut RuleFile, format: Option<FormatOverride>) {
    if let Some(format) = format {
        rule.input.format = match format {
            FormatOverride::Csv => InputFormat::Csv,
            FormatOverride::Json => InputFormat::Json,
        };
    }
}

pub(super) fn load_input_bytes_with_limit(
    path: &PathBuf,
    max_input_bytes: usize,
) -> Result<Vec<u8>, i32> {
    match read_file_with_limit(path, max_input_bytes) {
        Ok(value) => Ok(value),
        Err(message) => {
            eprintln!("failed to read input: {}", message);
            Err(1)
        }
    }
}

pub(super) fn read_file_with_limit(path: &PathBuf, max_bytes: usize) -> Result<Vec<u8>, String> {
    let metadata = fs::metadata(path).map_err(|err| err.to_string())?;
    if metadata.len() > max_bytes as u64 {
        return Err(format!("input exceeds max_input_bytes ({})", max_bytes));
    }
    let mut file = fs::File::open(path).map_err(|err| err.to_string())?;
    let mut bytes = Vec::new();
    Read::by_ref(&mut file)
        .take(max_bytes as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|err| err.to_string())?;
    if bytes.len() > max_bytes {
        return Err(format!("input exceeds max_input_bytes ({})", max_bytes));
    }
    Ok(bytes)
}

pub(super) fn load_context(path: &Option<PathBuf>) -> Result<Option<serde_json::Value>, i32> {
    match path {
        Some(path) => match fs::read_to_string(path) {
            Ok(data) => match serde_json::from_str(&data) {
                Ok(json) => Ok(Some(json)),
                Err(err) => {
                    eprintln!("failed to parse context JSON: {}", err);
                    Err(1)
                }
            },
            Err(err) => {
                eprintln!("failed to read context: {}", err);
                Err(1)
            }
        },
        None => Ok(None),
    }
}

pub(super) fn load_normalization_options(
    profile: Option<LimitsProfileArg>,
    file: Option<&PathBuf>,
    overrides: &[String],
) -> Result<NormalizationOptions, String> {
    let mut options = match profile.unwrap_or(LimitsProfileArg::Default) {
        LimitsProfileArg::Default => NormalizationOptions::default(),
        LimitsProfileArg::Large => NormalizationOptions::large(),
    };
    if let Some(file) = file {
        let raw = fs::read_to_string(file)
            .map_err(|err| format!("failed to read limits file: {}", err))?;
        let value = raw
            .parse::<toml::Value>()
            .map_err(|err| format!("failed to parse limits file: {}", err))?;
        let table = value
            .as_table()
            .ok_or_else(|| "limits file must contain a TOML table".to_string())?;
        for (name, value) in table {
            let value = value
                .as_integer()
                .ok_or_else(|| format!("limit `{}` must be an integer", name))?;
            apply_limit_override(&mut options, name, value.into())?;
        }
    }
    for item in overrides {
        let (name, value) = item
            .split_once('=')
            .ok_or_else(|| "limit override must use name=value".to_string())?;
        let value = value
            .parse::<i128>()
            .map_err(|_| format!("limit `{}` must be a positive integer", name))?;
        apply_limit_override(&mut options, name, value)?;
    }
    Ok(options)
}

pub(super) fn apply_limit_override(
    options: &mut NormalizationOptions,
    name: &str,
    value: i128,
) -> Result<(), String> {
    if value <= 0 {
        return Err(format!("limit `{}` must be a positive integer", name));
    }
    let value = usize::try_from(value)
        .map_err(|_| format!("limit `{}` is too large for this platform", name))?;
    if value == usize::MAX {
        return Err(format!("limit `{}` is too large", name));
    }
    match name {
        "input-bytes" => options.max_input_bytes = value,
        "records" => options.max_records = value,
        "depth" => options.max_depth = value,
        "array-len" => options.max_array_len = value,
        "text-bytes" => options.max_text_bytes = value,
        "yaml-aliases" => options.max_yaml_aliases = value,
        "yaml-expanded-nodes" => options.max_yaml_expanded_nodes = value,
        "xml-nodes" => options.max_xml_nodes = value,
        "html-nodes" => options.max_html_nodes = value,
        "excel-zip-entries" => options.max_excel_zip_entries = value,
        "excel-uncompressed-bytes" => options.max_excel_uncompressed_bytes = value,
        "excel-entry-uncompressed-bytes" => options.max_excel_entry_uncompressed_bytes = value,
        "excel-sheets" => options.max_excel_sheets = value,
        "excel-rows" => options.max_excel_rows = value,
        "excel-cells" => options.max_excel_cells = value,
        "excel-shared-strings" => options.max_excel_shared_strings = value,
        "excel-shared-string-bytes" => options.max_excel_shared_string_bytes = value,
        "excel-styles" => options.max_excel_styles = value,
        _ => return Err(format!("unknown limit `{}`", name)),
    }
    Ok(())
}
