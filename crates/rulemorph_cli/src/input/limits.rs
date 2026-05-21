use std::fs;
use std::path::PathBuf;

use rulemorph::NormalizationOptions;

use super::super::LimitsProfileArg;

pub(crate) fn load_normalization_options(
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

fn apply_limit_override(
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
