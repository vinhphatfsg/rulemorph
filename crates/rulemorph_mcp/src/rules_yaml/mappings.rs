use serde_yaml::Value as YamlValue;

use crate::diagnostics::parse_error_json;
use crate::errors::CallError;

use super::yaml_key;

pub(crate) fn yaml_mappings_sequence_mut(
    root: &mut YamlValue,
) -> Result<&mut Vec<YamlValue>, CallError> {
    let Some(root_map) = root.as_mapping_mut() else {
        let message = "rules yaml must be a mapping".to_string();
        return Err(CallError::Tool {
            message: message.clone(),
            errors: Some(vec![parse_error_json(&message, None)]),
        });
    };
    let Some(mappings_value) = root_map.get_mut(yaml_key("mappings")) else {
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

pub(crate) fn update_yaml_mapping(
    mappings: &mut [YamlValue],
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
        mapping_map.remove(yaml_key("value"));
        mapping_map.remove(yaml_key("expr"));
    } else {
        mapping_map.remove(yaml_key("source"));
        mapping_map.remove(yaml_key("expr"));
        mapping_map.insert(yaml_key("value"), YamlValue::Null);
        mapping_map.insert(yaml_key("required"), YamlValue::Bool(false));
    }
    Ok(())
}
