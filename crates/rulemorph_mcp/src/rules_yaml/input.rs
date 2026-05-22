use serde_yaml::{Mapping as YamlMapping, Value as YamlValue};

use super::yaml_key;

pub(crate) fn build_input_yaml(format: &str, records_path: Option<&str>) -> YamlValue {
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

pub(crate) fn update_yaml_input_spec(
    root: &mut YamlValue,
    format: Option<&str>,
    records_path: Option<&str>,
) {
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
