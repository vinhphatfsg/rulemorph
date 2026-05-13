use serde_json::Value as JsonValue;

pub(super) fn normalize_masking_rules(rules: &[String]) -> Vec<String> {
    if rules.is_empty() {
        return Vec::new();
    }
    rules
        .iter()
        .map(|rule| rule.trim().to_ascii_lowercase())
        .filter(|rule| !rule.is_empty())
        .collect()
}

fn mask_url_query(value: &str, rules: &[String]) -> Option<String> {
    if rules.is_empty() || (!value.contains('?') && !value.contains('#')) {
        return None;
    }
    let (without_fragment, fragment) = match value.split_once('#') {
        Some((prefix, fragment)) => (prefix, Some(fragment)),
        None => (value, None),
    };
    let (base, query) = match without_fragment.split_once('?') {
        Some((base, query)) => (base, Some(query)),
        None => (without_fragment, None),
    };
    let mut masked = false;
    let masked_query = query.map(|query| {
        let (value, did_mask) = mask_url_param_pairs(query, rules);
        masked |= did_mask;
        value
    });
    let masked_fragment = fragment.map(|fragment| {
        let (value, did_mask) = mask_url_fragment(fragment, rules);
        masked |= did_mask;
        value
    });
    let mut masked_value = String::with_capacity(value.len());
    masked_value.push_str(base);
    if let Some(query) = masked_query {
        masked_value.push('?');
        masked_value.push_str(&query);
    }
    if let Some(fragment) = masked_fragment {
        masked_value.push('#');
        masked_value.push_str(&fragment);
    }
    masked.then_some(masked_value)
}

fn mask_url_fragment(value: &str, rules: &[String]) -> (String, bool) {
    if let Some((route, query)) = value.split_once('?') {
        let (masked_query, did_mask) = mask_url_param_pairs(query, rules);
        if did_mask {
            return (format!("{route}?{masked_query}"), true);
        }
    }
    mask_url_param_pairs(value, rules)
}

fn mask_url_param_pairs(value: &str, rules: &[String]) -> (String, bool) {
    let mut masked = false;
    let mut parts = Vec::new();
    for pair in value.split('&') {
        if pair.is_empty() {
            parts.push(String::new());
            continue;
        }
        if let Some((key, _value)) = pair.split_once('=') {
            if should_mask_key(key, rules) {
                masked = true;
                parts.push(format!("{key}=[masked]"));
            } else {
                parts.push(pair.to_string());
            }
        } else if should_mask_key(pair, rules) {
            masked = true;
            parts.push(format!("{pair}=[masked]"));
        } else {
            parts.push(pair.to_string());
        }
    }
    (parts.join("&"), masked)
}

pub(super) fn apply_masking(value: &mut JsonValue, rules: &[String]) {
    match value {
        JsonValue::Object(map) => {
            for (key, entry) in map.iter_mut() {
                if should_mask_key(key, rules) {
                    *entry = JsonValue::String("[masked]".to_string());
                } else {
                    apply_masking(entry, rules);
                }
            }
        }
        JsonValue::Array(items) => {
            for item in items {
                apply_masking(item, rules);
            }
        }
        JsonValue::String(value) => {
            if let Some(masked) = mask_url_query(value, rules) {
                *value = masked;
            }
        }
        _ => {}
    }
}

fn should_mask_key(key: &str, rules: &[String]) -> bool {
    if rules.is_empty() {
        return false;
    }
    let key_lower = key.to_ascii_lowercase();
    rules.iter().any(|rule| key_lower.contains(rule))
}
