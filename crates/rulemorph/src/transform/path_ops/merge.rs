use super::*;

pub(in crate::transform) fn merge_object(
    target: &mut Map<String, JsonValue>,
    incoming: &Map<String, JsonValue>,
    deep: bool,
) {
    for (key, value) in incoming {
        if deep {
            if let (Some(JsonValue::Object(target_obj)), JsonValue::Object(incoming_obj)) =
                (target.get_mut(key), value)
            {
                merge_object(target_obj, incoming_obj, true);
                continue;
            }
        }
        target.insert(key.clone(), value.clone());
    }
}
