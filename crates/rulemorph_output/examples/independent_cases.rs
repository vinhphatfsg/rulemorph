//! Deterministic generated cases for the independent Python validator check.
use rulemorph_output::*;
use serde_json::{Value, json};

const TEXT_RULE: &str = "version: 2\ninput: {format: json, json: {}}\nmappings:\n - target: text\n   source: text\n   required: true\n   type: string\nfinalize:\n wrap: ['@out', {map: ['@item.text']}]\n";
const NUMBER_RULE: &str = "version: 2\ninput: {format: json, json: {}}\nmappings:\n - target: n\n   source: n\n   required: true\n   type: int\n";

fn emit(raw: &str, rule: &str, schema: Value, repair: RepairMode) {
    let input = RawInput {
        text: raw,
        completion: InputCompletion::Complete,
    };
    match normalize_to_schema_with_repair(input, rule, &schema, repair) {
        Ok(result) => println!(
            "{}",
            json!({"ok":true,"schema":schema,"value":result.value()})
        ),
        Err(error) => println!("{}", json!({"ok":false,"kind":format!("{:?}",error.kind)})),
    }
}

fn main() {
    let mut state = 0x9e3779b97f4a7c15_u64;
    for i in 0..256 {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        let n = (state >> 32) as i32 % 1000;
        let input = json!([{"n":n.to_string()}]).to_string();
        let schema = json!({"type":"array","minItems":1,"maxItems":1,"items":{"type":"object","required":["n"],"additionalProperties":false,"properties":{"n":{"type":"integer","minimum":if i%3==0 { 0 } else { -1000 },"maximum":1000}}}});
        emit(&input, NUMBER_RULE, schema, RepairMode::Reject);
        let text = format!("日本語😀 quote:\" backslash:\\ case:{i}");
        let input = format!(
            "[{} {}]",
            json!({"text":text}),
            json!({"text":format!("item-{n}")})
        );
        let schema = json!({"type":"array","items":{"type":"string","minLength":1,"maxLength":100},"minItems":2,"maxItems":2});
        emit(&input, TEXT_RULE, schema, RepairMode::MissingCommas);
    }
    emit(
        r#"[{"title":"do not guess"}]"#,
        TEXT_RULE,
        json!({"type":"array","items":{"type":"string"}}),
        RepairMode::Reject,
    );
}
