use super::*;

pub(super) fn infer_op(
    op_step: &V2OpStep,
    rule: &RuleFile,
    state: &mut InferenceState,
    scope: &Scope,
    input_type: FieldType,
    depth: usize,
) -> FieldType {
    if !state.enter_node(depth) {
        return FieldType::JsonValue;
    }

    if op_step.args.is_empty() && rule.defs.contains_key(&op_step.op) {
        return infer_custom_call(&op_step.op, rule, state, depth + 1);
    }

    match op_step.op.as_str() {
        "string" | "to_string" | "trim" | "lowercase" | "uppercase" | "concat" | "replace"
        | "pad_start" | "pad_end" | "date_format" | "to_base" => {
            FieldType::Primitive(PrimitiveType::String)
        }
        "int" | "len" | "to_unixtime" | "find_index" | "index_of" | "sign" => {
            FieldType::Primitive(PrimitiveType::Int)
        }
        "float" | "+" | "add" | "-" | "subtract" | "*" | "multiply" | "/" | "divide" | "round"
        | "abs" | "floor" | "ceil" | "trunc" | "sqrt" | "mod" | "pow" | "clamp" => {
            FieldType::Primitive(PrimitiveType::Float)
        }
        "range" => FieldType::Array(Box::new(FieldType::Primitive(PrimitiveType::Int))),
        "sum" | "avg" | "min" | "max" => {
            FieldType::Nullable(Box::new(FieldType::Primitive(PrimitiveType::Float)))
        }
        "bool" | "and" | "or" | "not" | "==" | "!=" | "<" | "<=" | ">" | ">=" | "~=" | "eq"
        | "ne" | "lt" | "lte" | "gt" | "gte" | "match" | "contains" => {
            FieldType::Primitive(PrimitiveType::Bool)
        }
        "split" | "keys" => FieldType::Array(Box::new(FieldType::Primitive(PrimitiveType::String))),
        "values" => FieldType::Array(Box::new(object_value_union(&input_type))),
        "entries" => FieldType::Array(Box::new(FieldType::Object(Box::new(SchemaNode {
            fields: vec![
                Field {
                    key: "key".to_string(),
                    field_type: FieldType::Primitive(PrimitiveType::String),
                    optional: false,
                    synthetic: false,
                },
                Field {
                    key: "value".to_string(),
                    field_type: object_value_union(&input_type),
                    optional: false,
                    synthetic: false,
                },
            ],
        })))),
        "from_entries" => FieldType::Map(Box::new(FieldType::JsonValue)),
        "map" => {
            let element = array_element_type(&input_type);
            let Some(arg) = op_step.args.first() else {
                return FieldType::Array(Box::new(FieldType::JsonValue));
            };
            let arg_scope = scope
                .clone()
                .with_pipe(element.clone())
                .with_item(Some(element));
            FieldType::Array(Box::new(infer_arg_expr(
                arg,
                rule,
                state,
                arg_scope,
                depth + 1,
            )))
        }
        "flat_map" => {
            let element = array_element_type(&input_type);
            let Some(arg) = op_step.args.first() else {
                return FieldType::Array(Box::new(FieldType::JsonValue));
            };
            let arg_scope = scope
                .clone()
                .with_pipe(element.clone())
                .with_item(Some(element));
            match infer_arg_expr(arg, rule, state, arg_scope, depth + 1) {
                FieldType::Array(inner) => FieldType::Array(inner),
                other => FieldType::Array(Box::new(other)),
            }
        }
        "filter" | "unique" | "distinct_by" | "sort_by" | "take" | "drop" | "slice" => input_type,
        "flatten" => match input_type {
            FieldType::Array(inner) => match *inner {
                FieldType::Array(nested) => FieldType::Array(nested),
                other => FieldType::Array(Box::new(other)),
            },
            other => other,
        },
        "chunk" => FieldType::Array(Box::new(FieldType::Array(Box::new(array_element_type(
            &input_type,
        ))))),
        "zip" => FieldType::Array(Box::new(FieldType::Array(Box::new(FieldType::JsonValue)))),
        "zip_with" => {
            let Some(arg) = op_step.args.last() else {
                return FieldType::Array(Box::new(FieldType::JsonValue));
            };
            let arg_scope = scope
                .clone()
                .with_pipe(FieldType::Array(Box::new(FieldType::JsonValue)))
                .with_item(Some(FieldType::Array(Box::new(FieldType::JsonValue))));
            FieldType::Array(Box::new(infer_arg_expr(
                arg,
                rule,
                state,
                arg_scope,
                depth + 1,
            )))
        }
        "group_by" => FieldType::Map(Box::new(FieldType::Array(Box::new(array_element_type(
            &input_type,
        ))))),
        "key_by" => FieldType::Map(Box::new(array_element_type(&input_type))),
        "partition" => FieldType::Array(Box::new(FieldType::Array(Box::new(array_element_type(
            &input_type,
        ))))),
        "find" | "first" | "last" => FieldType::Nullable(Box::new(array_element_type(&input_type))),
        "lookup" => FieldType::Array(Box::new(FieldType::JsonValue)),
        "lookup_first" => FieldType::Nullable(Box::new(FieldType::JsonValue)),
        "get" => infer_get(&input_type, &op_step.args),
        "pick" => infer_pick(&input_type, &op_step.args),
        "omit" => infer_omit(&input_type, &op_step.args),
        "merge" | "deep_merge" => {
            infer_merge(&input_type, &op_step.args, rule, state, scope, depth)
        }
        "object_flatten" => FieldType::Map(Box::new(FieldType::JsonValue)),
        "object_unflatten" => FieldType::JsonValue,
        "coalesce" => {
            let mut merged = input_type;
            for arg in &op_step.args {
                let arg_type = infer_arg_expr(
                    arg,
                    rule,
                    state,
                    scope.clone().with_pipe(merged.clone()),
                    depth + 1,
                );
                merged = merge_types(merged, arg_type);
            }
            merged
        }
        "reduce" => FieldType::JsonValue,
        "fold" => infer_fold(&input_type, &op_step.args, rule, state, scope, depth),
        _ => FieldType::JsonValue,
    }
}
