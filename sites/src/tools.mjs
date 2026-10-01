const text = { type: 'string' };
const rules = { rules_text: { ...text, description: 'YAML/JSONルール。最大16 KiB。HTTPの制限付きプロファイルを使用。' } };
const input = { input_text: text, input_json: {} };
function tool(name, description, properties, required) {
  return { name, description, inputSchema: { type: 'object', properties, required, additionalProperties: false },
    annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false } };
}
export const tools = [
  tool('transform', 'インライン入力をRulemorph coreで変換します。ファイル参照なし。', { ...rules, ...input }, ['rules_text']),
  tool('validate_rules', 'ルールとHTTPプロファイルの制約を検証します。', rules, ['rules_text']),
  tool('generate_dto', 'ルールからDTOのソース文字列を生成します。コード実行なし。', { ...rules, language: { type: 'string', enum: ['rust', 'typescript', 'python', 'go', 'java', 'kotlin', 'swift'] }, name: text }, ['rules_text', 'language']),
  tool('analyze_input', 'JSON/CSVのレコード数とパス・型を解析します。値のサンプルなし。', { ...input, format: { type: 'string', enum: ['json', 'csv'] }, records_path: text }, []),
];
