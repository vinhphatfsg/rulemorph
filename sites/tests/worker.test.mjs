import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { Miniflare } from 'miniflare';

let mf;
const rule = 'version: 2\ninput: {format: json, json: {}}\nmappings:\n  - target: name\n    source: name\n    type: string\n';
before(async () => {
  mf = new Miniflare({ modules: true, modulesRoot: '../', scriptPath: '../dist/server/index.js', compatibilityDate: '2025-11-25', modulesRules: [{ type: 'CompiledWasm', include: ['**/*.wasm'] }] });
  await mf.ready;
});
after(async () => { await mf?.dispose(); });
async function call(name, args, user = 'site-user') {
  const response = await mf.dispatchFetch('https://site.test/mcp', { method: 'POST', headers: { 'content-type': 'application/json', accept: 'application/json, text/event-stream', ...(user ? { 'oai-authenticated-user-id': user } : {}) }, body: JSON.stringify({ jsonrpc: '2.0', id: 7, method: 'tools/call', params: { name, arguments: args } }) });
  return { status: response.status, body: await response.json() };
}
function data(result) {
  assert.equal(result.status, 200);
  assert.equal(result.body.result.isError, false, JSON.stringify(result));
  return JSON.parse(result.body.result.content[0].text);
}

test('built Worker executes real WASM for all four tools', async () => {
  assert.deepEqual(data(await call('transform', { rules_text: rule, input_json: [{ name: '太郎' }] })).output, [{ name: '太郎' }]);
  assert.equal(data(await call('validate_rules', { rules_text: rule })).valid, true);
  assert.match(data(await call('generate_dto', { rules_text: rule, language: 'typescript' })).dto, /name/);
  const analyzed = data(await call('analyze_input', { input_json: [{ name: 'private-value' }] }));
  assert.equal(analyzed.summary.records, 1);
  assert.ok(!JSON.stringify(analyzed).includes('private-value'));
});

test('real WASM rejects file access, duplicate keys, huge data and recovers on next call', async () => {
  assert.equal((await call('transform', { rules_text: rule, input_json: [] }, null)).status, 401);
  assert.equal((await call('transform', { rules_text: rule, input_path: '../../secret' })).body.error.code, -32602);
  for (const args of [{ input_json: Array.from({ length: 129 }, () => ({ name: 'x' })) }, { input_text: '[{"x":1,"x":2}]' }, { input_json: { nested: Array.from({ length: 257 }, () => 1) } }]) {
    assert.equal((await call('analyze_input', args)).body.result.isError, true);
  }
  assert.equal((await call('validate_rules', { rules_text: 'version: 2\ninput: {format: json, json: {}}\nsteps:\n  - branch: {when: true, then: ../../secret}' })).body.result.isError, true);
  assert.equal(data(await call('transform', { rules_text: rule, input_json: [{ name: 'recovered' }] })).output[0].name, 'recovered');
});

test('raw HTTP JSON rejects duplicate keys before JavaScript parsing', async () => {
  const response = await mf.dispatchFetch('https://site.test/mcp', { method: 'POST', headers: { 'content-type': 'application/json', accept: 'application/json, text/event-stream', 'oai-authenticated-user-id': 'user' }, body: '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"analyze_input","arguments":{"input_json":[{"x":1,"x":2}]}}}' });
  assert.equal(response.status, 400);
  assert.equal((await response.json()).error.code, -32700);
});
