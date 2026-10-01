import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createHandler } from '../src/http.mjs';

const headers = { 'content-type': 'application/json', accept: 'application/json, text/event-stream' };
const rpc = (method, params = {}, id = 1) => ({ jsonrpc: '2.0', id, method, params });
const request = (body, extra = {}) => new Request('https://site.test/mcp', { method: 'POST', headers: { ...headers, ...extra }, body: JSON.stringify(body) });
let calls = 0;
const handler = createHandler(() => { calls++; return JSON.stringify({ content: [], isError: false }); });

test('initialize, discovery, ping and notifications are stateless and contain no private data', async () => {
  const init = await handler(request(rpc('initialize', { protocolVersion: '2025-11-25', capabilities: {}, clientInfo: { name: 'test', version: '1' } })));
  assert.equal(init.headers.get('mcp-session-id'), null);
  assert.equal((await init.json()).result.protocolVersion, '2025-11-25');
  const discovery = await handler(request(rpc('tools/list'), { 'oai-authenticated-user-email': 'private@example.test' }));
  const result = await discovery.json();
  assert.equal(result.result.tools.length, 4);
  assert.ok(!JSON.stringify(result).includes('private@example'));
  assert.ok(!JSON.stringify(result).includes('input_path'));
  assert.equal((await handler(request(rpc('ping')))).status, 200);
  const notification = await handler(request({ jsonrpc: '2.0', method: 'notifications/initialized' }));
  assert.equal(notification.status, 202);
  assert.equal(await notification.text(), '');
});

test('authentication and optional Site-scoped authorization run before tool execution', async () => {
  const body = rpc('tools/call', { name: 'analyze_input', arguments: { input_json: [] } });
  calls = 0;
  assert.equal((await handler(request(body))).status, 401);
  assert.equal((await handler(request(body, { 'oai-authenticated-user-email': 'user@example.test' }))).status, 401);
  assert.equal((await handler(request(body, { 'oai-authenticated-user-id': 'user-1' }), { ALLOWED_SITE_USER_IDS: 'user-2' })).status, 403);
  assert.equal((await handler(request(body, { 'oai-authenticated-user-id': 'user-1' }), { ALLOWED_SITE_USER_IDS: '' })).status, 403);
  assert.equal(calls, 0);
  assert.equal((await handler(request(body, { 'oai-authenticated-user-id': 'user-1' }), { ALLOWED_SITE_USER_IDS: 'user-1' })).status, 200);
  assert.equal(calls, 1);
});

test('transport rejects invalid origin, method, version, batch, JSON and oversized streams', async () => {
  assert.equal((await handler(request(rpc('ping'), { origin: 'https://evil.test' }))).status, 403);
  assert.equal((await handler(new Request('https://site.test/mcp'))).status, 405);
  assert.equal((await handler(new Request('https://site.test/other'))).status, 404);
  assert.equal((await handler(request(rpc('ping'), { 'mcp-protocol-version': 'wrong' }))).status, 400);
  assert.equal((await handler(request([rpc('ping')]))).status, 400);
  assert.equal((await handler(new Request('https://site.test/mcp', { method: 'POST', headers, body: '{' }))).status, 400);
  assert.equal((await handler(request(rpc('ping'), { accept: 'application/json' }))).status, 406);
  assert.equal((await handler(request(rpc('ping'), { 'content-type': 'text/plain' }))).status, 415);
  const stream = new ReadableStream({ start(controller) { controller.enqueue(new Uint8Array(96 * 1024)); controller.enqueue(new Uint8Array(1)); controller.close(); } });
  assert.equal((await handler(new Request('https://site.test/mcp', { method: 'POST', headers, body: stream, duplex: 'half' }))).status, 413);
});

test('JSON-RPC error codes and request ids are preserved', async () => {
  for (const [body, code] of [[rpc('unknown', {}, 'str-id'), -32601], [rpc('tools/call', { name: 'nope' }), -32602], [rpc('tools/call', { name: 'transform', arguments: { rules_text: 'x', output_path: '../../tmp' } }), -32602]]) {
    const response = await handler(request(body, { 'oai-authenticated-user-id': 'user-1' }));
    const result = await response.json();
    assert.equal(result.id, body.id);
    assert.equal(result.error.code, code);
  }
});
