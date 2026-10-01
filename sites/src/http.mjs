import { tools } from './tools.mjs';

const versions = ['2025-03-26', '2025-06-18', '2025-11-25'];
const maxBody = 96 * 1024;
const maxResponse = 512 * 1024;
const object = value => value !== null && typeof value === 'object' && !Array.isArray(value);
const json = (body, status = 200) => new Response(JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json', 'Cache-Control': 'no-store', 'X-Content-Type-Options': 'nosniff' } });
const error = (id, code, message, status = 200) => json({ jsonrpc: '2.0', id, error: { code, message } }, status);

async function readBody(request) {
  if (Number(request.headers.get('content-length')) > maxBody) throw new RangeError();
  const reader = request.body?.getReader();
  if (!reader) return '';
  let size = 0;
  const chunks = [];
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      size += value.byteLength;
      if (size > maxBody) { await reader.cancel(); throw new RangeError(); }
      chunks.push(value);
    }
  } finally { reader.releaseLock(); }
  const bytes = new Uint8Array(size);
  let offset = 0;
  for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.byteLength; }
  return new TextDecoder('utf-8', { fatal: true }).decode(bytes);
}

// Sites must be the only externally reachable ingress. No user identity fallback.
export function createHandler(callTool, validJSON = () => true) {
  return async function fetch(request, env = {}) {
    const url = new URL(request.url);
    if (url.pathname !== '/mcp') return new Response('Not Found', { status: 404 });
    const origin = request.headers.get('origin');
    if (origin && origin !== url.origin) return error(null, -32000, 'Origin is not allowed', 403);
    if (request.method !== 'POST') return new Response(null, { status: 405, headers: { Allow: 'POST' } });
    const version = request.headers.get('mcp-protocol-version') ?? versions[0];
    if (!versions.includes(version)) return error(null, -32600, 'Unsupported MCP protocol version', 400);
    if (request.headers.get('content-type')?.split(';')[0].trim().toLowerCase() !== 'application/json') return error(null, -32600, 'Content-Type must be application/json', 415);
    const accept = request.headers.get('accept') ?? '';
    if (!accept.includes('application/json') || !accept.includes('text/event-stream')) return error(null, -32600, 'Accept must include application/json and text/event-stream', 406);
    let message;
    try {
      const raw = await readBody(request);
      if (!validJSON(raw)) return error(null, -32700, 'Invalid JSON (duplicate keys or parser limits)', 400);
      message = JSON.parse(raw);
    }
    catch (e) { return error(null, e instanceof RangeError ? -32600 : -32700, e instanceof RangeError ? 'Request exceeds 96 KiB' : 'Invalid JSON/UTF-8', e instanceof RangeError ? 413 : 400); }
    if (!object(message) || message.jsonrpc !== '2.0' || typeof message.method !== 'string' || (message.id !== undefined && !(typeof message.id === 'string' || Number.isSafeInteger(message.id))) || (message.params !== undefined && !object(message.params))) return error(null, -32600, 'Invalid JSON-RPC request', 400);
    const id = message.id;
    const params = message.params ?? {};
    if (id === undefined) {
      if (message.method === 'notifications/initialized' || message.method === 'notifications/cancelled') return new Response(null, { status: 202 });
      return error(null, -32600, 'Unsupported notification', 400);
    }
    let result;
    switch (message.method) {
      case 'initialize': {
        if (typeof params.protocolVersion !== 'string' || !object(params.capabilities) || !object(params.clientInfo) || typeof params.clientInfo.name !== 'string' || typeof params.clientInfo.version !== 'string') return error(id, -32602, 'Invalid initialize parameters');
        result = { protocolVersion: versions.includes(params.protocolVersion) ? params.protocolVersion : versions.at(-1), capabilities: { tools: { listChanged: false } }, serverInfo: { name: 'rulemorph-sites', version: '0.3.4' } };
        break;
      }
      case 'ping': result = {}; break;
      case 'tools/list':
        if (params.cursor !== undefined) return error(id, -32602, 'Pagination is not supported');
        result = { tools }; break;
      case 'tools/call': {
        const user = request.headers.get('oai-authenticated-user-id');
        if (!user?.trim()) return error(id, -32001, 'Sites authentication required', 401);
        // Optional Site-scoped restriction; never authorize with email or request arguments.
        if (env.ALLOWED_SITE_USER_IDS !== undefined) {
          const allowed = String(env.ALLOWED_SITE_USER_IDS).split(',').map(x => x.trim()).filter(Boolean);
          if (!allowed.includes(user)) return error(id, -32003, 'User is not allowed', 403);
        }
        const tool = tools.find(tool => tool.name === params.name);
        if (!tool) return error(id, -32602, 'Unknown tool');
        const args = params.arguments ?? {};
        if (!object(args) || Object.keys(args).some(key => !(key in tool.inputSchema.properties)) || tool.inputSchema.required.some(key => !(key in args))) return error(id, -32602, 'Invalid tool arguments');
        try {
          result = JSON.parse(callTool(JSON.stringify({ name: params.name, arguments: args })));
          if (new TextEncoder().encode(JSON.stringify(result)).byteLength > maxResponse) return error(id, -32000, 'Response limit exceeded');
        } catch { return error(id, -32603, 'Tool execution failed'); }
        break;
      }
      default: return error(id, -32601, 'Method not found');
    }
    return json({ jsonrpc: '2.0', id, result });
  };
}
