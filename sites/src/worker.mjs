import wasm from '../pkg/rulemorph_wasm_bg.wasm';
import { initSync, call_tool, valid_request_json } from '../pkg/rulemorph_wasm.js';
import { createHandler } from './http.mjs';

initSync({ module: wasm });
export default { fetch: createHandler(call_tool, valid_request_json) };
