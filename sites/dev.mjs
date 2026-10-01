import { Miniflare } from 'miniflare';
const mf = new Miniflare({ modules: true, modulesRoot: '../', scriptPath: '../dist/server/index.js', compatibilityDate: '2025-11-25', modulesRules: [{ type: 'CompiledWasm', include: ['**/*.wasm'] }], host: '127.0.0.1', port: 8787 });
console.log(`ローカルMCP: ${await mf.ready}mcp (Sites認証ヘッダーはローカルテスト用に手動指定)`);
for (const signal of ['SIGINT', 'SIGTERM']) process.on(signal, async () => { await mf.dispose(); process.exit(0); });
