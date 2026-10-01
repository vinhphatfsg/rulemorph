import { build } from 'esbuild';
import { mkdir, copyFile } from 'node:fs/promises';
await mkdir('../dist/server', { recursive: true });
await build({ entryPoints: ['src/worker.mjs'], outfile: '../dist/server/index.js', bundle: true, format: 'esm', platform: 'neutral', target: 'es2022', external: ['*.wasm'] });
// Keep the Worker module import next to its entrypoint.
await copyFile('pkg/rulemorph_wasm_bg.wasm', '../dist/server/rulemorph_wasm_bg.wasm');
const { readFile, writeFile } = await import('node:fs/promises');
const entry = await readFile('../dist/server/index.js', 'utf8');
await writeFile('../dist/server/index.js', entry.replace('../pkg/rulemorph_wasm_bg.wasm', './rulemorph_wasm_bg.wasm'));
