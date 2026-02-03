# UI Data Directory Structure

A guide to the data directory structure and file placement rules used by Rulemorph UI.

## Directory Structure

The default data directory is `./.rulemorph`.

```
./.rulemorph/
├── traces/          # Trace manifests and chunks (JSON/NDJSON)
├── rules/           # Rules referenced by traces (YAML)
└── api_rules/       # Custom API rules (YAML)
```

| Directory | Purpose |
|-----------|---------|
| `traces/` | Transformation execution traces (`trace.json` manifest + chunks, legacy single JSON also supported) |
| `rules/` | Rule files referenced within traces |
| `api_rules/` | Rules defining `/api/*` endpoints |

> Adding `.rulemorph/` to `.gitignore` is recommended.

## File Placement Rules

### traces/

Place trace manifests and chunks under `traces/`. Subdirectories are optional, but organizing by date is recommended.

```
./.rulemorph/traces/
├── 2025/01/01/
│   ├── trace-users-001/
│   │   ├── trace.json
│   │   ├── records-0001.ndjson.zst
│   │   ├── nodes-0001.ndjson.zst
│   │   ├── finalize.json.zst
│   │   └── blobs/
│   │       └── sha256-<hash>.json.zst
│   └── trace-users-002/
│       └── trace.json
└── 2025/01/02/
    └── trace-orders-001/
        └── trace.json
```

`trace.json` is the trace manifest and can reference chunk files such as `records-*.ndjson`, `nodes-*.ndjson`, or `finalize.json`. If you want to tell the loader the expansion limit, set `max_chunk_bytes_uncompressed` (bytes). When omitted, the loader falls back to the hard cap (16MB), and the value is clamped to 16MB. The writer applies the same clamp and records the clamped value in the manifest. The loader also enforces total chunk count/byte/record/node budgets (records 200k / nodes 500k); if exceeded it downgrades detail to `basic` and adds `budget_exceeded` to `reason`. If a chunk fails I/O/parse/decode it also downgrades detail to `basic` and adds `chunk_error` to `reason`. If a single record/node/finalize exceeds `max_chunk_bytes_uncompressed`, detail is downgraded to `basic` and `chunk_too_large` is added to `reason`. `trace.json`/legacy JSON are capped at 20MB; oversized files are skipped (the writer retries after dropping `rule_source`, and fails the write if it is still over 20MB).
Legacy single JSON files under `traces/` are still supported.
By default, chunks are Zstd-compressed with a `.zst` suffix, and large payloads are externalized under `blobs/`.

### rules/

Place files at paths matching `rule.path` / `meta.rule_ref` referenced in traces.

```
./.rulemorph/rules/
├── users/
│   ├── endpoint.yaml
│   ├── get.yaml
│   └── list.yaml
└── orders/
    └── transform.yaml
```

### api_rules/

Place rules that provide `/api/*` endpoints in rules mode.

```
./.rulemorph/api_rules/
├── endpoint.yaml           # Root endpoint definition
└── network/
    ├── list.yaml
    └── get.yaml
```

## Troubleshooting

If traces are not appearing:

1. Verify `--data-dir` is set correctly
2. Check that `trace.json` manifests (or legacy JSON files) exist in `traces/`
3. Ensure no old processes are holding the port

```sh
lsof -nP -iTCP:8080 -sTCP:LISTEN
```

See [ui-run-and-verify-en.md](ui-run-and-verify-en.md) for startup instructions.
