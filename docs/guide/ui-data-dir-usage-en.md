# UI Data Directory Structure

A guide to the data directory structure and file placement rules used by Rulemorph UI.

## Directory Structure

The default data directory is `./.rulemorph`.

```
./.rulemorph/
├── traces/          # Trace files (JSON)
├── rules/           # Rules referenced by traces (YAML)
└── api_rules/       # Custom API rules (YAML)
```

| Directory | Purpose |
|-----------|---------|
| `traces/` | Transformation execution trace logs (1 file = 1 trace) |
| `rules/` | Rule files referenced within traces |
| `api_rules/` | Rules defining `/api/*` endpoints |

> Adding `.rulemorph/` to `.gitignore` is recommended.

## File Placement Rules

### traces/

Place trace files under `traces/`. Subdirectories are optional, but organizing by date is recommended.

```
./.rulemorph/traces/
├── 2025/01/01/
│   ├── trace-users-001.json
│   └── trace-users-002.json
└── 2025/01/02/
    └── trace-orders-001.json
```

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
2. Check that JSON files exist in `traces/`
3. Ensure no old processes are holding the port

```sh
lsof -nP -iTCP:8080 -sTCP:LISTEN
```

See [ui-run-and-verify-en.md](ui-run-and-verify-en.md) for startup instructions.
