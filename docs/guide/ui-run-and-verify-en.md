# UI Startup and Verification Guide

A guide for starting the Rulemorph UI server and verifying it works in the browser.

## Prerequisites

- Rust/Cargo installed
- Node.js/npm installed (if building UI from source)
- If using `rulemorph-server` from GitHub Releases, no build required (`ui/dist` included)

## Building the UI (First Time Only)

> Skip this step if using the release binary `rulemorph-server`.

When developing, you need to manually build the UI static files.

```sh
cd crates/rulemorph_ui/ui
npm install
npm run build
```

After building, `crates/rulemorph_ui/ui/dist` will be generated.

## Starting the Server

### ui-only Mode

Serves the UI static files and `/internal/*` endpoints for compatibility and diagnostics. The current Trace Console normally uses `/api/*`, so use rules mode below for browser verification of the full UI workflow. `ui-only` assumes UI serving is enabled and cannot be combined with `--no-ui`.

```sh
# Development
cargo run -p rulemorph_server -- --api-mode ui-only

# Release binary
rulemorph-server --api-mode ui-only
```

### rules Mode (Default)

Provides the UI plus custom APIs defined in YAML at `/api/*`. The bundled UI rules live under `assets/api_rules/`.

```sh
# Development
cargo run -p rulemorph_server -- \
  --api-mode rules \
  --rules-dir ./assets/api_rules \
  --allow-unauth-internal \
  --ssrf-allow-private

# Release binary
rulemorph-server --api-mode rules --rules-dir ./assets/api_rules --allow-unauth-internal --ssrf-allow-private
```

### Options

| Option | Description | Default |
|--------|-------------|---------|
| `--api-mode <MODE>` | `ui-only` or `rules` | `rules` |
| `--port <PORT>` | Listen port | `8080` |
| `--data-dir <PATH>` | Data directory | `./.rulemorph` |
| `--rules-dir <PATH>` | API rules directory | `./.rulemorph/api_rules` |
| `--no-ui` | Disable UI (API only) | - |
| `--internal-api-key <KEY>` | Internal key for `/internal/*` and `/api/import` | - |
| `--allow-unauth-internal` | Allow internal APIs without a key | - |
| `--ssrf-allow-private` | Allow private IP/localhost targets | - |

## Browser Verification

After starting the server, open in your browser:

```
http://127.0.0.1:8080
```

- Trace list is displayed
- Click a trace to view details
- When using `/api`, trace updates are refreshed by polling

If your setup requires an API key, pass it once in the query string. The UI stores it in localStorage and removes it from the URL.

```
http://127.0.0.1:8080/?api_key=<API_KEY>
```

For ZIP import and internal operations, also pass `internal_key`.

```
http://127.0.0.1:8080/?api_key=<API_KEY>&internal_key=<INTERNAL_KEY>
```

## Adding Sample Traces

The UI loads `trace.json` manifests and chunks under `data_dir/traces` as traces (legacy single JSON files are still supported).

```sh
mkdir -p ./.rulemorph/traces/2025/01/01/demo-001
cat <<'JSON' > ./.rulemorph/traces/2025/01/01/demo-001/trace.json
{
  "trace_schema_version": 1,
  "trace_id": "demo-001",
  "timestamp": "2025-01-01T00:00:00Z",
  "status": "ok",
  "summary": {
    "record_total": 1,
    "record_success": 1,
    "record_failed": 0
  },
  "max_chunk_bytes_uncompressed": 4194304
}
JSON
```

> Date folders are optional, but organizing by `YYYY/MM/DD` format is recommended.

Legacy single JSON files still work, but the manifest + chunk layout is recommended.

See [ui-data-dir-usage-en.md](ui-data-dir-usage-en.md) for directory structure details.

## Sample API Rules

In rules mode, you can define custom APIs using YAML files in `./.rulemorph/api_rules/`.

Examples:
- `endpoint.yaml`: Endpoint definitions
- `network_fetch.yaml`: External API calls (`type: network`)
- `network_body.yaml`: Request body generation rules

## Common Errors

| Symptom | Cause and Solution |
|---------|-------------------|
| Blank screen | `ui/dist` doesn't exist. Run `npm run build` |
| 404 errors | `endpoint.yaml` not found. Check `--rules-dir` |
| Port in use | Check with `lsof -nP -iTCP:8080 -sTCP:LISTEN` and terminate the process |
