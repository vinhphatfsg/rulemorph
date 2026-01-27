# Rulemorph

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A Rust CLI and library to transform CSV/JSON data into JSON using YAML rules.

Note: v1 rules are deprecated; use `version: 2`.

## Features

- **Input formats**: CSV and JSON with nested record extraction
- **Rule-based mapping**: Declarative YAML rules with static validation
- **Expressions (v2 pipe syntax)**: trim/lowercase/uppercase/concat, add/multiply, coalesce, lookup/lookup_first, plus `let`/`if`/`map` steps
- **Lookups**: Array lookups from external context data (lookup, lookup_first)
- **Conditions**: Conditional mapping with comparisons, regex, and logical ops
- **DTO generation**: Generate type definitions for Rust, TypeScript, Python, Go, Java, Kotlin, Swift
- **MCP server**: Available as a Model Context Protocol server for AI assistants

## Installation

### Homebrew (recommended)

```sh
brew install vinhphatfsg/tap/rulemorph
```

<details>
<summary>Other platforms</summary>

Download prebuilt binaries from [GitHub Releases](https://github.com/vinhphatfsg/rulemorph/releases):

- macOS (Apple Silicon): `rulemorph-<TAG>-aarch64-apple-darwin.tar.gz`
- macOS (Intel): `rulemorph-<TAG>-x86_64-apple-darwin.tar.gz`
- Linux (x86_64): `rulemorph-<TAG>-x86_64-unknown-linux-gnu.tar.gz`
- Windows (x86_64): `rulemorph-<TAG>-x86_64-pc-windows-msvc.zip`

</details>

## Quick Start

Transform user data from an external API response to your schema:

**rules.yaml**
```yaml
version: 2
input:
  format: json
  json:
    records_path: "users"
mappings:
  - target: "id"
    source: "user_id"
  - target: "name"
    expr:
      - "@input.full_name"
      - trim
  - target: "email"
    expr:
      - "@input.username"
      - concat: ["lit:@example.com"]
```

**input.json**
```json
{ "users": [{ "user_id": 1, "full_name": "Alice", "username": "alice" }] }
```

**Run**
```sh
rulemorph transform -r rules.yaml -i input.json
```

**Output**
```json
[{ "id": 1, "name": "Alice", "email": "alice@example.com" }]
```

## Rule Structure

```yaml
version: 2
input:
  format: json|csv
  json:
    records_path: "path.to.array"  # Optional
mappings:
  - target: "output.field"
    source: "input.field"    # OR value: <literal> OR expr: <pipe>
    type: string|int|float|bool
    when:
      eq: ["@input.status", "active"]  # Optional condition
```

Note: v2 condition comparisons are type-sensitive (`"1"` != `1`). Ordering (`gt/gte/lt/lte`) compares numerically when possible, otherwise compares strings lexicographically.

For full rule specification, see [docs/rules_spec_en.md](docs/rules_spec_en.md) (English) or [docs/rules_spec_ja.md](docs/rules_spec_ja.md) (Japanese).
UIの起動と確認手順は [docs/guide/ui-run-and-verify.md](docs/guide/ui-run-and-verify.md) を参照してください。

UIは `--api-mode rules` が既定です（`--api-mode ui-only` はUI専用モード）。
ユーザーAPIのみ動かす場合は `--no-ui` を付けてUIを無効化できます。
既定の `data_dir` は `./.rulemorph`、既定の `rules_dir` は `./.rulemorph/api_rules` です。

## DTO Generation

Generate type definitions from your rules:

```sh
rulemorph generate -r rules.yaml -l typescript
```

Output:
```typescript
export interface Record {
  id: number;
  name: string;
  email: string;
}
```

Supported languages: `rust`, `typescript`, `python`, `go`, `java`, `kotlin`, `swift`

## Library Usage (Rust)

```rust
use rulemorph::{parse_rule_file, transform};

let rule = parse_rule_file(&std::fs::read_to_string("rules.yaml")?)?;
let output = transform(&rule, &std::fs::read_to_string("input.json")?, None)?;
```

## MCP Server

An MCP server (`rulemorph-mcp`) is included for AI assistant integration:

```sh
claude mcp add rulemorph -- rulemorph-mcp
```
