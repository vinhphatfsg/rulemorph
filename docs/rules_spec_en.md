# Transformation Rules Spec (Implementation-Aligned)

This document describes the current v2 rule spec, references, expression syntax, and evaluation rules.
For the Japanese version, see `docs/rules_spec_ja.md`.

## How to read this spec

A normal Rulemorph transformation first normalizes raw input into an array of **JSON records**.
Mappings or steps then build output objects from those records, and `finalize` may optionally transform the whole output array.

```text
raw input
  -> input normalization
  -> records: [JSON object, ...]
  -> mappings or steps
  -> output records
  -> finalize
  -> JSON output
```

For a first rule, read the sections in this order:

1. Choose an `input` format and decide how records are selected.
2. Use `mappings` to create output fields.
3. Add `expr` and `conditions` for value transformations and conditional behavior.
4. Use `steps` only when you need ordered mapping/filter/assert/branch behavior.
5. Use `finalize` only when the whole output array needs post-processing.

## Conceptual model

| Area | Purpose | Main keys |
| --- | --- | --- |
| Input normalization | Convert CSV/JSON/YAML/TOML/XML/HTML/Excel into JSON records | `input` |
| Record filtering | Decide whether a record should be processed | `record_when` |
| Output mapping | Build one output object from one input record | `mappings` |
| Ordered execution | Run mappings, filters, asserts, and branches in sequence | `steps` |
| Array post-processing | Apply filter/sort/limit/wrap to the output array | `finalize` |
| References and expressions | Read input, context, and intermediate output values | `@input`, `@context`, `@out`, `expr` |

Use `mappings` for straightforward transformations. Use `steps` when the rule needs validation, branching, or multiple ordered phases.

## Semantic Trace API

The core library provides opt-in semantic trace APIs:

- `transform_input_with_trace(...)`
- `transform_input_with_trace_with_base_dir_and_options(...)`
- `transform_record_with_trace(...)`

A trace observes normalized records as they pass through `record_when`, `mappings` / `steps`, `expr`, operators, branches, and `finalize`, then returns the events as `TransformTrace`. Enabling trace must not change the output or warnings of the normal `transform` / `transform_input` / `transform_record` APIs.

`TransformTraceOptions::default()` uses `Raw` mode. Because the trace API is explicitly requested to inspect values, raw input / output / context values are included in value snapshots by default. The core does not automatically persist, log, send over the network, postMessage, or share raw traces.

Value handling is controlled by `TraceValueMode`.

| mode | Behavior | Typical use |
| --- | --- | --- |
| `Raw` | Include raw values in snapshots | Local transparency UI and debugging |
| `Redacted` | Drop values for secret-like paths or oversized snapshots | Safer human review |
| `MetadataOnly` | Return only state/type/bytes metadata without values | Default for export / share / network / postMessage adapters |

For `Redacted`, `redaction_reason` uses `"secret_like_path"` for secret-like paths. Composite object / array snapshots are metadata-only in the initial implementation.

Each value snapshot has enum `state` and `type` fields, so `missing`, `null`, and empty string stay distinct. `default` applies only when the value is `missing`. Use `coalesce` rather than `default` when a rule should fall back for `null`.

Trace `input_path` / `output_path` values use canonical forms. Input-side paths are `@input.*`, `@item.*`, `@acc.*`, `@context.*`, or `@out.*`; output-side paths are `$.*`. Bracket notation is preserved, for example `@input["@id"]`, `@input[0].name`, `@item[0].name`, and `$.items[0].name`.

Traces can be bounded with `max_events`, `max_trace_bytes`, and `max_snapshot_bytes`. If the event stream is truncated, `complete=false` and `truncation.reason` are set. `max_snapshot_bytes` drops only snapshot values and does not relax event parent/child structure.

Trace errors are returned as `TransformTraceError` with a partial trace. Its `Debug` / `Display` / `std::error::Error` output does not include the raw trace body or the underlying `TransformError` full message. External adapters should default to `MetadataOnly` and verify `contains_raw_values == false` before crossing export, share, network, or `postMessage` boundaries.

The planned WASM binding shape is `wasm_transform_trace({ rule, input, inputFormat?, traceMode? })`. `traceMode` is `"raw" | "redacted" | "metadata_only"`; local animation may use `raw`, while export / share / `postMessage` defaults to `metadata_only`.

## Rule File Structure

```yaml
version: 2
input:
  format: json
  json:
    records_path: "items"

record_when:
  all:
    - { gt: ["@input.score", 10] }
    - { eq: ["@input.active", true] }

mappings:
  - target: "user.id"
    source: "id"
    type: "string"
    required: true
  - target: "user.name"
    expr:
      - "@input.name"
      - trim
  - target: "meta.source"
    value: "api"
```

- `version` (required): fixed to `2`
- `input` (required): input format and options
- `mappings` (required): transformation rules (evaluated in order)
- `output` (optional): metadata (e.g., DTO name)
- `record_when` (optional): condition to include/exclude records
- `steps` (optional): ordered execution. Cannot be combined with top-level `mappings` or `record_when`
- `finalize` (optional): post-process the output array. Works with either `mappings` or `steps`

## Input

`input` converts raw input into Rulemorph's common data model: an array of JSON records.
`mappings`, `steps`, and `finalize` run against normalized JSON records, not against the original file format.

### Common
- `input.format` (required): `csv` / `json` / `yaml` / `toml` / `xml` / `html` / `excel`

| format | Record selection | Typical use |
| --- | --- | --- |
| `json` | root or `records_path` | API responses, JSON exports |
| `csv` | one record per row | CSV imports, business data |
| `yaml` | root or `records_path` | config files, YAML exports |
| `toml` | root or `records_path` | dependency and inventory data |
| `xml` | element path via `records_path` | XML feeds, legacy APIs |
| `html` | `records_selector` plus field selectors | extracting tables or lists |
| `excel` | rows from a sheet | `.xlsx` imports |

### Normalization contract

- Records are JSON objects.
- If `records_path` points to an array, each element becomes a record.
- If `records_path` points to an object, it becomes a single record.
- Scalars cannot be records.
- A missing reference is `missing`, which is distinct from `null`.
- Format-specific differences are absorbed by the parser layer before mappings, steps, and finalize run.

Parser safety invariants are not optional: duplicate JSON/YAML keys, XML DTD/entity/processing instruction input, HTML JavaScript execution/URL fetching, and Excel macro/external relationship/formula evaluation are not allowed. CLI resource limit overrides cannot relax these invariants.

### CSV
- `input.csv` is required when `format=csv`
- `has_header` (optional): default `true`
- `delimiter` (optional): default `","` (must be exactly 1 character)
- `columns` (optional): required when `has_header=false`

```yaml
input:
  format: csv
  csv:
    has_header: false
    delimiter: ","
    columns:
      - { name: "id", type: "string" }
      - { name: "price", type: "float" }
```

### JSON
- `input.json` is required when `format=json`
- `records_path` (optional): dot path to a record array or single record. If omitted, use the root value.
- If the root or `records_path` is an array, each element becomes a record.
- If the root or `records_path` is an object, it becomes a single record.
- Scalars cannot be records.

```yaml
input:
  format: json
  json:
    records_path: "items"
```

### YAML / TOML
- `input.yaml` / `input.toml` is required for the matching `format`
- `records_path` (optional): dot path to a record array or single record
- YAML/TOML input is normalized to JSON records before mappings, steps, and finalize run.
- Format-specific values such as YAML aliases/anchors and TOML datetimes are normalized by the parser layer into JSON values.

```yaml
input:
  format: yaml
  yaml:
    records_path: "users"
```

```yaml
input:
  format: toml
  toml:
    records_path: "users"
```

### XML
- `input.xml` is required when `format=xml`
- `records_path` (required): dot-separated element path including the root element
- Attributes become string fields with `attr_prefix`; direct text is stored under `text_key`
- Child elements are always arrays
- DTDs, entities, and processing instructions are rejected.

```yaml
input:
  format: xml
  xml:
    records_path: "users.user"
    attr_prefix: "@"
    text_key: "#text"
```

### HTML
- `input.html` is required when `format=html`
- `records_selector` selects record elements; each field uses a CSS selector relative to the record
- `value: text` / `html` / `attr` are supported. `html` is raw inner HTML string extraction; it is not sanitized or executed.
- The HTML parser does not execute JavaScript or fetch URLs.

```yaml
input:
  format: html
  html:
    records_selector: "table#users tbody tr"
    fields:
      id: { selector: "td:nth-child(1)", value: text }
      name: { selector: "td:nth-child(2)", value: text }
```

### Excel
- `input.excel` is required when `format=excel`
- Only `.xlsx` is supported. Macros, external relationships, and formula evaluation are rejected or not executed.
- With `has_header=true`, the header row provides field names.
- Empty cells are treated as `missing`. Empty strings are real values and remain distinct from `missing`.
- Formula cells are never evaluated. The default `formula` policy is `cached`; formula cells without cached values are errors. `formula: formula` reads the formula string, and `formula: error` rejects formula cells.

```yaml
input:
  format: excel
  excel:
    sheet: "Users"
    has_header: true
```

## Output

- Default output is a JSON array of records
- CLI `transform --ndjson` outputs one JSON object per line (streaming)
- If `records_path` points to an object, a single record is produced

## Resource limits

The CLI can relax finite resource limits for large local inputs:

```sh
rulemorph transform -r rules.yaml -i huge.csv --limit records=500000 --limit input-bytes=536870912
rulemorph transform -r rules.yaml -i trusted.json --limit range-items=50000
rulemorph transform -r rules.yaml -i trusted.json --limit range-items=unlimited
rulemorph transform -r rules.yaml -i huge.csv --limits-profile large
rulemorph transform -r rules.yaml -i workbook.xlsx --limits-file limits.toml
```

`range` emits at most 10,000 items by default. Use `range-items=<integer>` to change that cap. `range-items=unlimited` removes the per-range cap for trusted local input/rules. In `--limits-file`, write it as a string, for example `range-items = "unlimited"`. Even with `range-items=unlimited`, generated arrays from `range`, `map`, `flat_map`, `flatten`, and similar operators are still bounded by `array-len`.

These options only increase processing limits. Safety invariants such as duplicate key rejection, XML DTD/entity rejection, HTML no-network/no-JS behavior, Excel no-macro/no-formula-evaluation behavior, and MCP pathless branch guard are not configurable.

## Record filter (`record_when`)

`record_when` is an optional condition evaluated once per record before any mappings.
If it evaluates to `false`, the record is skipped (no output).
If evaluation fails, the record is skipped and a warning is emitted.

- `record_when` uses the same condition syntax as `when` and `if.cond`
- `record_when` may reference `@input.*` and `@context.*`
- `@out.*` references are invalid because outputs do not exist yet

## Mapping

Each mapping writes a single value into `target`.

```yaml
- target: "user.id"
  source: "id"
  type: "string"
  required: true
```

Fields:
- `target` (required): dot path in output JSON (array indexes are not allowed)
- `source` | `value` | `expr` (required, mutually exclusive)
  - `source`: reference path (see Reference)
  - `value`: JSON literal
  - `expr`: v2 pipe expression
- `when` (optional): condition. If `false` or evaluation error, mapping is skipped (warning)
- `type` (optional): `string|int|float|bool`
- `required` (optional): default `false`
- `default` (optional): literal used only when value is `missing`

### `when` behavior
- `when` is evaluated at the start of mapping
- `false` or evaluation error skips the mapping (error becomes warning)
- If skipped, `required/default/type` are not evaluated
- `missing` is treated as false

### `required`/`default` behavior
- If value is `missing`, use `default` if present
- If value is `missing` and `required=true`, it is an error
- `null` is **not** missing. If `required=true`, it is an error; otherwise `null` is kept

### `target` constraints
- `target` must be object keys only (no array indexes)
- If an intermediate path is not an object, it is an error

## Steps

`steps` controls evaluation order explicitly.
If `steps` is present, top-level `mappings` and top-level `record_when` cannot be used.

```yaml
version: 2
input: { format: json, json: { records_path: "items" } }

steps:
  - mappings:
      - target: "total"
        expr: ["@input.a", { "+": ["@input.b"] }]
  - record_when:
      gt: ["@out.total", 0]
  - asserts:
      - when: { gt: ["@out.total", 10] }
        error:
          code: "INVALID_TOTAL"
          message: "total must be > 10"
  - branch:
      when: { eq: ["@input.type", "premium"] }
      then: ./rules/premium.yaml
      else: ./rules/basic.yaml
      return: true

finalize:
  sort: { by: "total", order: "desc" }
```

Each step has exactly one step key, except for optional metadata such as `name`.

| Step | Description |
| --- | --- |
| `mappings` | Same syntax as v2 mappings |
| `record_when` | v2 condition. Excludes the record when false |
| `asserts` | Validation conditions. False raises an error |
| `branch` | Conditional transition to another rule |

Data flow:

- `@input` is the original normalized input record.
- `@out` is the accumulated output for the current record.
- `mappings` results are merged into `@out`.
- A branch target receives the current `@out` as its `@input`.

`branch.then` and `branch.else` are rule references only; inline rules are not supported.
If `return: true`, the branch output becomes the final output and later steps are skipped.
If `return` is omitted or false, the branch output is merged into `@out` and execution continues.

## Finalize

`finalize` runs after all records have been processed.
It post-processes the output array and can be used with either top-level `mappings` or `steps`.
`finalize` is not available with streaming output.

Supported keys:

- `filter`: v2 condition using `@item`
- `sort`: sort the output array
- `limit` / `offset`: pagination
- `wrap`: wrap the final array in an object

Examples:

```yaml
finalize:
  filter:
    eq: ["@item.status", "active"]
  sort:
    by: "created_at"
    order: "desc"
  limit: 10
```

`wrap` uses v2 expressions. `@out` is the current output array.

```yaml
finalize:
  wrap:
    data: "@out"
    meta:
      total:
        - "@out"
        - len
```

## Reference

References are `@`-prefixed namespaces + dot paths:
- `@input.*`: input record
- `@context.*`: injected external context
- `@out.*`: output values produced earlier in the same record
- `@item.*`: current element in a `map` step (`@item.index` is the 0-based index)
- `@item`: current element (whole value) in a `map` step
- `@acc.*`: accumulator in `reduce`/`fold` steps
- `@acc`: accumulator (whole value) in `reduce`/`fold` steps
- `@<var>`: let-bound variable (e.g., `@total`)

Whole-scope references are `@item`/`@acc` (no trailing dot). A trailing dot like `@item.` or `@acc.` is invalid.

`source` can omit the namespace **only for a single key** (defaults to `input.*`).
If you need dot paths or array indexes, you must use `input.*` explicitly.

Examples:
- `source: "id"` means `input.id`
- `source: "input.user.name"`
- `source: "input.items[0].id"`
- `source: "context.tenant_id"`
- `expr: "@out.text"`

### Dot paths
- Array indexes supported: `input.items[0].id`, `context.matrix[1][0]`
- Escape dotted keys with bracket quotes: `input.user["profile.name"]`
- Inside bracket quotes, only `\\` and quotes (`\"` / `\'`) are allowed
- `[` and `]` are not allowed inside bracket quotes
- Non-array or out-of-range indexes are treated as `missing`

## Expr (v2 pipe)

`expr` is a pipe array or a single start value.

```yaml
expr:
  - "@input.name"
  - trim
  - uppercase
```

Also valid:

```yaml
expr: "@input.name"
```

### Pipe format

A pipe is an array: `[start, step1, step2, ...]`.

Start value can be:
- `@` reference (`@input.*`, `@context.*`, `@out.*`, `@item.*`, `@var`)
- `$` (current pipe value)
- literal string/number/bool/null/object/array

Use `lit:` to force a literal string that would otherwise be treated as a ref or `$`:

```yaml
expr:
  - "lit:@input.name"
  - trim
```

### Steps

- **Op step**: string op name (`trim`) or object form
  - `{ op: "trim", args: [...] }`
  - `{ concat: ["@out.name"] }` (shorthand)
- **Let step**: `{ let: { varName: <expr>, ... } }`
- **If step**: `{ if: <cond>, then: <pipe>, else: <pipe?> }`
- **Map step**: `{ map: [ <step>, <step>, ... ] }`

Example with `let` and `if`:

```yaml
expr:
  - "@input.price"
  - let: { base: "$" }
  - if:
      cond:
        gt: ["@base", 100]
      then:
        - "$"
        - multiply: [0.9]
      else:
        - "$"
```

Example with `map`:

```yaml
expr:
  - "@input.items"
  - map:
    - "@item.value"
    - multiply: [2]
```

## Conditions

Conditions are used in `record_when`, `when`, and `if.cond`.

Supported forms:
- `all: [ <cond>, ... ]`
- `any: [ <cond>, ... ]`
- comparison objects: `eq`, `ne`, `gt`, `gte`, `lt`, `lte`, `match`

Comparison semantics (v2):
- `eq`/`ne` use **strict JSON equality** (type-sensitive). Example: `"1"` != `1`.
- `gt`/`gte`/`lt`/`lte` try numeric comparison first (numbers or numeric strings). If both sides are non-numeric strings, they are compared lexicographically using Rust's default `str` ordering (UTF-8 byte order / Unicode code point order). Otherwise, it is an error.

Examples:

```yaml
record_when:
  all:
    - { gt: ["@input.score", 10] }
    - { eq: ["@input.active", true] }

when:
  match: ["@input.email", ".+@example\\.com$"]
```

## Operations (v2)

Operations are applied as pipe steps. The current pipe value is the implicit first operand.
`args` only list additional arguments.

Support status:
- `runtime`: implemented in v2 runtime

### Operation categories

- String ops: `concat`, `to_string`, `trim`, `lowercase`, `uppercase`, `replace`, `split`, `pad_start`, `pad_end`
- JSON ops: `merge`, `deep_merge`, `get`, `pick`, `omit`, `keys`, `values`, `entries`, `len`, `from_entries`, `object_flatten`, `object_unflatten`
- Array ops: `map`, `filter`, `flat_map`, `flatten`, `take`, `drop`, `slice`, `chunk`, `zip`, `zip_with`, `unzip`, `group_by`, `key_by`, `partition`, `unique`, `distinct_by`, `sort_by`, `find`, `find_index`, `index_of`, `contains`, `sum`, `avg`, `min`, `max`, `reduce`, `fold`, `first`, `last`
- Numeric ops: `+`, `-`, `*`, `/`, `round`, `abs`, `floor`, `ceil`, `trunc`, `sqrt`, `sign`, `mod`, `pow`, `clamp`, `range`, `to_base`, `sum`, `avg`, `min`, `max`
- Date ops: `date_format`, `to_unixtime`
- Logical ops: `and`, `or`, `not`
- Comparison ops: `==`, `!=`, `<`, `<=`, `>`, `>=`, `~=` (aliases: `eq`, `ne`, `lt`, `lte`, `gt`, `gte`, `match`)
- Type casts: `string`, `int`, `float`, `bool`

### Naming conventions

- `to_*`: conversions (e.g., `to_string`, `to_base`, `to_unixtime`)
- `*_by`: key-based variants (`group_by`, `key_by`, `distinct_by`, `sort_by`)
- `object_*`: object-specific structural ops (`object_flatten`, `object_unflatten`)

### Core operations

| op | args | description | support |
| --- | --- | --- | --- |
| `concat` | `>=1` | Concatenate pipe value with args as strings. | `runtime` |
| `coalesce` | `>=1` | Return first non-null value from pipe + args. | `runtime` |
| `to_string` | `0` | Convert pipe value to string. | `runtime` |
| `trim` | `0` | Trim leading/trailing whitespace. | `runtime` |
| `lowercase` | `0` | Lowercase a string. | `runtime` |
| `uppercase` | `0` | Uppercase a string. | `runtime` |
| `replace` | `2-3` | Replace text (`pattern`, `replacement`, `mode?`). | `runtime` |
| `split` | `1` | Split string by delimiter(s). | `runtime` |
| `pad_start` | `1-2` | Pad to target length (`length`, `pad?`). | `runtime` |
| `pad_end` | `1-2` | Pad to target length (`length`, `pad?`). | `runtime` |
| `lookup` | `2-4` | Lookup all matches in an array. | `runtime` |
| `lookup_first` | `2-4` | Lookup first match in an array. | `runtime` |
| `+` | `>=1` | Numeric addition (alias: `add`). | `runtime` |
| `-` | `>=1` | Numeric subtraction (pipe value minus arg). | `runtime` |
| `*` | `>=1` | Numeric multiplication (alias: `multiply`). | `runtime` |
| `/` | `>=1` | Numeric division. | `runtime` |
| `round` | `0-1` | Round a number (`scale` as arg). | `runtime` |
| `abs` | `0` | Return the absolute value. | `runtime` |
| `floor` | `0` | Round down toward negative infinity. | `runtime` |
| `ceil` | `0` | Round up toward positive infinity. | `runtime` |
| `trunc` | `0` | Truncate toward zero. | `runtime` |
| `sqrt` | `0` | Return the square root. Negative input is an error. | `runtime` |
| `sign` | `0` | Return `-1`, `0`, or `1`. | `runtime` |
| `mod` | `1` | Return the Euclidean remainder. Division by zero is an error. | `runtime` |
| `pow` | `1` | Return exponentiation. Non-finite results are errors. | `runtime` |
| `clamp` | `2` | Clamp a value into `min..max`. `min > max` is an error. | `runtime` |
| `range` | `2-3` | Generate an integer sequence (`start`, `end`, `step?`; exclusive `end`). | `runtime` |
| `to_base` | `1` | Convert integer to base-N string (2-36). | `runtime` |
| `date_format` | `1-3` | Reformat date strings. | `runtime` |
| `to_unixtime` | `0-2` | Convert date strings to unix time. | `runtime` |
| `and` | `>=1` | Boolean AND. Prefer `all` conditions. | `runtime` |
| `or` | `>=1` | Boolean OR. Prefer `any` conditions. | `runtime` |
| `not` | `0` | Boolean NOT. | `runtime` |
| `==` | `1` | Equality comparison. Prefer `eq` conditions. | `runtime` |
| `!=` | `1` | Inequality comparison. Prefer `ne` conditions. | `runtime` |
| `<` | `1` | Numeric comparison. Prefer `lt` conditions. | `runtime` |
| `<=` | `1` | Numeric comparison. Prefer `lte` conditions. | `runtime` |
| `>` | `1` | Numeric comparison. Prefer `gt` conditions. | `runtime` |
| `>=` | `1` | Numeric comparison. Prefer `gte` conditions. | `runtime` |
| `~=` | `1` | Regex match. Prefer `match` conditions. | `runtime` |

Use `range` in explicit form, not as pipe-first shorthand. If the current pipe value is a boundary, pass `$` explicitly.

```yaml
expr:
  - "@input.n"
  - sqrt
  - floor
  - { "+": 1 }
  - range: [2, "$"]
```

`range: [start, end, step?]` excludes `end`. When `step` is omitted, it defaults to `1` for ascending ranges and `-1` for descending ranges. If direction and `step` do not match, the result is `[]`. `step: 0` is an error. The default cap is 10,000 emitted items; the CLI can change it with `--limit range-items=...`. Even with `range-items=unlimited`, generated arrays are still bounded by `array-len`.

### JSON operations

Path arguments:
- `pick`/`omit` accept one or more path strings as separate args.
- A single arg may also be an array of strings (e.g., `@context.paths`).

Example:

```yaml
- pick:
  - "name"
  - "price"
```

| op | args | description | support |
| --- | --- | --- | --- |
| `merge` | `>=1` | Shallow merge (rightmost wins). | `runtime` |
| `deep_merge` | `>=1` | Recursive merge for objects; arrays are replaced. | `runtime` |
| `get` | `1` | Get value at path; missing if path is absent. | `runtime` |
| `pick` | `>=1` | Keep only selected paths. | `runtime` |
| `omit` | `>=1` | Remove selected paths. | `runtime` |
| `keys` | `0` | Array of keys. | `runtime` |
| `values` | `0` | Array of values. | `runtime` |
| `entries` | `0` | Array of `{key, value}` entries. | `runtime` |
| `len` | `0` | Length of string/array/object. | `runtime` |
| `from_entries` | `>=1` | Build object from pairs or key/value. | `runtime` |
| `object_flatten` | `1` | Flatten object keys into path strings. | `runtime` |
| `object_unflatten` | `1` | Expand path keys into nested objects. | `runtime` |

### Array operations

Predicate expressions:
- `filter`, `partition`, `find`, `find_index` take a v2 expression that must evaluate to boolean.
- These are not condition objects; use comparison ops (`==`, `!=`, `>`, `>=`, `<`, `<=`, `~=`) with `@item`.
- Comparison op aliases (`eq`, `ne`, `lt`, `lte`, `gt`, `gte`, `match`) are also accepted.
- `missing`/`null` predicate values are treated as false; non-boolean values are an error.

Example:

```yaml
- filter:
  - ["@item", {"!=": null}]
```

Another example (partition):

```yaml
- partition:
  - ["@item.price", {">": 80}]
```

| op | args | description | support |
| --- | --- | --- | --- |
| `map` | `1` | Transform each element (use `map` step). | `runtime` |
| `filter` | `1` | Keep elements matching predicate. | `runtime` |
| `flat_map` | `1` | `map` + `flatten(1)`. | `runtime` |
| `flatten` | `0-1` | Flatten to specified depth. | `runtime` |
| `take` | `1` | Take from head/tail (negative counts from tail). | `runtime` |
| `drop` | `1` | Drop from head/tail (negative counts from tail). | `runtime` |
| `slice` | `1-2` | Slice range (`end` exclusive). | `runtime` |
| `chunk` | `1` | Split into fixed-size chunks. | `runtime` |
| `zip` | `>=1` | Zip to the shortest length. | `runtime` |
| `zip_with` | `>=2` | Combine elements with an expression. | `runtime` |
| `unzip` | `0` | Convert array-of-arrays to column arrays. | `runtime` |
| `group_by` | `1` | Group elements by key. | `runtime` |
| `key_by` | `1` | Map elements by key (last wins). | `runtime` |
| `partition` | `1` | Split into `[matched, unmatched]`. | `runtime` |
| `unique` | `0` | Remove duplicates by equality. | `runtime` |
| `distinct_by` | `1` | Remove duplicates by key. | `runtime` |
| `sort_by` | `1` | Sort by key. | `runtime` |
| `find` | `1` | First matching element. | `runtime` |
| `find_index` | `1` | Index of first match. | `runtime` |
| `index_of` | `1` | Index of first equal element. | `runtime` |
| `contains` | `1` | Whether the value exists. | `runtime` |
| `sum` | `0` | Sum of elements. | `runtime` |
| `avg` | `0` | Average of elements. | `runtime` |
| `min` | `0` | Minimum value. | `runtime` |
| `max` | `0` | Maximum value. | `runtime` |
| `reduce` | `1` | Reduce with accumulator. | `runtime` |
| `fold` | `2` | Reduce with initial value. | `runtime` |
| `first` | `0` | First element. | `runtime` |
| `last` | `0` | Last element. | `runtime` |

### Type casts

| op | args | description | support |
| --- | --- | --- | --- |
| `string` | `0` | Cast pipe value to string. | `runtime` |
| `int` | `0` | Cast pipe value to int. | `runtime` |
| `float` | `0` | Cast pipe value to float. | `runtime` |
| `bool` | `0` | Cast pipe value to bool. | `runtime` |

### Lookup arguments

Explicit `from`:

```yaml
expr:
  - lookup_first:
    - "@context.users"
    - id
    - "@input.user_id"
    - name
```

Implicit `from` (use pipe value as the array):

```yaml
expr:
  - "@context.users"
  - lookup_first:
    - id
    - "@input.user_id"
    - name
```

## Evaluation rules (notes)

### missing vs null
- `missing`: reference does not exist
- `null`: reference exists and is null

### Pipe evaluation
- Pipes run left-to-right.
- `@out.*` can reference previously produced outputs in the same record.

### Map step
- If the pipe value is `missing`, `map` returns `missing`.
- If the pipe value is not an array, `map` raises an error.
- `map` drops `missing` results from the output array.

### Lookup
- `lookup` and `lookup_first` require `from` to be an array.
- `match_key` and optional `get` must be strings.
- `lookup` returns an array of matches; `lookup_first` returns the first match or `missing`.

## Runtime semantics

- `record_when` is evaluated before any mappings; if `false` or error, the record is skipped
- `mappings` are evaluated top to bottom; `@out.*` can only reference previously produced values
- if `source/value/expr` is `missing`, apply `default/required` rules
- `type` casting happens after expression evaluation; failures are errors
- `when` evaluation errors are emitted as warnings

## Preflight validation

`preflight` scans real input to detect runtime errors ahead of time.
Input parsing and mapping evaluation follow the same rules as `transform`.
