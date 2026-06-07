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
| Reusable expressions | Name and reuse v2 pipes or mappings | `defs` |
| Provider typed value conversion | Convert between JSON values and DynamoDB / Firestore / MongoDB typed representations | `codecs`, `to_typed_value`, `from_typed_value` |
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
- `defs` (optional): named custom OP definitions built from v2 pipes or mappings
- `codecs` (optional): named profile bindings and field type contracts for provider typed value conversion
- `output` (optional): metadata (e.g., DTO name)
- `record_when` (optional): condition to include/exclude records
- `steps` (optional): ordered execution. Cannot be combined with top-level `mappings` or `record_when`
- `finalize` (optional): post-process the output array. Works with either `mappings` or `steps`

### DTO Type Inference

`generate_dto` first honors explicit `mapping.type` declarations.
When no explicit type is present, it statically infers `string` / `int` / `float` / `bool`, arrays, maps, and nested objects from literal values, terminal v2 pipe operators, and object/array operations.
Dynamic references, v1 expressions, and incompatible unions fall back to each language's JSON fallback type, such as Rust `serde_json::Value` or TypeScript `unknown`.

Inference is bounded for untrusted rules. Excessively deep objects, huge arrays, too many fields, or too many generated types fall back to JSON rather than a narrow generated type.
Huge or dynamic paths used by `get` / `pick` / `omit` also fall back to JSON.
`default` / `coalesce` do not narrow dynamic or unknown input into a concrete type by themselves.

`optional` means a field may be omitted. `nullable` means a present field may contain `null`.
Fields built by the `object` OP are inferred as optional because each field expression may evaluate to `missing`.
JSON integer literals that do not fit in a signed 64-bit integer are treated as JSON fallback types, because Rust / Go / JVM DTO integer outputs use `i64` / `int64` / `Long`.

`defs.*.returns` also participates in DTO inference. Object `returns` become nested DTO shapes. For custom OPs with a `mappings` body and no explicit `returns`, the object shape is synthesized from the body targets. Fields that cannot be narrowed use the language's JSON fallback type.

## defs (custom OPs)

`defs` defines rule-local custom OPs. A custom OP does not run external code or side effects; it names a typed v2 pipe or mappings body.

```yaml
defs:
  slug:
    input: string
    returns: string
    expr:
      - "$"
      - trim
      - lowercase

mappings:
  - target: slug
    expr:
      - "@input.title"
      - slug
```

Each definition requires `input` and exactly one of `expr` or `mappings`. `expr` bodies require `returns`. `mappings` bodies may omit `returns`; in that case the object return contract is synthesized from mapping targets.

### Types

`defs.*.input` and `defs.*.returns` support:

| Type | Meaning |
| --- | --- |
| `string` | JSON string |
| `int` | JSON integer |
| `float` | finite JSON number; integers are accepted |
| `number` | finite JSON number without int/float distinction |
| `bool` | JSON boolean |
| `json` | any JSON value; nested shape is not checked |
| `[T]` | homogeneous array |
| `{ field: T }` | object field map |

Object fields distinguish optional fields from nullable values.

```yaml
input:
  {
    name: string,
    nickname?: string,
    note: string?,
    memo?: string?
  }
```

Canonical field form is also supported.

```yaml
input:
  {
    nickname: { type: string, optional: true },
    note: { type: string, nullable: true },
    memo: { type: string, optional: true, nullable: true }
  }
```

An object containing only `{ type: string }` is not treated as canonical field form; it is an object type with a field named `type`. Canonical form is selected only when `optional` or `nullable` is present.

Direct object input calls use width matching: required fields must exist, and extra fields are allowed. `with` adapter input and object output contracts are exact. A `json` field does not validate nested shape. Numeric contracts do not parse strings, so `"2"` is not accepted as `int`, `float`, or `number`.

### Calls

A custom OP without call options receives the current pipe value `$`.

```yaml
expr:
  - "@input.title"
  - slug
```

Use these call forms depending on the input shape.

| Call form | When to use it | Example |
| --- | --- | --- |
| `- slug` | `input` is primitive, or the current pipe object should be passed as-is | `["@input.title", slug]` |
| `- line_total: [{ with: ... }]` | Adapt caller field names or shape to `defs.*.input`. This is the official form for calls with arguments | `with: { qty: "$.quantity" }` |
| `value` wrapper | Pass a string such as `$.field` as a literal rather than a reference | `{ value: "$.quantity" }` |
| `expr` wrapper | Explicitly mark a value inside an object as an expression | `{ expr: "$.quantity" }` |

When a custom OP call is the first pipe element, there may be no outer current pipe value. If the custom OP input depends on the current pipe, put an explicit start value before the call or pass the required fields with `with`.

When source field names differ from the `input` shape, use the official `with` adapter form:

```yaml
expr:
  - "@input.line"
  - line_total:
      - with: { qty: "$.quantity", unit_price: "$.price" }
```

Inside a custom OP body, prefer dot-path field access with `$.field` in official examples. The `get` OP form, such as `["$", { get: ["field"] }]`, remains valid, but `$.field` is shorter and clearer for simple field access.

```yaml
defs:
  line_total:
    input: { qty: int, unit_price: number }
    returns: number
    expr:
      - "$"
      - let:
          qty: ["$.qty", float]
          price: ["$.unit_price", float]
      - "@qty"
      - "*": ["@price"]
```

`with` values are evaluated as v2 expressions in the caller scope. Use `value` to pass a literal string/object, and `expr` to mark an expression explicitly.

```yaml
- decorate:
    - with:
        label:
          value: "$.quantity"
        qty:
          expr: "$.quantity"
```

Direct adapter objects are invalid.

```yaml
# invalid
- line_total:
    qty: "$.quantity"
```

Inside the body, `$` and `@input` refer to the custom OP input. The outer `@input` is not captured implicitly. `@context` capture, recursion, built-in OP shadowing, imports, generics, and overloads are currently not supported.

Custom OPs can return objects by declaring an object `returns` contract. For `expr` bodies, the returned value must match that object contract.

```yaml
defs:
  public_line:
    input: { sku: string, qty: int, secret?: string }
    returns: { sku: string, qty: int }
    expr:
      - "$"
      - pick: ["sku", "qty"]
```

For objects with computed fields, a `mappings` body is usually clearer. For `mappings` bodies, `returns` may be omitted; the object return contract is synthesized from mapping targets.

```yaml
defs:
  line_summary:
    input: { qty: int, unit_price: number }
    mappings:
      - target: qty
        expr: "$.qty"
      - target: total
        expr: ["$.unit_price", { "*": ["$.qty"] }]
```

### Validation, DTO, And Trace

Validation fails closed for unknown custom OPs, built-in shadowing, invalid identifiers, duplicate `expr` / `mappings`, missing `returns` on `expr` bodies, cycles, unknown or duplicate call options, and `with` shape mismatch. Runtime also checks `input` and `returns`; contract errors do not include raw offending values by default.

DTO generation propagates explicit `returns`. Object `returns` become nested DTO shapes. For mappings bodies without `returns`, the synthesized object contract is propagated; fields that cannot be narrowed use the language's JSON fallback type.

Semantic trace emits custom OP calls as spans with `kind=custom_op`. The span includes `name`, `def_path`, `call_path`, `input_type`, `output_type`, `with_adapter`, and `body_truncated`. Value snapshots follow the existing `TraceValueMode`.
`with` args are evaluated in the caller scope before the body. The body `expr` / `mappings` are expanded as child events under the custom OP span. Body errors remain inside the custom OP span. `MetadataOnly` / `Redacted` modes preserve this span/event structure while suppressing raw values.

## typed value codec

Rulemorph normally works with JSON values. Some providers represent value types with JSON wrappers: DynamoDB AttributeValue, Firestore REST `Value`, and MongoDB Extended JSON. Typed value codecs bridge those shapes.

```text
raw JSON
  -> to_typed_value
  -> provider typed value

provider typed value
  -> from_typed_value
  -> raw JSON
```

| Goal | OP | Example |
| --- | --- | --- |
| Build provider payloads from JSON | `to_typed_value` | `{ "age": 31 }` -> `{ "age": { "N": "31" } }` |
| Decode provider typed payloads back to JSON | `from_typed_value` | `{ "age": { "N": "31" } }` -> `{ "age": "31" }` |

`profile` selects a built-in provider conversion. Use a profile directly for simple cases. Use top-level `codecs` when field type intent should be named and shared.

| profile | Root meaning | Typical use |
| --- | --- | --- |
| `dynamodb_attribute_value` | One DynamoDB AttributeValue | Encode one value as `{ "S": ... }`, `{ "N": ... }`, and so on |
| `dynamodb_item` | DynamoDB Item attribute map | Build an `Item`-like map for put/update payloads |
| `firestore_value` | One Firestore REST `Value` | Encode one value as `stringValue`, `integerValue`, and so on |
| `firestore_fields` | Firestore `Document.fields` | Build only a fields map |
| `firestore_document` | Firestore REST `Document` body | Build a document body with `{ fields: ... }` |
| `mongo_extended_json` | MongoDB Extended JSON v2 | Build JSON containing `$oid`, `$date`, `$numberLong`, and related wrappers |

`to_typed_value: dynamodb_item` is shorthand for `profile: dynamodb_item`. This shorthand converts only what can be inferred from JSON types: `S`, `N`, `BOOL`, `NULL`, `L`, and `M`. It does not infer domain intent such as `SS`, `NS`, `B`, timestamp, or ObjectId; use `field_types` or `codec` for those cases.

### DynamoDB Item example

Input:

```json
{ "id": "u1", "age": "31", "tags": ["admin", "paid"] }
```

Rule:

```yaml
version: 2
input:
  format: json
  json: {}

codecs:
  ddb_user:
    profile: dynamodb_item
    field_types:
      age: number_string
      tags: string_set

mappings:
  - target: Item
    expr:
      - "@input"
      - to_typed_value:
          codec: ddb_user
```

Output shape:

```json
{
  "Item": {
    "id": { "S": "u1" },
    "age": { "N": "31" },
    "tags": { "SS": ["admin", "paid"] }
  }
}
```

`age` is a JSON string, but `number_string` encodes it as DynamoDB `N`. `tags` would otherwise look like a JSON list, so `string_set` is required to encode it as `SS`.

### Decoding a DynamoDB Item

Input:

```json
{
  "id": { "S": "u1" },
  "age": { "N": "31" },
  "tags": { "SS": ["admin", "paid"] }
}
```

Rule:

```yaml
version: 2
input:
  format: json
  json: {}

mappings:
  - target: user
    expr:
      - "@input"
      - from_typed_value:
          profile: dynamodb_item
```

Output shape:

```json
{
  "user": {
    "id": "u1",
    "age": "31",
    "tags": ["admin", "paid"]
  }
}
```

DynamoDB `N` / `NS` are strings on the wire. The default decode policy keeps them as strings for safety. Use `number_policy: parse_json_number_if_safe` only when safe JSON number parsing is desired.

### Firestore document example

```yaml
version: 2
input:
  format: json
  json: {}

mappings:
  - target: document
    expr:
      - "@input"
      - to_typed_value:
          profile: firestore_document
          field_types:
            age: integer
            created_at:
              type: timestamp
              on_missing: ignore
```

Input:

```json
{ "name": "Ada", "age": 31, "created_at": "2026-06-03T00:00:00Z" }
```

Output shape:

```json
{
  "document": {
    "fields": {
      "name": { "stringValue": "Ada" },
      "age": { "integerValue": "31" },
      "created_at": { "timestampValue": "2026-06-03T00:00:00.000000Z" }
    }
  }
}
```

Firestore REST `Value` is a oneof object. Decode rejects payloads with multiple value fields, such as both `stringValue` and `integerValue`. Direct nested arrays are also rejected according to Firestore semantics.

### MongoDB Extended JSON example

```yaml
version: 2
input:
  format: json
  json: {}

mappings:
  - target: document
    expr:
      - "@input"
      - to_typed_value:
          profile: mongo_extended_json
          mode: relaxed
          field_types:
            _id:
              type: object_id
              on_missing: ignore
            created_at:
              type: date
              on_missing: ignore
```

Input:

```json
{ "_id": "0123456789abcdef01234567", "created_at": "2026-06-03T00:00:00Z", "name": "Ada" }
```

Output shape:

```json
{
  "document": {
    "_id": { "$oid": "0123456789abcdef01234567" },
    "created_at": { "$date": "2026-06-03T00:00:00.000Z" },
    "name": "Ada"
  }
}
```

MongoDB Extended JSON mixes ordinary JSON objects and wrappers such as `$date` / `$oid`. To avoid wrapper injection, unhinted wrapper-shaped objects are rejected by default.

### Codec bindings and reverse conversion

Use `codecs` when encode and decode should share provider type intent.

```yaml
version: 2
input:
  format: json
  json: {}

codecs:
  ddb_user:
    profile: dynamodb_item
    field_types:
      age: number_string
      tags: string_set

mappings:
  - target: item
    expr:
      - "@input"
      - to_typed_value:
          codec: ddb_user
  - target: raw_again
    expr:
      - "@out.item"
      - from_typed_value:
          codec: ddb_user
```

`from_typed_value` decodes provider typed values into usable JSON. It is not a strict inverse by default. For example, DynamoDB `{ "N": "31" }` decodes to `"31"` by default; re-encoding that raw value without shared field type intent may produce `S`. Reuse the same `codec` and `field_types` when `N`, `SS`, `B`, and similar provider intent must be preserved.

`decode.mode: json_shape_roundtrip` tries to restore JSON number shapes. For fields such as `number_string`, where a decimal string should be re-encoded as `N`, the default `safe_json` decode is usually the better choice because it keeps the string.

### Common options

| option | Meaning |
| --- | --- |
| `profile` | Built-in profile name. Cannot be combined with `codec` |
| `codec` | Name from top-level `codecs`. Cannot be combined with `profile` |
| `type` | Provider domain type for the current value, such as `object_id`, `timestamp`, or `binary_base64` |
| `field_types` | Provider domain types by logical path. Provider wrapper keys are not included in paths |
| `hints` | Detailed form of `field_types`; prefer `field_types` for normal rules |
| `on_missing` | Root missing behavior: `error`, `propagate`, or `ignore` |
| `decode.mode` | `safe_json` or `json_shape_roundtrip` |
| `number_policy` | Decode number behavior: `string` or `parse_json_number_if_safe` |
| `mode` | MongoDB encode mode: `relaxed` or `canonical` |

`field_types` paths are logical raw paths before and after provider wrappers. For `dynamodb_item`, write `age`, not `M.age` or `Item.age.N`. For Firestore documents, write `age`, not `fields.age`.

Each `field_types` path is required by default. For optional fields, write `{ type: ..., on_missing: ignore }` or `{ type: ..., on_missing: propagate }`.

Common field types:

| field type | Profiles | Meaning |
| --- | --- | --- |
| `number_string` | DynamoDB | Treat a decimal string as `N` |
| `string_set` / `number_set` / `number_string_set` | DynamoDB | Treat an array as `SS` / `NS` |
| `binary_base64` | DynamoDB / MongoDB | Treat a base64 string as binary |
| `integer` | Firestore | Encode as `integerValue` |
| `timestamp` | Firestore | Encode an RFC3339 timestamp as `timestampValue` |
| `bytes_base64` | Firestore | Encode a base64 string as `bytesValue` |
| `object_id` | MongoDB | Encode a 24-hex string as `$oid` |
| `date` | MongoDB | Encode a timestamp string as `$date` |
| `int32` / `int64` / `double` / `decimal128` | MongoDB | Encode as Extended JSON numeric wrappers |

For safety, typed value codecs fail closed on unknown profiles, unknown options, and disabled inline codec options (`style` / `types`). Malformed provider wrappers, empty or duplicate DynamoDB sets, duplicated Firestore oneof fields, and malformed MongoDB wrappers are not silently accepted.

## Input

`input` converts raw input into Rulemorph's common data model: an array of JSON records.
`mappings`, `steps`, and `finalize` run against normalized JSON records, not against the original file format.

### Common
- `input.format` (required): `csv` / `json` / `yaml` / `toml` / `xml` / `html` / `excel` / `markdown`

| format | Record selection | Typical use |
| --- | --- | --- |
| `json` | root or `records_path` | API responses, JSON exports |
| `csv` | one record per row | CSV imports, business data |
| `yaml` | root or `records_path` | config files, YAML exports |
| `toml` | root or `records_path` | dependency and inventory data |
| `xml` | element path via `records_path` | XML feeds, legacy APIs |
| `html` | `records_selector` plus field selectors | extracting tables or lists |
| `excel` | rows from a sheet | `.xlsx` imports |
| `markdown` | whole document, heading sections, or table rows | docs, READMEs, Markdown tables |

### Normalization contract

- Records are JSON objects.
- If `records_path` points to an array, each element becomes a record. Array elements must also be JSON objects.
- If `records_path` points to an object, it becomes a single record.
- Scalars cannot be records.
- A missing reference is `missing`, which is distinct from `null`.
- Format-specific differences are absorbed by the parser layer before mappings, steps, and finalize run.

Parser safety invariants are not optional: duplicate JSON/YAML keys, XML DTD/entity/processing instruction input, HTML JavaScript execution/URL fetching, Excel macro/external relationship/formula evaluation, and Markdown raw HTML rendering/execution/fetching are not allowed. CLI resource limit overrides cannot relax these invariants.

### CSV
- `input.csv` is required when `format=csv`

| option | Required | Default | Description |
| --- | --- | --- | --- |
| `has_header` | Optional | `true` | When `true`, the first row provides field names. When `false`, `columns` provides field names. |
| `delimiter` | Optional | `","` | CSV delimiter. It must be a single-byte character. Multi-byte delimiters such as `"||"` or a full-width comma are invalid. |
| `columns` | Required when `has_header=false` | None | Field definitions for headerless CSV. Each item requires `name`; `type` is optional. |

- Headers and `columns[].name` values must be non-blank and unique.
- With `has_header=true`, a UTF-8 BOM is stripped from the first header only.
- `columns[].type` is accepted, but the current validation and normalization logic does not use it. CSV cells enter JSON records as strings.
- Each data row must have exactly the same field count as the header or `columns` list.

```yaml
input:
  format: csv
  csv:
    has_header: false
    delimiter: ","
    columns:
      - { name: "id" }
      - { name: "price", type: "float" }
```

### JSON
- `input.json` is required when `format=json`

| option | Required | Default | Description |
| --- | --- | --- | --- |
| `records_path` | Optional | Root | Dot path to a record array or single record object. |

- If the root or `records_path` is an array, each element becomes a record. Array elements must also be JSON objects.
- If the root or `records_path` is an object, it becomes a single record.
- Scalars cannot be records.
- `records_path` uses the normal Rulemorph path syntax. A missing path, or a path that points to a scalar, is an error.
- Duplicate keys and non-finite numbers are rejected.

```yaml
input:
  format: json
  json:
    records_path: "items"
```

### YAML / TOML
- `input.yaml` / `input.toml` is required for the matching `format`

| option | Required | Default | Description |
| --- | --- | --- | --- |
| `records_path` | Optional | Root | Dot path to a record array or single record object. |

- YAML/TOML input is normalized to JSON records before mappings, steps, and finalize run.
- If the root or `records_path` is an array, each element becomes a record. Array elements must also be JSON objects.
- `records_path` uses the normal Rulemorph path syntax. A missing path, or a path that points to a scalar, is an error.
- A YAML stream must contain exactly one document. Duplicate keys, non-string mapping keys, and custom tags are rejected.
- YAML aliases/anchors can be expanded, but alias count and expanded node count are bounded by resource limits.
- TOML datetimes are normalized to strings. TOML tables, arrays of tables, and inline tables are normalized to JSON objects and arrays.

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

| option | Required | Default | Description |
| --- | --- | --- | --- |
| `records_path` | Required | None | Dot-separated element path including the root element. `[]` indexes are not supported. |
| `attr_prefix` | Optional | `"@"` | Prefix for attribute field names. Must not be empty. |
| `text_key` | Optional | `"#text"` | Field name for direct text. Must not be empty or equal to `attr_prefix`. |
| `child_policy` | Optional | `array` | Child element storage policy. The current implementation only supports `array`, so child elements are always arrays. |
| `trim_text` | Optional | `true` | Trims leading and trailing whitespace from direct text and CDATA. |
| `collapse_whitespace` | Optional | `true` | Collapses consecutive whitespace in direct text and CDATA to one space. |
| `namespaces` | Optional | `qualified` | `qualified` keeps prefixed names visible. `strip` uses local names only. |

- Attributes become string fields named `attr_prefix + attribute_name`.
- Direct text and CDATA are stored under `text_key` when non-empty after normalization.
- Child elements become array fields grouped by name. A single child is still an array.
- In mixed content, child element text stays on the child; direct text on the record element goes to `text_key`.
- `namespaces: strip` errors when stripped local names collide. Attribute, text, and child key collisions also error.
- XML input must be a single-root document. DTDs, processing instructions, and non-blank text outside the root are rejected.

```yaml
input:
  format: xml
  xml:
    records_path: "users.user"
    attr_prefix: "@"
    text_key: "#text"
    child_policy: array
    namespaces: strip
```

### HTML
- `input.html` is required when `format=html`

| option | Required | Default | Description |
| --- | --- | --- | --- |
| `records_selector` | Required | None | CSS selector for record elements. Must not be empty. |
| `fields` | Required | None | Map from output field name to field extraction config. Must not be empty. |
| `trim_text` | Optional | `true` | Trims leading and trailing whitespace for `value: text` and `attr`. |
| `collapse_whitespace` | Optional | `true` | Collapses consecutive whitespace to one space for `value: text` and `attr`. |

`fields.<name>`:

| option | Required | Default | Description |
| --- | --- | --- | --- |
| `selector` | Optional | The record element itself | CSS selector relative to the record element. If omitted, the record element itself is used. |
| `value` | Optional | `text` | `text` / `html` / `attr`. |
| `attr` | Required when `value=attr` | None | Attribute name to extract. Must not be empty. |
| `multiple` | Optional | `false` | When `true`, returns an array of all matched values. |

- Field names must not be empty.
- With `multiple=false`, only the first matched element is used. If no element matches, the field is `missing`.
- With `multiple=true`, only matched values are included in the array. If nothing matches, the field is an empty array. For `value=attr`, elements without the attribute are skipped.
- `value: html` returns raw inner HTML. It does not trim or collapse whitespace, sanitize, or execute content.
- The HTML parser does not execute JavaScript or fetch URLs.

```yaml
input:
  format: html
  html:
    records_selector: "table#users tbody tr"
    fields:
      id: { selector: "td:nth-child(1)", value: text }
      name: { selector: "td:nth-child(2)", value: text }
      profile_url: { selector: "a.profile", value: attr, attr: href }
      tags: { selector: ".tag", value: text, multiple: true }
```

### Markdown
- `input.markdown` is required when `format=markdown`.
- An empty object `{}` is valid. It uses `records: document`, `flavor: gfm`, and `frontmatter: auto`.
- There is no rule-file-free shorthand for Markdown input. Use a rule file with `rulemorph transform -r rules.yaml -i input.md`.

```yaml
input:
  format: markdown
  markdown: {}
mappings:
  - target: "title"
    source: "input.title"
  - target: "body"
    source: "input.body_text"
  - target: "owner"
    source: "input.frontmatter.owner"
```

| option | Required | Default | Description |
| --- | --- | --- | --- |
| `flavor` | Optional | `gfm` | `commonmark` / `gfm`. GFM enables tables, task lists, strikethrough, and autolinks. |
| `frontmatter` | Optional | `auto` | `none` / `yaml` / `toml` / `auto`. Leading frontmatter becomes the `frontmatter` object. |
| `records` | Optional | `document` | `document` / `sections` / `table_rows`. Selects the record unit. |
| `section_levels` | Optional | `[1,2,3,4,5,6]` | Heading levels emitted by `records=sections`. `records=document` keeps every heading level. |
| `table_header_policy` | Optional | `strict` | `strict` / `index`. `strict` rejects empty or duplicate headers; `index` uses `col_0`, `col_1`. |
| `include.body_text` | Optional | `true` | Emits plain text as `body_text`. |
| `include.body_markdown` | Optional | `false` | Setting this to `true` is currently an error. |
| `include.blocks` | Optional | `true` | Emits the document-order block list as `blocks[]`. |
| `include.links` | Optional | `true` | Emits the extracted link index as `links[]`. |
| `include.images` | Optional | `true` | Emits the extracted image index as `images[]`. |
| `include.code_blocks` | Optional | `true` | Emits the extracted code block index as `code_blocks[]`. |
| `include.tables` | Optional | `true` | Emits the extracted table index as `tables[]`. |
| `include.raw_html` | Optional | `true` | Preserves raw HTML blocks and inline HTML as strings. |
| `include.sourcepos` | Optional | `false` | Setting this to `true` is currently an error. |
| `trim_text` | Optional | `true` | Trims extracted text. |
| `collapse_whitespace` | Optional | `true` | Collapses consecutive whitespace in extracted text to one space. |

`records: document` emits one record for the whole Markdown document. The document record is the source-of-truth Markdown structure: it keeps heading hierarchy, block order, inline structure, list structure, and table structure. Stable fields include `record_type: "document"`, `frontmatter`, `title`, `body_text`, `sections`, `section_index`, `blocks`, `links`, `images`, `code_blocks`, `tables`, and `raw_html`.

```json
[
  {
    "record_type": "document",
    "frontmatter": { "owner": "docs" },
    "title": "Guide",
    "body_text": "Guide Install Rulemorph.",
    "sections": [
      {
        "id": "s1-1",
        "level": 1,
        "heading": "Guide",
        "heading_block_id": "b1",
        "path": ["Guide"],
        "ordinal_path": [1],
        "content_block_ids": ["b2"],
        "child_ids": [],
        "children": []
      }
    ],
    "section_index": [
      { "id": "s1-1", "level": 1, "heading": "Guide", "path": ["Guide"], "ordinal_path": [1] }
    ],
    "blocks": [
      {
        "id": "b1",
        "type": "heading",
        "section_id": "s1-1",
        "parent_block_id": null,
        "level": 1,
        "text": "Guide",
        "inlines": [{ "type": "text", "text": "Guide" }]
      },
      {
        "id": "b2",
        "type": "paragraph",
        "section_id": "s1-1",
        "parent_block_id": null,
        "text": "Install Rulemorph.",
        "inlines": [{ "type": "text", "text": "Install Rulemorph." }]
      }
    ]
  }
]
```

`sections` is a nested tree, and `section_index` is a flat index. `blocks[]` is the document-order source of truth. Use `sections[].heading_block_id`, `sections[].content_block_ids`, and `blocks[].section_id` to connect sections and blocks. `body_text` is a convenience field; structure-aware rules should use `sections`, `section_index`, `blocks`, and `inlines`.

`content_block_ids` points to content blocks directly under that section. The heading block is referenced by `heading_block_id`, and blocks under child headings are reachable through `children`. In `records: sections`, each section record's `blocks[]` includes the section's direct blocks, nested container child blocks referenced by `item_ids` / `child_block_ids`, and descendant section heading and content blocks in document order. The section's own heading block is referenced by `heading_block_id`.

The main `blocks[]` `type` values are `heading`, `paragraph`, `list`, `list_item`, `blockquote`, `code_block`, `table`, `html_block`, and `thematic_break`. Ordered lists keep `ordered: true`, `start`, and `list_item.ordinal`. Task list items use `checked: true` / `false`; ordinary items use `checked: null`.

Inline structure is kept in `inlines[]`. Main inline `type` values are `text`, `soft_break`, `line_break`, `code`, `emphasis`, `strong`, `strikethrough`, `link`, `image`, and `html_inline`. `link`, `image`, and emphasis-family nodes keep nested inline content in `children`.

`records: sections` projects section records from the document record. With `section_levels: [2]`, each `##` record contains `record_type: "section"`, `document`, `id`, `level`, `heading`, `path`, `ordinal_path`, `body_text`, `heading_block_id`, `content_block_ids`, `blocks`, and `children`. Use mapping paths such as `input.heading`, `input.path`, `input.body_text`, and `input.blocks`.

`records: table_rows` projects Markdown table data rows from table blocks. Fields are `record_type: "table_row"`, `document`, `section`, `table`, `row_index`, `headers`, `cells`, and `object`. In `strict` mode, header text becomes the `object` key and empty or duplicate headers are errors. In `index` mode, use paths such as `input.object.col_0` and `input.object.col_1`.

Frontmatter is recognized only at the start of the document with `---` for YAML or `+++` for TOML. `auto` chooses YAML/TOML only when the opening delimiter has a matching closing delimiter. In `auto`, leading `---` / `+++` without a closing delimiter remains ordinary Markdown body text instead of frontmatter. With `frontmatter: yaml` / `toml`, an opening delimiter without a closing delimiter is an error. The frontmatter root must be an object. YAML duplicate keys, non-string keys, and custom tags are rejected. TOML datetimes are normalized to strings during JSON conversion.

Raw HTML is treated as ordinary Markdown source text. Rulemorph does not render it as HTML, sanitize it, fetch network resources, or execute JavaScript. If a downstream Web UI renders Rulemorph output as HTML, escaping, sanitization, and avoiding unsafe `innerHTML` usage are downstream responsibilities.

### Excel
- `input.excel` is required when `format=excel`

| option | Required | Default | Description |
| --- | --- | --- | --- |
| `sheet` | Optional | First sheet | Sheet name string, or 0-based sheet index. |
| `has_header` | Optional | `true` | When `true`, cells from `header_row` provide field names. When `false`, `columns` provides field names and selected columns. |
| `header_row` | Optional | `1` | Header row number, 1-based. Used when `has_header=true`. |
| `data_start_row` | Optional | With headers, `header_row + 1`; without headers, the `range` start row or row 1 | Data row start number, 1-based. |
| `range` | Optional | Effective sheet range | Cell window to read, in `A:D` or `A1:D100` form. |
| `columns` | Required when `has_header=false` | None | Headerless Excel field definitions. Each item requires `name` and `column`. |
| `empty_cell` | Optional | `missing` | The current implementation only supports `missing`; empty cells are omitted from the record. |
| `formula` | Optional | `cached` | `cached` / `formula` / `error`. Formula cells are never evaluated. |
| `date` | Optional | `iso8601` | `iso8601` / `serial` / `string`. Controls Excel datetime cell normalization. |
| `cell_error` | Optional | `error` | The current implementation only supports `error`; Excel error cells are rejected. |

- `range` columns use Excel column letters. `A:D` limits columns only; `B3:F100` limits both rows and columns.
- `header_row` cannot be before the selected range. With `range: "B3:F100"` and headers, usually set `header_row: 3`.
- With `has_header=true`, selected header cells become field names. Header names must be non-blank and unique.
- With `has_header=false`, `columns[].column` is an Excel column letter. `columns[].name` must be non-blank and unique.
- Empty cells are treated as `missing`. Empty strings are real values and remain distinct from `missing`.
- Formula cells are never evaluated. `formula: cached` reads cached values, and formula cells without cached values are errors. `formula: formula` reads the formula string. `formula: error` rejects formula cells.
- `date: iso8601` and `date: string` emit `YYYY-MM-DDTHH:MM:SS` strings. `date: serial` emits the Excel serial number as a JSON number.
- Only `.xlsx` is supported. Macros, external relationships, shared formula metadata, and ambiguous workbook structure are rejected.

```yaml
input:
  format: excel
  excel:
    sheet: "Users"
    has_header: true
    header_row: 1
```

```yaml
input:
  format: excel
  excel:
    sheet: 0
    has_header: true
    header_row: 3
    data_start_row: 4
    range: "B3:F100"
```

```yaml
input:
  format: excel
  excel:
    sheet: "Users"
    has_header: false
    data_start_row: 2
    columns:
      - { name: "id", column: "A" }
      - { name: "name", column: "B" }
```

## Output

- Default output is a JSON array of records
- CLI `transform --ndjson` outputs one JSON object per line (streaming)
- If `records_path` points to an object, a single record is produced

## CLI input

`rulemorph transform` accepts an input file with `-i/--input`. If `-i` is omitted and stdin is piped, stdin is used as the input. Use `-i -` to request stdin explicitly.

```sh
rulemorph transform -r rules.yaml -i input.json
cat input.json | rulemorph transform -r rules.yaml
cat input.json | rulemorph transform -r rules.yaml -i -
```

For a one-off expression without a rule file, use direct mode. The canonical option is `--rule`; the jq-like `-rule` and `-rule=...` aliases are also accepted.

```sh
echo '{ "test": 1 }' | rulemorph --rule '@input.test'
echo '{ "test": 1 }' | rulemorph -rule '@input.test'
echo '{ "test": 1 }' | rulemorph -rule=@input.test
```

Direct mode uses the normal v2 `expr` syntax. Use a pipe array when chaining operators.

```sh
echo '{ "a": 1, "b": 2 }' | rulemorph --rule '["@input.a", {"+": ["@input.b"]}]'
```

Direct mode resolves the input format from `-f/--format`, then the `-i` extension, then the first stdin token. Stdin is JSON when the first byte after UTF-8 BOM and ASCII whitespace is `{` or `[`; otherwise it is CSV. Unknown or missing `-i` extensions stay JSON for compatibility. If CSV data starts with `{` or `[`, pass `-f csv`. For Markdown, use a rule file with `input.format: markdown` and `transform -i input.md`.

For CSV direct input, a headered `.csv` file can be referenced by field name. Headerless CSV can either infer numeric fields from references such as `@input.0`, or receive field names with `-H/--headers`. `-h` remains the help option, so the short headers option is `-H`.

```sh
echo 'a,test,1' | rulemorph -rule '@input.0'
echo 'a,test,1' | rulemorph -H 'id,name,age' -rule '@input.id'
rulemorph -H 'id,name,age' -rule '@input.id' -i non_header.csv
rulemorph -rule '@input.id' -i with_header.csv
```

Direct mode can use the same pipe operators as normal `expr`.

```sh
echo 'a,test,1' | rulemorph -rule '["@input.0", {"concat": ["-", "@input.1"]}]'
# => "a-test"

echo 'u1, Alice ,42' | rulemorph -H 'id,name,age' -rule '["@input.name", "trim", "uppercase"]'
# => "ALICE"
```

For multiple output fields in direct mode, repeat `-F/--field <TARGET=EXPR>` or pass `--output-map <JSON_OBJECT>`. Both are sugar for normal `mappings`: the target is the output path, and the expr is the same v2 `expr` value used elsewhere. `--rule`, `-F/--field`, and `--output-map` are mutually exclusive.

```sh
echo 'u1,Alice,42' | rulemorph -H 'id,name,age' \
  -F id='@input.id' \
  -F name='["@input.name","uppercase"]' \
  -F kind='lit:user'
# => {"id":"u1","kind":"user","name":"ALICE"}

echo 'u1,Alice,42' | rulemorph -H 'id,name,age' \
  --output-map '{"user.id":"@input.id","user.name":["@input.name","uppercase"],"kind":"lit:user"}'
# => {"kind":"user","user":{"id":"u1","name":"ALICE"}}
```

When processing multiple records, the default output is a JSON array.

```sh
echo '[{"id":"u1"},{"id":"u2"}]' | rulemorph --rule '@input.id'
# => ["u1","u2"]

printf 'u1,Alice,42\nu2,Bob,7\n' | rulemorph -H 'id,name,age' \
  --output-map '{"id":"@input.id","age":["@input.age","int"]}'
# => [{"age":42,"id":"u1"},{"age":7,"id":"u2"}]
```

Use `--ndjson` when each record should be emitted as one line. `--rule` emits the direct value, while `-F/--field` and `--output-map` emit one object per line.

```sh
echo '[{"id":"u1"},{"id":"u2"}]' | rulemorph --ndjson --rule '@input.id'
# => "u1"
# => "u2"

printf 'u1,Alice,42\nu2,Bob,7\n' | rulemorph --ndjson -H 'id,name,age' \
  --output-map '{"id":"@input.id","age":["@input.age","int"]}'
# => {"age":42,"id":"u1"}
# => {"age":7,"id":"u2"}
```

`-F/--field` preserves CLI argument order as mapping order, so a later field can reference an earlier field through `@out.*`. `--output-map` does not assign meaning to JSON object key order, so evaluated `@out.*` references are rejected there. `--output-map` is not a recursive template. The key is the target and the value is the expr. Therefore `--output-map '{"user":{"id":"@input.id"}}'` assigns an object literal expr to the `user` field; it does not create a mapping for the `user.id` target.

Direct mode also accepts `-c/--context <JSON_FILE>`. `--rule`, `-F/--field`, and `--output-map` expressions can reference `@context.*`.

```sh
echo '{"tenant_id":"t1"}' > context.json
echo 'u1,Alice' | rulemorph -H 'id,name' -c context.json \
  -F id='@input.id' \
  -F tenant='@context.tenant_id'
# => {"id":"u1","tenant":"t1"}
```

Excel direct input is selected for `.xlsx` files or with `-f excel`. Excel requires `--excel-header-row` and `--excel-data-range`, where `--excel-data-range` is the data range and does not include the header row. Use `--excel-sheet <NAME>` or `--excel-sheet-index <INDEX>` to select a sheet; they cannot be used together.

```sh
rulemorph -rule '@input.id' -i users.xlsx --excel-header-row 1 --excel-data-range A2:D2
rulemorph -rule '@input.id' -i users.xlsx --excel-header-row 1 --excel-data-range A2:D3
rulemorph -rule '["@input.score", {"+": [7.5]}, "round"]' -i users.xlsx --excel-header-row 1 --excel-data-range A2:D2
# => 50
```

For `--rule`, CSV / Excel direct convenience mode outputs `[]` for zero records, the direct value for one record, and an array of direct values for multiple records. For `-F/--field` and `--output-map`, it outputs an object for one record and an object array for multiple records. With `--ndjson`, direct mode emits one JSON line per record and does not wrap records in an array. For compatibility, explicit `-f csv` without the new tabular options keeps the legacy array output even for one record. `--limit`, `--limits-profile`, and `--limits-file` apply the same resource limits as `transform`. `--rule` / `-F/--field` / `--output-map` cannot be used with a subcommand, and direct-mode top-level options placed before a subcommand are rejected.

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

The `object` OP can generate JSON objects from a rule, so it has dedicated limits as well. Defaults are `object-fields=10000`, `object-key-bytes=4096`, `object-depth=64`, `generated-json-nodes=100000`, and `generated-json-bytes=10485760`. Override them with names such as `--limit object-fields=...` or `--limit generated-json-bytes=...`; `--limits-file` uses the same names. `array-len` remains the array-generation cap and is not reused as the object safety boundary.

Markdown parsing is also bounded by `markdown-nodes` and `markdown-table-cells`. Both default to 1,000,000 and become 10,000,000 with `--limits-profile large`. Use `--limit markdown-nodes=2000000 --limit markdown-table-cells=2000000`, or `markdown-nodes = 2000000` and `markdown-table-cells = 2000000` in a limits file. `max_records` limits emitted records and is separate from Markdown AST node and table cell limits.

These options only increase processing limits. Safety invariants such as duplicate key rejection, XML DTD/entity rejection, HTML no-network/no-JS behavior, Excel no-macro/no-formula-evaluation behavior, Markdown raw HTML no-render/no-fetch/no-execute behavior, and MCP pathless branch guard are not configurable.

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
- JSON ops: `object`, `merge`, `deep_merge`, `get`, `pick`, `omit`, `keys`, `values`, `entries`, `len`, `from_entries`, `object_flatten`, `object_unflatten`
- Array ops: `map`, `filter`, `flat_map`, `flatten`, `take`, `drop`, `slice`, `chunk`, `zip`, `zip_with`, `unzip`, `group_by`, `key_by`, `partition`, `unique`, `distinct_by`, `sort_by`, `find`, `find_index`, `index_of`, `contains`, `sum`, `avg`, `min`, `max`, `reduce`, `fold`, `first`, `last`
- Numeric ops: `+` / `add`, `-` / `subtract`, `*` / `multiply`, `/` / `divide`, `round`, `abs`, `floor`, `ceil`, `trunc`, `sqrt`, `sign`, `mod`, `pow`, `clamp`, `range`, `to_base`, `sum`, `avg`, `min`, `max`
- Date ops: `date_format`, `to_unixtime`
- Provider typed value ops: `to_typed_value`, `from_typed_value`
- Logical ops: `and`, `or`, `not`
- Comparison ops: `==`, `!=`, `<`, `<=`, `>`, `>=`, `~=` (aliases: `eq`, `ne`, `lt`, `lte`, `gt`, `gte`, `match`)
- Type casts: `string`, `int`, `float`, `bool`

### Naming conventions

- `to_*`: conversions (e.g., `to_string`, `to_base`, `to_unixtime`)
- `*_by`: key-based variants (`group_by`, `key_by`, `distinct_by`, `sort_by`)
- `object`: builder that creates objects from v2 expressions
- `object_*`: object-specific structural ops (`object_flatten`, `object_unflatten`)

### Core operations

Most operators receive the current pipe value as their implicit first argument. Operators without additional arguments can be written as strings. Operators with arguments use object form.

```yaml
expr:
  - "@input.name"
  - trim
  - lowercase
  - replace: [" ", "-", "all"]
```

Numeric operators use the same pipe style. `range` is different: it generates an array rather than transforming the current value, so use explicit form instead of pipe-first shorthand.

| Goal | Expression | Result |
| --- | --- | --- |
| Absolute value | `[-7.5, abs]` | `7.5` |
| Square-root boundary | `[81, sqrt, floor]` | `9` |
| Exponentiation | `[2, { pow: 8 }]` | `256` |
| Euclidean remainder | `[-5, { mod: 3 }]` | `1` |
| Clamp into bounds | `["@input.score", { clamp: [0, 100] }]` | a value in `0..100` |
| Ascending range | `[{ range: [2, 8] }]` | `[2,3,4,5,6,7]` |
| Descending range | `[{ range: [8, 2] }]` | `[8,7,6,5,4,3]` |

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
| `+` / `add` | `>=1` | Numeric addition. | `runtime` |
| `-` / `subtract` | `>=1` | Numeric subtraction (pipe value minus arg). | `runtime` |
| `*` / `multiply` | `>=1` | Numeric multiplication. | `runtime` |
| `/` / `divide` | `>=1` | Numeric division. | `runtime` |
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
| `to_typed_value` | `1` | Convert JSON values into DynamoDB / Firestore / MongoDB provider typed values. | `runtime` |
| `from_typed_value` | `1` | Decode provider typed values into JSON values. | `runtime` |
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
| `eq` / `ne` / `lt` / `lte` / `gt` / `gte` / `match` | `1` | Aliases for the comparison ops above. | `runtime` |

Use `range` in explicit form, not as pipe-first shorthand. If the current pipe value is a boundary, pass `$` explicitly.

```yaml
expr:
  - "@input.n"
  - sqrt
  - floor
  - { "+": 1 }
  - range: [2, "$"]
```

`range: [start, end, step?]` excludes `end`. `start`, `end`, and `step` must be integers. When `step` is omitted, it defaults to `1` for ascending ranges and `-1` for descending ranges. If `start == end`, or if direction and `step` do not match, the result is `[]`. `step: 0` is an error. The default cap is 10,000 emitted items; the CLI can change it with `--limit range-items=...`. Even with `range-items=unlimited`, generated arrays are still bounded by `array-len` (default 1,000,000).

### JSON operations

`object` evaluates multiple v2 expressions and builds one JSON object.
When a field value evaluates to `missing`, that field is omitted. `null` remains `null`.
Keys are literal field names; `user.name` is one key named `"user.name"`, not a nested path.
Use nested `object` when nested output is needed.

```yaml
expr:
  - "@input"
  - object:
      name: ["$.name", uppercase]
      age: ["$.age", int]
      tags:
        value: ["new", "vip"]
      profile:
        - object:
            label: ["$.name", lowercase]
      missing: "$.missing"
```

In this example, the current pipe value before `object` is `@input`, so `$` inside field expressions points at the input record.
Use the `value` wrapper for literal array field values. Use the `expr` wrapper when an object-shaped field value must be interpreted as an expression.
When piping the `object` output to another OP in a multi-step pipe, put an explicit start value as shown above. To return only the object, use a single-step pipe such as `expr: [{ object: { id: "@input.id" } }]`.

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
| `object` | `1` | Build a JSON object from a field map of v2 expressions. Omit `missing` fields. | `runtime` |
| `merge` | `>=1` | Shallow merge (rightmost wins). | `runtime` |
| `deep_merge` | `>=1` | Recursive merge for objects; arrays are replaced. | `runtime` |
| `get` | `1` | Get value at path; missing if path is absent. | `runtime` |
| `pick` | `>=1` | Keep only selected paths. | `runtime` |
| `omit` | `>=1` | Remove selected paths. | `runtime` |
| `keys` | `0` | Array of keys. | `runtime` |
| `values` | `0` | Array of values. | `runtime` |
| `entries` | `0` | Array of `{key, value}` entries. | `runtime` |
| `len` | `0` | Length of string/array/object. | `runtime` |
| `from_entries` | `0-1` | Build object from pipe pairs, or from pipe key plus a `value` arg. | `runtime` |
| `object_flatten` | `0` | Flatten pipe object keys into path strings. | `runtime` |
| `object_unflatten` | `0` | Expand pipe path keys into nested objects. | `runtime` |

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
| `sort_by` | `1-2` | Sort by key. The second arg is `asc` / `desc`; default is `asc`. | `runtime` |
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
