# Transformation Rules Spec (Implementation-Aligned)

This document describes the current v2 rule spec, references, expression syntax, and evaluation rules.
For the Japanese version, see `docs/rules_spec_ja.md`.

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

## Input

### Common
- `input.format` (required): `csv` or `json`

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
- `records_path` (optional): dot path to a record array. If omitted, use the root value.

```yaml
input:
  format: json
  json:
    records_path: "items"
```

## Output

- Default output is a JSON array of records
- CLI `transform --ndjson` outputs one JSON object per line (streaming)
- If `records_path` points to an object, a single record is produced

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

## Reference

References are `@`-prefixed namespaces + dot paths:
- `@input.*`: input record
- `@context.*`: injected external context
- `@out.*`: output values produced earlier in the same record
- `@item.*`: current element in a `map` step (`@item.index` is the 0-based index)
- `@<var>`: let-bound variable (e.g., `@total`)

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
- Numeric ops: `+`, `-`, `*`, `/`, `round`, `to_base`, `sum`, `avg`, `min`, `max`
- Date ops: `date_format`, `to_unixtime`
- Logical ops: `and`, `or`, `not`
- Comparison ops: `==`, `!=`, `<`, `<=`, `>`, `>=`, `~=`
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

### JSON operations

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
