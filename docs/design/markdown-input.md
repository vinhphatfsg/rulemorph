# Markdown input 詳細設計

## 目的

`input.format: markdown` は Markdown 文書を Rulemorph の共通入力モデルである JSON record 配列へ正規化する。

Markdown は表、リンク、コードブロックだけでなく、見出し階層、段落、箇条書き、引用、inline 装飾を含む文書構造を持つ。`records: document` はその構造を 1 record の中で追える文書構造の正本とする。`records: sections` と `records: table_rows` は文書構造の正本から使いやすい record 粒度へ切り出す mode として扱う。

## 基本方針

- Markdown parser の内部 AST / event stream は public contract にしない。
- Rulemorph 固有の安定した Markdown Document IR を作り、その IR から JSON records を生成する。
- `records: document` は文書構造を落とさない。`records: sections` と `records: table_rows` は document record からの切り出しで、元の document へ対応づける metadata を持つ。
- `records: document` は default で文書の意味構造を保持する。
- byte-for-byte の Markdown 復元は目的にしない。空白、改行、marker の表記差分など、意味構造ではない source formatting は optional source metadata に閉じ込める。
- `body_text`、`links[]`、`images[]`、`tables[]` などは補助 index とする。正本は `blocks[]` と `sections`。

処理の境界は次の通り。

```text
Markdown text
  -> Markdown parser adapter
  -> Rulemorph Markdown Document IR
  -> records mode projection
  -> Vec<JsonValue>
  -> mappings / steps / finalize
```

Core が扱うのは Markdown の構文構造であり、段落の業務的な意味推定は扱わない。たとえば「これは価格」「これは注意事項」「これはメリット」といった抽出は rule 側、または外部 AI layer の責務にする。

## 非目標

- Markdown から業務意味 JSON を自動推定する。
- parser crate 固有の AST shape を JSON として露出する。
- MDX、Obsidian wikilinks、directives、admonitions、Mermaid、math などを標準 contract に含める。
- raw HTML を parse / sanitize / render / execute する。
- network fetch、画像取得、リンク解決を行う。
- Markdown text の自動型推論を行う。
- CSV / XML / HTML text の既存 normalization semantics を変える。

## Rule schema

`InputFormat` に `Markdown` を追加し、`InputSpec` に `markdown: Option<MarkdownInput>` を追加する。

```yaml
input:
  format: markdown
  markdown: {}
```

`format=markdown` のとき `input.markdown` は必須。ただし空 object は有効で、default options で動作する。

```rust
pub struct MarkdownInput {
    pub flavor: MarkdownFlavor,
    pub frontmatter: MarkdownFrontmatter,
    pub records: MarkdownRecordsMode,
    pub section_levels: Option<Vec<u8>>,
    pub table_header_policy: MarkdownTableHeaderPolicy,
    pub include: MarkdownInclude,
    pub trim_text: bool,
    pub collapse_whitespace: bool,
}
```

### Options

| option | 必須 | 既定 | 説明 |
| --- | --- | --- | --- |
| `flavor` | 任意 | `gfm` | `commonmark` / `gfm`。GFM では table、task list、strikethrough、autolink を有効にする。 |
| `frontmatter` | 任意 | `auto` | `none` / `yaml` / `toml` / `auto`。document 先頭の frontmatter を `frontmatter` object として読む。 |
| `records` | 任意 | `document` | `document` / `sections` / `table_rows`。JSON records の切り出し単位。 |
| `section_levels` | 任意 | `[1,2,3,4,5,6]` | `records=sections` で record 化する heading level。`records=document` では全 heading level を保持する。 |
| `table_header_policy` | 任意 | `strict` | `strict` / `index`。table row projection の `object` key 生成方針。 |
| `include` | 任意 | 下記 | 文書構造と補助 index の出力を制御する。 |
| `trim_text` | 任意 | `true` | text value の前後空白を取り除く。 |
| `collapse_whitespace` | 任意 | `true` | text value の連続空白を 1 space に畳む。 |

`include` の既定値:

| include field | 既定 | 説明 |
| --- | --- | --- |
| `body_text` | `true` | document / section / block の plain text。 |
| `body_markdown` | `false` | 原文 Markdown slice。source span が安定して取れる場合に出す。 |
| `blocks` | `true` | 文書順の block list。 |
| `links` | `true` | 抽出済み link index。 |
| `images` | `true` | 抽出済み image index。 |
| `code_blocks` | `true` | 抽出済み code block index。 |
| `tables` | `true` | 抽出済み table index。 |
| `raw_html` | `true` | raw HTML block / inline HTML index。Markdown 内の記述として文字列保持する。 |
| `sourcepos` | `false` | source position。parser adapter が安定提供できる場合に出す。 |

`records: document` では `sections` と `section_index` は常に出す。`include.blocks=false` は出力サイズを減らすための明示 opt-out であり、default では使わない。`include.blocks=false` の場合でも、section hierarchy と block id references の整合性を保つため、`sections[].heading_block_id` と `sections[].content_block_ids` は残す。

raw HTML は Markdown 文書では説明用・互換用の通常構文として扱う。Rulemorph は raw HTML を parse / sanitize / render / execute せず、Markdown input 由来の文字列として保持する。Rulemorph の出力を後段の Web UI や renderer が HTML として描画する場合、escape / sanitize / `innerHTML` 禁止などの XSS 対策は後段 system の責務である。Rulemorph の default 設定で raw HTML を拒否または削除して、その責務を parser layer に過剰に負わせない。

## Feature flag

Markdown parser dependency は optional feature にする。

```toml
[features]
default = ["excel", "html", "markdown"]
markdown = ["dep:comrak"]
```

`default-features = false` では Markdown parser を含めない。disabled build で `input.format=markdown` が指定された場合は HTML / Excel と同じ fail-closed contract にする。

```text
input format markdown is not enabled in this build
```

この error は `TransformErrorKind::InvalidInput` として返す。

## Parser adapter

`comrak` を第一候補にする。AST があり、heading、list、table、task list、code block、link/image、raw HTML を stable Document IR に写しやすいため。

`pulldown-cmark` などへ差し替える場合でも external JSON contract は Rulemorph Markdown Document IR に固定する。parser adapter は parser-specific node をそのまま露出せず、必ず Rulemorph の block / inline / section 型へ変換する。

Parser options は明示する。

- `commonmark`: CommonMark core syntax。
- `gfm`: table、task list、strikethrough、autolink を有効化。
- raw HTML は parse/render せず、HTML block / inline node として文字列保持する。
- unsafe HTML rendering option は使わない。
- frontmatter は parser 固有の挙動に頼らず、Rulemorph 側で source prefix を切り出して parse する。

## Document IR

内部 IR は parser crate から独立した型にする。

```rust
struct MarkdownDocument {
    frontmatter: JsonMap,
    title: Option<String>,
    body_text: Option<String>,
    body_markdown: Option<String>,
    sections: Vec<MarkdownSection>,
    section_index: Vec<MarkdownSectionSummary>,
    blocks: Vec<MarkdownBlock>,
    links: Vec<MarkdownLink>,
    images: Vec<MarkdownImage>,
    code_blocks: Vec<MarkdownCodeBlock>,
    tables: Vec<MarkdownTable>,
    raw_html: Vec<MarkdownRawHtml>,
}

struct MarkdownSection {
    id: String,
    level: u8,
    heading: String,
    heading_block_id: Option<String>,
    path: Vec<String>,
    ordinal_path: Vec<usize>,
    content_block_ids: Vec<String>,
    child_ids: Vec<String>,
    children: Vec<MarkdownSection>,
}
```

`blocks[]` が文書順序の正本である。`sections` は見出し階層 tree を表し、block object を重複保持せず `heading_block_id` と `content_block_ids` で `blocks[]` を参照する。`section_index[]` は mapping で扱いやすい flat summary とする。

`content_block_ids` はその section の直下にある本文 block を指す。見出し block は `heading_block_id`、子見出し以下の block は `children` から辿る。

`id` は document 内で安定した deterministic id とする。初期案は ordinal path ベースで、`s1`, `s1.2`, `s1.2.1` のように生成する。heading text から slug を作ると duplicate heading や unicode normalization の扱いが増えるため、`id` には slug を使わない。

`title` は最初の level 1 heading を優先する。存在しない場合は frontmatter の `title` が string ならそれを使う。どちらもなければ empty string とする。

### Section tree

`sections` は nested tree として出す。

```json
{
  "id": "s1.1",
  "level": 2,
  "heading": "Install",
  "heading_block_id": "b3",
  "path": ["Guide", "Install"],
  "ordinal_path": [1, 1],
  "content_block_ids": ["b4", "b5"],
  "child_ids": ["s1.1.1"],
  "children": [
    {
      "id": "s1.1.1",
      "level": 3,
      "heading": "macOS",
      "heading_block_id": "b6",
      "path": ["Guide", "Install", "macOS"],
      "ordinal_path": [1, 1, 1],
      "content_block_ids": ["b7"],
      "child_ids": [],
      "children": []
    }
  ]
}
```

`section_index` は同じ section を flat に並べる。

```json
[
  { "id": "s1", "level": 1, "heading": "Guide", "path": ["Guide"], "ordinal_path": [1] },
  { "id": "s1.1", "level": 2, "heading": "Install", "path": ["Guide", "Install"], "ordinal_path": [1, 1] }
]
```

Heading より前に content がある場合は synthetic section を作る。

```json
{
  "id": "preamble",
  "level": 0,
  "heading": "",
  "heading_block_id": null,
  "path": [],
  "ordinal_path": [],
  "content_block_ids": ["b1"],
  "child_ids": [],
  "children": []
}
```

### Blocks

`blocks[]` は document-order の flat list とする。container block は child block object を重複保持せず、`child_block_ids` や `item_ids` で参照する。

共通 field:

| field | 説明 |
| --- | --- |
| `id` | document 内 block id。例: `b1`。 |
| `type` | block type。 |
| `section_id` | 所属 section id。preamble なら `preamble`。 |
| `parent_block_id` | list item / blockquote など container 内 block の parent。top-level は `null`。 |
| `text` | block 全体の plain text。text を持たない block では `""`。 |
| `inlines` | inline structure。inline を持たない block では `[]`。 |

主要 block type:

| type | fields |
| --- | --- |
| `heading` | `level`, `text`, `inlines` |
| `paragraph` | `text`, `inlines` |
| `list` | `ordered`, `start`, `tight`, `item_ids` |
| `list_item` | `ordinal`, `checked`, `child_block_ids` |
| `blockquote` | `child_block_ids`, `text` |
| `code_block` | `language`, `info`, `text` |
| `table` | `table_index`, `alignments`, `header_row`, `rows` |
| `html_block` | `html` |
| `thematic_break` | 追加 field なし |

Ordered list は `ordered: true`、`start`、`list_item.ordinal` を保持する。Unordered list は `ordered: false`、`start: null`、`list_item.ordinal: null` とする。Task list item は `checked: true` / `false`、通常 item は `checked: null` とする。

例:

```json
[
  {
    "id": "b1",
    "type": "list",
    "section_id": "s1",
    "parent_block_id": null,
    "text": "Install Rust Add rulemorph Enable markdown Run tests",
    "ordered": true,
    "start": 1,
    "tight": false,
    "item_ids": ["b2", "b4"]
  },
  {
    "id": "b2",
    "type": "list_item",
    "section_id": "s1",
    "parent_block_id": "b1",
    "text": "Install Rust",
    "ordinal": 1,
    "checked": null,
    "child_block_ids": ["b3"]
  },
  {
    "id": "b3",
    "type": "paragraph",
    "section_id": "s1",
    "parent_block_id": "b2",
    "text": "Install Rust",
    "inlines": [{ "type": "text", "text": "Install Rust" }]
  }
]
```

### Inlines

Inline structure は block 内の `inlines[]` に保持する。`text` だけに潰さない。

| type | fields |
| --- | --- |
| `text` | `text` |
| `soft_break` | 追加 field なし |
| `line_break` | 追加 field なし |
| `code` | `text` |
| `emphasis` | `children` |
| `strong` | `children` |
| `strikethrough` | `children` |
| `link` | `url`, `title`, `children`, `text` |
| `image` | `url`, `title`, `alt`, `children` |
| `html_inline` | `html` |

Nested inline は `children` で表現する。Derived `links[]` / `images[]` は inline と同じ `block_id` を持ち、文書内の対応箇所を追えるようにする。

### Tables

Table block は Markdown table の構造を保持する。`table_rows` projection のためだけに table を平坦化しない。

```json
{
  "id": "b10",
  "type": "table",
  "section_id": "s2",
  "table_index": 0,
  "alignments": ["left", "center", "right"],
  "header_row": {
    "cells": [
      { "column_index": 0, "text": "name", "inlines": [{ "type": "text", "text": "name" }] }
    ]
  },
  "rows": [
    {
      "row_index": 0,
      "cells": [
        { "column_index": 0, "text": "id", "inlines": [{ "type": "text", "text": "id" }] }
      ]
    }
  ]
}
```

Table cell values are strings. 自動型推論はしない。

## Record modes

### `records: document`

文書全体を 1 record にする。これを default とする。document record は文書構造の正本であり、Markdown の見出し、block order、inline structure、list/table structure を追える。

```json
{
  "record_type": "document",
  "frontmatter": {},
  "title": "Guide",
  "body_text": "Guide Install Rulemorph.",
  "sections": [],
  "section_index": [],
  "blocks": [],
  "links": [],
  "images": [],
  "code_blocks": [],
  "tables": [],
  "raw_html": []
}
```

`body_text` は使いやすくするための補助 field であり、文書構造の正本ではない。構造処理では `sections`、`section_index`、`blocks`、`inlines` を使う。

### `records: sections`

見出し単位で 1 record にする。`section_levels` に含まれる heading を record 化する。section record は文書構造の正本から切り出した record であり、対象 section の metadata と block references を持つ。

```json
{
  "record_type": "section",
  "document": {
    "title": "Guide",
    "frontmatter": {}
  },
  "id": "s1.1",
  "level": 2,
  "heading": "Install",
  "path": ["Guide", "Install"],
  "ordinal_path": [1, 1],
  "body_text": "brew install rulemorph",
  "content_block_ids": ["b4", "b5"],
  "blocks": [],
  "children": []
}
```

`records=sections` の `blocks` はその section に属する content block object を含める。`heading_block_id` と `content_block_ids` は document record の block id と同じ id を使う。`section_levels` に含まれない descendant heading は独立 record にせず、直近の selected ancestor の `children` または `blocks` から辿れるようにする。

### `records: table_rows`

Markdown table の各 data row を 1 record にする。table row record は table block から切り出す。

```json
{
  "record_type": "table_row",
  "document": {
    "title": "Guide",
    "frontmatter": {}
  },
  "section": {
    "id": "s2",
    "heading": "Parameters",
    "path": ["API", "Parameters"]
  },
  "table": {
    "block_id": "b10",
    "table_index": 0,
    "alignments": ["left", "left"]
  },
  "row_index": 0,
  "headers": ["name", "type"],
  "cells": [
    { "column_index": 0, "text": "id", "inlines": [{ "type": "text", "text": "id" }] },
    { "column_index": 1, "text": "string", "inlines": [{ "type": "text", "text": "string" }] }
  ],
  "object": {
    "name": "id",
    "type": "string"
  }
}
```

`table_header_policy`:

- `strict`: header text は trim / collapse 後に non-empty かつ unique である必要がある。違反した table は error。`object` を必ず作る。
- `index`: header が空または重複しても `col_0`, `col_1` のような key を使って `object` を作る。

Default は `strict`。曖昧な header を silent に補正しない。

## Frontmatter

Frontmatter は document 先頭にある場合だけ認識する。body の途中にある delimiter は通常 Markdown として扱う。

対応する delimiter:

```text
---
YAML
---

+++
TOML
+++
```

`frontmatter: auto` では delimiter から YAML / TOML を判定する。`frontmatter: yaml` / `toml` では対応 delimiter だけを受け付ける。`frontmatter: none` では frontmatter extraction を行わない。

YAML frontmatter は既存 YAML normalization と同じ安全方針に合わせる。

- duplicate key reject
- non-string key reject
- custom tag reject
- alias / expanded node limits を適用

TOML frontmatter は既存 TOML normalization と同じく TOML datetime を string に正規化する。

Frontmatter root は object である必要がある。scalar / array frontmatter は invalid input。

## Resource limits

既存 limit を必ず適用する。

- `max_input_bytes`
- `max_records`
- `max_depth`
- `max_array_len`
- `max_text_bytes`
- `max_generated_json_nodes`
- `max_generated_json_bytes`

Markdown 専用 limit を追加する。

```rust
pub struct NormalizationOptions {
    pub max_markdown_nodes: usize,
    pub max_markdown_table_cells: usize,
}
```

既定値:

- `max_markdown_nodes = 1_000_000`
- `max_markdown_table_cells = 1_000_000`

`large()` では 10,000,000 まで広げる。

Node count は parser adapter が Document IR を構築するときに加算する。post-parse node limit だけに依存してはいけない。採用 parser が full AST を先に構築する場合は、parse 前の lightweight preflight で block / table delimiter / approximate node budget を確認するか、parse 中に budget を消費できる adapter を使う。`max_input_bytes` は最後の防衛線であり、Markdown 専用 node / table-cell budget の代替にはしない。

`table_rows` は table cell count と record count の両方を確認する。巨大 table で `max_records` を超えた場合は `input exceeds max_records` とする。

## Security invariants

- Markdown parser は network access をしない。
- Link / image destination は文字列として保持するだけで、fetch しない。
- raw HTML は parse / sanitize / render / execute / fetch しない。
- raw HTML は default で Markdown 文書中の文字列として保持する。
- 後段が Rulemorph output を HTML として描画する場合、escape / sanitize / trusted rendering policy は後段 system の責務とする。
- `include.raw_html=false` のとき raw HTML は `raw_html[]` derived index と `html_block` / `html_inline` dedicated node には出さない。ただし `body_markdown=true` の原文 slice には含まれ得る。
- Markdown text に自動型推論を追加しない。
- frontmatter parser の duplicate key / custom tag / non-string key 制約を緩和しない。
- Parser-specific unsafe render option は使わない。
- Resource limit override で parser security invariant を緩和しない。
- `records=table_rows` の header ambiguity は default で fail-closed。

## Validation

`validate_input` に Markdown branch を追加する。

検証項目:

- `input.markdown` が存在する。
- `section_levels` は 1..=6 の unique list。空 list は invalid。
- `section_levels` は `records=sections` にだけ影響する。`records=document` は全 heading level を保持する。
- `frontmatter` / `flavor` / `records` / `table_header_policy` は serde enum で unknown を拒否する。
- raw HTML 入力自体は拒否しない。`include.raw_html=false` では dedicated raw HTML node と derived index を出さないだけで、`body_markdown=true` の原文 slice には含まれ得る。
- `body_markdown=true` かつ parser adapter が source slice を提供できない build では validation error にする。

既存 pattern に合わせるなら `MissingMarkdownSection` error code を追加する。

## Docs

以下を更新する。

- `docs/rules_spec_ja.md`
- `docs/rules_spec_en.md`
- `docs/agent-guides/input-normalization-security.md`

Spec には以下を明記する。

- `markdown` は raw Markdown document を JSON records に正規化する format。
- `document` は文書構造の正本であり、`sections` tree、`section_index`、`blocks`、inline structure、補助 indexes を持つ。
- `sections` / `table_rows` は document record からの切り出し。
- body text と table cell text は string、自動型推論なし。
- frontmatter parser の安全制約。
- raw HTML / links / images は fetch / execute / sanitize / render されない。
- raw HTML を HTML として描画する後段 system は、自身の XSS 対策を持つ必要がある。
- feature disabled build の fail-closed error。

## Tests

Core tests:

- document mode: heading hierarchy、paragraph、ordered/unordered list、nested list、task list、blockquote、link、image、inline code、strong/emphasis/strikethrough、code block、table、raw HTML を含む Markdown が 1 record の文書構造になる。
- document mode: `sections` tree と `section_index` と `blocks[].section_id` が一致する。
- document mode: list block と list item block が `item_ids` / `child_block_ids` で追える。
- document mode: inline structure が `text` に潰れず `inlines[]` に残る。
- sections mode: h1/h2/h3 nesting、preamble、`section_levels` の filter。
- table_rows mode: strict header で row object が作られる。
- table_rows strict duplicate header: invalid input。
- table_rows strict empty header: invalid input。
- table_rows index policy: `col_0` fallback。
- frontmatter YAML: metadata と body 分離。
- frontmatter TOML: metadata と body 分離。
- frontmatter scalar / array: invalid input。
- raw HTML default: string として出るが、parse / sanitize / render / execute / fetch されない。
- raw HTML opt-out: dedicated raw HTML node と derived index は出ない。
- resource limits: markdown nodes / table cells / records / text bytes。
- parser resource budgets: preflight または parse 中 enforcement が post-parse-only でないこと。
- `default-features=false`: `input format markdown is not enabled in this build`。

Fixture tests:

- `markdown_document_structure`
- `markdown_sections_projection`
- `markdown_table_rows_projection`
- `markdown_nested_lists`
- `markdown_inline_structure`
- `markdown_frontmatter`
- `markdown_raw_html`
- `markdown_resource_limits`

Validation tests:

- missing `input.markdown`
- invalid enum value
- invalid `section_levels`
- unsupported `body_markdown` when source slices are unavailable

Feature split verification:

```sh
cargo test -p rulemorph --no-default-features
cargo test -p rulemorph --features markdown --no-default-features
cargo test -p rulemorph
```

Rust 実装を変更する場合は repository guideline に従い `cargo fmt` と `cargo test` を実行する。

## 実装上の確認事項

- 現在の document summary 形状はこの設計と整合しない。`records: document` の出力を文書構造の正本として作り直す。
- `include.blocks` の default は `true` にする。
- `include.body_markdown` と `include.sourcepos` は parser adapter が source slice/source position を安定提供できる場合に有効化する。提供できない場合は validation error にする。
- `records: sections` と `records: table_rows` は document builder の後段で切り出す。
- `body_text`、`links[]`、`images[]`、`code_blocks[]`、`tables[]`、`raw_html[]` は補助 index とし、`blocks[]` と `block_id` で対応づける。
