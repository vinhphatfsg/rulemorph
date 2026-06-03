# Rulemorph Core Performance

この文書は `crates/rulemorph` 本体の性能測定だけを扱います。CLI、MCP、server、UI の性能は対象外です。

## ローカル測定

```bash
PERF_TARGET="$(mktemp -d /tmp/rulemorph-perf-target.XXXXXX)"
CARGO_TARGET_DIR="${PERF_TARGET}" cargo bench -p rulemorph --bench transform_bench
CARGO_TARGET_DIR="${PERF_TARGET}" cargo bench -p rulemorph --bench parse_bench
CARGO_TARGET_DIR="${PERF_TARGET}" cargo bench -p rulemorph --bench normalize_bench
CARGO_TARGET_DIR="${PERF_TARGET}" cargo bench -p rulemorph --bench exec_mode_bench
CARGO_TARGET_DIR="${PERF_TARGET}" cargo bench -p rulemorph --bench trace_bench
CARGO_TARGET_DIR="${PERF_TARGET}" cargo bench -p rulemorph --bench v2_bench
python3 scripts/perf_report.py --criterion-dir "${PERF_TARGET}/criterion" --json-output crates/rulemorph/PERF.json > crates/rulemorph/PERF.md
```

PR や作業ブランチで committed snapshot と比較する場合:

```bash
python3 scripts/perf_report.py --criterion-dir target/criterion --baseline-json crates/rulemorph/PERF.json --json-output perf-report.json > perf-report.md
```

## CI 測定モード

`pull_request` では wall-clock 全体を測らず、短い Criterion canary だけを実行します。対象は `transform/batch/simple/records_5k` と `trace/simple/trace_off` です。GitHub runner の wall-clock は揺れるため、この結果は advisory とし、Markdown summary では未実行 benchmark の `missing` 行を表示しません。artifact の `perf-report.json` には `missing_from_current` と `hidden_missing_count` を残します。

`workflow_dispatch` と nightly schedule では全 core benchmark target を実行します。full run では `missing` が 0 になることを期待し、baseline 更新や全体傾向の確認に使います。

CI は Criterion 実行前に `target/criterion` を削除します。Rust cache から古い Criterion 結果が復元されても、今回実行していない benchmark が report に混ざらないようにするためです。

## 指標

- `records/sec`: record evaluator や batch transform の主指標。
- `MB/sec`: normalization の主指標。
- `mean ns/iter`: Criterion が直接測る反復単位の wall-clock。`crates/rulemorph/PERF.md` の delta 判定に使う。
- `ns/record`: レポート列には出さず、必要なときに `mean ns/iter` を record 数で割って傾きを確認する補助指標。
- allocation canary: PR で fail してよい安定指標。
- Criterion wall-clock: `crates/rulemorph/PERF.json` との delta と artifact で確認する advisory 指標。

## 判定ラベル

- `improvement`: baseline 比で mean ns/iter が 5% 以上改善した。
- `ok`: baseline 比の変化が +10% 未満。
- `warn`: baseline 比で +10% 以上遅くなった。PR 説明に理由を書く。
- `regression`: baseline 比で +20% 以上遅くなった。CI は fail しないが、意図した劣化でなければ修正する。
- `new`: baseline に存在しない benchmark。新規追加時は `crates/rulemorph/PERF.json` を更新する。
- `missing`: baseline には存在するが、今回実行されなかった benchmark。PR short canary の Markdown では表示せず、JSON artifact に残す。full baseline 更新時には空にする。

## ゲート方針

wall-clock の Criterion regression は GitHub runner で揺れるため、単独では fail しません。fail してよいのは `cargo test -p rulemorph --test perf_allocation -- --test-threads=1` のような deterministic canary です。`iai-callgrind` を導入するまでは allocation canary を blocking gate とし、Criterion delta は advisory として扱います。

allocation canary の閾値は初回実測値から決めます。初期値は `cargo test -p rulemorph --test perf_allocation -- --test-threads=1 --nocapture` の観測値の 1.25 倍以上にし、複数回の CI 実行で安定してからだけ tighten します。

## CI セキュリティ

performance CI は PR の code を `cargo test` / `cargo bench` で実行するため、`pull_request_target` ではなく `pull_request` だけを使います。workflow 権限は `permissions: contents: read` に固定し、secrets や write 権限に依存しません。checkout は `persist-credentials: false` にし、未信頼 code を実行する step では `GITHUB_STEP_SUMMARY=/dev/null` を設定します。

human-facing report は base branch から checkout した trusted `scripts/perf_report.py` で生成します。PR head 側の script は cargo bench の対象にはなりますが、summary renderer としては使いません。初回導入 PR のように base branch に trusted renderer がまだ無い場合は、PR script に fallback せず、固定文の report と Criterion JSON artifact だけを残します。

将来 PR comment を自動投稿する場合は、この benchmark 実行 job に `pull-requests: write` を足しません。別 workflow/job に分け、PR job 由来 artifact は未信頼 data として schema、size、文字種を検証します。artifact 内の script や HTML は実行せず、PR head も checkout せず、base branch の trusted parser だけで Markdown を再生成します。

`$GITHUB_STEP_SUMMARY` に出す benchmark ID は Markdown table 用に escape します。benchmark ID は通常 repo 内定数ですが、report spoofing を避けるため script 側で `|` と改行を無害化します。

trusted renderer は未信頼 PR code が生成した Criterion JSON を読むため、入力サイズにも上限を置きます。`scripts/perf_report.py` は symlink / non-file JSON、Criterion result file 数、JSON file size、benchmark ID 長、mean / throughput 値の範囲を制限し、異常な artifact は Markdown 生成前に拒否します。

## 更新ルール

性能改善または意図した性能劣化がある PR では、`crates/rulemorph/PERF.md` と `crates/rulemorph/PERF.json` を同時に更新します。挙動変更では `cargo test -p rulemorph` を必ず通し、normalization を触る場合は `docs/agent-guides/input-normalization-security.md`、trace を触る場合は `docs/agent-guides/semantic-trace.md` の invariant を確認します。

## Future Work

- Linux runner で `iai-callgrind` を導入し、instruction count を deterministic gate にできるか検証する。
- HTML / Excel normalization bench は resource limit と fixture サイズを固定してから feature-gated bench として追加する。
- 複数回の CI 実行結果を見て、allocation canary の閾値を tighten する。
- PR comment 自動投稿を入れる場合は、benchmark 実行 workflow から分離した read-only artifact parser として設計する。
