# 最終出力検証と保守的修復の実装計画

1. Sites対応とは独立したmain起点branch/worktreeで作業する。新しいrulemorph_output crateのAPIとして、strict final validationと明示rules normalizationを区別する。
2. mature Rust jsonschema 0.58.3（Rust 1.88互換を確認）を採用。Draft2020-12の明示subset、未知keyword/format拒否、全refs拒否、resolver機能無効+offline compilerを固定する。
3. 完全性を呼び出し側から受け、切断/Unknownを拒否。カンマ修復だけをopt-in候補+位置traceとして提供し、引用符/括弧補完や曖昧候補は拒否する。
4. bounded direct mappingと必須target projectionでtextを明示抽出する。欠損を推測せず、warningsも成功扱いしない。成功は完全値のschema validation後のみ。
5. fixture、性質テスト、独立Python validator、Rust1.88互換、全workspace回帰、既存CIチェック、日本語文書を用意して別draft PRを作成する。CIの実行可否を当該commitで確認する。

LLM再生成、provider依存、認証API、既存CLI/MCPツール追加、公開/デプロイ/mergeは範囲外。Schema適合は意味保存の証明ではなく、修復範囲・schema/ルールsubsetの拡大は別途境界検証を必要とする。
