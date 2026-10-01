# Sites HTTP MCP対応計画と検証方針

1. 既存coreのWASM target互換性を確認する。Excel/HTML/Markdownを外した構成で成立することを確認済み。core/stdio/CLIの意味論は変更しない。
2. 独立したrulemorph_wasm adapterとWorkers ESM wrapperを追加する。4ツールをinline-onlyで提供し、Sites認証、同一Origin、固定上限、制限付きscalar rule profileを実装する。
3. Rust境界・既存workspace回帰・HTTP JSON-RPC・実WASM/Workersテストを追加する。重複キー、未知引数、file branch、大きいarray target、processing/output超過、認証拒否と次回呼出しの回復を検証する。
4. Sites manifest、構築/ローカル実行/認証境界/制限の日本語文書、CIを追加し、専用branchをpushしてdraft PRを作成する。そのcommitのCIを確認する。

公開、デプロイ、プラグイン登録、権限変更、UI追加、出力JSON Schema保証はこのPRの範囲外。Sites実ホストの接続と公開Site/private plugin配布範囲は未検証。演算範囲の拡大にはcore全体を覆う評価budgetが先に必要。
