# Agent Guides

`AGENTS.md` は常時読む entrypoint です。詳細な invariant や task-specific な test 条件は、この directory の guide に分けます。

各 guide は次の形式で書きます。

## When to read
- どの変更対象、module、feature、docs に触る前に読むべきかを書く。
- 複数 guide にまたがる場合は、より危険な invariant を持つ guide から読む。

## Do not break
- その領域で守るべき挙動、security boundary、public/internal contract を書く。
- `AGENTS.md` に同じ説明を重複させず、この section に詳細を寄せる。

## Required tests
- 変更後に最低限実行する command を書く。
- docs-only 変更などで実行しない場合は、final response に理由を明記する。

## Notes
- 参照すべき spec、設計 docs、fixtures、review 観点を短く書く。
- 実装細部は、最新の source code と tests を truth source とする。
