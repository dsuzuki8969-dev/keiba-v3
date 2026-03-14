# Cursor 内で Claude Code を使う最適な作業環境

このドキュメントは、Cursor と Claude Code を同じプロジェクトで安全に併用するための手順とルールです。

## 1. 前提

- **Cursor**: IDE 連携・既存ルールに沿った実装・このリポジトリの慣習に強い。
- **Claude Code**: ターミナル・CLI 中心の作業や外部ツールのセットアップに向く。
- 両方同時に**同じファイルを編集しない**ことが、最適な環境の条件です。

## 2. 推奨セットアップ

### 2.1 Claude Code の起動

プロジェクトルートで Claude Code を起動し、作業ディレクトリを揃えます。

```powershell
cd c:\Users\dsuzu\keiba\keiba-v3
claude code
```

（`claude code` のインストール・API キー設定は [Anthropic のドキュメント](https://docs.anthropic.com/claude-code) を参照。）

### 2.2 Cursor 側のルール

プロジェクトには **`.cursor/rules/claude-code-coexistence.mdc`** が入っています。

- 「Claude Code 使用中」と伝えると、Cursor は該当ファイルを直接編集せず、テキストで提案やハンドオフ用のまとめを出します。
- 役割の目安に従い、同じタスクを Cursor と Claude Code で二重にやらないようになります。

## 3. ワークフロー

### パターン A: 作業を分ける（推奨）

| タイミング | やること |
|-----------|----------|
| 開始時 | 「今から Claude Code で ○○ をやる」と Cursor に一言伝える |
| Claude Code 側 | ターミナル・スクリプト・外部ツール中心の作業 |
| Cursor 側 | その間は同じファイルを編集しない。必要なら「編集内容をテキストで」と依頼 |
| 終了時 | 「Claude Code の作業終わった」と伝えると、Cursor が通常どおり編集できる |

### パターン B: Cursor → Claude Code へハンドオフ

1. Cursor で「これを Claude Code に引き継ぎたい」と伝える。
2. Cursor が「やるべきこと・対象ファイル・注意点」を箇条書きで出す。
3. そのテキストをコピーし、Claude Code のプロンプトに貼って続きの作業をする。

任意で、**`docs/claude_code_handoff_template.md`** にメモを書いてから Claude Code に渡すと、内容が揃いやすくなります。

### パターン C: Claude Code → Cursor へ戻す

1. Claude Code での作業が一段落したら、保存して終了。
2. Cursor で「Claude Code の作業終わった。続きは ○○ をして」と指示。
3. Cursor が IDE の状態（開いているファイル・Lint など）を見て続きを対応。

## 4. 競合を防ぐコツ

- **同時編集しない**: どちらか一方を「今の担当」にし、もう一方は読むだけか提案だけにする。
- **作業範囲を言う**: 「`src/foo/` は Claude Code が触る」「バックエンドは Cursor」など、ざっくりでよいので伝える。
- **保存してから切り替え**: ツールを切り替える前に、必ず保存（Ctrl+S）してからもう一方に渡す。

## 5. このリポジトリで Cursor に任せた方がよいもの

- ダッシュボードの起動（「ダッシュボード開いて」など）
- 競馬場カバレッジ（中央・地方 24 場）に絡む実装やマスタ
- `.cursor/rules/` に書かれたプロジェクト固有のルールに沿った変更
- Lint や型エラーを踏まえた修正

## 6. まとめ

- **ルール**: `.cursor/rules/claude-code-coexistence.mdc` で Cursor の振る舞いを固定済み。
- **運用**: 「Claude Code 使用中」と伝える → 編集競合を避ける → ハンドオフは箇条書きでコピペ。
- この運用で、Cursor 内で Claude Code を使う作業環境を安定して使えます。
