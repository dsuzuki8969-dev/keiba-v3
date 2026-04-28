# LM Studio セットアップガイド — D-AI Keiba v3

## 概要

D-AI Keiba v3 では厩舎コメントのパラフレーズ処理に  
LM Studio のローカル LLM（Qwen2.5-7B-Instruct）を使用します。

`daily_maintenance.bat` の `[7b2/9]` ステップが毎日 23:00 に  
`lms load` コマンドでモデルを自動ロードし、`[7c/9]` でパラフレーズ処理を実行します。

PC 起動時に LM Studio が自動起動していないと lms ロードが失敗し、  
パラフレーズ処理がスキップされます。以下の手順で Auto-start を設定してください。

---

## 1. LM Studio 本体の Auto-start 設定

### 手順

1. LM Studio を起動する
2. 左下の歯車アイコン → **Settings（設定）** を開く
3. **General** タブを選択
4. **「Start LM Studio on login」** をオンにする  
   （Windows ログオン時に LM Studio が自動起動するようになる）
5. **「Start server on launch」** もオンにする  
   （起動と同時にローカルサーバー port 1234 を立ち上げる）
6. 設定を保存して LM Studio を閉じる

> 注意: 設定画面の名称は LM Studio のバージョンにより若干異なる場合があります。

---

## 2. Windows スタートアップフォルダへのショートカット配置（バックアップ方法）

LM Studio の Auto-start 設定が効かない場合の代替手順です。

1. `Win + R` → `shell:startup` と入力して Enter  
   → スタートアップフォルダが開く
2. LM Studio のショートカットをスタートアップフォルダにコピーする  
   - 通常のインストール先: `%LOCALAPPDATA%\Programs\LM Studio\LM Studio.exe`
3. PC を再起動して LM Studio が自動起動することを確認する

---

## 3. CLI（lms コマンド）の PATH 確認

`daily_maintenance.bat` は `where lms` でコマンドの存在を確認します。  
`lms` が見つからない場合は以下を確認してください。

### 確認コマンド（コマンドプロンプト）

```bat
where lms
lms --version
lms server status
```

### PATH が通っていない場合

LM Studio は CLI ツール `lms.exe` を以下のいずれかに配置しています:

```
%LOCALAPPDATA%\Programs\LM Studio\
%LOCALAPPDATA%\LM Studio\bin\
```

1. 上記パスを確認し `lms.exe` が存在するフォルダを特定する
2. システムの環境変数 → Path に該当フォルダを追加する  
   （`Win + S` → 「環境変数」→「システム環境変数の編集」→ Path 編集）
3. コマンドプロンプトを再起動して `where lms` が通ることを確認する

---

## 4. 動作確認

PC 再起動後、以下のコマンドで LM Studio ローカルサーバーが稼働していることを確認します。

```bat
lms server status
```

期待する出力例:

```
LM Studio Server is running on http://localhost:1234
```

また、手動でモデルロードのテストができます:

```bat
lms load qwen2.5-7b-instruct
```

成功すると `[7b2/9]` ステップが正常終了し、  
`[7c/9]` のパラフレーズ処理が実行されるようになります。

---

## 5. daily_maintenance.bat における自動化フロー

```
[7b/9]   pred.json 再注入（run_dev 更新）
    ↓
[7b2/9]  lms コマンド存在チェック
          ├─ lms なし  → [WARN] ログ出力 → skip_paraphrase へ
          └─ lms あり  → lms load qwen2.5-7b-instruct
                          ├─ ロード失敗 → [WARN] ログ出力 → skip_paraphrase へ
                          └─ ロード成功 → 次ステップへ
[7c/9]   local_llm_paraphrase.py --recent 7
          └─ エラー時は [WARN] のみ（バッチ全体は止まらない）
:skip_paraphrase
[QC]     データ品質チェック
```

フォールバック設計:
- `lms` コマンドが見つからない場合 → **警告のみ・バッチ継続**
- `lms load` に失敗した場合 → **警告のみ・バッチ継続**
- パラフレーズ自体がエラー → **警告のみ・バッチ継続**

パラフレーズ処理は補助機能のため、失敗しても他のメンテナンス処理には影響しません。

---

## 6. トラブルシューティング

| 症状 | 確認ポイント | 対処 |
|------|------------|------|
| `[WARN] lms コマンドが見つかりません` | `where lms` が通らない | PATH に lms.exe のフォルダを追加 |
| `[WARN] lms load 失敗` | LM Studio が起動していない | Auto-start 設定 or スタートアップ登録を確認 |
| LLM パラフレーズがスキップされ続ける | 上記 2 件の複合 | LM Studio を手動起動後 `lms server status` で確認 |
| `lms server status` がエラー | ポート 1234 が占有 | タスクマネージャーで 1234 を使うプロセスを確認 |

ログは `log\maintenance_YYYYMMDD.log` に蓄積されます。  
`[WARN] lms` で grep すれば失敗状況を追跡できます。
