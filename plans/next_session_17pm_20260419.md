# 17:00 再開セッション向けアクションプラン（2026-04-19 作成）

## 背景

- 15:37 に Claude CLI のレート制限に到達（`You've hit your limit · resets 5pm (Asia/Tokyo)`）
- MAX プランの OAuth 枠を消費しきった状態。17:00 JST でリセット
- paraphrase 再処理は 75/490 件でストップ
- Claude Code 本体（Anthropic API）の残メッセージ枠も残り僅少（15:37 時点で約4%）

---

## タスク A：paraphrase v2.1 再処理の再開（最優先）

### 現状

- キャッシュ `data/cache/stable_comment_paraphrase.json` に **75 件**保存済み
- バックアップ: `data/cache/stable_comment_paraphrase.v1_20260419.json.bak`（旧 v1、保持中）
- pred.json (`data/predictions/20260419_pred.json`) は v1 bullets のまま（未書き換え）

### 再開手順（17:00 以降）

```bash
cd /c/Users/dsuzu/keiba/keiba-v3
PYTHONIOENCODING=utf-8 PYTHONUNBUFFERED=1 python -u -X utf8 \
  scripts/paraphrase_stable_comments.py 2026-04-19 \
  > log/paraphrase_v2_resume_20260419.log 2>&1 &
echo "PID=$!"
```

- キャッシュ内の 75 件は自動スキップ（hash_text ベースのキー一致でヒット）
- 残り 415 件を処理。推定 **約 2 時間 30 分**（22 秒/件 × 415）
- **完了予定: 19:30 前後**

### 進捗監視（Monitor）

```
tail -F log/paraphrase_v2_resume_20260419.log |
  grep -E --line-buffered "件完了|キャッシュ命中|所要時間|pred.json 更新|注入:|Traceback|エラー|連続失敗|⚠"
```

- timeout 1時間（3600000ms）で Monitor を張り、足りなければ再起動

### 完了後の検収

1. `python scripts/verify_paraphrase.py 2026-04-19` → `490/490 (100.0%)` 確認
2. ブラウザ（dash.d-aikeiba.com）で Ctrl+Shift+R
3. 複数レース・複数馬で bullets が自然な口語になっていることを目視確認
   - 推奨レース：中山 11R 皐月賞（18 頭）、福島 11R 牝馬ステークス
   - 「厩舎コメント」タブ内 + 各馬カード展開時の両方で表示

### 注意

- **paraphrase 再実行中に他の Claude CLI 呼び出しを入れない**（レート消費競合回避）
- もし再度 429 に当たったら、長時間 sleep がスクリプト内に仕込まれている（LONG_REST_SECONDS=180、CONSECUTIVE_FAIL_LIMIT 超過で発動）ので一旦放置

---

## タスク B：買い目指南機能の改善プラン（要マスター決定）

### 現状分析（15:40 時点で把握済み）

- 該当 UI：`frontend/src/pages/TodayPage/TabGroup3Horse.tsx` L188-197
  ```tsx
  const hasTickets =
    (race.tickets && race.tickets.length > 0) ||
    (race.formation_tickets && race.formation_tickets.length > 0);
  if (!hasTickets) {
    return <p>買い目の推奨がありません（自信度が基準未満、もしくはデータ未算出）。</p>;
  }
  return <TicketSection race={race} />;
  ```

- 関連ファイル：
  - `frontend/src/pages/TodayPage/TicketSection.tsx`（285 行）— UI レンダリング
  - `frontend/src/pages/TodayPage/MarkSummary.tsx`（433 行）— 印サマリ
  - `frontend/src/api/client.ts`（498 行）— 型定義
  - バックエンド（要調査）：`race.tickets` / `race.formation_tickets` を生成するロジック
    - 候補：`src/engine.py` もしくは `src/calculator/` 配下
    - 自信度しきい値の所在を Grep で特定する（次セッション冒頭で実施）

- 中山 11R 皐月賞（スクショ）では `tickets` も `formation_tickets` も空 → 自信度しきい値未満と推測

### マスターのゴール（未確定・次セッションで確認）

以下 A〜D から選択（15:37 で選択肢提示済、回答待ち状態で中断）：

- **A. 表示ロジックの改善**
  - 自信度しきい値の調整（下げて推奨を増やす）
  - 「推奨なし」時に代替として「参考買い目」を出す
  - しきい値の UI 切替

- **B. 買い目の質・種類の拡充**
  - 券種追加
  - フォーメーション・流し目パターン追加
  - 期待値フィルタ（オッズ考慮）

- **C. マスター自身の買い目戦略を形式化**
  - 条件別ルールをシステムに組み込む
  - 印（◎○▲△★☆）別の構成パターン

- **D. 他 / 組み合わせ**

### 決定後の共通タスク（ゴール確定次第 着手）

1. バックエンド側の買い目生成ロジックを特定（`Grep "tickets"` など）
2. 自信度算出式の把握（どの変数の和・積で決まっているか）
3. マスターのゴールに対応する修正箇所を洗い出す
4. 影響範囲・リスク評価
5. 実装 → ビルド → 動作確認

---

## タスク C：UI 修正の最終確認（paraphrase 完了後）

既に `npm run build` + `src/static/` 反映済み（15:26 完了）。
ハードリロード後に以下が確認できる：

- 「厩舎の話」→ **「厩舎コメント」** に統一（HorseCardPC / HorseCardMobile / HorseDiagnosis）
- **箇条書き優先表示**（bullets があれば ul、なければ原文フォールバック）
- 調教短評は辞書置換済み（paraphrase.ts）で著作権対応 OK

---

## 推奨実行順序（17:00 以降）

```
1. [即] paraphrase 再開 （バックグラウンド実行）
2. [即] Monitor で進捗監視
3. 並行でマスターとタスク B のゴール確定（A/B/C/D 選択）
4. タスク B の実装に着手（バックエンド調査 → 修正）
5. paraphrase 完了通知を待つ → verify_paraphrase.py → UI 確認
6. タスク B の動作確認とデプロイ
```

---

## 既知の未処理アラート

- Monitor `bsiphz7ar` は 1 時間タイムアウト済み（16:11 頃）。再アーム不要（paraphrase 停止中のため）
- タイムラインログ：`log/paraphrase_v2_20260419.log`（70 件目まで残存、参考資料）
