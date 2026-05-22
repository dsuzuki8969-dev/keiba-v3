# Handoff 2026-05-22 夜間メンテナンス

## セッション概要

マスター就寝中の全見直し・全修正・全改善・メンテナンス一括実行。
前セッション (Opus 4.7) で 5/22 NAR 36R 結果データが corrupted → 復旧 + P1 バグ 5 件修正後、
マスター指示「寝るから全見直しと全修正と全改善をやっておいて。メンテナンスしておいて」に基づく自走作業。

## コミット

| commit | 内容 |
|---|---|
| `649f933` | fix: P1 バグ 5 件修正 (scraper 払戻上書き / TS 型安全 / 印重複解消) |
| `e618866` | refactor: 夜間メンテナンス — ESLint 46→10 件 + 型安全強化 + dead code 削除 + セキュリティ修正 |

## 変更詳細

### P1 バグ修正 (649f933) — 5 件

| ID | 修正 | ファイル |
|---|---|---|
| A | StatsCard `\|\|` → `??` 全 20 フィールド統一 (0 が偽値トラップ回避) | StatsCard.tsx |
| B | `classifyHorsesByMark` himo を `[△★☆]` に修正 (partner ○▲ と重複解消) | TicketSection.tsx |
| C | `Phase4HybridFormation` totalStake を `meta.stake_total` 優先に変更 | TicketSection.tsx |
| D | `TicketsByMode._meta` 型定義拡充 → `as` キャスト除去 | RaceDetailView.tsx + TicketSection.tsx |
| E | `_parse_vertical/horizontal_payouts` `payouts[key]=entries` → `setdefault().extend()` | official_nar.py |

### 夜間メンテナンス (e618866) — 16 ファイル, -131 行

#### フロントエンド型安全 (HIGH)
- **`||` → `??` 統一 60 箇所以上**: AbilityTable / HorseDiagnosis / HorseCardPC / HorseCardMobile / TicketSection
  - 偏差値 0.0 / 確率 0.0 / EV 0.0 が偽値トラップで正しく表示されなかったバグを全修正
- **RaceResultPanel `as any` 25 件 → 0 件**: `RaceResultEntry` に `time_sec` / `win_odds` 追加
- **RaceDetailView setTimeout リーク防止**: `oddsMsgTimerRef` + `useEffect` cleanup
- **HorseCardMobile rank 計算 useMemo 化**: 毎 render O(n²)×4 → horses 変更時のみ
- **HorseCardPC `evColorCls`**: `!ev` → `ev == null` (ev=0 表示修正)
- **RaceResultPanel**: `order` / `payouts` を `useMemo` で参照安定化

#### Dead code 削除
- **TabGroup1Actions.tsx**: orphan 155 行削除 (どこからも import されていない)
- **PastRunsTable**: `fmtTime` / `condCls` export 除去 (react-refresh 修正)
- **HorseDiagnosis**: `getIntensityCls` 関数 → 定数 `INTENSITY_CLS` に簡素化

#### ESLint 改善
- **46 errors → 10 errors** (36 errors 修正)
- ESLint config: `no-irregular-whitespace` に `skipRegExps` / `skipJSXText` 追加
- client.ts: ESLint disable コメント正規化
- PaceFormation: `any` → `Record<string, number>` 型指定

#### バックエンド (Python)
- **dashboard.py**: Basic 認証 `hmac.compare_digest` 化 (タイミング攻撃防止)
- **official_nar.py**: `_fallback_from_race_log` DB 接続 `with` 文化 (例外時リーク防止)
- **official_nar.py**: `_parse_vertical_payouts` `current_bet_key` 過剰リセット修正 + regex `^\d+(-\d+)*$` に改善

## ヘルスチェック結果 (全 7 項目正常)

| 項目 | 状態 |
|---|---|
| Dashboard プロセス | ✅ PID 12020 稼働中 (port 5051) |
| cloudflared トンネル | ✅ 2 プロセス稼働中 |
| フロントエンドビルド | ✅ 3.68s, dist/index.html 最新 |
| 5/22 結果データ | ✅ 36R / 380 頭 / time_sec=0: 0 件 / last_3f=0: 0 件 |
| SQLite DB | ✅ integrity_check OK / 40,996 レース |
| スケジュールタスク | ✅ 4 タスク全登録済 |
| ログ | ✅ ERROR/CRITICAL 0 件 |

## レビューで検出した残課題 (LOW — 次セッション候補)

### フロントエンド
- `react-hooks/set-state-in-effect` 3 件 (MovieEmbed / RaceCard / PastPredictions — 意図的パターン)
- `react-refresh/only-export-components` 3 件 (shadcn UI badge / button / tabs — ライブラリコード)
- `fmtTime` 4 重複 / `condCls` 3 重複 (constants.ts に集約可能)
- HorseCardPC 外側 wrapper の memo 未適用 (MEDIUM)
- `RaceDetailView.tsx` OddsResponse 型に server 返却フィールド不足 → double cast 残存

### バックエンド (Python reviewer HIGH 指摘 — 安全だが改善余地あり)
- `_parse_race_mark_table` ヘッダー検出: `馬番` 存在チェック未実施
- `jockey_name` フォールバック index `7` がハードコード
- `time_sec = 0.0` / `last_3f = 0.0` デフォルトが「データなし」と区別不可 → `None` 推奨
- `last_3f` 有効範囲 `30 <= val <= 50` が短距離で機能しない
- `_analyzer_state` / `_predictions_cache` のスレッド安全性 (threading.Lock 推奨)
- `_get_race_count` の `except Exception: return 0` がデバッグ情報を破棄
