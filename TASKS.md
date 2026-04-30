# TASKS.md — D-AI Keiba v3 タスクボード

> **運用ルール**: CLAUDE.md「TASKS.md / MEMORY.md 運用ルール」を参照。
> マスター指示はすべてここに追加。完了したタスクは「終わったタスク」へ移動。
> 教訓・反省は `~/.claude/projects/C--Users-dsuzu-keiba-keiba-v3/memory/` に永続化。

---

## 🔴 作業中のタスク

### 次セッション (5/1 朝以降) 最優先タスク

### T-054 (P0・新規) — DAI_Keiba_Dashboard タスクスケジューラ起動経路修復
- 現状: Bash 経由 PID 7148 起動 (PC ログオフで死ぬ)
- wscript→start_dashboard_hidden.vbs→start_dashboard.bat 経路で **dashboard 起動失敗**
- T-046 と同根の cmd /c ""..."" クォート問題の可能性
- 修正方針: vbs/bat の見直し or タスクスケジューラ Action を直接 python 呼びに変更 (管理者必要)
- 工数: 30-60 分

### T-046 Phase 2 (P0・継続) — 5/1 06:00 bat_trace.log 確認
- 診断装置 commit 72d18c6 で仕込み済 → 5/1 06:00 で初検証可能
- 確認手順は handoff_2026-04-30.md「Step 1-4」参照
- 工数: 15 分 + 修正

### T-047 (P1) — 結果自動取得不全 構造修正
- 真因 (b) 確定: `_auto_fetch_post_races` がブラウザ polling 依存の fire-and-forget で無人放置時に完全停止
- 本日実例: 06:29 朝確認後 polling 停止 → 18:04 帰宅まで 11 時間 34 分 auto-fetch 未発火 → 11:40 発走の水沢 1R が 6+ 時間結果未取得
- 暫定対応 (30 分): `DAI_Keiba_Watchdog` (5 分間隔) に `/api/home/today_stats` 自己 HTTP リクエスト追加
- 構造修正 (60 分・推奨): `src/dashboard.py` に `_start_background_result_fetcher` スレッド追加 (Flask 起動時 10 分間隔で自律実行)

---

## 🟡 将来課題（次セッション以降）

### P0 (即着手・最優先)

| 優先度 | 項目 | 状態 / 条件 |
|:---:|---|---|
| **P0** | **LIVE STATS 三連単 F 集計修正** | ✅ 解決済み — 三連複+単勝切り替えで置換 (5/1 深夜 dashboard.py + frontend 対応完了) |

### P1

| 優先度 | 項目 | 状態 / 条件 |
|:---:|---|---|
| P1 | 取消馬誤検知 (水沢 12R 等) 真因究明 | 「取消馬により買い目無効」テキストの出所特定 (frontend/src grep ヒットなし、src/output/ や別経路の可能性) |
| P1 | build_sanrentan_tickets vs pred.json fixed 整合性検証 | LIVE STATS 修正完了後にバックテストで突合 |
| P1 | B_prefix 1,253 件の対応 | NAR 公式コードとの突合 or netkeiba 馬詳細スクレイピング等、別アプローチ要検討 |
| P1 | 2023 年生まれ若駒 339 件 | netkeiba 403 エラー → 自動補完待ち（馬 DB に存在しない可能性あり） |
| P1 | B skipped 6,609 件の再 apply | キャッシュ蓄積後に `restart_backfill_b.ps1` で再実行（2023-10〜12月が主体） |
| P1 | ML 47 モデル再学習 (retrain_all.py) | B 完走 +34,477 行で AUC 向上余地、半日〜1日タスク。GPU 計算 + 旧モデル比較バックテスト要 |

### P2

| 優先度 | 項目 | 状態 / 条件 |
|:---:|---|---|
| P2 | netkeiba 並列リクエスト禁止の構造強化 | feedback_netkeiba_concurrent_throttle 関連 |
| P2 | B_prefix race_log 残存 33,779 件 | 整合済みだが将来的に netkeiba_id 統合の余地あり |

---

## 🟢 終わったタスク

### 🟢 本セッション完了 (2026-05-01 深夜)

| タスク | 内容 | 結果 |
|---|---|---|
| LIVE STATS 三連複+単勝切り替え | dashboard.py L1327 `_collect_strategy_tickets` + `api_home_today_stats` + `_build_race_card_results` + frontend StatsCard/RaceCard 対応 | ✅ |
| T-055 4/30 pred.json T-050 再生成 | `__pycache__` 問題発見→修正→再生成 | ✅ |
| T-029 5/1 paraphrase 完了 | 264 items 処理、119 件書き込み、所要 61.4 秒 | ✅ |

### 🟢 本セッション完了 (2026-04-30) — T-050 採用戦略確定 + 12 commits push 済

**最終結論**: **A-NONE 馬単なし 2 券種 (三連複動的 + 単勝 T-4)** 採用確定
- 純利 **+8,253,510 円 / 849 日** / 年換算 **+355 万** / ROI **190.0%** / DD 34,950
- 馬単抜きで純利 +90 万改善 / ROI +22.7pt

| commit | 内容 |
|---|---|
| `0e88e59` | T-050 フォーメーション構造表示 + dashboard.py honmei_odds fallback |
| `6af980e` | TicketSection 三連複+単勝 表示対応 |
| `28463e0` | 過去成績ページ 新戦略集計 + dispatch_backtest --no-umatan |
| `be24379` | T-050 本番実装 (betting.py 新規 4 関数 + engine 統合) |
| `af4b273` / `3bb2621` / `701244b` | paraphrase 完コピ対策 SAFE_MAP |
| `f7c3f29` | TASKS.md 結論反映 |
| `6f60ce9` | T-049/T-050 数値整合性 (EV フォールバック + NAR skip) |
| `72d18c6` | T-046 診断装置 (bat + vbs) |
| `54ecb73` | T-050 案 C 6 ケースバックテスト (旧) |
| `324567f` | T-047 Flask background thread |

詳細: `~/.claude/projects/.../memory/handoff_2026-04-30.md` ★★★

### T-041 (棄却) — 三連単 F 戦略最善化
- 2026-04-29 棄却: Phase A v2 で ROI 49.8% 赤字確定 / 三連複動的 (T-050) で置換

---

### 🟢 本セッション完了（2026-04-29）— 7 commits push 完了

| タスク | commit | 内容 |
|---|---|---|
| T-040 (P0) | 59b7213 | LIVE STATS 三連単集計を T-039 ロジックに統一 |
| T-042 (P1) | 4b4852f | 取消馬誤検知 1 行修正 |
| T-042.fix | 114c73a | _check_ticket_hit 三連単 payouts 英字キー/list 形式対応 |
| T-043 (P0) | 388bb69 | untracked スクリプト 50+ ファイル git 管理化 |
| T-044 (P0) | Sonnet | 厩舎コメント文体異常解消 (5 一括対応) |
| T-044.fix | dfa2a2a / 19f93f0 | prefix strip 漏れ修正、DB cache 19,822 行全消去、4/29 再 paraphrase 完了 |
| T-045 (P1) | 65c9100 | 取消馬反映構造バグ修正 (is_scratched 優先化、pytest 125 passed) |

---

### 🟢 本セッション完了（2026-04-28）— T-033 P0 + T-037 + T-038 全完了

| タスク | commit | 内容 |
|---|---|---|
| T-033 Phase 1 | bbcdf44 | date_from_race_id() バグ修正 (221 日分 / 7,029 件の誤パス配置解消) |
| T-033 Phase 2 D-1 | d00ca64 | pred.json venue 異常 フルクリーニング (race_log cleanup 7,133 行) |
| T-033 D-2 | c9138cf | pred.json race.venue 1,691 件不整合修正 (audit A/B/C/D 全 0 件) |
| T-037 | 4fb032d | audit_pred_venue.py JRA venue_code 位置誤読修正 (偽陽性 6,971 → 真不整合 1,691) |
| T-038 Phase 1+3+4 | T-038 統合 | 開催カレンダー機能 (kaisai_calendar.json 259KB / 1,583 開催日 + React CalendarPage) |
| T-039 | a82cd84 | レースカード的中バッジ + 赤外枠 |
| 払戻金キー英字対応 | 6d0939e | RaceResultPanel PayoutCard フロント対応 |
| keibabook Referer | a065bab | 大井 11R/12R 取得対応 |
| 自己修繕プロトコル | 8b04e0e | 応答前 5 秒自己診断 + テンプレ脳トリガー語句 CLAUDE.md 格上げ |
| B 持ち越し完走 | b1ef530 等 | 2023 下半期 race_log バックフィル (+34,477 行) / speed_dev=20 残 1 件解消 / 同名 horse_id 再評価 / horses マスター D Phase 1-3 |
| CI 統合 | 355462b | GitHub Actions lint / unittest / import-check |

---

### 🟢 セッション 4/27 朝〜夕方 完了サマリ（11 commits v6.1.23-32）

- v6.1.23-32 全完了: ローカル LLM 統合 / paraphrase 永続化 / race_log finish_time 復元 / 着差バグ完全解消 / 偏差値 20.0 床貼り付き解消 / ability_total 床貼り付き解消
- Plan-α: ability_total -50 拡張 / Plan-γ Phase 1-3: relative_dev + hybrid_total 実装
- T-010〜T-021: LIVE STATS 手動更新ボタン / 帯広行揃い / 注目レース高さ揃え / T-020 pending 不整合 / T-021 追切印「−」等
- マスター指摘 13 件全件対応 ✅

詳細: `~/.claude/projects/.../memory/handoff_2026-04-27_v3.md`、`handoff_2026-04-27_v4.md`、`handoff_2026-04-27_v5.md`

---

### 🟢 直近完了（2026-04-26）

- T-003 能力プロファイル → ヒートマップ表 置き換え ✅
- T-004 出馬表 馬体重表示 + 発走 15 分前自動取得 ✅
- T-005 Hero セクション スタイリッシュ刷新 ✅
- T-006 全ページ統一レイアウト ✅
- T-007 PastPredictions React 違反修正 ✅
- T-008 前三走データ欠損 緊急修正 ✅ (騎手名 slice バグ / 偏差値 20.0 根絶 / NAR カバレッジ拡張)

詳細: `~/.claude/projects/.../memory/handoff_2026-04-27.md`、`handoff_2026-04-27_v2.md`
