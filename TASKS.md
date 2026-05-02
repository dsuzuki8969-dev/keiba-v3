# TASKS.md — D-AI Keiba v3 タスクボード

> **運用ルール**: CLAUDE.md「TASKS.md / MEMORY.md 運用ルール」を参照。
> マスター指示はすべてここに追加。完了したタスクは「終わったタスク」へ移動。
> 教訓・反省は `~/.claude/projects/C--Users-dsuzu-keiba-keiba-v3/memory/` に永続化。

---

## 🔴 作業中のタスク

### 5/3 セッション 完了済タスク (本日)

### T-062 ✅ 完了 — M' 戦略本実装 (自信度別 三連複フォーメーション)
- バックテスト純利 +¥28.1M / 年換算 +¥12M / ROI 216.7%
- SS=E(4点) / S/A=C(7点) / B/C/D=D(10点) / E=skip / 1点100円
- commit: bf287e2 (バックエンド) / b18dd3f (検証スクリプト) /
  53fe71c (フロント+LIVE STATS) / 508f13c (4 dev フィールド)

### T-064 ✅ 完了 — 能力指数 γ案修正 (海外除外 + 異常値除外)
- commit: a1e9138
- クロワデュノール: ability_wa 19.85 → 53.92 (+34.07pt)
- 5/3 単日: Phase 3b 完了 / 全 850 日: Phase 3c BG 稼働中 (~9 時間)

### 5/2 セッション 完了済タスク (継続)

### T-056 ✅ 完了 — Dashboard 直接再起動 (5/2 PID 12712 稼働中)
### T-057 ✅ 完了 — results.json 三連複払戻バックフィル (12,588 件 + combo バグ修復 16,208 件)
### T-059 ✅ 完了 — 過去 pred.json 全件 T-050 化 + ev 補完 + 実オッズ判別
### T-060 ✅ 完了 — 印補正 (× 削除 19,918 / ☆ 補完 6,253)
### T-061 ✅ 完了 — 三連複 9 パターンバックテスト (A-I) JRA/NAR/自信度別

### 5/3 以降 マスター判断待ち

### T-065 (P0・新規) — 能力指数 構造的過小評価の調査
- γ案で海外除外は対応したが ability_total そのものが G1 実績馬で低い問題は残る
- クロワデュノール (G1 3 勝) ability_max=55.67 / total=54.80
  → タガノデュード (実績下) ability_max=62.25 / total=58.89 が上回る
- 真因仮説: G1/G2/G3 勝利数の直接ボーナス未加算 / クラス補正の効きが弱い
- 工数: 90-120 分 (調査) + 実装

### T-066 (P0・新規) — Phase 3c 完了確認 + commit 7
- 全 850 日 ability 再計算が 16:00 頃完了見込み
- 完了後ログを確認して commit 作成 (内容変更ファイルは pred.json のみ・gitignore で git に出ない)
- 完了後 dashboard 再起動 (Phase 6 改修 + フロント M' 表示反映)

### T-067 (P0・新規) — dashboard 再起動 + フロント M' 表示確認
- Phase 6 改修 (dashboard.py + hybrid_summary.py の M' 集計対応) 反映
- src/static/ ビルド成果物 (M' 表示) 反映
- 再起動方法: 黒画面回避 (累犯 3) のため Bash &or タスクスケジューラ経由

### T-063 (P1・新規) — Backfill 完了済データの最終検証
- 全期間で異常値ないか確認 (現在 ROI 200%+ 確認済)
- 月別・印別・自信度別の整合性チェック
- 工数: 30 分

### T-058 (P1・新規) — engine.py running_style バグ恒久対策
- 現象: 5/1 4頭で running_style/predicted_corners 空
- 暫定: results_tracker フォールバック追加済 (commit d622506)
- 真因仮説: engine.py L1466 `ev.pace=None` または `_style_map` 欠落
- 工数: 60-90 分

### T-054 (P0・継続) — DAI_Keiba_Dashboard タスクスケジューラ起動経路修復
- 5/1 タスクスケジューラ再登録済み (setup_scheduler.ps1 完了)
- DAI_Keiba_Dashboard が「logon」trigger で起動・現状動作中
- 残課題: T-046 経路 (vbs/bat) は不要になった可能性 (要確認)
- 工数: 15 分 (確認のみ)

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

### 🟢 本セッション完了 (2026-05-01 早朝・マスター激怒指摘連続対応)

| commit | 内容 |
|---|---|
| `653df20` | LIVE STATS 三連複+単勝切替 + 4/30 pred.json T-050 再生成 + paraphrase + TASKS.md 整理 |
| `8163f99` | 過去成績ページ 旧三連単成績削除 + T-050 ハイブリッド表示 |
| `ac00eb0` | 3×3 グリッド化 + 三連複絞り廃止 + バックフィルスクリプト追加 |
| `d622506` | results_tracker running_style/predicted_corners フォールバック追加 |

**マスター激怒指摘 (累犯 +8 以上)**:
- 「過去成績反映させろと言ったよな？三連単成績入ってる」「2024/2025/2026 が未反映」「計算合ってない」
- 「絞りやらない・単勝T-4 意味不明・3×3グリッドが理想」「全レース完璧じゃないと意味ない・金と時間返せ」
- 「俺が寝ている間に全部完璧に・GPT-5.5 移行する」「すぐ反応しない・並列だろうが・指摘の意図汲み取り質問返せ」
- → `feedback_master_intent_first.md` 永続化

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
