# TASKS.md — D-AI Keiba v3 タスクボード

> **運用ルール**: CLAUDE.md「TASKS.md / MEMORY.md 運用ルール」を参照。
> マスター指示はすべてここに追加。完了タスクは git log + handoff_*.md に集約済のため本ファイルから削除する。
> 教訓・反省は `~/.claude/projects/C--Users-dsuzu-keiba-keiba-v3/memory/` に永続化。

---

## 🔴 作業中のタスク

### T-069 (P0 → 暫定対応済 / P2 残課題) — DAI_Keiba_Predict Disable で恒久回避
- **暫定対応 (5/4 完了)**: `Disable-ScheduledTask -TaskName "DAI_Keiba_Predict"` で無効化済
  - 明朝 06:00 の `VBS_END ec=255` 失敗ログ累積が停止
  - 代替: Predict_Tomorrow (前日 17:00) で予想生成済のため実害ゼロ
- **真因 (記録)**: Principal.LogonType=Interactive → マスター就寝中 (ログオフ状態) で cmd 起動不可
- **本格修復が阻まれた理由**: マスターアカウントが Microsoft アカウント (`dsuzuki8969@gmail.com`) かつ「Windows Hello のみ許可」が ON = ローカルパスワード入力経路が UI 上消えている
- **将来の本格修復手順** (P2・優先度低・Predict_Tomorrow で代替できているため不急):
  1. Microsoft アカウントのパスワードを確認/リセット (`https://account.live.com/password/reset`)
  2. 管理者 PowerShell で `.\scripts\fix_predict_task_logon.ps1` 実行 → Microsoft アカウントパスワード入力
  3. `Enable-ScheduledTask -TaskName "DAI_Keiba_Predict"` で再有効化
  4. 翌朝 06:00 で `bat_trace.log` の BAT_START 行確認

### T-070 (P1・新規・マスター手動必須) — タスクスケジューラ整理
- ✅ 削除済: D-AI Keiba Dashboard (Disabled)、DAI_Batch_Reanalyze (Ready/3 月以降未実行)
- ❌ アクセス拒否で削除不可 (要管理者権限):
  - `KeibaStreamlit` (Disabled)
  - `DAI_Keiba_Tunnel` (旧版・D-AI Keiba Cloudflared と重複)
- **実行コマンド** (管理者 PowerShell):
  ```
  Unregister-ScheduledTask -TaskName "KeibaStreamlit" -Confirm:$false
  Unregister-ScheduledTask -TaskName "DAI_Keiba_Tunnel" -Confirm:$false
  ```

### T-063b (P1・準備完了・マスター手動実行待ち) — 2025 年三連複 payouts 再取得
- **対象**: 16,208 件 (dry-run 結果)
- **スクリプト**: `scripts/backfill_sanrenpuku_payouts_2025.py` (準備完了)
- **推定所要時間**: 約 9.0 時間
- **安全装置 5 項目**: --execute 必須 / 危険時間帯自動 abort / 競合プロセス検出 / 中断再開 / レート制限 2.0 秒/件
- **マスター実行コマンド** (推奨 24:00 以降):
  ```bash
  python scripts/backfill_sanrenpuku_payouts_2025.py --dry-run    # 再確認
  python scripts/backfill_sanrenpuku_payouts_2025.py --execute &  # 本実行
  tail -f logs/backfill_sanrenpuku_*.log                          # 進捗確認
  ```

### T-NEW-P1 (P1・新規) — HorseEvaluation.is_scratched 属性追加
- **発見経緯**: T-NEW-P0 緊急バグ修正中に副次バグとして発見
- **問題**: `src/calculator/betting.py` L2500/2683/2787/2867 の 4 箇所で `getattr(e, "is_scratched", False)` が常に False を返す。HorseEvaluation / Horse 両方に is_scratched 属性なし。取消馬除外フィルタが effectively no-op
- **暫定**: is_tokusen_kiken フィルタが代替動作中のため致命的影響なし
- **修正方針**: src/models.py の HorseEvaluation に is_scratched: bool = False を追加 + engine.py で実体化時に horse.is_scratched (なければ追加) から伝搬
- **工数**: 60 分

---

## 🟡 将来課題（次セッション以降）

### P1

| 優先度 | 項目 | 状態 / 条件 |
|:---:|---|---|
| P1 | 取消馬誤検知 (水沢 12R 等) 真因究明 | 「取消馬により買い目無効」テキストの出所特定 |
| P1 | build_sanrentan_tickets vs pred.json fixed 整合性検証 | LIVE STATS 修正完了後にバックテスト突合 |
| P1 | B_prefix 1,253 件の対応 | NAR 公式コード突合 or netkeiba スクレイピング |
| P1 | 2023 年生まれ若駒 339 件 | netkeiba 403 → 自動補完待ち |
| P1 | B skipped 6,609 件の再 apply | キャッシュ蓄積後 `restart_backfill_b.ps1` 再実行 |
| P1 | ML 47 モデル再学習 (retrain_all.py) | B 完走 +34,477 行で AUC 向上余地 |

### P2

| 優先度 | 項目 | 状態 / 条件 |
|:---:|---|---|
| P2 | netkeiba 並列リクエスト禁止の構造強化 | feedback_netkeiba_concurrent_throttle 関連 |
| P2 | B_prefix race_log 残存 33,779 件 | 整合済だが将来的に netkeiba_id 統合余地 |

---

## 🟢 過去の完了タスク

過去セッションの完了タスクは git log + handoff_*.md に集約済。本ファイルからは削除した。

参照先:
- 5/4 後半: 本日 commit 群 (fcf96b5 整理 / c2150f3 T-063 / acc1b99 T-058 / e1cda93 T-047 / 717dbaf T-065)
- 5/3-5/4: `memory/handoff_2026-05-04.md` (M' 戦略本実装 + γ案修正 + Phase 3c)
- 5/2: `memory/handoff_2026-05-02.md`
- 5/1: `memory/handoff_2026-05-01.md`
- 4/30: `memory/handoff_2026-04-30.md`
- 4/29: `memory/handoff_2026-04-29.md`
- 4/28: `memory/handoff_2026-04-28_v2.md`
- 4/26-27: `memory/handoff_2026-04-27_v5.md`
