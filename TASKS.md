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

### T-063b (P1・5/4 23:35 BG 自動起動済) — 2025 年三連複 payouts 再取得
- **状態**: 5/4 21:43 BG 予約起動済 (PID 94864 / sleep 6711 秒で 23:35 起動)
- **対象**: 16,208 件 / 推定 9.0 時間 (朝 8:35 完走見込み)
- **ログ**: `log/backfill_sanrenpuku_20260504.log`
- **進捗確認**: `tail -f log/backfill_sanrenpuku_*.log`
- **完走後の処理**: B 系 / B_prefix / 2023 若駒 を順次起動可能 (詳細は下記)

### T-NEW-P1 (P1・新規) — HorseEvaluation.is_scratched 属性追加
- **発見経緯**: T-NEW-P0 緊急バグ修正中に副次バグとして発見
- **問題**: `src/calculator/betting.py` L2500/2683/2787/2867 の 4 箇所で `getattr(e, "is_scratched", False)` が常に False を返す。HorseEvaluation / Horse 両方に is_scratched 属性なし。取消馬除外フィルタが effectively no-op
- **暫定**: is_tokusen_kiken フィルタが代替動作中のため致命的影響なし
- **修正方針**: src/models.py の HorseEvaluation に is_scratched: bool = False を追加 + engine.py で実体化時に horse.is_scratched (なければ追加) から伝搬
- **工数**: 60 分

---

## 🟡 将来課題（次セッション以降）

### P1 (T-063b 完走後・朝 8:35 以降に直列実行)

netkeiba 並列禁止 (★★ 違反歴 1 回) のため、T-063b 完走を待ってから直列実行する。

| 優先度 | 項目 | 起動コマンド (T-063b 完走後) |
|:---:|---|---|
| P1 | B skipped 6,609 件の再 apply | `powershell -ExecutionPolicy Bypass -File scripts\restart_backfill_b.ps1` |
| P1 | B_prefix 1,253 件の対応 | 別途専用スクリプト準備が必要 (NAR 公式コード突合 or netkeiba 馬詳細) |
| P1 | 2023 年生まれ若駒 339 件 | netkeiba 403 自動補完待ち or 手動 |
| P1 (完了) | ~~ML 47 モデル再学習~~ | ✅ 5/4 完走 (commit e851118) |

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
