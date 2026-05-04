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
| P1 | **B_prefix 1,253 件の netkeiba_id 補完** ← スクリプト準備完了 | 下記「連鎖起動コマンド」参照 |
| P1 | **2023 年生まれ若駒 568 件の horses 登録** ← スクリプト準備完了 | 下記「連鎖起動コマンド」参照 |
| P1 (完了) | ~~ML 47 モデル再学習~~ | ✅ 5/4 完走 (commit e851118) |

#### 連鎖起動コマンド一式 (T-063b 完走確認後・順次実行)

```
# ステップ 0: T-063b 完走確認 (朝 8:35 頃)
tail -5 log/backfill_sanrenpuku_20260504.log

# ステップ 1: B skipped 6,609 件 (既存スクリプト)
# 所要時間: 約 4 時間
powershell -ExecutionPolicy Bypass -File scripts\restart_backfill_b.ps1

# ステップ 2: B_prefix 1,253 件の netkeiba_id 補完 (新規スクリプト)
# 事前確認 (dry-run)
python scripts/backfill_b_prefix_horses.py --dry-run
# 本実行 (B skipped 完走後・または並列可能なら同時)
# 所要時間: 約 42 分 (1,253 件 × 2.0 秒)
python scripts/backfill_b_prefix_horses.py --execute

# ステップ 3: 2023 年生まれ若駒 568 件 horses 登録 (新規スクリプト)
# 事前確認 (dry-run)
python scripts/backfill_horses_2023h_retry.py --dry-run
# 本実行 (B_prefix 完走後)
# 所要時間: 約 12 分 (JRA 345 件 × 2.0 秒 + NAR 223 件)
python scripts/backfill_horses_2023h_retry.py --execute
```

**注意事項**:
- ステップ 1〜3 は必ず直列実行 (netkeiba 並列禁止 ★★ 違反歴 1 回)
- 危険時間帯 (06:00-06:30 / 22:00-23:30) は自動 abort
- 中断した場合は `--execute` を再実行するだけで再開 (マーカーファイルで管理)
- smoke test: `--max-fetch 10` オプションで少数件数テスト可能

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
