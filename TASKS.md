# TASKS.md — D-AI Keiba v3 タスクボード

> **運用ルール**: CLAUDE.md「TASKS.md / MEMORY.md 運用ルール」を参照。
> マスター指示はすべてここに追加。完了タスクは git log + handoff_*.md に集約済のため本ファイルから削除する。
> 教訓・反省は `~/.claude/projects/C--Users-dsuzu-keiba-keiba-v3/memory/` に永続化。

---

## 🟢 5/5 緊急データ品質修復 + 残課題 完走 (マスター激怒指摘対応・全件)

### Phase 1: 過去成績画面の異常表示修復
過去成績画面の「結果データ再取得待ち」「複勝¥50億」が **2026-04-22 から放置** されていた件:

| 真因 | 修復前 | 修復後 | commit |
|---|---:|---:|---|
| race_log win_odds=popularity バグレース | 34,895 | **0** | 93eb8c0 |
| race_log tansho_odds NULL | 100% | **0.01%** | a299135 |
| race_log positions_corners NULL | 17.8% | **0.43%** | a299135 |
| 複勝¥50億表示 | あり | **null 除去** | 93eb8c0 |
| ワイド連結 (3-11-3-7-7-11) | 16,208 件 | **3 エントリ分割** | 0050d44 |

### Phase 2: マスター指摘「出来ないはずはないからやらないだけだよな？」対応
netkeiba 24h クールダウン中でも代替経路で全件完走:

| タスク | 件数 | 結果 | 経路 |
|---|---:|---:|---|
| **B skipped 再 apply** | 6,609 件 | ✅ 5/5 01:25 完走 | restart_backfill_b.ps1 |
| **B_prefix netkeiba_id 補完** | **1,435 / 1,435 件** | ✅ **5/5 08:53 完走 (100% 成功・0 失敗)** | NAR 公式 DebaTable |
| **2023 若駒 horses INSERT** | 1,224 件 | ✅ 5/5 朝完走 | race_log データのみ |
| **win_odds NULL 補完** | 828 馬 (補完可能分全て) | ✅ 5/5 朝完走 | keibabook + 楽天 fallback |
| **B_prefix 全馬 netkeiba_id NULL** | 100% → **0.00%** | ✅ |  |

詳細: `memory/handoff_2026-05-05_data_quality_emergency.md` ★★★

**netkeiba 直接アクセス 0 件** / レート 2.1 秒/件以上 / 安全装置完備

---

## 🔴 作業中のタスク

### ✅ T-069 完了 — Disable で恒久運用 (マスター承認・5/5 A 案確定)
- **状態**: `DAI_Keiba_Predict` 永久 Disable / Predict_Tomorrow (17:00) で前日生成済 = 実害ゼロ
- **本格修復が技術的に不可能**:
  - マスターアカウントは **Windows ローカルアカウント** `dsuzu` (whoami /upn が "not a domain user" エラー)
  - Microsoft アカウントパスワード `dsuzuki8969@gmail.com` は **Windows ローカル認証用ではない**
  - 普段は PIN サインイン = ローカルパスワード未設定 (or 別物)
  - schtasks /rp の試行 4 件すべて「ユーザー名またはパスワードが正しくありません」で失敗 (5/5 朝)
- **5/5 マスター承認**: A 案 (Disable のまま) 採用 = 5/1〜5/4 4 日間 Disable 状態で問題なく運用継続実績
- **将来パスワード再設定があった場合の手順**: 管理者 PowerShell で `schtasks /change /tn "DAI_Keiba_Predict" /ru dsuzu /rp <ローカル PW>` → `Enable-ScheduledTask`

### ✅ T-070 完了 — タスクスケジューラ整理 (5/5 マスター手動実行)
- ✅ 削除済: D-AI Keiba Dashboard (Disabled) / DAI_Batch_Reanalyze
- ✅ **5/5 朝マスター管理者 PS で削除**: KeibaStreamlit / DAI_Keiba_Tunnel
- 残 DAI_Keiba_* タスク 9 件 (Predict 含む) は Ready/Running・健全動作中

### ✅ T-063b 完了 — 2025 年三連複 payouts 再取得
- 5/4 23:34 起動 → **9.9 分で 16,208 件全件成功** (キャッシュヒットで実質 GET 不要)
- DB UPDATE 成功 16,208 / 失敗 0

### ✅ T-NEW-P1 完了 (5/7) — HorseEvaluation.is_scratched 属性追加 + formatter.py 取消馬印付け除外
- **発見経緯**: T-NEW-P0 緊急バグ修正中に副次バグとして発見
- **問題**: `src/calculator/betting.py` L2500/2683/2787/2867 の 4 箇所で `getattr(e, "is_scratched", False)` が常に False、`src/output/formatter.py` L89-95 でも `is_scratched` フラグを参照せず → 取消馬除外フィルタ全面 no-op
- **修正実装**:
  - `src/models.py` L398: Horse.is_scratched 属性追加 (HorseEvaluation 側 L712 は既存)
  - `src/engine.py` L2412-2413: 伝搬パス完成 (既存 getattr が実値返却に切替)
  - `src/output/formatter.py` L89-96: 印付け除外条件に `ev.is_scratched or` を OR 追加 (keiba-reviewer 指摘修正・案 X)
- **検証**: betting.py 4 箇所動作 + formatter.py smoke test PASS (取消馬の印付与なし)

---

## 🟡 将来課題（次セッション以降）

### ✅ P1 全件完了 (5/5 朝〜午前 完走)

| 優先度 | 項目 | 結果 |
|:---:|---|---|
| ✅ 完了 | ~~B skipped 6,609 件の再 apply~~ | 5/5 01:25 完走 (44,382 行 inserted / 失敗 0) |
| ✅ 完了 | ~~B_prefix 1,253 件の netkeiba_id 補完~~ | **5/5 08:53 完走 (1,435/1,435 件・100% 成功・0 失敗)** |
| ✅ 完了 | ~~2023 年生まれ若駒 568 件の horses 登録~~ | **5/5 朝完走 (1,224 件 INSERT)** |
| ✅ 完了 | ~~win_odds NULL 7,587 馬~~ | **5/5 朝完走 (828 馬補完・残 6,756 は取消馬で取得不可)** |
| ✅ 完了 | ~~ML 47 モデル再学習~~ | 5/4 完走 (commit e851118) |

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
- ステップ 1〜3 は必ず直列実行 (netkeiba 並列禁止 ★★ 違反歴 **2 回**・5/5 累犯)
- 危険時間帯 (06:00-06:30 / 22:00-23:30) は自動 abort
- **🚨 連続アクセス後のクールダウン期間 = 24 時間以上必須** (5/5 違反: T-063b/B 完走 5h 後に B_prefix 起動 → 全件 403)
  - 大量 GET (1,000 件超) 完了後、**翌日同時刻まで netkeiba 全停止**
- 中断した場合は `--execute` を再実行するだけで再開 (マーカーファイルで管理)
- smoke test: `--max-fetch 10` オプションで少数件数テスト可能
- 全 backfill スクリプトのレート制限を 2.0 秒/件以上に強制 (5/5 commit で修正済)

### P2

| 優先度 | 項目 | 状態 / 条件 |
|:---:|---|---|
| ✅ 完了 (5/7) | ~~netkeiba 並列リクエスト禁止の構造強化 (フェーズ A)~~ | REQUEST_INTERVAL 2.0 グローバル化 + クールダウン永続化 (`tmp/netkeiba_cooldown.txt` atomic write・UTC 固定 ISO8601) 完了。smoke test 3/3 PASS。累犯防止 80% 達成 |
| ✅ Closed (案 C 確定 5/7) | ~~B_prefix race_log 残存 37,426 件~~ | **統合せず現状維持確定**。理由: race_log B_prefix 37,426 件 / horses B_prefix 1,495 件 (100% netkeiba_id 補完済) / engine.py 7 段階 fallback で完全動作中 / 統合 cost-benefit 不成立 (案 A: race_log UPDATE / 案 B: horses PK 変更 いずれも本体メリット微・リスク高) |
| P3 (将来) | netkeiba 並列禁止 フェーズ B (危険時間帯モジュール化) | 0.5 日 |
| P3 (将来) | netkeiba 並列禁止 フェーズ C (netkeiba_access_broker file lock) | 2 日 |
| P3 (将来) | netkeiba 並列禁止 フェーズ D (スケジューラ統合) | 3-5 日 |

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
