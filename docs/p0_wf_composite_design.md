# P0-b 設計: WF backtest を本番 composite 印で評価する

> 2026-06-23 自走セッションで作成。roadmap 根本①の本丸。**実装方針(A/B/C)は master 決定事項**。
> 関連: `memory/project_improvement_roadmap.md` / `feedback_production_vs_wf_pred_distinction`

## 1. 目的
WF backtest(`scripts/walk_forward_backtest.py`)が本番の 7因子 composite で印を付け直し、
過去の全ROI数値を「本番で実際に走る系」で測れるようにする。現状 WF は ML複勝確率 `prob×100`
だけで印を決め(`:498`)、ability/pace/course/jockey/bloodline を 0.0 ハードコード(`:505-520`)。

## 2. 本番 composite 式 (`src/models.py:777-795` — 再現対象)
```
v = ability.total × w["ability"] × training_mult     # 能力偏差値
  + pace.total    × w["pace"]    × training_mult     # 展開偏差値
  + course.total  × w["course"]                      # コース適性
  + jockey_dev    × w["jockey"]   (None→50)          # 騎手
  + trainer_dev   × w["trainer"]  (None→50)          # 調教師
  + bloodline_dev × w["bloodline"](None→50)          # 血統
  + calc_weight_change_adjustment(...)               # 馬体重増減
  + odds_consistency_adj                             # オッズ整合性
  + ml_composite_adj                                 # ★ML信号 ±5pt (engine.py:1784-1804)
  + market_anchor_adj
→ clamp[DEVIATION["composite"]min/max]
```
重み: `get_composite_weights(venue, surface, field_size, distance)`(`config/settings.py`)

## 3. WF が「持つ」vs「持たない」
| 因子 | WF で再現可能か | 手段 |
|---|---|---|
| jockey_dev / trainer_dev / bloodline_dev | ✅ 可能 | WF の `RollingStatsTracker`/`sire_tracker`(shobu_score で既使用) |
| ml_composite_adj (±5pt) | ✅ 可能 | WF の ML win_prob から engine.py:1784-1804 の z-score 式を移植 |
| 馬体重補正 / odds整合性 / market_anchor | △ 一部 | weight_change は horse data にあり / odds系は要オッズ |
| composite 重み | ✅ 可能 | `get_composite_weights` をそのまま import |
| **ability.total** | ❌ 困難 | engine の ability エンジン(過去走 deviation)要 |
| **pace.total** | ❌ 困難 | engine の pace 予測(脚質/隊列)要 |
| **course.total** | ❌ 困難 | engine の course DB 適性要 |

→ **困難な3因子(ability/pace/course)が composite 重みの大半**を占める(`w["ability"]`等が支配的)。
ここを再現せずに composite を組むと本番非再現のまま=P0 の目的未達。

## 4. leak 考慮(重要)
- ability/pace/course は**過去走ベース=理論上 leak-free**(対象レースの結果を使わない)。
- ただし**本番 pred.json の流用は不可**: L-1 timestamp leak(`feedback_production_vs_wf_pred_distinction`)で
  過去レースが後追い予想=時系列リーク。WF 内で **point-in-time 再計算**が唯一安全。

## 5. 実装選択肢(master 決定)
| 案 | 内容 | faithful度 | 速度 | リスク |
|---|---|---|---|---|
| **A. full engine 統合** | WF ループで `RaceAnalysisEngine.analyze()` を point-in-time 実行し composite を取得 | ◎ 完全 | ✕ 大幅増(現2-3h→数倍) | engine の DB/tracker を WF の leak-free 状態で駆動する配線が複雑 |
| **B. 部分 composite** | jockey/trainer/bloodline/ml_adj は実計算、ability/pace/course は ML prob を代理 | △ 不完全 | ◎ 速い | 本番非再現=P0目的を半分しか満たさない(誤誘導リスク) |
| **C. ハイブリッド** | ability を WF tracker の過去走 deviation で簡易再現、pace/course は簡略 | ○ 中 | ○ 中 | 簡易再現の精度検証が research |

## 6. 推奨
- **本筋は A(full engine 統合)**。P0 の目的「本番系を正しく測る」を満たす唯一の faithful 案。
- ただし速度(イテレーション不能)が課題 → **A + WF高速化(roadmap補助項目)をセットで**検討。
- **C は A への踏み石**として有効(ability だけ先に再現し Δ を測る段階実装)。
- B は「速いが間違った物差し」=現状(prob×100)と同類で**非推奨**。

## 7. 段階実装プラン(A 採用時)
1. **P0-b1**: ml_composite_adj + jockey/trainer/bloodline_dev を WF に移植(WF が既に持つ因子で composite を部分構築)。← 低リスク・着手可
2. **P0-b2**: ability.total を WF の過去走 deviation で再現(engine `ability.py` の deviation 計算を point-in-time 化)。← research
3. **P0-b3**: pace/course を engine 直呼び or 再現。← 重い
4. 各段階で WF を1月走らせ、印一致率(本番 pred vs WF)+ ROI Δ を測定。

## 8. 既に完了(本セッション P0-a)
- **較正指標 Brier/logloss を WF 評価ループに追加済**(`walk_forward_backtest.py` `_process_month`)。
  ML複勝確率の calibration を全月加重平均で出力。印ロジック不変・測定のみ。

## 9. master への質問(回答済 2026-06-23)
master 決定: **C ルート(段階実装)で足場から着手。手順は委任。バグ・欠陥なく慎重に。**
→ §10 の検証で「C(特徴量近似)は不正確」と判明 → 段階実装を**案A(engine統合)ベースに精緻化**。

---

## 10. 検証結果(2026-06-23 自走・並列Explore 2体 + 直接裏取り) — 経路確定 + leak解消

### 経路確定: **案A(engine統合)**。案C(特徴量近似)は降格
- **案C 降格(不採用)**: WF特徴量は**着順ベース**(dev_run1_adj 等 `lgbm_model.py:1849`)。本番 ability.total は**走破タイムベース偏差値**(`ability.py:1451-1750` `calc_ability_deviation`)。近似精度 70-80% =「不正確な物差し」で P0 目的(正しく測る)に反する。
- **案A 確定**: ability/pace/course を historical 日付の engine で再現。計算箇所 = `ability.py:1451`(能力)/ `pace_course.py:766`(展開)/ `pace_course.py:1455`(コース)。

### ✅ leak-safety 検証済(最大リスク = 解消)
- 本番 course_db 構築は **`_window_end = 対象日 − 1日`**(`run_analysis_date.py:231-232`)で対象日**より前**に限定。派生DB(course_style/gate_bias/position_sec)も `target_date=DATE` でフィルタ(`:248-250`)。
- → **engine を historical 日付で leak-free に駆動する機構は既存**。WF は同じ windowed build を `_window_end = race_date − 1` で複製すればよい。
- システムは過去日再生成の leak を認識済(「学習リーク有」警告 `run_analysis_date.py:107`)= 安全側設計。

### 実装の実体(= なぜ 2-3日 MEDIUM か)
engine は `course_db` を必須引数に要求(`engine.py:352`)+ 派生 5-6 DB(course_style/gate_bias/position_sec/l3f/trainer_baseline)+ Horse(past_runs 付き)供給 + `analyze()`。
**= `run_analysis_date.py` の前処理パイプライン(`:218-252`)を WF に leak-free 複製**する作業。WF は現状 course_db を持たない(ML tracker のみ)。

### fidelity 注意
- WF の win_prob は `prob×0.4` の**代用**(`walk_forward_backtest.py:500`)。本番 ml_composite_adj は実 win_prob の z-score(`engine.py:1784`)。忠実には WF にも実 win モデル要(or place-prob 代替+差分明示)。

### 🎯 重要発見: 再利用テンプレートが既存(from-scratch 不要)
`scripts/batch_repredict.py` が **既に「historical 日付で leak-safe に engine build → analyze」を実装済**:
- windowed course_db `_window_end=DATE-1`(`batch_repredict.py:138-139,143-144`)= leak-safe
- 派生DB build(`:151-154`)/ engine 生成(`:220`)/ `engine.analyze(race_info, horses)`(`:231`)
- → **P0-b は from-scratch でなく、この実証済パターンの流用**。統合が大幅に簡素化+低リスク化。
- ✅ **past_runs leak も解消確認**: `target_date=DATE` が `scraper.fetch_race(target_date=DATE,prefer_cache=True)`(`:172-174`)/ `build_course_db_from_past_runs(target_date=DATE)`(`:192`)/ `RaceAnalysisEngine(target_date=DATE)`(`:229`)に**横断伝播**=履歴を対象日前にフィルタ。`prefer_cache=True` で net 回避(既分析日はキャッシュヒット)。**leak-safety は target_date 伝播で完全担保**。

### 慎重な段階手順(各ステップで検証・leak を gate に)
1. **隔離 probe(追加・本番非改変)**: `batch_repredict.py:73,133-231` の engine-build-analyze を read-only に流用し、1 historical 日付×数レースで composite を取得 → (a)値が妥当か (b)perf 秒/レース (c)horse past_runs が leak-free か を実証。
2. **隔離 probe**: 1 historical 日付 × 数レースで `analyze()` → composite が出るか + perf(秒/レース)+ window 境界(=date−1)で leak-free を実証。**本番/WF を一切触らない**。
3. probe OK → WF `_process_month` に組込み(月初 engine build・各レース `analyze()` で composite 取得)→ `_assign_marks` を composite 順に切替。
4. **1月WF**で「本番 pred 印 vs WF 印 一致率」+ Brier/logloss(P0-a)測定。
5. 妥当なら全期間。**各段階で commit**。ability/pace/course が揃うまで印は prob 維持し composite は parallel 測定(印崩壊回避)。

### 推奨(更新)
**案A・段階実装。Step1(engine builder 追加=本番非改変・低リスク)から。leak 検証を各ステップ gate に。** 盲目的な印切替はしない。

### 本セッションの到達点
経路確定(案A)+ leak-safety 検証済 + 正確スコープ + **隔離 probe で案A end-to-end 実証完了**(§11)。

---

## 11. ✅ probe 実証結果(2026-06-23・案A PROVEN)

`scripts/diag_p0_engine_composite.py`(batch_repredict.py 流用・read-only・gitignore ローカル diag)を **2026-06-21** で実行(本番非影響):

- **本番7因子 composite が leak-free に計算された**。例 函館3R:
  | 馬 | 印 | composite | ability | pace | course | ml_adj |
  |---|---|---|---|---|---|---|
  | グレイトソン | ◎ | **62.41** | 61.56 | 64.32 | 59.19 | 4.39 |
  | マーマレードスカイ | ○ | 56.32 | 59.40 | 57.19 | 47.50 | 0.72 |
  → **ability 主導の本番式そのもの**(WF の `prob×100`・7因子0.0 とは別物)。印も composite 順で本番同等。
- **leak-free**: window=2025-06-21〜2026-06-20 + target_date 伝播。
- **read-only 確認**: DB・ファイル書込なし。
- ⚠️ **perf = 12.7秒/レース**(engine を毎レース生成込)。setup ~3分/月。
  - 全期間WF=数時間/月 → **WF高速化が P0-b 実用化に必須**(roadmap補助)。
  - **最適化余地**: engine を月1回生成(course_db/派生DBは月共通・jockey/trainer_dbのみ全件渡し)+ analyze のみループ → 毎レース init オーバーヘッド除去で短縮可。
  - 検証は当面 **1月サンプル or レース数限定**で回す。

### → 案A は PROVEN。残りの実装
- **Step3(WF統合)**: `_process_month` に「月1回 engine build + 各レース analyze → composite で `_assign_marks` 再ランク」を移植(probe パターン流用)。perf のため engine は月1回生成。
- **Step4**: 1月サンプルWF で「本番 pred 印 vs WF 印 一致率」+ Brier/logloss 測定。
- **高速化**: 上記 engine 月1回生成 + 必要なら analyze の重い部分を間引き。
- 各段階 commit。印切替は composite が揃った Step3 で(probe で揃うこと実証済=安全に切替可)。
