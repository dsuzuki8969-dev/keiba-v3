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

## 9. master への質問
1. 実装方針: **A(faithful・遅い)/ C(段階)/ 保留** のどれで進めるか
2. WF高速化(補助)を P0-b と並行するか
3. P0-b1(WF が持つ因子だけで部分 composite)を先行着手してよいか(低リスク・Δ測定の足場)
