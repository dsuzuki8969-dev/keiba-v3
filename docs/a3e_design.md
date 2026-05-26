# A-3e 設計書: フル engine 経由 calc_shobu_score を WF backtest に接続

> 作成: 2026-05-26 / **状態: Lv3 実装完了 (v3 セッション中)** / 残: 全期間 WF backtest 実行 + D-1c/D-1d Lv3 再検証

## ✅ Lv3 実装完了 (2026-05-26 v3)

| Step | 状態 | 成果物 |
|---|---|---|
| 1. RollingStatsTracker スキーマ把握 | ✅ | get_jockey_features / get_trainer_features / phase10b 確認 |
| 2. jockey 偏差値 helper | ✅ | `_jockey_winrate_to_dev` (Z 変換 中央 0.10 / 1σ=0.05) |
| 3. trainer 偏差値 + recovery_break + short_momentum 拡張 | ✅ | `_calc_shobu_score_wf_lv3` 内に統合 |
| 4. Lv3 関数 + KishuPattern.A 完全再現 | ✅ | `_horse_history` から前走 jockey 取得 → engine 仕様判定 |
| 5. mock テスト Lv1/Lv2/Lv3 比較 | ✅ | 8 ケース検証 (Case 7/8 で乗り替わり判定 engine 通り) |
| **6. 全期間 WF backtest --shobu-lv 3 --force** | 🔄 **実行中** (本セッション) | `logs/wf_lv3_full.log` |
| 7. D-1c/D-1d Lv3 再検証 | ⏳ Step 6 完了後 | `scripts/diag_d1d_strategy_a_overlap_jra.py` 再実行 |

---

## 背景

A-3d Lv1 (2026-05-26 マスター承認・commit 01ca494) で `_calc_shobu_score_wf` を WF backtest に追加したが、
これは RollingStatsTracker の win_rate ベースで主要 6 因子を簡易再現したもの。
フル engine `src/calculator/jockey_trainer.py:230 calc_shobu_score` との一致率は **8-9 割の見込み**。

A-3e でフル engine 経由に切り替え、戦略 B の WF backtest 精度を **100% 一致**まで上げる。

## フル engine `calc_shobu_score` の依存

```python
def calc_shobu_score(
    horse: Horse,                                    # is_jockey_change
    trainer: TrainerStats,                           # short_momentum / recovery_break / deviation
    jockey: JockeyStats,                             # (実際は使用していない)
    jockey_change_pattern: Optional[KishuPattern],   # KishuPattern.A 判定が必要
    is_long_break: bool,
    grade: str,
    last_grade: str,
    days_since_last_run: Optional[int],
) -> float:
```

加点ルール (合計 -1.0 〜 +10.5 pt):
| 因子 | 条件 | 加点 |
|---|---|---:|
| 騎手強化 | `KishuPattern.A` | +2.0 |
| 初コンビ | `horse.is_jockey_change` | +0.5 |
| 格上げ | `class_order` で grade > last_grade | +1.5 |
| 厩舎好調 | `trainer.short_momentum == "好調"` | +1.5 |
| 休み明け回収率高 | `is_long_break and trainer.recovery_break >= 120` | +1.5 |
| 休み明け精密 | `calc_break_adjustment(days, recovery, is_long_break)` | -1.0〜+2.0 |
| 調教師偏差値 | `trainer.deviation` 4 段階 | -0.5〜+1.5 |

## Lv1 (現状) との乖離

| 因子 | フル engine | Lv1 (現状) | 乖離原因 |
|---|---|---|---|
| 騎手強化 | KishuPattern.A (偏差値ベース) | jockey 90d win_rate > 15% | 尺度違い (偏差値 vs win_rate) |
| 厩舎好調 | trainer.short_momentum (短期-長期 dev 差) | trainer 90d win_rate > 12% | momentum 判定式が異なる |
| 休み明け精密 | calc_break_adjustment 関数 | **未実装** | tracker に休み明け回収率データなし |
| 調教師偏差値 | trainer.deviation (Z変換) | win_rate 4 段階 | 尺度違い |

→ **約 4 因子で乖離あり、合計 max 5.5 pt 程度のズレ可能性。一致率 8-9 割の根拠**。

## A-3e 実装ステップ (Lv1 → Lv2 → Lv3)

### Lv2 (本セッションで部分実装): tracker の win_rate を偏差値に変換 + calc_shobu_score 直接呼び

実装範囲:
1. **`_build_trainer_stats_from_tracker(tid, venue, date)` helper 追加**
   - tracker.get_trainer_features → win_rate を擬似偏差値に変換 (Z 変換: 平均 50, 標準偏差 10)
   - short_momentum: 90d win_rate > all_period win_rate + 0.05 で "好調"
   - recovery_break: 0.0 で固定 (tracker に集計なし)
2. **`_build_horse_obj(h)` helper**
   - is_jockey_change のみ Horse 互換 dict で代用
3. **`_calc_shobu_score_wf_lv2(h, race, tracker)` 新関数**
   - 上記 helper で構築 → `calc_shobu_score` 直接呼び
   - `jockey_change_pattern` は Lv1 と同様 `j_wr_90d > 0.15 → KishuPattern.A` で代用
   - `is_long_break = days_since_last_run >= 60`
4. **CLI `--shobu-lv {1,2}` フラグ追加**
   - デフォルト 1 (既存挙動温存)、2 でオプトイン
5. **mock test 追加** `scripts/test_shobu_lv1_vs_lv2.py`
   - 同 race / horse で Lv1, Lv2, フル engine の 3 通り出力を比較

期待: Lv1 と Lv2 で **数値は近いが完全一致せず** (KishuPattern 判定が偏差値ベースになるため微差)。一致率 9-9.5 割。

### Lv3 (次セッション以降): tracker 拡張 + 完全一致

実装範囲:
1. **RollingStatsTracker 拡張**
   - `get_jockey_features` に `upper_short_dev / upper_long_dev / lower_short_dev / lower_long_dev` 4 偏差値を追加
   - `get_trainer_features` に `deviation_z / short_momentum_z / recovery_break` 追加
   - 全期間 backfill 必要 (1 GB DB scan × 月数 = 数十分)
2. **KishuPattern 判定を tracker から完全再現**
   - 前走 jockey の偏差値が必要 → horse 履歴データから取得
3. **`calc_break_adjustment` を tracker の `recovery_break` フィールドで呼び出し**
4. **mock test で Lv3 と engine が完全一致を確認**
5. **戦略 B WF backtest を Lv3 で再実行 → D-1c の集計を更新**

工数: 2-3 日 (tracker 拡張 1d + 全期間 backfill 0.5d + 検証 0.5d + WF 再実行 0.5d)

## マスター承認事項 (5/26 帰宅後)

- A-3c shobu_score 近似 (×8.0) → 据え置き (A-3d Lv1 で再実装済)
- **A-3d を次セッション最優先 → A-3e でフル engine 実装** (Lv3 要求)
- Lv2 は隔離 (新関数 `_calc_shobu_score_wf_lv2`) で先行実装、既存パイプラインに影響なし

## リスク・トレードオフ

| 項目 | リスク | 対策 |
|---|---|---|
| tracker 拡張で WF backtest 速度低下 | 1 race あたり計算量 +30% 程度 | Lv3 はオプトイン (`--shobu-lv 3`)、デフォルトは Lv2 |
| 偏差値 Z 変換のサンプル数不足 | 短期 (2 ヶ月) で N<30 の jockey は σ ≈ 0 で偏差値が極端化 | サンプル N<30 では deviation=50.0 (中央) で固定 |
| 既存 WF backtest 結果との不一致 | A-3c で commit 済の shobu_score 値が変わる | A-3d で既に置換済、Lv3 で再度更新するが集計影響は再評価 |

## 関連ファイル

| ファイル | 役割 |
|---|---|
| `src/calculator/jockey_trainer.py:230` | フル engine `calc_shobu_score` |
| `src/models.py:421` `JockeyStats` / `:477` `TrainerStats` | データクラス |
| `src/scraper/improvement_dbs.py:588` `calc_break_adjustment` | 休み明け補正 |
| `scripts/walk_forward_backtest.py:148` `_calc_shobu_score_wf` | Lv1 簡易 (A-3d) |
| (新規) `scripts/walk_forward_backtest.py:?` `_calc_shobu_score_wf_lv2` | Lv2 (A-3e Step 1) |
| `src/ml/lgbm_model.py` `RollingStatsTracker` | tracker 本体 (Lv3 で拡張) |

## 検証計画

- **mock test**: 既知データで Lv1/Lv2/engine の数値一致率測定
- **integration test**: WF backtest 1 ヶ月 (5/26 直近) で `--shobu-lv 1` vs `--shobu-lv 2` 出力 diff
- **regression**: 戦略 B (TOP2) JRA ROI が D-1c の 141.8% から大きくずれないこと
