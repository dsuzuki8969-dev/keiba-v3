# Plan-γ: 能力指数 ハイブリッド設計（絶対比較 × 他馬比較）

**作成日**: 2026-04-27 17:50
**承認状態**: マスター承認待ち
**前提**: Plan-α (results_tracker.py:311 -50 拡張) commit 済み (7f434a8)

---

## Context（なぜやるか）

### 現状
- 能力偏差値は **完全にタイム比較**（コース基準タイム vs 走破タイム）
- 帯広ばんえい 200m で **上限 100 張り付き 12,148 件 / 下限 -50 張り付き 19,718 件**（全張り付きの 98%）
- 平地でも `ability_total = 20.0` 張り付きが起きていた（→ Plan-α で 1 行修正済み）

### マスター指示
- 「能力指数を他馬比較に変えたらどうメリットとデメリットが発生しそう？」
- 結論: **完全置換は推奨しない（D-Aikeiba 哲学「絶対実力」と矛盾）**
- → **ハイブリッド設計**（絶対指標を維持しつつ相対指標を追加）

### 期待される成果
1. ばんえい / 後着馬の張り付き完全解消
2. 馬券予想の本質「このレースで誰が強いか」と一致
3. 既存の馬指数推移（HorseHistoryChart）は壊さない
4. ML 特徴量に相対指数を追加して予測精度向上

---

## 設計

### 新カラム 2 種

| カラム | テーブル / 場所 | 計算式 | 用途 |
|---|---|---|---|
| `relative_dev` | `race_log` | 過去走の `run_dev` を同 race_id 内で z-score 正規化 | 履歴データの相対評価、ML 特徴量 |
| `race_relative_dev` | `pred.json` (各馬) | 当該レースの `ability_total` を同レース内で z-score 正規化 | 予想時のフィールド内位置付け、印付与 |

### 計算式（共通）

```python
def calc_relative_dev(values: list[float], target_value: float, sigma_floor: float = 5.0) -> float:
    """
    値群を 50 中心 σ=10 で正規化、±3σ クランプ
    sigma_floor: 標準偏差の下限（小レース等で σ が小さすぎる時の安定化）
    """
    if len(values) < 2:
        return 50.0
    mu = statistics.mean(values)
    sigma = max(statistics.stdev(values), sigma_floor)
    z = (target_value - mu) / sigma
    z = max(-3.0, min(3.0, z))  # ±3σ クランプ
    return 50.0 + 10.0 * z  # → 範囲: 20.0 〜 80.0
```

**設計判断**:
- z-score 採用（順位ベースより情報量多い、タイム差を反映）
- σ_floor=5.0 で小レース・同タイム多数時の安定化
- ±3σ クランプで外れ値の極端表示を抑制
- 範囲は 20-80 に収まるため、表示レンジ問題なし

### ハイブリッド合算

```python
ability_total_hybrid = ability_total * (1 - β) + race_relative_dev * β
# β = 0.30 (初期値、config/settings.py で可変)
```

**β の意味**:
- β=0.0: 完全に既存（絶対指標のみ）
- β=1.0: 完全に他馬比較（相対指標のみ）
- β=0.3: 絶対 70% + 相対 30%（推奨初期値）

---

## フェーズ分割（段階承認）

### Phase 1: データ層 (DB + バックフィル)
**所要**: 2-3 時間

- `race_log` に `relative_dev REAL` カラム追加（schema migration）
- 新規 `scripts/backfill_relative_dev.py`:
  - 全 race_id を走査
  - 同 race_id 内の `run_dev` を z-score 正規化
  - UPDATE で `relative_dev` 注入
- 検証: 全 race の `relative_dev` 平均≈50, σ≈10 になるか分布確認
- 帯広ばんえいで張り付き解消されるか確認

**🔔 マスター承認ポイント 1**: 分布の妥当性確認後 Phase 2 へ

### Phase 2: エンジン層 (pred.json 出力)
**所要**: 1-2 時間

- `engine.py` に `_calc_race_relative_dev()` ヘルパー追加（race_id 単位の正規化）
- `HorseEvaluation` に `race_relative_dev: float` フィールド追加 ([models.py](src/models.py))
- `results_tracker.py:311` 周辺に `race_relative_dev` 出力追加
- pred.json schema 拡張（旧フィールドは保持、互換性維持）
- 当日予想で `race_relative_dev` が出ることを確認

### Phase 3: ハイブリッド合算ロジック
**所要**: 2-3 時間

- `models.py AbilityDeviation` に `hybrid_total` プロパティ追加
- `config/settings.py` に `USE_HYBRID_SCORING: bool = False` フラグ + `HYBRID_BETA: float = 0.30`
- フラグ ON 時のみ印付与の判定値を `hybrid_total` に切替（A/B 比較可能）
- engine.py で `assign_marks` 関数の判定値を切替

**🔔 マスター承認ポイント 2**: β=0.30 の妥当性 / 印分布の変化確認後 Phase 4 へ

### Phase 4: ML 特徴量追加 + 再学習
**所要**: 半日 〜 1 日（学習時間含む）

- `src/ml/features.py` に `relative_dev` 特徴量追加
- `src/ml/lgbm_model.py` の `FEATURE_COLUMNS` に追加（159 → 160 features）
- `retrain_all.py` で再学習
  - 旧モデルは `data/models/.bak_pre_relative_dev/` にバックアップ
- バックテスト: 過去 3 ヶ月で旧モデル vs 新モデル比較

**🔔 マスター承認ポイント 3**: ML 精度劣化なしを確認後 Phase 5 へ

### Phase 5: フロント表示
**所要**: 1-2 時間

- `pred.json` から `race_relative_dev` をフロントに露出
- 馬カードに「絶対/相対 切替トグル」追加
  - HorseCardPC.tsx / HorseCardMobile.tsx
- 履歴グラフ（HorseHistoryChart）は既存通り `run_dev` ベース維持
- 「相対指数」タブ新規追加 → 同レース内の馬同士を z-score 表示

### Phase 6: バックテスト検証
**所要**: 1 日

- 新規 `scripts/backtest_hybrid_vs_absolute.py`
  - 直近 1-3 ヶ月の予想再評価（既存 race_log データのみで再計算）
  - 印的中率 / 回収率 / 三連単 F フォーメーション ROI を絶対 vs ハイブリッド比較
- 帯広ばんえいの張り付き解消 % 確認
- 結果次第で β 再調整（マスター承認）

**🔔 マスター最終承認**: バックテスト結果 OK で USE_HYBRID_SCORING=True に切替

---

## 工数合計

| Phase | 工数 | 累計 |
|---|---|---|
| Phase 1: データ層 | 2-3h | 3h |
| Phase 2: エンジン | 1-2h | 5h |
| Phase 3: 合算ロジック | 2-3h | 8h |
| Phase 4: ML 再学習 | 半日〜1日 | 16h |
| Phase 5: フロント | 1-2h | 18h |
| Phase 6: バックテスト | 1日 | 26h |
| **合計** | **3-4 セッション分** | |

---

## リスク・注意点

### リスク 1: ML 精度劣化
- **発生確率**: 中
- **対策**: 旧モデル保持、A/B 比較でロールバック可能
- **判断**: バックテストで AUC / Brier Score 等が劣化したら Phase 4 を撤回

### リスク 2: メンバー数依存の標準偏差差異
- **問題**: 5 頭立て以下で z-score 不安定
- **対策**: σ_floor=5.0 でクランプ、`field_count < 5` のレースは relative_dev 計算スキップ（NULL のまま、ハイブリッド合算では絶対指数のみ使用）

### リスク 3: pred.json schema 変更で旧フロント壊れる
- **対策**: 新フィールド `race_relative_dev` は **追加** のみ、既存フィールド削除なし
- フロントは未対応でも従来通り表示可能

### リスク 4: 帯広ばんえいで σ が極端に小さい
- **問題**: 全 8 頭立てで全頭近接タイム → σ_floor 効いても張り付き
- **対策**: ばんえい専用に **順位ベース** に切替（フォールバック）
  - `relative_dev = 50 + 10 × clip((N - rank) / N - 0.5, -0.5, 0.5) × √12`

### リスク 5: 既存 race_log 全件バックフィルの時間
- **問題**: race_log 467,457 行 × 同 race_id 内集計
- **対策**: SQL 1 文で `WINDOW 関数` 使用、推定 1-2 分で完了（PostgreSQL 風だが SQLite 3.25+ も対応）

---

## 中止・撤回条件

- ML 精度が AUC で 0.005 以上劣化したら Phase 4 撤回
- マスターが「絶対指標だけで十分」と判断したら全 Phase 撤回 → race_log.relative_dev は ML 特徴量として残し、表示はしない
- 帯広以外で異常な印分布が出たら Phase 3 撤回

---

## 関連ファイル

### 修正対象
- `src/database.py` (Phase 1: schema migration)
- `scripts/backfill_relative_dev.py` (Phase 1: 新規)
- `src/engine.py` (Phase 2-3: 計算ヘルパー + 印付与切替)
- `src/models.py` (Phase 2-3: HorseEvaluation + hybrid_total プロパティ)
- `src/results_tracker.py` (Phase 2: pred.json 出力)
- `config/settings.py` (Phase 3: フラグ + β)
- `src/ml/features.py` / `src/ml/lgbm_model.py` (Phase 4)
- `frontend/src/pages/TodayPage/HorseCardPC.tsx` / `HorseCardMobile.tsx` (Phase 5)
- `scripts/backtest_hybrid_vs_absolute.py` (Phase 6: 新規)

### 関連 memory
- [feedback_minimal_change_principle.md](~/.claude/projects/.../memory/feedback_minimal_change_principle.md): 段階承認 + 撤回可能性
- [feedback_no_easy_escape.md](~/.claude/projects/.../memory/feedback_no_easy_escape.md): 「order rank base にして安易な解決」を選ばない
- [project_kpi_targets.md](~/.claude/projects/.../memory/project_kpi_targets.md): 印別成績 / 自信度別的中率の影響評価

---

## マスター承認待ち項目

### 質問 1: フェーズ進行方針
- **A**: 全 Phase を一気に走らせる（私が責任もって完走）
- **B**: Phase 1 だけ実装してマスター承認 → Phase 2 以降は別セッション
- **C**: Phase 1 + 2 まで本セッション内、Phase 3 以降は別セッション

私の推奨: **B**（β=0.30 の妥当性は実データ見ないと判断できず、Phase 3 の前にマスター判断必要）

### 質問 2: β 初期値の方針
- **A**: 0.30（既存指標重視、保守的）
- **B**: 0.50（半々、フラットな設計）
- **C**: バックテスト結果から最適化（Phase 6 で決定）

私の推奨: **A → C**（最初は 0.30 で慣れて、Phase 6 で最適化）

### 質問 3: USE_HYBRID_SCORING フラグの初期値
- **A**: False（既存挙動維持、フラグで切替可能だけ用意）
- **B**: True（Phase 3 完了時点で全予想が hybrid に切替）

私の推奨: **A**（マスター確認後に True 切替）

### 質問 4: 帯広ばんえいの扱い
- **A**: 共通ロジック（σ_floor のみで対応）
- **B**: 順位ベースにフォールバック（リスク 4 の対策）

私の推奨: **B**（ばんえいは特殊仕様）

---

## 検証方法（各フェーズ）

| Phase | 検証内容 |
|---|---|
| 1 | `SELECT venue_code, AVG(relative_dev), STDEV(relative_dev) FROM race_log GROUP BY venue_code` で全 venue が μ≈50, σ≈10 |
| 2 | `python -c "import json; pred=json.load(open('data/predictions/最新_pred.json')); ..."` で `race_relative_dev` 出力確認 |
| 3 | フラグ ON で印分布が大きく変わらないことを Playwright で目視（HOMEページ） |
| 4 | `python scripts/backtest_*.py` で旧 vs 新 AUC 比較 |
| 5 | Playwright で「絶対/相対」切替動作確認 |
| 6 | 印別 ROI 表示で hybrid 採用判断 |

---

## 補足: なぜ「他馬比較完全置換」を選ばないか

| 観点 | 完全置換 | ハイブリッド (本案) |
|---|---|---|
| 張り付き解消 | ◎ | ○ |
| ばんえい問題 | ◎ | ○ |
| D-Aikeiba 哲学一致 | ✗ | ◎ |
| 履歴グラフ | ✗（無意味化） | ◎（既存維持） |
| 重賞経験馬の格差 | ✗（消失） | ◎（保持） |
| ML 連続性 | ✗ | ○ |
| 実装工数 | 中（破壊的） | 大（追加的） |

**結論**: 工数は大きいが、ハイブリッドは「壊さずに拡張する」アプローチで、撤回も可能。完全置換は不可逆的。

---

以上、マスター承認お願いします。
