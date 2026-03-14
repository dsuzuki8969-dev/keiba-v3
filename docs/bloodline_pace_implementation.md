# 血統×馬場・ペース別成績の実装ガイド

## 概要

父馬・母父馬の産駒成績を「距離別」「コース・馬場別」の両方で取得し、
さらに馬・騎手・調教師・血統の「ペース別成績」を補正に組み込む実装の詳細。

---

## 1. 血統×距離×馬場（horse/sire.html）

### 1.1 取得ソース

| type | 内容 | キー | 用途 |
|------|------|------|------|
| 2 | 距離別 | (bucket, surface) | sprint/mile/middle/long × 芝/ダート |
| 1 | コース・馬場別 | (surface, condition) | 芝/ダート × 良/稍重/重/不良 |

### 1.2 実装

- `src/scraper/sire_stats.py`
  - `fetch_sire_distance_stats()` … type=2
  - `fetch_sire_course_condition_stats()` … type=1
  - `parse_sire_course_condition_page()` … type=1 のHTMLパース
- `src/scraper/improvement_dbs.py`
  - `build_bloodline_db()` … type=2 と type=1 の両方を取得・統合
  - `calc_bloodline_adjustment()` … condition を追加し、距離60%+馬場40%でブレンド

### 1.3 補正ロジック

- 血統DB: `{sire: {id: {distance: {...}, course_condition: {...}}}, bms: {...}}`
- 今回レースの (distance, surface, condition) で照合
- 勝率・複勝率から -2.5〜+2.5pt の補正

---

## 2. ペース別成績

### 2.1 ペースの付与

馬の戦績テーブルにはペース列「35.7-36.0」のみで H/M/S ラベルがないため、
前半3F(秒)から距離・馬場ごとの閾値でペースを推定する。

- `src/utils/pace_inference.py`
  - `infer_pace_from_first3f(distance, surface, first_3f_sec) → PaceType`
  - 距離バケット×芝/ダートごとの閾値で HH/MM/SS を推定
- `src/scraper/netkeiba.py`
  - 戦績パース時に `first_3f` があれば `infer_pace_from_first3f` で pace を付与
- `src/scraper/improvement_dbs.py`
  - `ensure_pace_on_past_runs(horses)` … pace がない PastRun に推定値を付与

### 2.2 ペース別DB

- `build_pace_stats_db(horses)` で以下を集計:
  - horse: 馬ごとの H/M/S 別勝率・複勝率
  - jockey: 騎手ごと
  - trainer: 調教師ごと
  - sire / bms: 父馬・母父馬の産駒（past_runs から）

- 5段階 (HH/HM/MM/MS/SS) は `normalize_pace_to_3level()` で H/M/S に正規化して集計

### 2.3 補正

- `calc_pace_adjustment(horse, pace_type, pace_db)`
  - 今回予測ペースでの実績が良い馬・騎手・血統にプラス
  - 馬: 5走以上で勝率20%超 → +1pt、10走以上で8%未満 → -0.5pt
  - 騎手: 20走以上で勝率15%超 → +0.3pt
  - 血統: 15走以上で勝率12%超 → +0.2pt
  - 合計 -1.5〜+1.5pt

---

## 3. フロー（engine.analyze）

1. `build_jockey_horse_combo_db(horses)`
2. `build_bloodline_db(horses, netkeiba_client)` … type=2 と type=1 を取得
3. `build_pace_stats_db(horses)` … 内部で `ensure_pace_on_past_runs` 実行
4. 各馬に対して `_evaluate_horse(..., pace_db=pace_db)`
5. `calc_ability_deviation(..., bloodline_db, pace_db, pace_type)`
   - bloodline_adjustment（距離×馬場）
   - pace_adjustment
   - → total_adjustment に加算

---

## 4. 閾値・調整

### 4.1 ペース閾値（pace_inference.py）

距離バケット×芝/ダートごとに (H境界, S境界) を定義。  
例: 芝マイル 35.0/37.0 → 前半3F 35秒未満=H、37秒未満=M、以上=S。

### 4.2 血統補正の重み

- 距離別: 60%
- コース・馬場別: 40%
- 必要に応じて `improvement_dbs.py` 内の係数を変更

---

## 5. 今後の拡張案

- **race.netkeiba.com の data_list**: 競馬場・枠順・脚質別の種牡馬・母父成績
- **BLOODLINE_DB_PATH キャッシュ**: スクレイプ結果の永続化
- **調教師ペース補正**: `build_pace_stats_db` に trainer が含まれているため、`calc_pace_adjustment` で調教師分を追加可能
