# 各項目の実装解説

枠順バイアス、コース脚質バイアス、騎手展開影響、騎手コース影響、厩舎評価、調教、勝負気配、改修後コースフィルタの実装概要。

---

## 1. 枠順バイアス (G-2)

**場所**: `src/calculator/pace_course.py` の `calc_gate_bias()`

**入力**: 馬番、頭数、コース（CourseMaster）

**ロジック**:
1. 7頭以下 → 0.0（バイアスなし）
2. 8〜9頭 → 3ゾーンに分割
3. 10頭以上 → 5ゾーンに分割（内→外の順）
4. 芝×5ゾーン: 内枠有利（zone 0→+2pt, 1→+1pt, 2→0, 3→-1pt, 4→-2pt）
5. ダート×5ゾーン: 外枠有利（zone 0→-1.5pt, 4→+1.5pt など）
6. その他 → 0.0

**出力**: -5〜+5 pt（展開偏差値の内訳❸）

**データソース**: なし（ルールベース・固定値）

---

## 2. コース脚質バイアス

**2種類ある**:

### 2a. ルールベース（実装で使用中）

**場所**: `PaceDeviationCalculator._calc_course_style_bias()`

**入力**: CourseMaster（直線長・坂タイプ・コーナー）、馬の脚質（RunningStyle）

**ロジック**:
- 直線 ≥420m × 差し/追い込み → +3pt
- 直線 ≤300m × 逃げ/先行 → +3pt
- 急坂 × 逃げ/先行 → -2pt
- 小回り × 逃げ/先行 → +2pt

**出力**: -5〜+5 pt（展開偏差値の内訳❹）

### 2b. データ駆動（未使用）

**場所**: `calc_style_bias_for_course()`  
**呼び出し**: `enrich_course_aptitude_with_style_bias()` ※現状どこからも呼ばれていない

**想定ロジック**: course_style_stats_db（コース×脚質グループ別複勝率）から、馬の脚質グループの複勝率と全体平均の差を ±5pt にスケール  
**現状**: course_style_stats_db は常に `{}` なので 0.0。enrich も未呼び出し。

---

## 3. 騎手展開影響 (H-3)

**場所**: `src/calculator/jockey_trainer.py` の `JockeyChangeEvaluator.evaluate()`

**入力**: 馬（乗り替わりフラグ）、新騎手の JockeyStats、レースグレード

**ロジック**:
1. **乗り替わりでない** → 0.0
2. **乗り替わりの場合**:
   - 乗り替わり理由パターン推定（A〜F）:
     - A（戦略的強化）: 上位人気偏差値 ≥60 の騎手への乗り替わり → +1.5pt
     - B（戦術的）: 減量騎手など → +0.5pt
     - C/F: 0pt / D: -0.5pt / E（見切り）: -2pt
   - **テン乗りペナルティ**: 騎手×馬の過去コンビが0走 → -1.0pt
3. 合計 = テン乗り + パターン補正

**出力**: -4〜+4 pt（展開偏差値の内訳❺）

**データソース**: netkeiba 騎手成績、馬の過去走（jockey_id）

---

## 4. 騎手コース影響 (H-2)

**場所**: `CourseAptitudeCalculator._calc_jockey_course()`

**入力**: JockeyStats、今回の course_id、all_courses

**ロジック**:
1. 騎手がいない → 0
2. **該当コース成績**（sample_n ≥10）: (all_dev - 50) / 5 × 1.5 を -3〜+3 にクリップ
3. **無い場合**: 類似コース（CourseMaster.similarity_score）の成績を類似度で重み付けして補間

**出力**: -3〜+3 pt（コース適性偏差値の内訳❸）

**データソース**: netkeiba 騎手コース別成績（course_records）、all_courses

---

## 5. 厩舎評価 (J-1)

**場所**: `src/scraper/personnel.py` の `TrainerScraper.fetch()`

**取得データ** (TrainerStats):
- **rank**: A/B/C/D（勝率×出走数で判定。例: 勝率18%かつ100走以上→A）
- **kaisyu_type**: 信頼型/穴型/過剰評価/標準（上位人気回収率・下位人気回収率から）
- **recovery_break**: 休み明け回収率（%）
- **short_momentum**: 好調/不調（直近2ヶ月 vs 長期の勝率差で判定）
- **rotation_type**, **break_type**, **good_venues**, **bad_venues** など

**利用箇所**:
- **勝負気配**: short_momentum="好調" → +1.5pt、recovery_break ≥120 かつ長期休養明け → +1.5pt
- **表示**: HTML で「厩舎ランク（回収タイプ）」として表示
- 総合偏差値には**直接**は入らない（勝負気配経由で間接的に影響）

**データソース**: netkeiba 調教師成績ページ

---

## 6. 調教 (J-4)

**場所**: `src/calculator/jockey_trainer.py` の `TrainingEvaluator.evaluate()`

**入力**: 調教記録リスト、trainer_baseline_db（厩舎別・コース別の平均3F・標準偏差）

**ロジック**:
1. trainer_baseline_db が空 or 該当コースなし → sigma_from_mean=0、強度ラベルはスクレイパー由来のまま
2. **baseline あり**: 調教の最後3Fタイムと厩舎平均の差を σ で割る  
   `sigma = (mean_3f - last_3f) / std_3f`
3. TRAINING_INTENSITY で強度ラベル付け（猛時計/やや速い/通常/やや軽め/軽め）

**現状**: trainer_baseline_db は常に `{}` のため、基準との比較は行われず、競馬ブック等スクレイパー由来の強度ラベルのみ使用。

**出力**: 各調教記録に sigma_from_mean, intensity_label を付与。表示用。

**データソース**: 競馬ブック調教データ、厩舎別 baseline（未構築）

---

## 7. 勝負気配 (J-2)

**場所**: `src/calculator/jockey_trainer.py` の `calc_shobu_score()`

**ロジック**:
- 騎手強化（乗り替わりパターンA）: +2.0
- 初コンビ（テン乗り）: +0.5
- 格上げ（クラス昇級）: +1.5
- 厩舎好調（short_momentum="好調"）: +1.5
- 休み明け × 厩舎の休み明け回収率 ≥120: +1.5

**出力**: スコア合計。4以上で「🔺勝負気配」表示。

**利用**: 表示・穴馬検知の補足。総合偏差値には直接は入らない。

---

## 8. 改修後コースフィルタ

**場所**: `src/calculator/calibration.py` の `filter_post_renovation_runs()`

**入力**: course_db の走リスト、場コード、分析対象レース日付

**ロジック**:
1. RENOVATION_EVENTS に競馬場別の改修イベントを定義（開始日・終了日・内容）
   - 例: 東京 2020年A→B移行、阪神 2022年芝張替 など
2. `is_pre_renovation(venue, race_date, analysis_date)`:
   - 改修が分析日より前に完了している場合、
   - 過去走の race_date が改修開始前なら「改修前」と判定
3. 改修前と判定された走を course_db から除外

**利用箇所**: 分析開始前の `engine.analyze()` 内。StandardTimeCalculator と Last3FDBBuilder が受け取る course_db は、このフィルタを通過したもの。

**効果**: コース改修後のレース分析时に、改修前の古いデータで基準タイム・上がり3F基準を汚さないようにする。

---

## まとめ

| 項目 | 主なデータソース | 総合偏差値への反映 |
|------|------------------|---------------------|
| 枠順バイアス | ルール固定 | 展開偏差値経由（30%） |
| コース脚質バイアス | CourseMaster 形状 | 展開偏差値経由（30%） |
| 騎手展開影響 | netkeiba 騎手成績・馬の過去走 | 展開偏差値経由（30%） |
| 騎手コース影響 | netkeiba 騎手コース成績 | コース適性経由（15%） |
| 厩舎評価 | netkeiba 厩舎成績 | 勝負気配のみ（偏差値非反映） |
| 調教 | 競馬ブック・厩舎baseline(未構築) | 表示のみ |
| 勝負気配 | 乗り替わり・厩舎・休み明け等 | 表示・穴馬補足のみ |
| 改修後フィルタ | RENOVATION_EVENTS テーブル | course_db の前処理 |
