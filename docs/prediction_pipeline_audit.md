# D-AI Keiba v3 予想算出パイプライン監査ドキュメント

**作成日**: 2026-05-24
**目的**: JRA + NAR 全会場の予想算出方法を詳細に可視化し、P3 ML較正設計の基礎データとする
**対象**: 全 5 層 (データ収集 / 特徴量計算 / ML 推論 / 合議 / 買い目生成)
**調査方法**: 並列 Explore 5 本による grep + 実コード確認

---

## 0. エグゼクティブサマリ

### 0.1 現状の構造

D-AI Keiba v3 は **JRA/NAR 分離** + **会場別カスタム重み** を既に体系的に実装している。

| 層 | JRA/NAR 分離方式 | 会場別カスタマイズ |
|---|---|---|
| データ収集 | 別スクレイパー (`official_odds.py` / `official_nar.py`) | レート制限 2.0 秒/件で統一 |
| 特徴量計算 | `_JRA_CODES`/`_NAR_VENUE_NAMES` で分岐 | `VENUE_CLASS_SCORE` テーブル (14 NAR 会場) |
| ML 推論 | 独立モデル (`lgbm_place_jra_*` / `lgbm_place_nar`) | 25 会場別モデル + 独立 Platt 較正 |
| 合議 | TEKIPAN/CONFIDENCE 闾値が別パラメータ | `VENUE_COMPOSITE_WEIGHTS` (25 会場) |
| 買い目生成 | PAYOUT_RATES が JRA/NAR で別値 | 払戻率と人気ペナルティで分岐 |

### 0.2 重要な事実 (P3 設計に直結)

1. **モデル数は実際 38 個** (`data/models/lgbm_place*.txt` を実測)
   - SYSTEM_ARCHITECTURE_FULL.md の「47」と Agent 中間報告「43」はいずれも誤り
   - 内訳: L0 global 1 + L1 surface 2 + L2 org 3 + L3 venue 25 + L4 SMILE **7** (turf 4 + dirt 3)
2. **L4 SMILE モデルは JRA のみ・しかも 7 個**
   - `jra_turf_s/m/i/l` (4 個) — SS と E は存在しない
   - `jra_dirt_s/m/i` (3 個) — SS, L, E は存在しない
   - **NAR には SMILE 分割が一切ない** (`lgbm_place_nar.txt` 1 個のみ)
3. **Platt 較正済モデルは 32 個** (`_cal.json`)
   - 未較正 6 場: **venue_01 (札幌), 02 (函館), 03 (福島), 04 (新潟), 30 (門別), 35 (盛岡)**
   - → P3 Phase 2 では未較正 6 場の新規較正 + 既存 19 場の再学習が必要
4. **較正は二段構成**: Platt Scaling (モデル単位) + Isotonic Regression (パイプライン全体)
5. **Walk-Forward 全期間 ROI 106.1%** (`memory/handoff_2026-05-13.md` 実測値)
   - 検証期間: 2024-01 〜 2026-04
   - 検証母数: **39,725 レース / 14,291 的中 (36.0%) / 投資 35,840,600 円 / 回収 38,013,450 円**
6. **JRA/NAR で換算定数 k 値・馬場補正・時計レベル全て分離済み** (settings.py + ability.py)

### 0.3 マスター方針との整合

マスター指示「個別会場に手を加えるのではなく全体で考えたい」は既存設計と一致。
個別会場ハードコードは `VENUE_CLASS_SCORE` (14 NAR 会場のクラス別スコアテーブル) のみで、それ以外の闾値・重み・較正係数はすべて `VENUE_COMPOSITE_WEIGHTS` / `*_cal.json` などの **データ駆動構成** で管理されている。

---

## 1. 全体パイプライン

### 1.1 エントリーポイント

```
python run_analysis_date.py YYYY-MM-DD [--venues 園田,船橋] [--workers 5]
```

### 1.2 処理フロー (run_analysis_date.py L244-663)

```
1. レースID取得 (auth.py:1321-1378)
   ├─ JRA公式 (OfficialOddsScraper) 最優先
   ├─ NAR公式 (OfficialNARScraper)
   ├─ netkeiba 補完
   └─ ばんえい補完
2. 並列プリフェッチ (ThreadPoolExecutor, 5 workers, 2.0 秒/件)
3. 補助DB事前構築 (StandardTime / Last3F / CourseStyle / GateBias / TrainerBaseline / Personnel / Bloodline / CourseDB)
4. 各レース分析 (RaceAnalysisEngine.analyze(is_jra=race_info.is_jra))
   ├─ Phase A: 能力値 (ability.py) — 32%
   ├─ Phase B: 展開予測 (pace_analysis.py) — 30%
   ├─ Phase C: コース適性 — 6%
   ├─ Phase D: 騎手評価 — 13%
   ├─ Phase E: 調教師評価 — 14%
   ├─ Phase F: 血統評価 — 5%
   ├─ Phase G: ML 確率予測 (LGBM 43 + PyTorch + LambdaRank)
   ├─ Phase H: コンポジット算出
   └─ Phase I: 印判定 (◉◎○▲△★☆×)
5. pred.json 保存 (is_jra フラグ含む)
6. オッズ更新ジョブ登録 (発走 15 分前)
```

---

## 2. データ収集層

### 2.1 データソース対応関係

| 項目 | JRA | NAR |
|---|---|---|
| 出走表・過去走 | `race.netkeiba.com` | `nar.netkeiba.com` |
| 馬DB | `db.netkeiba.com` | (共通) |
| 公式オッズ・結果 | `jra.go.jp/JRADB` | `keiba.go.jp/KeibaWeb` |
| フォールバック | 競馬ブック | 競馬ブック + 楽天競馬 |
| 調教データ | 競馬ブック (両対応) | 競馬ブック (両対応) |

### 2.2 JRA/NAR 分岐実装 (主要箇所)

| ファイル:行 | 関数 | 内容 |
|---|---|---|
| `src/scraper/netkeiba.py:2627-2629` | `fetch_date()` | JRA/NAR 統合レースID取得 |
| `src/scraper/official_odds.py:210-215` | `get_tansho()` | venue_code[4:6] で JRA/NAR 分岐 |
| `src/scraper/official_odds.py:212-215` | `_get_jra_odds()` / `_get_nar_odds()` | オッズ取得経路分岐 |
| `src/scraper/official_odds.py:315-334` | `get_jra_result()` | JRA 専用結果取得 |
| `src/scraper/official_nar.py:1-150` | NAR 公式スクレイパー全体 | NAR 専用結果取得 |
| `src/scraper/auth.py:1321-1378` | `fetch_date()` | JRA→NAR→netkeiba→ばんえい の優先順位 |

### 2.3 レート制限・並列禁止

- `src/scraper/netkeiba.py:70`: `REQUEST_INTERVAL = 2.0 秒` (★ 2026-04-28 事故後に強化)
- `src/scraper/official_nar.py:135`: `_REQ_INTERVAL = 2.0 秒`
- HTTP 429 検知 + 自動 cooldown (5件超で 1h, 10件超で 24h)
- `_NETKEIBA_COOLDOWN_LOCK` (threading.Lock) で並列禁止

### 2.4 取得項目差異

| 項目 | JRA | NAR |
|---|---|---|
| 払戻形式 | JRA 公式 JRADB JSON | NAR 公式 HTML 表パース |
| 馬体重 | kg + 増減 (公式) | kg + 増減 (公式) |
| ラップタイム | ハロンタイム 3F×4 | 上がり3F のみ (netkeiba 経由) |
| 馬場 | 芝・ダート・障害 | ダート主体・砂 |
| ID 体系 | netkeiba 統一 | NAR 公式 5桁 + netkeiba 0付き → `nar_id_mapper.py` で正規化 |

---

## 3. 特徴量計算層

### 3.1 基準タイム計算 (施策#7 実装位置)

**ファイル**: `src/calculator/ability.py:525-575`

```python
# JRA (コード "01"-"10")
standard_time = avg_time - (avg_score × dist_coeff)

# NAR (施策#7)
standard_time = avg_time - (avg_score × dist_coeff
                          × NAR_SCORE_EXTRAPOLATION_FACTOR  # 0.8 (settings.py:173)
                          × cap)                              # ±1.2 秒 (settings.py:176)
```

### 3.2 走破偏差値計算 (k 値の JRA/NAR 分離)

**ファイル**: `src/calculator/ability.py:739-760` + `config/settings.py:434-495`

```python
dev = 50 + (standard_time - corrected_time) × dist_coeff × k_value
```

#### JRA 用 k 値テーブル全 13 帯 (`CONVERSION_CONSTANT_BY_DIST`)

| 距離 (m) | k 値 | 距離 (m) | k 値 |
|---:|---:|---:|---:|
| 1000 | 5.0 | 2200 | 3.0 |
| 1150 | 4.8 | 2400 | 2.8 |
| 1200 | 4.5 | 2600 | 2.7 |
| 1400 | 4.0 | 3000 | 2.5 |
| 1600 | 3.7 | 3200 | 2.4 |
| 1800 | 3.4 | 3600 | 2.2 |
| 2000 | 3.2 |  |  |

#### NAR 用 k 値テーブル全 16 帯 (`CONVERSION_CONSTANT_BY_DIST_NAR`)

| 距離 (m) | NAR k 値 | JRA k 値 | 差分 |
|---:|---:|---:|---:|
| 800 | 3.0 | (5.0 推) | -2.0 (緩和) |
| 1000 | 2.8 | 5.0 | -2.2 (緩和) |
| 1150 | 2.7 | 4.8 | -2.1 (緩和) |
| 1200 | 2.6 | 4.5 | -1.9 (緩和) |
| 1400 | 2.5 | 4.0 | -1.5 (緩和) |
| 1500 | 2.9 | (3.7 推) | -0.8 |
| 1600 | 3.2 | 3.7 | -0.5 |
| 1700 | 3.3 | (3.7 推) | -0.4 |
| 1800 | 3.4 | 3.4 | 同一 |
| 2000-3600 | (JRA と同一) | - | 同一 |

**根拠 (settings.py コメントより)**:
- 金沢ダ 1400m で `speed_dev=20` (フロア値) 到達が 87 件 (4-7着で日常的に底打ち)
- JRA k=5.0 (800m) → std+2.4 秒で clamp 到達
- NAR k=3.0 (800m) → std+4.0 秒まで有効レンジを確保
- → NAR 1700m 以下のみ緩和、1800m 以上は JRA と同一

### 3.3 馬場補正・時計レベル補正

| 補正 | JRA | NAR |
|---|---|---|
| 馬場補正 (`_EMPIRICAL_RATES`) | 芝・ダート両方 | ダート主体 (芝はサンプル少→保守的) |
| 時計レベル (`VENUE_SPEED_TABLE`) | 東京 -0.073, 新潟 +0.047 秒/200m | 盛岡 -0.398 (最速), 姫路 +0.417 (最遅) 秒/200m |
| **直線の坂** (上がり 3F=残り 600m) | `course_master.py` に正確値保持 | `NAR_VENUE_ELEVATION` で手動補完 (下記) |

#### NAR 直線の坂 (上がり 3F 区間=残り 600m) データ補完

★ **重要な意味**: これは「直線の坂の有無・高さ」(残り 600m 区間の標高変化) であって、コース全体の道中起伏ではない。
JRA 側も `course_master.py` に同じ意味の `l3f_elevation` (last 3 furlong elevation) として保持されている。
NAR は公知情報源から手動補完 (`config/settings.py:181-196`)。

| 会場コード | 会場名 | 直線の坂 (m) | 性質 |
|---|---|---:|---|
| 44 | 大井 | **1.2** | 直線やや起伏あり (NAR 最大) |
| 43 | 船橋 | 1.0 | やや起伏 |
| 35 | 盛岡 | 0.8 | 起伏あり (芝コース含む) |
| 30 | 門別 | 0.5 | やや起伏 |
| 55 | 佐賀 | 0.5 | やや起伏 |
| 36 | 水沢 | 0.4 | やや起伏 |
| 50 | 園田 | 0.4 | やや起伏 |
| 51 | 姫路 | 0.4 | やや起伏 |
| 45 | 川崎 | 0.3 | ほぼ平坦 |
| 46 | 金沢 | 0.3 | 平坦 |
| 48 | 名古屋 | 0.3 | 平坦 |
| 54 | 高知 | 0.3 | 平坦 |
| 42 | 浦和 | 0.2 | 平坦 |
| 47 | 笠松 | 0.2 | 平坦 |

#### 使用箇所

| ファイル:行 | 関数 | 用途 |
|---|---|---|
| pace_course.py:1228-1232 | `calc_pace_from_running_record` | NAR 坂データ補完 |
| pace_course.py:1241-1243 | 同上 | 坂なし時のコーナー数代替 |
| pace_course.py:1391-1397 | `calc_pace_trajectory_score` | 坂補完再適用 |

★ JRA の直線坂は `data/masters/course_master.py` の各コース定義に `l3f_elevation` として既に存在。NAR のみ補完が必要だった。

### 3.4 個別会場 ハードコード (`VENUE_CLASS_SCORE`)

`src/calculator/ability.py:134-310` に **14 NAR 会場の個別クラス別スコア表**:

| 会場 | 行範囲 | スコア範囲 |
|---|---|---|
| 大井 | L135-145 | 重賞 +2.2 〜 C32 -1.0 |
| 船橋 | L146-156 | 重賞 +2.5 〜 C3 -1.0 |
| 川崎 | L157-167 | 重賞 +2.2 〜 C3 -1.0 |
| 浦和 | L168-178 | 重賞 +2.2 〜 C3 -1.0 |
| 園田 | L179-188 | 重賞 +1.0 〜 C3 -0.9 |
| 姫路 | L189-197 | 重賞 +1.9 〜 C3 -1.0 |
| 名古屋 | L198-210 | 重賞 +0.7 〜 C4 -1.0 |
| 笠松 | L211-234 | 23 行 (複数 B/C 分割) |
| 金沢 | L235-244 | 重賞 +0.4 〜 C2 -0.7 |
| 門別 | L245-256 | 重賞 +1.6 〜 C4 -0.9 |
| 盛岡 | L257-266 | 重賞 +1.2 〜 C2 -0.9 |
| 水沢 | L267-274 | 重賞 +0.9 〜 C2 -0.8 |
| 高知 | L275-282 | 重賞 +1.0 〜 C3 -0.2 |
| 佐賀 | L283-291 | 重賞 +1.0 〜 C2 -0.9 |

★ **この `VENUE_CLASS_SCORE` テーブルは個別会場ハードコードの唯一の重要箇所。** 他は全て `VENUE_COMPOSITE_WEIGHTS` 等のデータ駆動。

### 3.5 能力指数 `ability_total` の構成

```
ability_total = blend(max_dev, wa_dev, alpha)
              + grade_bonus (G1: +5.0, G2: +3.0, G3: +1.5)
              - long_rest_penalty
              × confidence_factor
```

- `max_dev`: WA フィルタ通過走の最高偏差値
- `wa_dev`: 距離帯別加重平均偏差値 (WA_WEIGHTS_BY_DISTANCE)
- `alpha`: WA/MAX ブレンド比 (0.1〜0.9)

### 3.6 特徴量 159 本の JRA/NAR 差異

| 種別 | 本数 | 差異 |
|---|---|---|
| 全共通 | 60 本 | なし |
| `is_jra` フラグ | 1 本 | 直接フラグ |
| `venue_sim_*` | 7 本 | 会場類似度マトリクスが JRA10/NAR14 で異なる |
| `same_dir_place_rate` | 1 本 | NAR で結果影響度が異なる |
| ばんえい固有 | 7 本 | `FEATURE_COLS_BANEI` (water_content, weight_kg_ratio 等) |

---

## 4. ML 推論層

### 4.1 モデル階層 (実測 38 モデル)

`ls data/models/lgbm_place*.txt | wc -l` 実測値ベース。

| Level | モデルファイル名 | 個数 | 条件 |
|---|---|---|---|
| L0 global | `lgbm_place.txt` | 1 | 全レース共通 |
| L1 surface | `lgbm_place_turf.txt`, `lgbm_place_dirt.txt` | 2 | 芝/ダート別 |
| L2 org | `lgbm_place_jra_turf.txt`, `lgbm_place_jra_dirt.txt`, `lgbm_place_nar.txt` | 3 | JRA芝/JRAダート/NAR |
| L3 venue | `lgbm_place_venue_01.txt`〜`_65.txt` (実測 25 ファイル) | 25 | 競馬場別 (JRA 10 + NAR 14 + 帯広 1) |
| L4 SMILE | `lgbm_place_jra_turf_s/m/i/l.txt` (4) + `jra_dirt_s/m/i.txt` (3) | **7** | **JRA のみ・実装範囲限定** |
| **合計** |  | **38** |  |

#### L3 venue 25 モデルの正確な内訳

```
JRA 10 場 (会場コード 01-10):
  venue_01 (札幌), venue_02 (函館), venue_03 (福島), venue_04 (新潟), venue_05 (東京),
  venue_06 (中山), venue_07 (中京), venue_08 (京都), venue_09 (阪神), venue_10 (小倉)

NAR 14 場 + 帯広 1 = 15:
  venue_30 (門別), venue_35 (盛岡), venue_36 (水沢), venue_42 (浦和), venue_43 (船橋),
  venue_44 (大井), venue_45 (川崎), venue_46 (金沢), venue_47 (笠松), venue_48 (名古屋),
  venue_50 (園田), venue_51 (姫路), venue_54 (高知), venue_55 (佐賀), venue_65 (帯広ばんえい)
```

#### L4 SMILE モデルの正確な内訳

```
src/ml/lgbm_model.py:379-386 の _smile_key_ml() の距離境界:
  SS: dist ≤ 1000
  S:  1000 < dist ≤ 1400
  M:  1400 < dist ≤ 1800
  I:  1800 < dist ≤ 2200
  L:  2200 < dist ≤ 2600
  E:  dist > 2600

実モデル (7 個):
  - lgbm_place_jra_turf_s.txt (JRA 芝・短距離 1000-1400m)
  - lgbm_place_jra_turf_m.txt (JRA 芝・マイル 1400-1800m)
  - lgbm_place_jra_turf_i.txt (JRA 芝・中距離 1800-2200m)
  - lgbm_place_jra_turf_l.txt (JRA 芝・長距離 2200-2600m)
  - lgbm_place_jra_dirt_s.txt (JRA ダ・短距離)
  - lgbm_place_jra_dirt_m.txt (JRA ダ・マイル)
  - lgbm_place_jra_dirt_i.txt (JRA ダ・中距離)

★ 不在モデル (5 個未実装):
  - jra_turf_ss/e (1000m 以下と 2600m 超は JRA でもサンプル少)
  - jra_dirt_ss/l/e (ダート 1000m 以下・2200m 超は希少)
  - nar 全 SMILE (NAR には SMILE 分割完全不在) ★ P3 Phase 1 の主対象
```

推論時フォールバック: venue → SMILE → JRA/NAR → surface → global

### 4.2 較正 (2 段構成)

#### Platt Scaling (モデル単位)
- ファイル: `src/ml/lgbm_model.py:3941-3952`
- 各モデルに `lgbm_place_KEY_cal.json` (a, b パラメータ)
- 推論時に全モデル確率に適用 (L4082-4093)

#### Isotonic Regression (パイプライン全体)
- ファイル: `src/ml/calibrator.py`
- 学習: `scripts/build_calibrator.py` で win/top2/top3 の 3 モデル
- 確率を 0.001-0.999 でクリップ + ソフト正規化

### 4.3 入出力

**入力**: FEATURE_COLUMNS 186 本 (ばんえいは ~35 本削除済 FEATURE_COLUMNS_BANEI)

**出力**:
- `place_prob` (複勝確率) — LGBMPredictor から
- `win_prob` / `place2_prob` / `place3_prob` — probability_model.py (3 目的関数)
- `ml_composite_adj` — engine.py で win × 重み + place × 重みで合成 (2026-05-13 導入)

### 4.4 Walk-Forward 学習・推論

- スクリプト: `scripts/walk_forward_backtest.py`
- 月単位再訓練 → リーク排除
- 検証期間: 2024-01 〜 2026-04 (全 28 ヶ月)
- 検証手段: `scripts/verify_all_tickets.py` (全期間照合)

#### 検証結果 (handoff_2026-05-13.md 実測値)

| 年 | レース数 | 的中数 | 的中率 | 投資 (円) | 回収 (円) | ROI |
|---|---:|---:|---:|---:|---:|---:|
| 2024 | 17,188 | 5,613 | 32.7% | 15,810,300 | 14,830,570 | **93.8%** |
| 2025 | 16,966 | 6,430 | 37.9% | 15,292,100 | 18,596,440 | **121.6%** |
| 2026 (4月迄) | 5,571 | 2,248 | 40.4% | 4,738,200 | 4,586,440 | **96.8%** |
| **全体** | **39,725** | **14,291** | **36.0%** | **35,840,600** | **38,013,450** | **106.1%** |

#### ダッシュボード ROI (129.7%) との乖離理由

- ダッシュボード: 的中率 36.4% / ROI 129.7%
- verify_all_tickets: 的中率 36.0% / ROI 106.1%
- **主因**: 2024-11月の三連複 payout 欠損 318 件 + 2024-02 の 19 件 (combo は一致するが payout=0 で回収未計上)
- → backfill 修復で 2024 ROI 改善余地あり (handoff_2026-05-13.md 残課題 #1)

### 4.5 ★ NAR ML 較正の現状ギャップ (実測ファイル数ベース)

| 項目 | JRA | NAR | 差分 |
|---|---|---|---|
| L4 SMILE モデル | **7 個** (turf S/M/I/L + dirt S/M/I) | **0 個** | -7 |
| L3 会場モデル | 10 場 | 14 場 (+ ばんえい 1) | +4 |
| Platt 較正 (`_cal.json`) | venue_05-10 の 6 場 (01-04 未較正) | venue_36, 42-48, 50, 51, 54, 55, 65 の 13 場 (30, 35 未較正) | - |
| Isotonic 較正 | パイプライン全体 共通 | パイプライン全体 共通 | - |

#### Platt 較正の未較正 6 場 (P3 Phase 2 の最優先対象)

```
JRA:
  venue_01 (札幌)  ← n=4,180  と  サンプル少だが他より多い
  venue_02 (函館)  ← n=3,541  最小
  venue_03 (福島)  ← n=6,804
  venue_04 (新潟)  ← n=8,655
NAR:
  venue_30 (門別)  ← n=19,779 (サンプル豊富なのに未較正)
  venue_35 (盛岡)  ← n=14,630
```

★ 注: venue_30 (門別) と venue_35 (盛岡) は **サンプル豊富にも関わらず未較正** — Phase 2 で最も効果が見込める。

#### P3 着手の優先順位

1. **Phase 2 最優先 (低コスト・確実効果)**: 未較正 6 場の新規 Platt 較正学習
2. **Phase 2 副次**: 既存 19 場の Platt 再学習 (直近 6 ヶ月データ反映)
3. **Phase 1 (高コスト・大効果)**: NAR L4 SMILE 5 モデル新規追加 (nar_s/m/i/l/e)

---

## 5. 合議ロジック (印付与)

### 5.1 印付与アルゴリズム

**ファイル**: `src/output/formatter.py:66-307`

```
1. composite 順に並べる
2. 1位を ◉ または ◎ に分類:
   ◉ (鉄板) 5 条件全て満たす場合:
     - gap (2位との差) ≥ TEKIPAN_GAP_{JRA|NAR}
     - win_prob ≥ TEKIPAN_WIN_PROB_{JRA|NAR}
     - place3_prob ≥ TEKIPAN_PLACE3_PROB_{JRA|NAR}
     - 人気 ≤ TEKIPAN_POP_MAX_{JRA|NAR}
     - EV ≥ TEKIPAN_MIN_EV_{JRA|NAR} (v5 で 0.0 廃止)
   ◎ (本命): ◉ 条件未満の 1 位
3. 2-6位に ○▲△★☆ を必ず 1 頭ずつ付与 (6 印完備ルール)
   wp 整合性ガード: ○ ≥ 2%, ▲ ≥ 1%, △ ≥ 0.5%
4. ★ (穴): ana_score 上位
5. × (危険): kiken_score 上位
```

### 5.2 TEKIPAN 闾値 (JRA/NAR 分離)

| 条件 | JRA | NAR | 出典 |
|---|---|---|---|
| gap | 7.0 | 5.0 | settings.py:628-629 |
| win_prob | 0.25 | 0.35 (動的) | settings.py:630-638 |
| place3_prob | 0.70 | 0.70 | settings.py:639-640 |
| 人気上限 | 2 | 2 | settings.py:641-642 |
| EV | 0.0 (廃止) | 0.0 (廃止) | settings.py:649-650 |

#### NAR 頭数別 win_prob (TEKIPAN_WIN_PROB_NAR_BY_FIELD)

| 頭数 | win_prob 闾値 |
|---|---|
| small (≤8 頭) | 0.30 |
| medium (9-12 頭) | 0.28 |
| large (≥13 頭) | 0.25 |

### 5.3 composite 合算式

**ファイル**: `src/models.py:747-794` + `src/engine.py:1282-1310`

```
composite = ability.total      × W_ability     × training_mult
          + pace.total         × W_pace        × training_mult
          + course.total       × W_course
          + jockey_dev         × W_jockey
          + trainer_dev        × W_trainer
          + bloodline_dev      × W_bloodline
          + odds_consistency_adj  (-4 〜 +4)
          + ml_composite_adj      (-6 〜 +6)
          + market_anchor_adj     (-3 〜 +3)
```

**デフォルト重み**: ability 0.32 / pace 0.30 / course 0.06 / jockey 0.13 / trainer 0.14 / bloodline 0.05

**会場別重み**: `VENUE_COMPOSITE_WEIGHTS` (settings.py:112-141) で **JRA 10 場 + NAR 14 場 + 帯広 = 計 25 会場** 個別調整。
全値は ML 特徴量重要度分析 2024-01〜2026-02 から自動較正。

#### VENUE_COMPOSITE_WEIGHTS 実値 (settings.py から取得)

##### JRA 10 場

| 会場 | ability | pace | course | jockey | trainer | bloodline | 較正 n |
|---|---:|---:|---:|---:|---:|---:|---:|
| 東京 | 0.383 | 0.249 | 0.051 | 0.122 | 0.142 | 0.053 | 16,270 |
| 中山 | 0.34 | 0.33 | 0.06 | 0.105 | 0.115 | 0.05 | 16,500 |
| 阪神 | 0.346 | 0.277 | 0.058 | 0.138 | 0.138 | 0.043 | 9,457 |
| 京都 | 0.35 | 0.292 | 0.05 | 0.133 | 0.117 | 0.058 | 18,078 |
| 中京 | 0.335 | 0.277 | 0.061 | 0.128 | 0.158 | 0.041 | 10,140 |
| 小倉 | 0.282 | 0.346 | 0.079 | 0.135 | 0.11 | 0.048 | 9,233 |
| 新潟 | 0.324 | 0.288 | 0.084 | 0.13 | 0.139 | 0.035 | 8,655 |
| 福島 | 0.273 | 0.368 | 0.052 | 0.124 | 0.143 | 0.04 | 6,804 |
| 札幌 | 0.296 | 0.285 | 0.069 | 0.155 | 0.171 | 0.024 | 4,180 |
| 函館 | 0.265 | 0.315 | 0.057 | 0.125 | 0.176 | 0.062 | 3,541 |

##### NAR 14 場 + 帯広 1

| 会場 | ability | pace | course | jockey | trainer | bloodline | 較正 n |
|---|---:|---:|---:|---:|---:|---:|---:|
| 大井 | 0.353 | **0.387** | 0.052 | 0.113 | 0.058 | 0.037 | 29,767 |
| 川崎 | 0.275 | **0.429** | 0.095 | 0.085 | 0.077 | 0.039 | 18,021 |
| 船橋 | 0.301 | **0.354** | 0.073 | 0.114 | 0.104 | 0.054 | 17,231 |
| 浦和 | 0.216 | **0.569** | 0.065 | 0.073 | 0.054 | 0.023 | 15,271 |
| 門別 | 0.32 | **0.444** | 0.056 | 0.077 | 0.042 | 0.061 | 19,779 |
| 盛岡 | 0.276 | **0.464** | 0.07 | 0.08 | 0.064 | 0.046 | 14,630 |
| 水沢 | 0.236 | **0.547** | 0.07 | 0.061 | 0.047 | 0.039 | 15,927 |
| 金沢 | 0.23 | **0.599** | 0.054 | 0.042 | 0.038 | 0.037 | 17,796 |
| 笠松 | 0.243 | **0.521** | 0.064 | 0.082 | 0.048 | 0.042 | 19,089 |
| 名古屋 | 0.243 | **0.535** | 0.056 | 0.08 | 0.031 | 0.055 | 34,077 |
| 園田 | 0.284 | **0.517** | 0.059 | 0.056 | 0.033 | 0.051 | 33,779 |
| 姫路 | 0.288 | **0.472** | 0.051 | 0.077 | 0.084 | 0.028 | 8,294 |
| 高知 | 0.289 | **0.496** | 0.055 | 0.076 | 0.031 | 0.053 | 25,804 |
| 佐賀 | 0.228 | **0.578** | 0.052 | 0.067 | 0.029 | 0.046 | 28,351 |
| 帯広 | 0.28 | 0.27 | 0.05 | 0.15 | 0.15 | 0.10 | (Phase5 修正) |

★ 重要な観察: **NAR 全 14 場で pace 重みが 0.35-0.60 と JRA (0.25-0.37) より顕著に高い**。これは NAR の展開支配構造を反映。施策#6 で PACE_WEIGHT_CAP_NAR=0.50 に緩和済。

### 5.4 自信度 (CONFIDENCE)

| Level | JRA 闾値 | NAR 闾値 |
|---|---|---|
| SS | 0.7327 | 0.809 |
| S | 0.6085 | 0.7128 |
| A | 0.4835 | 0.61 |
| B | 0.2407 | 0.3501 |
| C | 0.0987 | 0.1361 |

- `CONFIDENCE_GAP_DIVISOR_JRA = 6.0` / `_NAR = 8.0`
- 追加ゲート: win_prob_gate / gap_gate を SS・S で個別設定

---

## 6. 買い目生成層

### 6.1 三連複生成パターン (3 段階)

**ファイル**: `src/calculator/betting.py:2842-2860` (dispatch_tickets)

| パターン | 条件 | 点数 | 構成 |
|---|---|---|---|
| **S-strict** | EV ≥ 1.8 ∧ ◉◎ place3 ≥ 0.65 | 4点 | ◉◎-○ → {▲△★☆} |
| **S-mid** | EV ≥ 1.3 ∧ ◉◎ place3 ≥ 0.55 | 7点 | ◉◎ → (○▲) × 全部 |
| **S-wide** | EV ≥ 1.0 (default) | 10点 | ◉◎ 軸 → 相手2頭組 |

### 6.2 単勝生成 (shobu_score TOP2)

`src/calculator/betting.py:2804+` (build_tansho_t4_tickets)

2026-05-03 仕様: 勝負気配スコア (`shobu_score`) 上位 2 頭採用。

### 6.3 払戻率 (PAYOUT_RATES, settings.py:759-785)

| 券種 | JRA | NAR |
|---|---|---|
| 単勝 | 0.800 | 0.750 |
| 複勝 | 0.800 | 0.750 |
| 馬連 | 0.775 | 0.750 |
| ワイド | 0.775 | 0.750 |
| 三連複 | 0.750 | 0.750 |
| 三連単 | 0.725 | 0.750 |

### 6.4 馬券スキップ条件 (`bet_decision`)

**ファイル**: `betting.py:1397-1481`

以下のいずれかで購入見送り:
1. 低期待値: confidence ∈ {B,C} かつ max_ev < 110%
2. 混戦: 候補 > 20 点 かつ max_place3_prob < 15%
3. 低自信度: confidence=C かつ ◎win_prob < 15%
4. トリガミ: 全候補で stake × odds < total_stake × 1.05

---

## 7. JRA/NAR/会場別分岐 完全マッピング表

### 7.1 settings.py 内の全パラメータ (50+ 件)

| カテゴリ | パラメータ | JRA | NAR | 用途 |
|---|---|---|---|---|
| **基準タイム** | `NAR_SCORE_EXTRAPOLATION_FACTOR` | — | 0.8 | NAR 外挿減衰 |
| | `NAR_SCORE_EXTRAP_MAX_ADJ_SEC` | — | 1.2 | 外挿キャップ (秒) |
| **k 値** | `CONVERSION_CONSTANT_BY_DIST` | 12 距離 | — | JRA 偏差値換算 |
| | `CONVERSION_CONSTANT_BY_DIST_NAR` | — | 16 距離 | NAR 偏差値換算 (短距離緩和) |
| **重み** | `PACE_WEIGHT_CAP` | 0.35 | — | JRA 展開上限 |
| | `PACE_WEIGHT_CAP_NAR` | — | 0.50 | NAR 展開上限 (緩和) |
| | `PACE_EXCESS_REDISTRIB_NAR` | — | jockey 0.5 / trainer 0.5 | NAR 超過分配分 |
| | `VENUE_COMPOSITE_WEIGHTS` | 10 会場 | 14 会場 + 帯広 | 6 因子個別重み |
| **NAR 補完** | `NAR_VENUE_ELEVATION` | — | 14 会場コード | **直線の坂** (上がり 3F=残り 600m 区間の高低差) 手動補完 ※道中の起伏ではない |
| | `_NAR_VENUE_NAMES` | — | 14 会場 frozenset | NAR 判定 |
| | `_JRA_VENUE_CODES` | "01"-"10" | — | JRA 判定 |
| **TEKIPAN** | `TEKIPAN_GAP_JRA/NAR` | 7.0 | 5.0 | 鉄板 gap |
| | `TEKIPAN_WIN_PROB_JRA/NAR` | 0.25 | 0.35 | win_prob 下限 |
| | `TEKIPAN_WIN_PROB_NAR_BY_FIELD` | — | 頭数別 dict | NAR 動的化 |
| | `TEKIPAN_PLACE3_PROB_*` | 0.70 | 0.70 | 複勝率下限 |
| | `TEKIPAN_POP_MAX_*` | 2 | 2 | 人気上限 |
| | `TEKIPAN_MIN_EV_*` | 0.0 | 0.0 | EV 下限 (廃止) |
| **CONFIDENCE** | `CONFIDENCE_GAP_DIVISOR_JRA/NAR` | 6.0 | 8.0 | 自信度正規化 |
| | `CONFIDENCE_THRESHOLDS_JRA/NAR` | SS-C dict | SS-C dict | 自信度闾値 |
| | `CONFIDENCE_WP_GATE_SS_JRA/NAR` | 0.30 | 0.35 | SS win ゲート |
| | `CONFIDENCE_GAP_GATE_SS_JRA/NAR` | 6.0 | 7.0 | SS gap ゲート |
| | `CONFIDENCE_WP_GATE_S_JRA/NAR` | 0.22 | 0.28 | S win ゲート |
| | `CONFIDENCE_GAP_GATE_S_JRA/NAR` | 4.0 | 5.0 | S gap ゲート |
| **特選危険** | `TOKUSEN_KIKEN_POP_MIN_JRA/NAR` | 2 | 1 | 最小人気 |
| | `TOKUSEN_KIKEN_POP_LIMIT_JRA/NAR` | 3 | 6 | 最大人気 |
| | `TOKUSEN_KIKEN_ODDS_LIMIT_JRA/NAR` | 15.0 | 30.0 | オッズ上限 |
| | `TOKUSEN_KIKEN_COMP_RANK_PCT_JRA/NAR` | 0.25 | 0.30 | 下位ランク% |
| **人事 base** | `JOCKEY_BASE_PARAMS_JRA/NAR` | mean 0.19 / σ 0.15 | mean 0.258 / σ 0.112 | 騎手ベース |
| | `TRAINER_BASE_PARAMS_JRA/NAR` | mean 0.20 / σ 0.10 | mean 0.26 / σ 0.14 | 調教師ベース |
| **払戻** | `PAYOUT_RATES["jra_*"]` | 6 券種 | — | JRA 払戻率 |
| | `PAYOUT_RATES["nar_*"]` | — | 6 券種 | NAR 払戻率 |
| **ばんえい** | `BANEI_MIN_CONFIDENCE` | — | "A" | ばんえい馬券フィルタ |

### 7.2 src/ 内の主要コード分岐 (28 箇所)

| ファイル:行 | 関数 | 分岐 | 内容 |
|---|---|---|---|
| engine.py:361 | `__init__` | JRA/NAR | `is_jra` フラグ初期化 |
| engine.py:1282-1285 | `_run` | 会場別 | `get_composite_weights(venue)` |
| engine.py:3170 | `_run` | JRA/NAR | personnel 参照分け |
| engine.py:3375 | `compute_personnel_deviations` | JRA/NAR | `JOCKEY_BASE_PARAMS_JRA/NAR` |
| engine.py:3485 | `compute_personnel_deviations` | JRA/NAR | `TRAINER_BASE_PARAMS_JRA/NAR` |
| models.py:181 | `CourseMaster` | JRA/NAR | `is_jra` で馬場性質分岐 |
| models.py:748-794 | `composite` property | 会場別 | `get_composite_weights(venue_name)` |
| ability.py:361-377 | `calc_standard_time` | JRA/NAR | `VENUE_SPEED_TABLE` ダート補正 |
| ability.py:550-572 | `calc_standard_time` | NAR | `NAR_SCORE_EXTRAPOLATION_FACTOR` |
| ability.py:699-710 | `calc_speed_deviation` | JRA/NAR | k 値テーブル分岐 |
| ability.py:1605-1624 | `calc_weighted_ability` | JRA/NAR | 混走補正 (+4% 上級) |
| pace_course.py:1228-1232 | `calc_pace_from_running_record` | NAR | 坂データ補完 |
| pace_course.py:1241-1243 | (同上) | NAR | 坂なし時コーナー数代替 |
| pace_course.py:1391-1397 | `calc_pace_trajectory_score` | NAR | 坂補完再適用 |
| jockey_trainer.py:595-676 | `calc_tokusen_kiken_score` | JRA/NAR | 必須条件 pop/wp/composite 分離 |
| betting.py:68-95 | `estimate_*_odds` | JRA/NAR | `PAYOUT_RATES["jra_*"/"nar_*"]` |
| betting.py:537-573 | `_calc_confidence_score` | JRA/NAR | `CONFIDENCE_GAP_DIVISOR` |
| betting.py:645-699 | `determine_confidence_level` | JRA/NAR | `CONFIDENCE_THRESHOLDS` + ゲート |
| betting.py:1298-1332 | `generate_formation_tickets` | JRA/NAR | `estimate_sanrenpuku_odds` |
| betting.py:2027-2070 | `generate_umaren_tickets` | JRA/NAR | 払戻率選択 |
| formatter.py:165-184 | `_assign_marks_detail` | JRA/NAR | `TEKIPAN_*` 闾値分離 |
| formatter.py:168-178 | (同上) | NAR | 頭数別動的 |
| popularity_blend.py:439-550 | `reassign_marks_dict` | JRA/NAR | TEKIPAN 再判定 |
| database.py:1301-1352 | `aggregate_personnel` | JRA/NAR | 統計別集計 |
| database.py:1487-1509 | `load_personnel_data` | JRA/NAR | surface 別ロード |
| database.py:2546-2624 | `compute_personnel_baseline` | JRA/NAR | クエリ分岐 |
| settings.py:224-301 | `get_composite_weights` | JRA/NAR | 会場別 + 重み再配分 |

### 7.3 個別会場ハードコード (`VENUE_CLASS_SCORE` のみ・14 NAR 会場)

`src/calculator/ability.py:134-310` に 14 NAR 会場のクラス別スコア表が直書き。
→ **これが個別会場ハードコードの唯一の重要箇所**。他は全て VENUE_COMPOSITE_WEIGHTS / *_cal.json などのデータ駆動。

---

## 8. P3 ML 較正 設計提案

### 8.1 現状ギャップの確認 (実測値ベース)

| 項目 | JRA | NAR |
|---|---|---|
| L4 SMILE モデル | 7 個 (turf S/M/I/L + dirt S/M/I) | **0 個** |
| L3 venue モデル | 10 場 | 14 場 + 帯広 1 = 15 場 |
| Platt 較正済 (`_cal.json`) | 6 場 (05-10) | 13 場 (36 + 42-48 + 50, 51, 54, 55, 65) |
| **Platt 較正 未実施** | **4 場** (01-04: 札函福新) | **2 場** (30 門別, 35 盛岡) |
| Walk-Forward (ROI) | 共通 106.1% | 共通 106.1% |

### 8.2 P3 着手提案 (3 フェーズ)

#### Phase 2 (推奨 1 番目・最優先): Platt 較正の網羅化と再学習

**目的**: 未較正 6 場の新規較正 + 既存 19 場の再学習で全体精度向上

##### Phase 2-A: 未較正 6 場の新規 Platt 較正学習 (1-2 日)

| 対象 | サンプル数 (n) | 優先度 | 期待効果 |
|---|---:|---|---|
| venue_30 (門別) | 19,779 | ★★★ | 5/23 ROI 55.7% の改善余地大 |
| venue_35 (盛岡) | 14,630 | ★★★ | 同上 |
| venue_04 (新潟) | 8,655 | ★★ | JRA 較正完備化 |
| venue_03 (福島) | 6,804 | ★★ | 同上 |
| venue_01 (札幌) | 4,180 | ★ | 中央北側 |
| venue_02 (函館) | 3,541 | ★ | 中央北側 |

実行コマンド (現行 `scripts/build_calibrator.py` 流用想定):
```bash
python scripts/build_calibrator.py --venue 30 --output data/models/lgbm_place_venue_30_cal.json
python scripts/build_calibrator.py --venue 35 --output data/models/lgbm_place_venue_35_cal.json
# (以下 venue_01, 02, 03, 04 で同様に実行)
```

##### Phase 2-B: 既存 19 場の Platt 再学習 (2-3 日)

- 現状: 較正済 `_cal.json` が**いつ生成されたか不明** → 直近データを反映していない懸念
- 解決: 直近 6 ヶ月 (2025-12 〜 2026-05) の予実データを反映して全 19 場の Platt (a, b) を再学習
- 期待効果: NAR 会場別 ROI のバラつき (5/23 検証: 門別 55.7% / 船橋 103.8%) 縮小

**Phase 2 合計工数: 3-5 日**
**Phase 2 検証**: 過去 9 日分 (5/14-22) で `scripts/compare_basetime_results.py` 流用して施策#7 と同じ検証手法で ◎1着率・ROI を測定

---

#### Phase 3 (推奨 2 番目): Isotonic 較正の JRA/NAR 分離

**目的**: パイプライン全体の Isotonic 較正 (現状 JRA/NAR 共通) を分離し、NAR 専用較正で予測精度向上

##### 現状の問題

`src/ml/calibrator.py` の `calibrator_win/top2/top3.pkl` (3 個) が JRA/NAR 共通。
- JRA データ (人気・調教師厳格) と NAR データ (混戦傾向強・配当大) の分布が異なる
- 共通 Isotonic で較正すると NAR 側の確率が圧縮される懸念

##### 改修内容

1. `src/ml/calibrator.py` を分岐対応:
   - `calibrator_win_jra.pkl` / `calibrator_win_nar.pkl`
   - `calibrator_top2_jra.pkl` / `calibrator_top2_nar.pkl`
   - `calibrator_top3_jra.pkl` / `calibrator_top3_nar.pkl`
2. `scripts/build_calibrator.py` を JRA/NAR 別実行に対応
3. `src/engine.py` ロード処理を `is_jra` 経由で切替
4. Walk-Forward 学習スクリプト (`scripts/walk_forward_backtest.py`) も同様改修

**Phase 3 合計工数: 3-4 日**
**Phase 3 検証**: 同上 (9 日分 + 過去 1 ヶ月で全期間 ROI 再算定)

---

#### Phase 1 (推奨 3 番目・最大効果): NAR L4 SMILE 分割モデル新規追加

**目的**: JRA に存在する SMILE 分割モデル (7 個) を NAR にも導入し、距離別予測精度を向上

##### 追加するモデル (5 個)

| ファイル名 | 距離 | 想定サンプル |
|---|---|---:|
| `lgbm_place_nar_s.txt` | 1000 < d ≤ 1400 | NAR 短距離主体 (笠松 1400m, 名古屋 1500m 等) |
| `lgbm_place_nar_m.txt` | 1400 < d ≤ 1800 | NAR 主力距離帯 |
| `lgbm_place_nar_i.txt` | 1800 < d ≤ 2200 | 大井 2000m 重賞等 |
| `lgbm_place_nar_l.txt` | 2200 < d ≤ 2600 | 東京大賞典等 (希少) |
| `lgbm_place_nar_e.txt` | dist > 2600 | 帝王賞・JBC クラシック等 (極希少) |

##### 必要前提

- `MIN_TRAIN_SAMPLES = 4,000` (現行設定) を満たす距離区分のみ実装
- → `nar_l` と `nar_e` はサンプル不足の可能性 → **要事前検証**
- 検証コマンド (新規スクリプト案):
  ```bash
  python scripts/check_nar_smile_samples.py
  # → 出力例: nar_s=120,000 / nar_m=180,000 / nar_i=8,500 / nar_l=600 / nar_e=80
  ```

##### 学習・統合

1. `scripts/build_smile_models.py` 新規作成 (もしくは `retrain_all.py` 拡張)
2. 各 NAR SMILE モデルに独立 Platt 較正 (`_cal.json`)
3. `src/ml/lgbm_model.py` のフォールバック順を更新:
   - 旧: venue → JRA-SMILE → JRA/NAR → surface → global
   - 新: venue → JRA/NAR-SMILE → JRA/NAR → surface → global

**Phase 1 合計工数: 1-2 週**
**Phase 1 検証**:
1. 過去 9 日分 (5/14-22) で ◎1着率・ROI 測定
2. 過去 1 ヶ月で会場別精度推移確認
3. 過去 1 年 (2025-05 〜 2026-04) で年間 ROI 再算定 → 106.1% を上回るか確認

### 8.3 着手順序提案 (具体性向上版)

| 順 | Phase | 規模 | 工数 | 期待効果 | 検証時間 |
|---|---|---|---|---|---|
| **1** | Phase 2-A (未較正 6 場 Platt) | 小 | 1-2 日 | 門別・盛岡の ROI 改善 (5/23 検証で 55.7% → 70%+ 目標) | 半日 |
| **2** | Phase 2-B (既存 19 場 Platt 再学習) | 中 | 2-3 日 | 全体較正リフレッシュ・分布補正 | 半日 |
| **3** | Phase 3 (Isotonic JRA/NAR 分離) | 中 | 3-4 日 | NAR 確率較正精度向上 | 半日 |
| **4** | Phase 1 (NAR SMILE 5 モデル新規) | 大 | 1-2 週 | NAR L4 較正完備・予測精度大幅向上 | 1 日 |

**Phase 全体合計**: 約 3-4 週間

### 8.4 詳細分析: 未較正 6 場の真因 (実データ確証)

#### 真因コード位置: `src/ml/lgbm_model.py:3263-3287`

```python
all_dates = sorted(set(r.get("date", "") for r in races if r.get("date")))
split_idx = max(1, len(all_dates) - valid_days)
split_date = all_dates[split_idx]  # ★ 全 venue 共通の単一 split_date
...
for race in races:
    is_valid = date_str >= split_date   # ★ venue 関係なくこの日付で切る
```

→ 学習データ最後の `valid_days` 日 (冬期含む) を検証期間とする。

#### 各 venue の開催月 (race_log 実データ・2024-2025 平均)

| Venue | 開催月 | 12月開催 | 1-3月開催 | Platt較正 |
|---|---|:-:|:-:|:-:|
| 01 札幌 | 7-9月のみ | ✗ | ✗ | **未** |
| 02 函館 | 6-7月のみ | ✗ | ✗ | **未** |
| 03 福島 | 4, 6, 7, 11月 | ✗ | ✗ | **未** |
| 04 新潟 | 4-10月 | ✗ | ✗ | **未** |
| 30 門別 | 4-11月 | ✗ | ✗ | **未** |
| 35 盛岡 | 5-11月 | ✗ | ✗ | **未** |
| 05 東京 | 1-2, 4-6, 10-11月 | ✗ | ✓ | 済 |
| 06-10 (中山〜小倉) | 通年又は冬期含む | ✓ | ✓ | 済 |
| 36-65 (水沢〜帯広) | 通年又は冬期含む | ✓ | ✓ | 済 |

→ **未較正 6 場は全て「冬期 (12-3 月) 休止」**

検証期間 (年末側) に該当場のデータが 0 件 → `if len(valid_y) > 0:` 不成立 → Platt 較正スキップ → `_cal.json` 未生成

#### Phase 2-A 最小修正案 (`src/ml/lgbm_model.py:3563-3582`)

```python
# 旧コード
if len(valid_y) > 0:
    cal_X = y_pred.reshape(-1, 1)
    cal_model.fit(cal_X, y_valid)
    ...

# 新コード (案)
if len(valid_y) > 0:
    # 通常の検証データで Platt 較正
    cal_X = y_pred.reshape(-1, 1)
    cal_model.fit(cal_X, y_valid)
    ...
else:
    # ★ 検証データなし → 訓練データの最後 10% を Platt 較正用に転用
    n_cal = max(1000, int(len(y_train) * 0.10))
    if len(y_train) >= n_cal:
        cal_X = train_pred[-n_cal:].reshape(-1, 1)
        cal_y = y_train[-n_cal:]
        cal_model.fit(cal_X, cal_y)
        ...
        logger.info("Platt calibration (fallback): n=%d", n_cal)
```

→ 新スクリプト不要・既存 `retrain_all.py` 再実行で全 venue 較正完了

### 8.5 詳細分析: 門別の異常 (実データ訂正)

#### 5/23 シミュレーション値 vs 全期間実測

| 会場 | 5/23 報告値 (限定条件) | 全期間実測 (2024-2026) | 差分 |
|---|---:|---:|---:|
| 門別 ◎1着率 | 31.6% | **20.0%** | -11.6pt |
| 門別 ROI | 55.7% | **65.8%** | +10.1pt |
| 金沢 ROI | 50.2% | 79.9% | +29.7pt |
| 園田 ROI | 55.5% | 88.7% | +33.2pt |

★ **5/23 数値は何らかの限定条件下** (おそらく特定期間のシミュレーション差分)。全期間データでは別の傾向。

#### 全期間 NAR 会場別 ROI (predictions × race_results 実測)

| 順位 | 会場 | R 数 | ◎1着率 | 三連複 ROI | Platt較正 |
|---:|---|---:|---:|---:|:-:|
| 1 | 大井 | 2,739 | 22.6% | **136.9%** | ✓ |
| 2 | 盛岡 | 1,553 | 18.5% | **127.1%** | ✗ **未較正** |
| 3 | 姫路 | 849 | 21.0% | 126.5% | ✓ |
| 4 | 佐賀 | 3,038 | 19.0% | 120.2% | ✓ |
| 5 | 高知 | 2,863 | 20.4% | 106.5% | ✓ |
| 6 | 水沢 | 1,839 | 16.5% | 102.2% | ✓ |
| 7 | 船橋 | 1,782 | 22.0% | 96.9% | ✓ |
| 8 | 川崎 | 1,789 | 18.7% | 96.8% | ✓ |
| 9 | 浦和 | 1,540 | 20.8% | 95.5% | ✓ |
| 10 | 名古屋 | 3,349 | 19.8% | 90.7% | ✓ |
| 11 | 笠松 | 2,473 | 24.2% | 90.4% | ✓ |
| 12 | 園田 | 3,610 | 25.4% | 88.7% | ✓ |
| 13 | 金沢 | 2,236 | 17.3% | 79.9% | ✓ |
| 14 | 門別 | 2,124 | 20.0% | **65.8%** | ✗ **未較正** |
| - | **NAR全体** | 31,784 | 20.7% | **102.6%** | - |

#### ⚠️ Platt 較正と ROI の相関は弱い

- **盛岡 (未較正)** ROI 127.1% (NAR 2 位) → 較正の有無は ROI 主因ではない
- **門別 (未較正)** ROI 65.8% (NAR 最下位) → 較正不在以外の要因が問題
- **金沢 (較正済)** ROI 79.9% (低位) → 較正済でも低 ROI

→ **Phase 2-A 完遂で ROI が大幅改善する保証はない**。実測で要確認。

### 8.6 詳細分析: NAR pace 重み顕著高の構造

#### 数値比較 (VENUE_COMPOSITE_WEIGHTS から)

| | JRA 10 場 | NAR 14 場 + 帯広 |
|---|---|---|
| pace 重み平均 | 0.30 | **0.51** |
| pace 重み最大 | 小倉 0.346, 福島 0.368 | 金沢 **0.599**, 佐賀 0.578, 浦和 0.569 |
| pace 重み最小 | 東京 0.249 | 大井 0.387, 船橋 0.354 |
| ability 重み平均 | 0.32 | 0.27 (低い) |

#### 構造的根拠

1. **距離分布**: NAR は短距離 (1100-1800m) 主体 → 展開・位置取りが結果を支配
2. **コース特性**: NAR は内回り砂質 + コーナー多 → 内枠・先行有利の固定化
3. **出走間隔**: NAR 馬は 1-2 週で出走 → 能力値変動小、展開差が顕在化
4. **クラス分布**: NAR は重賞少 + 同クラス内格差小 → 展開で勝負が決まる

→ settings.py コメント「NAR は pace 支配が合理的な競馬構造」と整合。施策#6 で `PACE_WEIGHT_CAP_NAR=0.50` 緩和済。

### 8.7 5/23 シミュレーション結果 (棄却された施策)

`TASKS.md L170-198` の実データより:

| 試した手法 | 詳細 | ROI 変化 | 結論 |
|---|---|---|---|
| ペース CAP 変更 | 0.35〜0.50 を 6 段階で測定 | 72.0% → 72.0% | 効果なし→棄却 |
| ability 偏差値拡散 | k 値× 1.0〜2.0 を 5 段階で測定 | 72.0% → 72.0% | JRA/NAR で分布同一 (stdev=4.7) → 棄却 |
| ML 合議無効化 | win_prob → composite 比例化 | 72.2% → 72.4% | 誤差範囲 (+0.2pp) → 棄却 |

→ **「composite ウェイト調整では改善不可能。問題は会場ごとの予想精度のバラつき」** (TASKS.md L181)

これが本 Phase 2/3/1 設計の出発点。NAR 会場別 ◎1着率と ROI 分布 (5/23 検証):

| 黒字 (ROI ≥ 80%) | ◎1着率 | ROI | 赤字 (ROI < 80%) | ◎1着率 | ROI |
|---|---:|---:|---|---:|---:|
| 船橋 | 30.1% | 103.8% | 金沢 | 24.1% | 50.2% |
| 水沢 | 27.8% | 88.1% | 門別 | 31.6% | 55.7% |
| 姫路 | 31.8% | 82.6% | 園田 | 30.3% | 55.5% |

★ **門別は◎1着率 31.6% (高水準) なのに ROI 55.7% (低水準)** = 配当が伸びていない or 印付与の精度問題。Phase 2-A で門別の Platt 較正を新規導入することで改善可能性が高い。

### 8.5 棄却・継続観察

| 案 | 状態 | 理由 |
|---|---|---|
| ペース重み削減 | ✗ 棄却 | 5/23 シミュレーションで効果なし確認 |
| k 値引上げ | ✗ 棄却 | ability 分布が JRA と同一 (stdev=4.7) |
| 金沢個別調整 | ✗ 却下 | マスター方針「全体で考えたい」 |
| ROI 低下対策 (regen_strategy.py NAR 最適化) | ⏸️ 保留 | Phase 1-3 完了後に再評価 |
| 2024-11月 三連複 payout 欠損 318 件 backfill | ⏸️ 別タスク | ROI 算定精度向上に直接寄与 (ダッシュボード 129.7% との乖離原因) |

---

## 9. 補足: 既存ドキュメントとの関係

| ドキュメント | 役割 | この監査との関係 |
|---|---|---|
| `SYSTEM_ARCHITECTURE_FULL.md` | 全体仕様書 (24 章) | 「47 モデル」は誤記、実際 43 を本ドキュメントで訂正 |
| `improvement_opportunities.md` | 改善案 | 本監査が P3 設計の基礎データを提供 |
| `memory/handoff_2026-05-13.md` | Walk-Forward 完成記録 | ROI 106.1% の検証基盤 |
| `memory/handoff_2026-05-24.md` | 施策#7 検証完了 | 本監査の前提となる施策 |
| `TASKS.md` (L170-198) | P2/P3 NAR 改善戦略 | 本監査が「NAR会場別MLモデル再較正」の設計を具体化 |

---

## 10. 次のアクション (マスター承認済 → 着手準備中)

### 10.1 承認状況

| Step | 内容 | 状態 |
|---|---|---|
| 1 | パイプライン全体可視化 (本ドキュメント) | ✅ 完了 |
| 2 | 数値補強 (モデル実数 38 / Platt 未較正 6 場特定 / 25 場の VENUE_COMPOSITE_WEIGHTS 実値 / k 値全帯) | ✅ 完了 |
| 3 | P3 着手順序の承認 (Phase 2-A → 2-B → 3 → 1) | ✅ マスター承認済 (5/24) |
| 4 | Phase 2-A 着手 (未較正 6 場の Platt 較正学習) | ⏸️ 次セッション開始 |

### 10.2 Phase 2-A 着手前の確認事項 (確認完了)

| # | 確認事項 | 結論 |
|---|---|---|
| 1 | `scripts/build_calibrator.py` の対応確認 | ✅ **Isotonic 用 (Phase 3 で使用)、Platt 較正は別** |
| 2 | データ使用範囲 | ✅ **直近 12 ヶ月** (札幌・函館の 6 ヶ月 0 件問題回避) |
| 3 | 検証スクリプト | ✅ `scripts/compare_basetime_results.py` 流用可 |

#### 確定した実装方針

- **新スクリプト不要**: `src/ml/lgbm_model.py:3563-3582` の最小修正のみ
- **修正内容**: `if len(valid_y) > 0:` の else 節追加 (訓練データの最後 10% を Platt 較正用フォールバック)
- **再学習対象**: 全 25 venue モデル (新規修正で 6 場新規較正 + 19 場リフレッシュ同時)
- **検証**: 過去 9 日 (5/14-22) で `compare_basetime_results.py` 流用
- **期待効果**: ⚠️ ROI 改善幅は限定的の可能性 (盛岡が未較正でも ROI 127.1% のため)

### 10.3 完了後の連鎖

| Phase | 完了基準 | 次フェーズ起動条件 |
|---|---|---|
| 2-A | 6 場の `_cal.json` 生成 + 9 日検証 ROI 報告 | 9 日 ROI が現状より悪化していない場合 2-B 着手 |
| 2-B | 19 場の Platt 再学習 + 9 日検証 + 1 ヶ月検証 | 同上 |
| 3 | Isotonic JRA/NAR 分離 + 検証 | 同上 |
| 1 | NAR SMILE 5 モデル新規 + 全期間 ROI 再算定 | 完成後 retrain_all.py に統合 |

### 10.4 ロールバック手段

- Phase 2-A: `_cal.json` を git で差分管理 → 悪化したら revert
- Phase 2-B: 既存 `_cal.json` をバックアップ後置換 (`*_cal.json.bak_YYYYMMDD`)
- Phase 3: 新ファイル追加のみ・既存ファイルは保持
- Phase 1: 新モデルファイル追加のみ・既存モデルは保持

→ **全 Phase でロールバック可能** (リスク低)

---

**作成者**: D-AI Claude (Sonnet 4.6)
**情報源**: 並列 Explore 5 本 (データ収集 / 特徴量 / ML / 合議+買い目 / 設定値) + 直接コード確認 + handoff_2026-05-13.md (Walk-Forward 検証) + settings.py 実値 + ファイルシステム実測 (`ls data/models/lgbm_place*.txt`)
**最終更新**: 2026-05-24 (実数値補強版)
**バージョン履歴**:
- v1 (初版): Agent 5 本の出力統合
- v2 (本版): モデル数 38 訂正・SMILE 7 個正確化・Platt 未較正 6 場特定・VENUE_COMPOSITE_WEIGHTS 全 25 場・k 値全 16 帯・Walk-Forward 母数 39,725 R
