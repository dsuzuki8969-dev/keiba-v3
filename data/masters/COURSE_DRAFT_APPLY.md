# たたき台を course_master.py に適用する際のマッピングと注意点

## フィールド対応

| たたき台(JSON) | CourseMaster |
|----------------|--------------|
| venue | venue |
| venue_code | venue_code |
| distance | distance |
| surface | surface |
| direction | direction |
| straight_m | straight_m |
| corner_count | corner_count |
| corner_type | corner_type |
| first_corner | first_corner |
| slope_type | slope_type |
| inside_outside | inside_outside |
| is_jra | is_jra |

※ `_raw_corner`, `_raw_start` はデバッグ用で CourseMaster には不要。

---

## 適用方法

1. `python scripts/draft_to_course_master.py` を実行
2. 生成された `data/masters/course_master_generated.py` の `ALL_COURSES` 部分を
3. `data/masters/course_master.py` の `ALL_COURSES` にコピーして差し替え

---

## 主な差異・注意点

### 1. venue_code（地方競馬）

| 競馬場 | 既存 course_master | たたき台 / venue_master |
|--------|--------------------|--------------------------|
| 大井   | 22                 | **44**                   |
| 川崎   | 21                 | **45**                   |
| 船橋   | 19                 | **43**                   |
| 浦和   | 20                 | **42**                   |
| 盛岡   | 46                 | **36**                   |
| 水沢   | 47                 | **37**                   |
| 金沢   | 37                 | **46**                   |
| 笠松   | 35                 | **47**                   |
| 名古屋 | 36                 | **48**                   |
| 園田   | 30                 | **49**                   |
| 高知   | 42                 | **54**                   |
| 佐賀   | 41                 | **55**                   |

**重要**: たたき台は `venue_master` および netkeiba/race_id の形式を使用。既存の course_master の地方 venue_code は他モジュールと不整合の可能性あり。適用により整合する。

### 2. コーナータイプ・坂の差異（CSV vs 既存の手動値）

| 競馬場 | 距離・面 | 既存 | たたき台(CSV由来) |
|--------|----------|------|---------------------|
| 札幌   | 芝全般   | 小回り | 大回り |
| 中山   | 芝・ダート | スパイラル, 急坂 | 小回り, 坂なし |
| 福島   | 芝・ダート | 小回り | スパイラル |
| 新潟   | 芝       | 大回り | スパイラル |

※ CSVの「大回/小回/ス曲」をそのままマッピングしたため、既存の専門的な分類と異なる場合がある。必要に応じて個別に修正。

### 3. 追加される競馬場

- **門別** (venue_code: 51)
- **姫路** (venue_code: 50)

既存 course_master には含まれていなかったが、たたき台には存在。

### 4. course_id の設計

`CourseMaster.course_id` は `{venue_code}_{surface}_{distance}` のみ。  
阪神芝1600「内」と「外」は別の `CourseMaster` インスタンスだが、`course_id` は同じ `"08_芝_1600"` になる。  
`build_course_index()` で辞書化すると後勝ちで1件だけ残る点に注意。

### 5. 内外（inside_outside）

内外がある競馬場（阪神・京都・新潟など）は、内回り・外回りで **straight_m** や **first_corner** が異なる。たたき台は CSV の内外情報を反映している。

### 6. スタート〜初角（first_corner）

距離が不明なため、CSV の定性値（短い/平均/長い/直線のみ 等）をそのまま保持。適当な数値に変換しない。

---

## 推奨

- **venue_code**: たたき台の値（= venue_master）を採用し、他モジュールとの整合を取る。
- **corner_type / slope_type**: たたき台をベースにしつつ、中山・東京など主要場は公式資料で確認して必要なら修正する。
