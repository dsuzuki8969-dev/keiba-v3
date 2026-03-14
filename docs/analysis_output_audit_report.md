# 分析結果HTML出力 精査レポート

レース情報・レース概要・コース適性・展開予測・全頭評価・個別評価・印・穴馬・危険馬・買い目まわりを精査し、発見した不具合と修正内容をまとめる。

---

## 1. レース情報・レース概要（formatter レベル1）

### 1.1 修正済み

| 項目 | 問題 | 修正 |
|------|------|------|
| **内/外回り表示** | `c.inside_outside[0]` をそのまま使用していたため、`inside_outside="なし"` のときに「**右な回り**」と誤表示されていた | 「内」「外」のみ1文字を使い、それ以外は空にして「右回り」「左回り」と表示するよう変更。`io_char = (c.inside_outside if c.inside_outside in ("内", "外") else "") or ""` |
| **スタート〜初角** | `c.first_corner.isdigit()` を呼んでいたため、`first_corner` が `None` や空のときに AttributeError の可能性 | `first_corner` を `getattr(c, "first_corner", None) or ""` で取得し、数値時のみ "m" を付与。空のときは「—」表示 |
| **条件（サブ行）** | `r.condition` が空のとき、日付と「出走○頭」の間が欠けて見づらい | `r.condition or "—"` で未設定時は「—」を表示 |
| **グレード表示** | G1/G2/G3 のみバッジ表示で、OP・L・3勝・NAR（C3/B1等）がタイトルに何も出ていなかった | G1/G2/G3 は従来どおりバッジ、それ以外は `r.grade` をテキストで表示（`grade-text` スタイル） |

### 1.2 データ経路（確認済み）

- **レース情報**: `RaceInfo`（engine に渡る `race`）← スクレイパー／ダッシュボードで構築
- **レース概要**: `RaceAnalysis` の `favorable_gate` / `favorable_style` / `favorable_style_reason` ← `generate_pace_comment`（calibration）
- **コース特性**: `CourseMaster`（直線・コーナー・スタート〜初角）← netkeiba 等で構築

---

## 2. コース適性

- **計算**: `CourseAptitudeCalculator.calc()`（pace_course.py）→ `ev.course`（total, course_record, venue_aptitude, gate_bias, shape_compatibility 等）
- **表示**: formatter の個別評価カード（_hcard）で「コース適性」「当コース実績」「競馬場適性」「枠の有利不利」として表示
- **精査結果**: 上記「スタート〜初角」の None/空対策で表示まわりは修正済み。計算ロジックの不具合は今回未検出。

---

## 3. 展開予測

- **算出**: `PacePredictor.predict_pace` → 逃げ/好位/中団/後方のリスト、`generate_pace_comment` でコメント・有利脚質
- **表示**: レベル1の「展開予測」ブロック（ペース予測・前半3F/後半3F・逃げ/好位/中団/後方・見解）
- **精査結果**: `estimated_front_3f` / `estimated_last_3f` の None 時は「—」で表示済み。表示側の追加不具合は未検出。

---

## 4. 全頭評価・個別評価

- **全頭評価**: レベル2（馬番一覧）・レベル3（カードリスト）で `HorseEvaluation` の composite / ability / pace / course / 勝率・連対率・複勝率を表示
- **個別評価**: _hcard で能力・展開・コース・騎手・厩舎・調教・馬場適性・血統補正・close_race_win_rate 等を表示
- **精査結果**: `close_race_win_rate` は `(0,0)` デフォルトで `max(..., 1)` により除算エラー回避済み。表示まわりで追加の不具合は未検出。

---

## 5. 印・穴馬・危険馬（marks.py）

### 5.1 修正済み

| 項目 | 問題 | 修正 |
|------|------|------|
| **穴馬「妙味あり」** | 理由文で `h.odds` をそのまま参照しており、前日モードなどで `h.odds is None` のときに「**None倍の妙味あり**」と表示されていた | オッズありなら「○倍の妙味あり」、なしなら「オッズ未確定の妙味あり」と分岐 |

### 5.2 その他

- 危険馬の理由文では既に `os = f"{h.odds:.1f}倍" if h.odds else "—"` を使用しており、同様の誤表示はなし。
- 印・穴馬・危険馬の**判定ロジック**（engine の Step 6〜8、jockey_trainer の calc_ana_score / calc_kiken_score、formatter の assign_marks）は今回変更していない。

---

## 6. 買い目（レベル5）

- **生成**: `generate_tickets`（馬連等）＋ `generate_formation_tickets`（col1/col2/col3 × 馬連・三連複）
- **表示**: `BettingMixin._level5` で formation の `umaren` / `sanrenpuku` を表示。各チケットは `odds` / `appearance`（出現率%）/ `ev` を参照。
- **精査結果**: formation チケットには `appearance: prob * 100` が入っており、表示側のキー（`appearance`）と一致。買い目表示まわりの追加不具合は未検出。

---

## 7. 今後の確認推奨

- **RaceInfo.condition** の設定元（スクレイパー／ダッシュボード）が空になる条件の有無。
- **CourseMaster.first_corner** に数値以外（「短い」「平均」等）が入る場合の表示意図の統一。
- 地方競馬・NAR グレード表記がレースタイトルで意図どおり出ているかの実機確認。

---

## 修正ファイル一覧

- `src/output/formatter.py`: レース情報1行の内/外回り・スタート〜初角・condition・グレード表示
- `src/output/marks.py`: 穴馬理由文のオッズ未設定時表記

以上。
