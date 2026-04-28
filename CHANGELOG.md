# D-AI Keiba v3 — CHANGELOG

---

## v6.2.14 （2026-04-28）— CI 強化 + D 追加バックフィル + 同馬統合

**commit**: bb02b9e

### 概要
CI パイプライン（GitHub Actions）の安定化、horses マスターへの追加バックフィル、同名異馬の統合処理を一括実施。

### 主な変更
- CI: Python 環境ビルドキャッシュ追加 + テスト並列化
- horses マスター: 追加バックフィルで `netkeiba_id` 補完率を向上
- 同名異馬統合: `merge_duplicate_horses.py --apply` で重複 horse_id を統一
- `audit_pred_venue.py`: 統合後の venue 整合性を全件確認

---

## v6.2.13 （2026-04-28）— sample 系昇格 + featureFlags 整理 + venue audit（後追い）

**commit**: 3cc6ba6

### 概要
前セッションで「サンプル」として隔離していたコンポーネントをマスター承認後に本実装へ統合。featureFlags.ts の不要フラグを整理し、venue audit スクリプトを修正。

### 主な変更
- sample 系コンポーネント → 既存ファイルへ統合（`feedback_sample_vs_implementation.md` 準拠手順）
- `frontend/src/lib/featureFlags.ts`: 廃止フラグ削除・整理
- `scripts/audit_pred_venue.py`: JRA venue_code 位置誤読バグ修正（v6.2.13 で恒久修正）

---

## v6.1.50 （2026-04-28）— D Phase 2+3: horses.netkeiba_id カラム追加 + 42,515 件補完

**commit**: 3698789

### 概要
horses マスターテーブルに `netkeiba_id` カラムを新設し、`backfill_horses_netkeiba_id.py` で全 42,515 頭の外部 ID を補完。残 2,847 件は 403 エラー（主に 2023 年若駒）で継続バックフィル中。

### 主な変更
- `data/masters/horses_master.db`: `netkeiba_id TEXT` カラム追加（ALTER TABLE）
- `scripts/backfill_horses_netkeiba_id.py`: 新規。netkeiba 馬詳細ページから ID 取得・UPDATE
- 補完率: 42,515 / 45,362 = 93.7%（残 2,847 件は追加バックフィル対象）

---

## v6.2.12 （2026-04-28）— 持ち越し C+D Phase 1: CI 統合 + JRA horses マスター

**commit**: 355462b

### 概要
前セッション持ち越しの CI 統合（GitHub Actions workflow 新設）と JRA 馬マスター構築（Phase 1）を実施。`scripts/build_horses_master.py` で race_log から全競走馬を集約。

### 主な変更
- `.github/workflows/ci.yml`: 新規。Python lint + テスト + build 自動化
- `scripts/build_horses_master.py`: 新規。race_log → horses マスター構築
- `data/masters/horses_master.db`: 初期構築（git 管理対象に追加）

---

## v6.2.11 （2026-04-28）— 園田 venue_code 49→50 統一: netkeiba race_id 主軸

**commit**: 21b8791

### 概要
園田競馬場の venue_code が 49（旧）と 50（新）で混在していたため、netkeiba race_id を主軸に 50 へ統一。2026 年度から新コード対応。

### 主な変更
- `src/scraper/venue_master.py`: 園田を `49 → 50` に変更
- `config/settings.py`: 園田 venue_code 定数を 50 に統一
- race_log の既存レコードは horse_id JOIN のため影響なし（新規取得分から適用）

---

## v6.2.10 （2026-04-28）— /api/force_refresh_today admin 制限除去

**commit**: fdc715a

### 概要
`/api/force_refresh_today` エンドポイントのアクセス制限を `_is_admin()` から除去し、Cloudflare 経由の一般ユーザーでも手動更新ボタンが動作するよう修正。セキュリティは IP + Rate limit で維持。

### 主な変更
- `src/dashboard.py`: `force_refresh_today` の `_is_admin()` ガード削除
- IP 単位 5 秒 Rate limit / threading.Lock 連打防止は継続維持

---

## v6.2.9 （2026-04-28）— 「Bを再開して」コマンド対応（restart_backfill_b.ps1）

**commit**: e643f37

### 概要
バックフィル B（backfill_b）の中断再開を単一コマンドで安全に実行できる PowerShell スクリプトを新設。PID ファイル方式の二重起動防止を実装。

### 主な変更
- `scripts/restart_backfill_b.ps1`: 新規。PID 確認 → 生存なら SKIP、停止なら detach 起動
- BOM 付き UTF-8 で保存（Windows PowerShell 5.1 対応）
- ログ: `logs/backfill_b.log` に追記

---

## v6.2.8 （2026-04-28）— 持ち越し B: 2023 下半期 race_log バックフィルスクリプト

**commit**: c7e13bf

### 概要
2023 年下半期の race_log データ欠損を補完するバックフィルスクリプトを新設。チェックポイント方式で中断再開可能。

### 主な変更
- `scripts/backfill_nar_course_db.py`: 2023 下半期 NAR データ補完
- チェックポイント: `tmp/backfill_b_checkpoint.json`

---

## v6.2.7 （2026-04-28）— 持ち越し E: race_log.horse_id 空 6,742 → 8 件に削減

**commit**: bdf1488

### 概要
race_log の `horse_id = NULL` または空文字レコードが 6,742 件存在していた問題を調査・修正。`scripts/refix_empty_course_id.py` で 6,734 件を正常値に復元。

### 主な変更
- `scripts/refix_empty_course_id.py`: 新規。horse_id 空レコードを horse_name × race_date の照合で復元
- 復元率: 6,742 件 → 残 8 件（真の意味で不明な馬のみ残存）

---

## v6.2.6 （2026-04-28）— 持ち越し P1 系: e2e + horse_id 真因 + race_log 監査

**commit**: 9a4e718

### 概要
e2e テストの追加、horse_id 空欄の真因調査（スクレイパーの INSERT 漏れ）、race_log 全件の整合性監査を実施。

### 主な変更
- `tests/test_e2e_pipeline.py`: 新規 e2e テスト
- `src/scraper/netkeiba.py`: horse_id 取得漏れパターンを修正（429 エラー時の fallback）
- `scripts/daily_data_quality_check.py`: 新規。race_log 整合性を毎日チェック

---

## v6.2.5 （2026-04-28）— Plan-α MEDIUM + P2 lms 自動化: 完走最終

**commit**: b5af968

### 概要
Plan-α の MEDIUM 優先度タスク（ability_total レンジ拡張の後追い検証）と LM Studio の自動起動スクリプトを完成。

### 主な変更
- `scripts/local_llm_paraphrase.py`: LM Studio 未起動時に自動起動する前処理を追加
- ability_total 分布検証: 4/28 pred.json で -36 〜 100 の連続分布を確認（張り付き 0 件）

---

## v6.2.4 （2026-04-28）— 持ち越し P1 系: finish_time / horse_id / Step 5

**commit**: bdf5d93

### 概要
前セッション持ち越しの finish_time_sec 正規化、horse_id 統合ロジック、MultiSourceEnricher Step 5（血統データ注入）を実施。

### 主な変更
- `src/database.py`: `finish_time_sec` の異常値（99=失格コード）防御を強化
- `src/scraper/horse_db_builder.py`: MultiSourceEnricher Step 5 — 血統データを horses テーブルに補完
- horse_id 統合: `horses_master.db` の horse_id と race_log.horse_id を照合・整合

---

## v6.2.3 （2026-04-28）— Plan-γ Phase 5+6 + T-029 完了: 絶対/相対切替 + バックテスト + 用語辞書

**commit**: 9110346

### 概要
Plan-γ の最終 Phase（5: フロント切替表示 / 6: ROI バックテスト）と T-029（深層用語辞書）を一括完了。

### 主な変更
- `frontend/src/pages/TodayPage/HorseCardPC.tsx` / `HorseCardMobile.tsx`: 絶対/相対偏差値トグルスイッチ追加
- `frontend/src/lib/featureFlags.ts`: `USE_RELATIVE_DEV` フラグ追加
- `scripts/backtest_phase4_relative_dev.py`: 旧モデル vs 新モデル ROI 比較バックテスト
- `src/nlp/racing_term_dict.py`: 競馬専門用語辞書（新規）

---

## v6.2.2 （2026-04-28）— T-038 Phase 3 補完: 開催カレンダー日付クリック遷移実装

**commit**: 9d3aa42

### 概要
開催カレンダー（v6.2.1）の UI 機能を補完。日付クリック時に成績ページまたは予想ページへ遷移するルーティングを追加。

### 主な変更
- `frontend/src/pages/CalendarPage.tsx`: 日付クリック → `/results?date=YYYY-MM-DD` or `/today?date=YYYY-MM-DD` へ遷移
- 遷移先判定: カレンダーに予想 JSON が存在する日付は `today` ページ、それ以外は `results` ページ

---

## v6.2.1 （2026-04-28）— T-038 開催カレンダー機能（Phase 1+3+4）: venue/date バグ恒久予防

**commit**: dbbf315

### 概要
JRA + NAR 全開催日（2022-01〜2026-12）をカレンダーマスタとして構築し、ダッシュボードに月別グリッド UI を追加。パイプライン整合性の ground truth として活用。

### 主な変更
- `data/masters/kaisai_calendar.json`: 新規（1,583 開催日 / 259KB）
- `scripts/build_kaisai_calendar.py`: 新規。netkeiba カレンダーページから構築
- `src/scraper/kaisai_calendar_util.py`: 新規。`validate_race_against_calendar` 関数
- `frontend/src/pages/CalendarPage.tsx`: 新規。月別グリッド表示 UI
- `src/dashboard.py`: `/api/kaisai_calendar` エンドポイント追加
- `run_analysis_date.py` / `backfill_ml_from_cache.py`: カレンダー照合 hook 追加（非開催日 early exit）

---

## v6.2.0 （2026-04-28）— T-033 Phase 1+2: pred.json venue フルクリーニング

**commit**: d00ca64 / bbcdf44

### 概要
T-033 (venue_code 不整合) の根本修正。`date_from_race_id()` の JRA race_id 誤判定を修正（Phase 1）し、過去全 pred.json の venue フィールドを一括クリーニング（Phase 2）。

### Phase 1（commit: bbcdf44）
- `src/scraper/netkeiba.py`: `date_from_race_id()` の JRA race_id パターンマッチを修正
- 修正前: `race_id[0:2]` が NAR 形式と衝突するケースで venue_code を誤判定
- 修正後: JRA race_id の構造（8 桁 + 先頭 2 桁が venue_code）を正しく解析

### Phase 2（commit: d00ca64）
- `scripts/fix_pred_json_venue.py`: 新規。過去 pred.json の `race.venue` フィールドを全件 UPDATE
- 対象: 1,691 件の不整合レコードを修正

---

## v6.1.49 （2026-04-28）— T-032: モバイル馬カード「能力/展開/適性 + EV + 勝率」重複表示削除

**commit**: adde812

### 概要
モバイル版馬カードで「能力/展開/適性」「EV」「勝率」が 2 箇所に重複表示されていたバグを修正。

### 主な変更
- `frontend/src/pages/TodayPage/HorseCardMobile.tsx`: 重複する JSX ブロックを削除

---

## v6.1.48 （2026-04-28）— T-031: 過去成績ページに選択日成績カード追加（StatsCard 共通化）

**commit**: b1d07a4

### 概要
過去成績ページ（ResultsPage）に「選択日の成績サマリー」カードを追加。StatsCard コンポーネントを共通化し、ホーム画面と成績ページで同一コンポーネントを再利用。

### 主な変更
- `frontend/src/components/StatsCard.tsx`: 新規共通コンポーネント
- `frontend/src/pages/ResultsPage.tsx`: 選択日成績カード追加
- `frontend/src/pages/HomePage.tsx`: StatsCard に移行

---

## v6.1.47 （2026-04-28）— T-027 中層: Qwen prompt 強化 + paraphrase 全 cache 再生成

**commit**: d3c1e8d

### 🎯 背景
マスター指摘「だよ」「と思う」じゃねーんだわ + 8 件の品質チェック → regex で対応不可な体言止め化・語順自然化を Qwen 2.5-7B prompt 強化で対応。

### ✨ prompt 強化 (scripts/local_llm_paraphrase.py)
- 旧 6 ルール → **新 9 ルール + 8 例 + 悪い例セクション**
- 「条件→結果」語順 / 体言止め優先 / 冗長表現削除を明示
- 「〜と思う」「〜だなあ」禁止を悪い例で明示
- 中国語混入フィルタ強化 (15 種 frozenset)

### 📊 マスター期待 4 例 全完璧結果
| 元 | 新 paraphrase |
|---|---|
| ひと開催あけて体をケアした | **ひと開催あけて馬体のケア** |
| 距離は問題ないので、自分のリズムで運べれば | **自分のリズムで運べれば距離は問題ない** |
| 休み明けを２連勝した後も好調だ | **休み明け連勝後も好調を維持** |
| 再始動期待 | **改めてここから期待** |

### 📊 再生成統計
- 直近 30 日 / 19,043 件新規 LLM 処理 / 80 分
- 最終キャッシュ 18,944 件 / 中国語混入 0 件
- 平均文字長 11.9 字 (旧 10.2 字より向上)

---

## v6.1.46 （2026-04-28）— T-030: 「閲覧モード」バッジ削除

**commit**: 941a962

### 🎯 マスター指示
「表示なくていいかな」 → ヘッダーバッジ削除。

### 🔧 修正
`frontend/src/components/layout/TopNav.tsx`:
- `{!isAdmin && <span>閲覧モード</span>}` ブロック削除
- `useAuth` import 削除 (TopNav 内で参照ゼロ)

### 🛡 セキュリティ維持
バックエンド `_is_admin()` + 9 箇所の API 403 ガードはそのまま。**バッジを消すだけで機能維持**。

### 検証
Playwright で `has_browse_mode: false` 確認 ✅

---

## v6.1.45 （2026-04-28）— T-028: 口語残存 4 種削除

**commit**: 87f852f

### 🐛 マスター品質チェック (重大 + 中程度) regex 対応分
- 「動けるんじゃない」 → 「動ける」 (口語疑問形)
- 「初マイル戦になるけど」 → 「初マイル戦になる」 (口語接続)
- 「いい経験にしたいところ」 → 「いい経験に」 (婉曲)
- 「あとは、同型を捌ければ」 → 「同型を捌ければ」 (口語接続詞)

### 🔧 修正
`parseStableComment.ts` ASSERTIVE_RULES に 3 ルール追加 + sentence 冒頭の口語接続詞削除パターン (5b) 追加。

### 検証
Playwright で 4 項目全て 0 件確認 ✅

---

## v6.2.0-phase4 （2026-04-28）— Plan-γ Phase 4: ML 特徴量 + 47 LightGBM モデル再学習

**commit**: c2612e8

### ✨ 新機能
- `src/ml/features.py` + `src/ml/lgbm_model.py` に 3 列追加:
  - `relative_dev_mean_5` / `relative_dev_max_5` / `relative_dev_recent`
- 159 → 162 features

### 📊 学習結果 (Sonnet.11 / 17.3 分)
- 38 モデル成功 / 5 モデルスキップ
- global AUC 0.7543 / Top1 64.0%
- venue 別 AUC 0.65 (帯広) 〜 0.85 (jra_turf_l 重賞)

### バックアップ
`data/models/.bak_pre_relative_dev/` (93 ファイル / 371MB / git 管理外、撤回可能)

### 🚧 残 Phase
- Phase 5: フロント表示 (絶対/相対 切替)
- Phase 6: バックテスト (旧 vs 新モデル ROI 比較)

---

## v6.1.44 （2026-04-28）— T-026: カテゴリタグ削除 + T-027 表層: 「かなあ/だなあ/！」末尾削除

**commit**: af06061

### T-026
4 ファイル (HorseCardPC/Mobile/StableCommentPanel/HorseDiagnosis) からカテゴリ黄色バッジ JSX 削除。

### T-027 表層
`parseStableComment.ts` に長母音感嘆 (「かなあ/だなあ」) + 「！」末尾削除ルール追加。

### 検証
Playwright で 「ローテ/適性/期待/馬体」バッジ 0 件 + 「かなあ/だなあ/！」0 件確認 ✅

---

## v6.1.43 （2026-04-28）— T-023/24/25: 厩舎コメント prefix/箇条書き完全統一

**commit**: 7ad9131

### 🐛 マスター指摘 4 件
1. モバイル箇条書き化欠落
2. 「・」マーカー欠落
3. 「○馬名(短評)」「[人名]師――」「担当厩務員――」「○○助手――」prefix 残存
4. 「と思う」「だよ」を言い切り化したい

### 🔧 修正
- `parseStableComment.ts`: パターン4/6 を `(師|厩務員|助手|マネジャー)` 拡張 + ASSERTIVE_RULES 追加
- 4 表示コンポーネント統一: `bullets ? bullets.join('\n') : stableComment` を `parseStableComment` に通す

### 検証
Playwright PC + モバイル両 viewport / 9 馬で歯抜けゼロ確認 ✅

---

## v6.1.42 （2026-04-28）— T-022 リトライ: HORIZONTAL BAR + 全角空白対応

**commit**: 5634989

### 🐛 真因 (前回見落とし)
1. 実データのダッシュは `―` (HORIZONTAL BAR U+2015) なのに regex は `[—\-－]` (3 種)
2. 改行直後に全角スペース `　` (U+3000) で「\n + 真島師」マッチ失敗

### 🔧 修正
- ダッシュ regex を `[—\-－―]` (4 種) に拡張
- パターン4 を `(^|\n)[\s　]*([^\s　\n。．]+?師)[—\-－―]+\s*` に修正

### Python シミュレーション PASS

---

## v6.1.40 （2026-04-28）— P0: src/output/ 8 ファイル (3,301 行) git 管理外救済

**commit**: 5bb7aac

### 🚨 真因
`.gitignore` の `output/` パターンが root ディレクトリの output/ (HTML 生成物) を意図したものだったが、`src/output/` (本実装ソースコード) も巻き込んで git 管理外になっていた。

### 影響範囲（全て git 管理対象に救済）

| ファイル | 行数 | 役割 |
|---|---:|---|
| src/output/formatter.py | 1,925 | 印付与 (assign_marks) / Plan-γ Phase 3 _scoring_value 含む |
| src/output/marks.py | 354 | 印マーク定義 |
| src/output/betting.py | 283 | 買い目生成 |
| src/output/past_runs.py | 245 | 前三走表示 |
| src/output/narrative.py | 219 | レースナラティブ |
| src/output/css.py | 206 | HTML CSS |
| src/output/grade_helpers.py | 68 | グレード判定 |
| src/output/__init__.py | 1 | パッケージ |
| **合計** | **3,301 行** | |

### 🔧 修正
- `.gitignore`: `output/` → `/output/` に変更 (root only に限定)
- `.gitignore`: `!src/output/` 例外指定追加
- `git add -f` 不要、通常 add で 8 ファイル全て stage 可能になった

### ⚠ 経緯と教訓
- 2026-04-27 深夜に Plan-γ Phase 3 を Sonnet.9 へ委託
- Sonnet.9 は formatter.py を編集したと報告するも `git status` に出ず
- Opus が `git check-ignore -v` で原因特定 → P0 案件としてマスター承認待ち
- 2026-04-28 朝マスター A 案承認 → 即実施
- 教訓: 新規ファイル追加時は `git status` 確認、何も出なければ `git check-ignore -v` で確認

---

## v6.2.0-phase3 （2026-04-27）— Plan-γ Phase 3: hybrid_total + USE_HYBRID_SCORING フラグ

**commit**: b3f045a

### 🎯 背景
Phase 1/2 完了 (race_log.relative_dev / pred.json.race_relative_dev) を受けて、
ability_total と race_relative_dev を β=0.30 でブレンドした hybrid_total プロパティを追加。
USE_HYBRID_SCORING フラグで印付与の判定値を切替可能 (default False で従来動作維持)。

### ✨ 新機能
- `config/settings.py`: `USE_HYBRID_SCORING: bool = False`, `HYBRID_BETA: float = 0.30`
- `HorseEvaluation.hybrid_total` @property: `at*(1-β) + rrd*β` で DEVIATION クランプ
- `pred.json` に `hybrid_total` 出力
- `src/output/formatter.py` の `_scoring_value()` で USE_HYBRID_SCORING 切替 (※git 管理外、別 commit で救済)

### 🛡 動作仕様
- USE_HYBRID_SCORING=False (default): 従来動作完全維持 (composite ベース印付与)
- USE_HYBRID_SCORING=True: hybrid_total 採用 (※本番切替は Phase 6 バックテスト後)

### ⚠ 既知の漏れ
`src/output/formatter.py` 含む src/output/ 7 ファイル (3,301 行) が `.gitignore` の
`output/` パターンで git 管理外。翌朝マスター承認後に .gitignore 例外指定 + 救済 commit 予定。

### 🚧 残 Phase
- Phase 4: ML 特徴量追加 + 再学習
- Phase 5: フロント表示 (絶対/相対 切替) + Plan-β 統合
- Phase 6: バックテスト ROI 比較

---

## v6.1.39 （2026-04-27）— T-021: 調教 (追切) 印 全頭◎固定 → データなし時「−」表示

**commit**: 18ea149

### 🎯 背景
マスター指摘「調教記載がない競馬場で調教（追切）の印が全頭◎固定になっている。これは微妙だから「−」にしよう」

### 🐛 真因
1. INDEX_DEFS 追切軸: `getValue: (h) => h.training_dev ?? 0` (null→0)
2. `calcRanks()` で全頭 value=0 → 全員ランク 1 位
3. `rankToAxisMark(1) = "◎"` を hasVal チェックなしに無条件表示
4. → 調教データなし会場 (門別 等) で全頭◎固定

### 🔧 修正
- `frontend/src/pages/TodayPage/HorseCardPC.tsx:397-399` (AxisCell)
- `frontend/src/pages/TodayPage/HorseCardMobile.tsx:527-531` (インライン 8 軸)

```diff
- const axMark = rankToAxisMark(rank);
+ const axMark = hasVal ? rankToAxisMark(rank) : "−";
```

### 📊 検証
- 門別 (training_dev=None): 全頭「−」表示確認
- 大井 (training_dev あり): ◎/○/▲ 従来表示維持

### 副次発見
バックエンド (engine.py の `_compute_training_devs`) は既に正しく
「3 頭未満なら _training_dev=None」を実装済み。問題はフロントのみで完結。

---

## v6.1.38 （2026-04-27）— T-020: force_refresh_today pending 不整合解消

**commit**: 50adc1e

### 🎯 背景
T-017 (v6.1.34) で実装した手動更新ボタンが、画面 LIVE STATS の「集計 X / 終了 Y」と乖離していた。マスター指摘「お前の目には」級の確認漏れ防止のため、Playwright で実機検証時に発見された pending 計算不整合を解消。

### 🐛 真因
- `_get_pending_fetch_stats` (LIVE STATS) = 発走時刻直後から pending
- `_count_pending_races` (force_refresh_today) = 発走+10分経過後のみ pending
- → ボタン押下時 force_refresh の pending=0 で「変化なし」を返していた

### 🔧 修正
- `_count_pending_races(date, force=False)` に force 引数追加
  - `force=True` で 10 分閾値解除 (LIVE STATS と一致)
- `_auto_fetch_post_races` line 5440 の 10 分閾値も force=True で bypass
- `/api/force_refresh_today` で `_count_pending_races(date, force=True)` を呼ぶ

### 🛡 安全性
- USE_HYBRID_SCORING=False / 自動 fetch (force=False) は完全従来動作維持
- netkeiba 未掲載 race への試行は errors+= で記録、レートリミットは独立制御

---

## v6.2.0-phase2 （2026-04-27）— Plan-γ Phase 2: race_relative_dev (当該レース内 z-score) 出力

**commit**: 8089a3f

### 🎯 背景
Phase 1 で `race_log.relative_dev` (過去走の同 race_id 内 z-score) 全期間バックフィル完了。Phase 2 では当該レースの `ability_total` を同レース内で z-score 正規化した `race_relative_dev` を pred.json に出力。

### ✨ 新機能
- `HorseEvaluation.race_relative_dev: float = 50.0` フィールド追加
- `engine._calc_race_relative_dev(evaluations, ...)` ヘルパー追加
  - σ_floor=5.0, ±3σ クランプ → 範囲 20.0〜80.0
  - field_count<5 / ability=None はスキップ (50.0 維持)
- `pred.json` に `race_relative_dev` フィールド出力
- `scripts/verify_phase2_race_relative_dev.py` 新規

### 📊 検証結果 (4/28 大井 12 race / 146 馬)
- 全 race で μ=50.00 ピッタリ (z-score 正規化完璧)
- SIGMA_FLOOR=5.0 が横並びレース (R1 σ=2.78) で適切に作動
- 全 146 馬で race_relative_dev フィールド NOT NULL (100%)

### 🚧 残 Phase
- Phase 3: hybrid_total + USE_HYBRID_SCORING フラグ
- Phase 4: ML 特徴量追加 + 再学習
- Phase 5: フロント表示 (絶対/相対 切替)
- Phase 6: バックテスト ROI 比較

---

## v6.1.37 （2026-04-27）— T-019 リトライ: TOP3 内部要素サイズ完全統一

**commit**: bff455d

### 🐛 経緯
v6.1.36 (03d982a) で `padding="lg"→"md"` だけ変更したが、内部 CardContent の `large=true` プロパティで筆頭だけ高さ膨らみが残っていた。マスター指摘「お前の目にはこれは高さが揃って見えるのか？」を受けて完全リトライ。

### 🔧 修正
- `<CardContent r={first} large />` から `large` プロパティ削除
- 全 3 枚で内部要素 (タイトル/レース名/馬名/数字) サイズ完全統一

### 📐 ピクセル単位検証
| カード | width | height | top | bottom |
|---|---:|---:|---:|---:|
| 筆頭 | 292 | 189 | 201 | 389 |
| 次点 | 292 | 189 | 201 | 389 |
| 第3候補 | 292 | 189 | 201 | 389 |

→ 全 3 枚 width/height/top/bottom 完全一致

### ⚠ 反省
- `feedback_test_verification_strict.md ★` 違反 1 件
- 教訓: UI 揃いの判定は `getBoundingClientRect()` ピクセル測定が標準

---

## v6.1.36 （2026-04-27）— T-019 不完全版（v6.1.37 で完全解消）

**commit**: 03d982a (リトライで上書き)

`padding="lg"→"md"` のみで対応したが、内部要素サイズ差を見落とした。

---

## v6.1.35 （2026-04-27）— T-018: 帯広/大井 行揃いズレ修正 (表層+中層)

**commit**: e6f769d

### 🎯 背景
ホーム「本日の開催競馬場」カードで大井に「大雨」表示があるのに帯広は天気欄が空 → カード行高ズレ。

### 🐛 真因
帯広の venue_code 不一致:
- venue_master.py = "帯広": "65" (netkeiba race_id 準拠)
- dashboard.py VENUE_COORDS = "52" のみ (SPAT4 互換)
- → `VENUE_COORDS.get("65")=None` で天気取得スキップ

### 🔧 修正
- 表層 (HomePage.tsx): 天気行を `min-h-[1rem]` プレースホルダ化
- 中層 (dashboard.py): `VENUE_COORDS["65"] = (42.93, 143.20)` 追加

### 📊 実機検証
- Flask 再起動後 `/api/home_info` で `weather["帯広"] = "くもり"` 取得確認
- Playwright で UI 反映確認

---

## v6.1.34 （2026-04-27）— T-017: リアルタイム成績 手動更新ボタン + 自動更新高速化

(v6.1.34 詳細は v6.1.34 セクションへ)

---

## v6.2.0-phase1 （2026-04-27）— Plan-γ Phase 1: ハイブリッド能力指数（絶対 × 他馬比較）

**commit**: 5b9ebbc

### 🎯 背景
能力偏差値が完全タイム比較（コース基準タイム vs 走破タイム）であるため、帯広ばんえい 200m で上限/下限張り付き 50,124 件発生（全張り付きの 98%）。マスター発案「能力指数を他馬比較に変えたら？」を契機にハイブリッド設計（絶対指標を維持しつつ相対指標を追加）を採用。

### ✨ 新機能 (Phase 1: データ層)
- **`race_log.relative_dev` カラム新設**: 過去走 `run_dev` を同 race_id 内で z-score 正規化
- **計算式**: `50 + 10 × clip((run_dev − μ) / max(σ, 5.0), -3, 3)` → 範囲 20.0〜80.0
- **帯広(venue_code=65) 順位ベースフォールバック**: ばんえい特殊仕様に対応
- **field_count<5 / run_dev=NULL はスキップ**: 信頼性なし時は NULL 維持
- **`scripts/backfill_relative_dev.py`**: 全期間バックフィル（--dry-run/--date-from/--date-to/--force）
- **`tests/test_backfill_relative_dev.py`**: 単体テスト 20 件全 pass
- **設定追加** (`config/settings.py`): `RELATIVE_DEV_SIGMA_FLOOR=5.0`, `Z_CLAMP=3.0`, `BANEI_VENUE_CODE=65`, `MIN_FIELD=5`

### 📊 バックフィル結果
- 全期間 723,046 行中 710,439 行 NON-NULL（97.5%）
- **帯広(65) >= 100 張り付き: 24,905 件 → 0 件** ★完全解消
- **帯広(65) <= -50 張り付き: 25,219 件 → 0 件** ★完全解消
- 範囲外異常値（<0 OR >100）: 0 件
- 副次バグ修正: 99=失格コード防御（異常値 -420.1 検出 → 50.0 固定）+ テスト 2 件追加

### 🚧 残 Phase
詳細プラン: `plans/plan-gamma-hybrid-relative-dev.md`
- Phase 2: engine.py で当該レース内 race_relative_dev 計算 + pred.json 出力
- Phase 3: hybrid_total プロパティ + USE_HYBRID_SCORING フラグ + 印付与切替
- Phase 4: ML 特徴量追加 + 再学習（旧モデル保持）
- Phase 5: フロント表示（絶対/相対 切替）+ Plan-β（ZONE_BANDS -50 追従）統合
- Phase 6: バックテスト（絶対 vs ハイブリッド ROI 比較）

---

## v6.1.34 （2026-04-27）— T-017: リアルタイム成績 手動更新ボタン + 自動更新高速化

**commit**: daa921c

### 🎯 背景
マスター指摘「ホーム LIVE STATS の集計が 19R で、終了 24R に対して 5R 遅延している。即座に更新できるボタンが欲しい」。
真因: `_AUTO_FETCH_COOLDOWN_SEC = 60秒` がフロント polling 2 分と相性悪く、連続終了時に未集計レースが最大 5R 積み上がっていた。

### ✨ 新機能
- **「↻ 更新」ボタン** (HomePage TodayStatsPanel): クリック 1 回で未集計レース全取得 + 即集計
  - 5 秒連打防止 / Loading state / aria-label / `useEffect` クリーンアップ
- **`POST /api/force_refresh_today`** 新規 endpoint:
  - threading.Lock 連打防止 (BUSY 409)
  - IP 単位 5 秒 rate limit (RATE_LIMITED 429)
  - 127.0.0.1 + Cloudflare IP のみ許可 (FORBIDDEN 403)
  - date 形式 validation (INVALID_DATE 400)
- **`useForceRefreshToday()` hook** (TanStack Query mutation)

### ⚡ 性能改善
- **`_AUTO_FETCH_COOLDOWN_SEC` 60 → 30 秒**: 自動更新も高速化（マスターがボタン押さなくても遅延 30 秒以内）
- **`_auto_fetch_post_races(force=False)` 引数追加**: cooldown bypass オプション
- **戻り値 dict 化**: {fetched, aggregated, skipped, errors, elapsed_ms}
- **`_count_pending_races(date)` ヘルパー**: DRY 化

### 🔒 セキュリティレビュー (security-reviewer 全 PASS)
- CSRF: 既存 POST endpoint と同一ポリシー (127.0.0.1 のみ)
- IP 偽装防止: CF-Connecting-IP/CF-Ray/X-Forwarded-For 複合チェック (v6.1.19 機構流用)
- date validation: regex `^\d{4}-\d{2}-\d{2}$` + strptime パースのみ (SQL/シェル無し)
- Exception 漏洩なし: クライアント側は `"internal error"` のみ

### 📋 委託体制
- Sonnet.5 (バック) + Sonnet.6 (フロント) を Plan-γ Phase 1 と並列起動
- Sonnet.片付け 3 体（verify / TASKS 更新 / etc）も同時進行 = 同時最大 5 体並列

---

## v6.1.33 （2026-04-27）— Plan-α: ability_total -50 拡張 results_tracker 追従

**commit**: 7f434a8

### 🎯 背景
マスター追求「もう全て変更したの？-50.0〜100.0 で表してる？」から発覚。
真因: `src/results_tracker.py:311` の二重クランプ `max(20.0, ...)` が DEVIATION["ability"]["min"]=-50 拡張（2026-04-26 承認済み）を潰していた。

### 🐛 バグ修正
- **`src/results_tracker.py:311`** の `max(20.0, ...)` → `max(-50.0, ...)` に修正（1 行リテラル変更）
- `AbilityDeviation.total` プロパティは既に -50 でクランプ済み → results_tracker 側の二重クランプが真因

### 📊 検証結果
- 4/28 pred.json で `ability_total = 20.0` 張り付き **176 件 (53%) → 0 件**
- min: 20.00 → **-36.42** で連続的分布回復
- python-reviewer APPROVE / MEDIUM 3 件 (DEVIATION 参照化、calibration の or 0 パターン、テスト追加) は別タスク化

---

## v6 （2026-04-23）

### 🎯 主要成果
**Phase 3 リリース後の集中メンテナンスセッション**。
買い目指南・リアルタイム成績・オッズ反映・スクレイプ信頼性・著作権配慮を網羅的に強化。

### ✨ 新機能
- **ホーム リアルタイム成績パネル**: ◉◎単勝 X-X-X-X + 三連単F 予想/的中/回収率を 2分間隔で自動更新
- **レース詳細タブ固定**: 買い目指南を開いた状態で他Rを押しても買い目指南のまま（sessionStorage 永続化）
- **レース結果・オッズ自動取得**:
  - `/api/home/today_stats` で発走+10分経過レースを裏で自動 fetch
  - `/api/results/race` に同様の auto-fetch 前処理
  - `/api/race_odds` に `auto=true` fire-and-forget モード追加（cooldown 付き）
- **◉ / ◎ 分離集計**: 印別成績で ◉鉄板 を独立行表示（234件/勝率 68%）

### 🐛 バグ修正
- **◉鉄板 誤付与漏れ修復**: 過去 840 日で条件を満たす 234 レースを ◎→◉ に昇格
- **誤 is_scratched 復元**: ML予測ありの 3,439 馬を `is_scratched=False` に復元 + 確率再正規化
- **園田 調教・厩舎コメント取得**: venue_code `50→49` 変更未対応バグを修正（今年から新コード）
- **三連単F セクション欠落（2024/2025/全期間）**: sanrentan_summary キャッシュと `invalidate_aggregate_cache` を連動
- **auto-odds 不完全**: 初期実装が `get_tansho` のみで ticket odds 更新されず → 自己POST で全 tickets_by_mode 更新
- **netkeiba HTTP 429/503 検知 + cooldown**: レート制限時は即フォールバック（threading.Lock 付き）
- **stale bet_decision 修復**: 取消馬ゼロなのに「取消馬により買い目無効」表示残留バグ
- **empty tickets_by_mode.fixed**: 誤取消で tickets が空になったレースを `build_sanrentan_tickets` で再生成
- **三連単F チャート 2024-11 起点問題**: 2024-01〜10 の results.json 欠損を backfill で修復 (OK=2319/15047)
- **白味バグ**: `dashboard.py` で ML予測持ち馬を自動取消扱いしないガード条件追加
- **タイムアウトの UI 固まり**: Home 画面の `useHomeTodayStats` polling は 2分間隔・refetchOnWindowFocus
- **20240108_pred.json 破損修復**

### 🏗️ インフラ改善
- **バックフィル 3 本**: `backfill_recent_days.py` / `backfill_2026_gaps.py` / `backfill_payouts.py` で 2024-01〜2026-04 の欠損を全件補完
- **チェックポイント中断再開**: backfill は `tmp/*_checkpoint.json` で再開可
- **Rate limit**: netkeiba 1.5秒 / 公式 3秒間隔、Lock 付き cooldown

### 🧑‍⚖️ 著作権配慮
- **`bulletize_stable_comments.py` 新規**: 厩舎コメント完コピを規則ベースで bullets 変換（最大 5 要素 × 28 字）
- **調教短評**: 辞書ベース `paraphraseTrainingComment` で意味保持の言い換え（TOP150 フレーズ）
- **規正表現**: JRA/NAR 両形式のプレフィックス除去（`○馬名【調教師】` / `○馬名(短評) 調教師師――`）

### 🛠️ 運用ルール（CLAUDE.md / memory 追加）
- **モデル階層 2 階層**: 既定 Sonnet、Opus は (1) アーキ判断 (2) 複雑バグ根本原因 (3) 並列Explore集約 のみ
- **並列 Explore 推奨**: 未知領域の調査は 1 つずつ grep しない、2-3 並列 Task で一発特定
- **専門 reviewer**: python / typescript / database / security / keiba-reviewer を実装直後に呼ぶ
- **プログレスバー可視化**: Chat 本文に必ず `[████░░░░] X%` + タスクリスト表示（違反歴 2 回目・最重要）

### 🔧 修正ファイル（主要）
#### Backend Python
- `src/scraper/netkeiba.py`（429/503 検知 + Lock cooldown）
- `src/scraper/keibabook_training.py`（園田 venue_code 49 対応）
- `src/scraper/auth.py`（園田 venue 対応に追随）
- `src/dashboard.py`（auto-fetch, auto-odds, ticket 更新拡張）
- `src/results_tracker.py`（invalidate 連動、◉/◎ 分離集計、ML予測ガード）
- `src/engine.py`（bet_decision 最新印で再生成）
- `src/models.py`、`src/calculator/betting.py`、`src/calculator/popularity_blend.py`
- `src/analytics/sanrentan_summary.py`（invalidate_cache）

#### Frontend TS/React
- `frontend/src/api/hooks.ts`（useHomeTodayStats, useSanrentanSummary staleTime 短縮）
- `frontend/src/api/client.ts`（homeTodayStats, OddsRequest に auto 追加）
- `frontend/src/pages/HomePage.tsx`（TodayStatsPanel 追加）
- `frontend/src/pages/ResultsPage.tsx` + `ResultsPage/{SummaryCards,TrendCharts,DetailedAnalysis}.tsx`（三連単F 対応）
- `frontend/src/pages/TodayPage/RaceDetailView.tsx`（auto-trigger useEffect）
- `frontend/src/pages/TodayPage/TabGroup3Horse.tsx`（タブ永続化 sessionStorage）

#### 新規スクリプト
- `scripts/patch_tekipan.py`（◉鉄板 過去遡及付与）
- `scripts/patch_false_scratched.py`（誤取消復元）
- `scripts/renormalize_probs.py`（確率再正規化）
- `scripts/fix_stale_bet_decision.py`（bet_decision 取消理由残留解消）
- `scripts/regen_tickets_today.py`（空 tickets 再生成）
- `scripts/bulletize_stable_comments.py`（規則ベース箇条書き）
- `scripts/inject_training_for_date.py`（調教 training 強制注入）
- `scripts/backfill_payouts.py`（2024-01〜11 全 15047R 払戻再取得）
- `scripts/backfill_recent_days.py`（直近6日分）
- `scripts/backfill_2026_gaps.py`（2026-02〜04 不完全日）

#### 運用・ドキュメント
- `CLAUDE.md`（モデル階層 + Chat可視化ルール追加）
- `~/.claude/projects/.../memory/feedback_model_tiering.md`（新規）
- `~/.claude/projects/.../memory/feedback_progress_bar.md`（違反歴 2 回目記録）
- `~/.claude/agents/keiba-reviewer.md`（新規ドメイン専門レビュアー）
- `~/.claude/agents/security-reviewer.md`（model: sonnet → opus 昇格）

### 📊 データ修復サマリー
| 項目 | 件数 |
|---|---|
| ◉鉄板 追加 | 234 races |
| is_scratched 復元 | 3,439 horses |
| 確率再正規化 | 36,722 races |
| 園田 training 再取得 | 134 horses |
| 厩舎コメント bullets 生成 | 251 horses |
| 2024 payouts backfill | 2,319 三連単取得成功 / 15,047 scanned |
| bet_decision 取消残留修正 | 17 races |
| 空 tickets 再生成 | 7 races |

### ⚠️ 既知の残課題
- 名古屋の調教データは競馬ブック側が「非対応」（構造的制約・対応不可）
- 2026-04 の 10 レースで place2_prob 異常（全馬 0 または 1 頭集中・ML モデル側要調査）
- オッズ 4 段フォールバック（公式→netkeiba→競馬ブック→楽天）は競馬ブック/楽天のオッズスクレイパー未実装のため 2 段まで
- 過去 predictions/_prev.json バックアップ一覧の整備は未対応
