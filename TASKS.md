# TASKS.md — D-AI Keiba v3 タスクボード

> **運用ルール**: CLAUDE.md「TASKS.md / MEMORY.md 運用ルール」を参照。
> マスター指示はすべてここに追加。完了タスクは git log + handoff_*.md に集約済のため本ファイルから削除する。
> 教訓・反省は `~/.claude/projects/C--Users-dsuzu-keiba-keiba-v3/memory/` に永続化。

---

## ✅ 過去成績ページ 4点改修 完了（2026-06-30・実画面検証済・未commit）

> master指示4点。ワークフロー(backend+frontend並列→build→keiba/typescript-reviewer)で実装→P0修正→cache再構築→dashboard再起動→playwright実画面検証(全体+JRA)。

| # | 指示 | 実装 | 検証 |
|---|---|---|---|
| 1 | 上部カード順を 勝率-連対率-複勝率 に統一 | `SummaryCards.tsx` JSX並替 | ✅ 勝36.1/連55.8/複68.0 |
| 2 | 印別成績 ☆をOUT・穴をIN | `DetailedAnalysis.tsx` MarkTableに `displayMark()` 適用(☆→穴) | ✅ 最終行「穴」表示 |
| 3 | 自信度別 的中率 削除(自信度は軸馬の信頼度3段に置換済) + M'戦略セクションごと削除 | ConfTable/byConf削除・SummaryCardsのM'/買い目回収一掃 | ✅ 消滅 |
| 4 | 偏差値別 7区分 新規(全頭・**降順**・実データ較正) | backend: detail cache v2→v3(dev_finish全頭)+ `by_deviation`集計(_new/_add/_finalize)+ `_composite_to_dev_bucket` / frontend: DevTable(`DEV_BUCKET_ORDER`降順) | ✅ 候補D境界(〜45/45-53/53-62/62-72/72-82/82-90/90〜)・90〜:複勝72.8%(先頭)→〜45:11.8% 綺麗な単調勾配・JRA/NAR/venue切替も動作 |

> **偏差値区切りの較正(2026-06-30・master指示「ターゲットのバランス感に」)**: 全頭449,820の実複勝率カーブを計測→上位を細分化し下中盤を広く取る**候補D**を採用(`scripts`相当の分析は scratchpad)。実データの honest 知見: 複勝75%級は偏差値90+のエリート帯でのみ(70〜全体は61%)・中盤は実態がやや平坦。dev_finish不変ゆえ version bump不要・aggregate cache無効化のみで再集計(33秒)。dashboard PID14512。

> **タブ連動(2026-06-30・master指示)**: 詳細分析の全体/JRA/NARタブ切替で上部の本命的中カード(勝率/連対率/複勝率)も連動。`cat`状態をResultsPageへ持ち上げ→SummaryCardsが `detailed[cat].stats` の本命集計を読む(backend不要・frontendのみ)。カードラベルに(全体/JRA/NAR)表示。全体は36.1→36.2%(detailed基準=帯広除外整合)。実画面で3カテゴリ連動確認。

**技術**: `_DETAIL_CACHE_VERSION`=3で全910日 detail cache 再構築(35秒)。`invalidate_aggregate_cache` に detail/サブdir削除追加(reviewer P0)。frontend型を `MarkStatRow`/`DetailedStats` で配線(reviewer P0)。dashboard PID17276稼働。**未commit**(master判断待ち)。

---

## 🔴 T-7 表示率較正の本番ON — choice A実行 → **NO-GO**（マスター最終判断待ち・2026-06-30 v4）

> 詳細: `memory/handoff_2026-06-30_v4.md`。WF版(leak-free)較正テーブルを生成→**本番ON不可**を確証。本番完全無改変。

**結論**: **T-7 WF版較正 = NO-GO**。WF probe の composite偏差値が本番より圧縮（月次 sparse course_db）→ 本番composite(75+常在)に適用不可。リーク無の真値は高composite馬が実際に高率＝Phase0で見えた「飽和」はリーク版の産物。**全gammaで飽和は悪化**（複勝>90%馬数 gamma1.0:15→15 / 1.5:→40 / 2.0:→58・最大複勝→100%）。

**証拠（本番版 vs WF版・複勝率%・逆転）**: 60-64 [42.0→**74.8**] / 65-69 [46.6→**91.8**] / **75+ [69.2→ WF n=0]**。WFの「65-69」=WFサンプル内最強馬≈本番「75+」相当＝binラベル一致でも別物。inversion は太いbin(n159-361)でも頑健＝小サンプル由来でなく構造。

**進捗**:
- ✅ Phase0/1 完了（`fedd756`/`29103e3`）。probe配線実証。
- ✅ フルWF実行（3回レンジ修正・実測ドリブン）→ 2026-01〜05 cap80・235R測定・2,711頭。WF版テーブル `data/_diag/calibration_composite_wf.json` 生成（本番版別名・無改変）。
- ✅ Phase2プレビュー(`--table wf`)で飽和悪化＋大波及（軸馬度|Δ|2.89pt・◉入替±2）を確認。
- ❌ Phase3-4 本番ON = **NO-GO**（適用しない）。

**マスター判断事項**:
1. **T-7撤退**（推奨・WF-calibrationは confound・救済=richpreloadは-35回帰でrevert済）か
2. **choice B(リーク版で本番ON)再評価**（cosmeticに飽和解消は可・但しそもそも飽和が実問題か疑問）か
3. **別アプローチ**（raw composite→rate写像の上限cap等・別途設計・[[feedback_indices_post_hoc_limit]]構造天井注意）
[[feedback_production_vs_wf_pred_distinction]] [[project_engine_course_db_richpreload]] [[feedback_indices_post_hoc_limit]]

---

## 🟡 見える化2軸 仕上げ（印6種化/馬柱軸穴EV/道悪着度数）— 残: 検証→commit / 馬柱バランス / 自信度

> 詳細: `memory/handoff_2026-06-28_v5.md` + 前セッション続き。「収益→実力の見える化」を軸馬度/穴馬度2軸に統一。土台4 commit(c86be32/140ea03/12251dc/1266c4a)は**push済(HEAD=origin=5eb2c72)**。その後の仕上げ A〜H は**未commit**。dashboard PID23968稼働。

**確定設計（不変）**:
- 軸馬度 jiku_score = 実力(composite)40%+堅実(複勝率)30%+断トツ(レース内順位)30%。0-100。本命信頼度。
- 穴馬度 ana_do = 過小評価(人気-実力順位)70%+穴スコア30%。実力削除(1人気≈0)。0-100。
- 絶対軸=軸馬度TOP5 / 穴馬=穴馬度 / 危険=人気上位かつ軸馬度低 / 拮抗=jiku_gap3<8 / 自信度=jiku_gap(SS≥22/S≥16/A≥11/B≥6/C≥3/D<3)
- jiku_score/ana_do/honmei_chaku/baba_record着度数 は全て**表示専用**(ML/印/買い目 非汚染)

**前セッション成果（未commit・A〜H）**: A印6種化(◉◎○▲△★穴・MarkBadge+markDisplay.ts新規) / B三連複表示削除 / Cカード勝率→軸馬度 / D馬柱バグ修正(`_compute_jiku_ana`モジュール化・NameError解消) / E馬柱に軸/穴/EV配置 / F並び順(軸馬度・穴馬度降順) / Gカード枠=軸馬結果色 / H道悪着度数

**重要な学び（永続）**: 重み最適化は循環参照(予測指数で結果予測=同義反復)で不可→意味配分 / `overall_confidence`は買い目使用で変更禁止(表示自信度は別指標) / playwrightキャッシュ罠(`?cb=`回避・見た目だけで判断せずAPI確認) / dashboard再起動は`:5051 PID`をGet-NetTCPConnectionで動的取得 / frontend/distは.gitignore対象(commitはソースのみ)

**進捗（2026-06-29 深夜 自走・マスター就寝中）**: commit 2本 `8aab17c`(A〜H + displayMark適用漏れ7箇所修正) / `fe1248f`(EV倍率統一 + 穴馬度差を絶対値のみ)。**未push**(master決定=後でまとめて)。検証ゴミ(home_*.md等)掃除済。

**残タスク**:
| 優先 | ID | 項目 | 状態 |
|---|---|---|---|
| ✅ | T-1 | A〜H 実画面検証→commit | 完了 `8aab17c`(印6種displayMark7箇所漏れ修正含む) |
| ✅ | T-4 | 印断層タブ確率行 縦2段化検証 | 完了(実画面OK) |
| ✅ | — | EV表記を倍率(3.87)に統一(馬柱PC/Mobile) | 完了 `fe1248f`(MarkSummary/OddsPanel と整合) |
| ✅ | T-5 | 穴馬度を絶対値のみ表示(2位差削除) | 完了 `fe1248f`(master決定) |
| ✅ | T-2 | 馬柱バランス改善(本丸) | 完了。軸馬度を主役化(大22px緑バッジ)+穴馬度全馬表示+確率内訳化(13px)+最上段に集約(馬名/騎手/オッズ/脚質/通過順/前日想定)+父は性齢斤量馬体重の下+馬体重1行。master「だいぶすっきりした」 |
| ✅ | T-3 | 自信度=軸馬の信頼度(SS〜D→断トツ/優位/拮抗3段) | 完了 `ce84e1b`/`dfcabb4` push済。jiku_gap3場別較正・ホーム4カード+一覧+印断層に展開。検証で穴馬度ベース荒れ度(2軸案)は不成立=不採用。詳細 `memory/project_jiku_confidence_grade.md` |
| ✅ | push | T-3コミット | `ce84e1b`/`dfcabb4` push済(origin=dfcabb4)。モバイル馬名修正`c0b6e81`・保留物`1f64e80`も済 |
| ✅ | 保留物 | track_condition / baba_pdf_cache / 分析script×7 | 完了 `1f64e80`。track_condition資産commit / baba_pdf_cache+分析script×7をgitignore |
| ✅ | モバイル | レース一覧・印断層の新バッジ モバイル幅実画面目視 | 完了。390pxで一覧=断トツ緑バッジ・印断層=軸馬の信頼度断トツ・横崩れ0・横スクロール無し |
| ✅ | 修正 | ホーム「N場」表記に帯広(ばんえい)混入 | 完了。`_get_todays_venues`(home_info専用)がNAR公式の帯広65 race_idをvenuesに取込→「実態+1場」化。`is_banei()`除外を追加+帯広を積極補完する「ばんえい安全策」削除([[feedback_banei_excluded]]逆行)。pred側は帯広除外済の非対称を根治。表示専用でML/印/買い目 非汚染。dashboard再起動(PID10592)で本番反映・今日は帯広非開催で5場不変=回帰なし |

---

## ✅ 馬場③ T-C(当日馬場状態) + T-B(hook発火確認/hardening) + T-A(残骸停止) 完了 (6/28続き・commit 5e27714 push済)

> 詳細: `memory/handoff_2026-06-28_v3.md`。master「全部完走して順番は任せる」全権委任。前 handoff_v2 残課題(P1 当日track_condition / P1 hook発火確認) + セッション発見(残骸プロセス) を完遂。

**T-A 残骸プロセス停止**: 6/24起動の孤児python(PID8316・:5051非LISTEN・8GB占有・idle・親消滅)を停止しメモリ解放。dashboard 無影響。

**T-B ③hook 発火確認 + hardening**: ③ライブfetchを手動実行し**実動作確認**(福島/小倉/函館の当日cushion/moist/馬場状態)。🔑観測性ギャップ発見=baba発火が logger-only で dashboard_out.log 非到達(前2セッション「起動確認のみ」の真因)。**hardening**: ①baba取得を odds-error gate の外へ(NAR odds失敗でもJRA baba実行) ②print(flush)で発火を dashboard_out.log に可視化。**14:00 odds-scheduler発火で実ログ確認可**(master監視点)。

**T-C 当日馬場状態(良/重)取得・見える化**: JRA馬場情報ページ(index{N}.html)の公式馬場状態(芝/ダート)+天候+時刻を ③parser拡張で取得→baba_detail→frontendバッジ。🔑**鮮度**=良/重ラベルはレース直前まで金曜正午値(JRA運用)→condition_time表示で明示。⚠️**リーク防止**(keiba-reviewer P1): ML名フィールド(race.condition/track_condition_*)へ当日値を書かず baba_detail経由のみ。発見: 当日 race.condition は scraper(odds更新)が設定するため「未発表」は時間帯依存。**実画面(福島5R)で馬場バッジ「芝稍重/ダート稍重」「天候:曇(金曜正午現在)」描画確認**。dashboard :5051 再起動(**PID 17884**)反映済。

**commit `5e27714` push済**(5ファイル)。keiba-reviewer **P0ゼロ**・npm build型0・parser独立検証OK。

---

## ✅ 馬場見える化 ①A+②B+③含水率ライブ 完了 (6/28・3commit・①②push済/③未push)

> 詳細: `memory/handoff_2026-06-28.md`。master「①②③の順で」。①A見える化commit / ②B WF検証→off commit / ③含水率ライブ取得を新規構築。本番(dashboard PID 22216)デプロイ済。

**① A見える化 = commit `4854224` + push済**: 各馬の道悪複勝率(baba_record・リーク無)を pred付与 + 馬カード表示。frontend3カード + finalize Step3 + `add_baba_record_to_pred.py` + src/static。印/確率不変(追加フィールドのみ)。

**② B展開指数 = commit `1924ae8` + push済(フラグoff・本番不変)**: WF検証は**reach測定で代替判断**。897ダート道悪レース実測で **Bは◎を<0.5%しか動かさない**(◎-○ composite gap中央値11.5pt >> nudge≤0.9pt)= ROI影響ノイズ確定 → 忠実WF(数時間+netkeiba/汚染リスク)は結論確定につき省略。因果は実証済だがcomposite希釈で効かない=効かせるには買い目/印直接の再設計要(将来課題)。

**③ 含水率/クッション値 ライブ取得+見える化 = commit `97ceb07`(未push)・本番デプロイ済**:
- アーカイブは~1週ラグで当日不可と判明 → **JRA公式ライブ(`_data_cushion.html`/`_data_moist.html`・Shift_JIS・playwright不要・requests+cp932)** から取得する `scripts/fetch_jra_baba_live.py` 新規。
- `add_baba_record_to_pred.py` で `race.baba_detail` 紐付け(venue_code突合・JRA限定・NAR null) + RaceDetailView にバッジ(クッション値/含水率/使用コース)。
- dashboard odds-scheduler に非致命ライブ取得hook(8時=測定後・finalize前)。keiba-reviewer 規約違反なし・実画面検証済(小倉R1スクショ)。
- **dashboard 再起動済(PID 22216)で本番反映**・odds-scheduler稼働(次回11:00で③hook初発火)。

**残課題** (6/28続きで P0/P1 全解消):
| 優先 | 項目 | 状態 |
|---|---|---|
| ~~P0~~ | ③ push判断 | ✅ `97ceb07` + T-C `5e27714` push済 |
| ~~P1~~ | 当日track_condition(良/重) | ✅ T-C完了(JRA公式馬場状態を③parser拡張で取得・バッジ表示) |
| ~~P1~~ | ③ hook発火確認 | ✅ T-B完了(手動実証 + hardening: gate外出し + print可視化) |
| P2 | results cache払戻集計バグ | 9226acb で修正済(別task `task_d2d72d29` は冗長・取下げ可) |
| 監視 | 14:00 odds-scheduler で③baba発火を dashboard_out.log 確認 | T-B hardening の本番初発火点 |
| 将来 | B展開指数を買い目/印に直接効かせる再設計 | composite希釈回避・ROIへ効かせる場合のみ |

---

## ✅ 見える化転換 実装 完了 (6/27夜 全権委任自走・commit 9f6b8d9 push済)

> 詳細: `memory/handoff_2026-06-27_v3.md`。master「撤退しない・両方並行」→「思いつく最善を完走」。本番フロント改変(可逆)・backend非改変。

**(a) 見える化フロント = 完了・push済**:
- ✅ Step1a 実力vs市場タブ(`AbilityVsMarketTab.tsx`新規 + `TabGroup3Horse.tsx` `HIDE_BAIME_TAB=true`で買い目タブ非表示)。**◎緑(emerald-600)実画面確証**(積み残し解消)。
- ✅ Step1b: `StatsCard.tsx`的中率主役+回収率(参考)撤去 / `HomePage.tsx`乖離ショーケース(妙味=緑・危険枠を「**拮抗・波乱注意**」amber化=master A承認・複勝92%本命を赤=危険と断じる違和感を解消・上位拮抗検知ロジックは維持)。
- ✅ **成績ページ(ResultsPage)再有効化**(2026-05-29非表示を解除・master A承認): 回収率/ROI/純利/買い目/高配当を**全排除**し的中率特化(複勝/連対/勝率・印別成績・自信度別的中率・過去予想は残置)。`App.tsx`+`constants.ts`(ナビ復活)+`ResultsPage.tsx`(TrendCharts撤去・回収率hooks整理)+`DetailedAnalysis.tsx`(回収率列/競馬場ROIバッジ/単勝高配当TOP10削除・印別成績は残置)。
- ✅ 全体通し検証: PC+モバイル(375px横崩れ無し)・console0・build型0。
- ⬜ **Step2(買い目backend撤去・不要component物理削除)= master確認後・P2**。現状 hybrid=null / HIDE_BAIME_TAB で非表示化(可逆)。物理削除対象=UmarenCards/SanrenpukuExtendedCards/MPrimeSummarySection/TrendCharts.tsx + backend買い目算出。dead import残置中。

**(b) 別構造探索 = 黒字無し確定**:
- ◎軸以外(○-▲/○-△/▲-△ 馬連ワイド)・複勝単体・◎単勝条件層化・◎軸少点数三連複・馬単 を2025実データ検証 → **全クラス赤字**。三連複「黒字」6件は `_roi_val`(L780)の bet約分バグによる偽陽性(真値77-82%赤字)。市場効率の壁を再確認。`data/_diag/alt_bet_structures.csv`。

**commit `9f6b8d9` push済**(frontend一式 + src/static[assets 30→38クリーン化])。分析scripts4本・馬場データ(`track_condition_daily.json` 479日)・`baba_pdf_cache` は **未commit**(P3資産・commit/gitignore判断は次セッション)。

---

## ✅ 買い目徹底分析 → 黒字化の道なし確定 + 見える化転換プレゼン (6/27)

> 詳細: `memory/handoff_2026-06-27.md`。master「あらゆる角度から徹底解析・徹底分析」+「見える化にせざるを得ない際のUI/構図を詳細プレゼン」。「会社行くからゆっくりでいい」で自走中。

**結論: 黒字化の道は無い(あらゆる角度で確定)**。◎○▲△の馬連・ワイドを2025実データ(印=ML複勝確率上位4頭・2023-24学習→2025検証リーク無)で徹底分析 → **全クラス赤字**(馬連 全場79.3%/JRA83.1%/NAR78.3%、ワイド79-82%)。指数/印四方向天井 + P3新データNO-GO + 買い目戦術全滅 + 留保詰めても不変。◎印精度は高い(複勝74.5%/勝率42.5%)が控除率の壁。

- **🚨馬連母数バグ発見・修正**: 集計1,099 vs 実在17,036レース。原因=NARの馬連comboがハイフンなし`'1012'`形式(92%)で `_parse_combo` の `split('-')` 失敗→母数スキップ。修正で16,749(99.8%)。v1の「NAR大頭数馬連109%」は12件/109件の偽陽性→**真値87.7%(赤字)**。`scripts/analyze_bet_combinations_v2.py` / `data/_diag/bet_combinations_v2.csv`。
- **留保2点詰めても不変(盤石)**: ①曖昧パース(`'112'`=1-12か11-2)誤差上限3.1pt << 黒字化ギャップ18pt ②composite印は ML確率印より**+3.5pt**(馬連77.40% vs 73.89%)だが77%止まり(黒字化まで23pt不足)。2025本番predはcomposite=ml_place_prob×100(7因子0.0未計算)=ML確率印そのもの。
- **見える化転換プレゼン**(master指示): 収益ツール→実力可視化ツール。実力序列 vs 市場人気の対比、`divergence_signal`で妙味(過小評価)/危険(過大評価)を名指し、買い目廃止、的中実績(74.5%)で信頼担保、購入判断はユーザーへ。UI/構図+実力vs市場モック提示済。実装は既存データ・部品の再配置中心=軽い。ホーム・成績モックは自走で追加予定。

**🚨 master判断待ち**: (a)見える化に振り切る / (b)◎軸以外の構造をなお探す / (c)撤退。新規script(analyze_bet_combinations*.py)未commit。本番(src/pred/engine)非改変。

---

## ✅ P3 馬場データ(クッション値/含水率) ML特徴量化パイロット = NO-GO (6/26深夜・承認なし自走完走)

> 詳細: `memory/handoff_2026-06-26_v3.md`。master「寝るから完走しといて」→ 自走。本番非改変(diag script・pred非書込・engine不変)。

**結論: JRAクッション値/含水率を生のML特徴量に直接投入してもROIは改善しない = NO-GO**。
- **Phase A**: JRA公式archive PDF取得器 `scripts/fetch_jra_baba_archive.py`(新規)。479測定日(2023:160/2024:158/2025:161)・JRA10場。**生PDF↔JSON突合でリーク無確証**(金曜=前日測定含む)。PDFフォーマット年次変動(2024以前=旧形式)に旧パーサ内蔵。出力 `data/masters/track_condition_daily.json`。
- **Phase B/C**: `scripts/diag_p3_pilot_baba_features.py`(diag_phase2複製・**lgbm_model.py/src不変**)。単一split(学習2023-24/検証2025通年)で baseline_new(109) vs +baba(112)。
- **結果**: JRA芝 **ΔROI -0.88pt** / 全レース -0.08pt / 馬場悪化 -2.08pt(AUC +0.04pt微増だがROI転換せず)。SHAP=**odds圧倒1位(babaの約130倍)**・baba中位(moist_corner23/cushion34/moist_goal53位)・cushion非単調=弱いノイズ的信号。
- **意義**: roadmap留保「市場が既に織り込み済」を実証。指数/印四方向天井+**新規データ(馬場生値)も控除率を破れず=市場効率の壁**。

**🚨朝承認待ち**: ①git commit可否(新規2script+`track_condition_daily.json`[479日資産]+`baba_pdf_cache`[生PDF・gitignore推奨]・**全未commit**) ②P3撤退の最終確認 or odds抜き/派生特徴量/風データ等の追加検証 ③次の戦略方向。**Phase E(本実装/retrain/本番注入)は未着手**(NO-GO)。

**🚨反省(ツール記法累犯)**: `antml:`プレフィックス欠落で生テキスト化→マスター激怒。原因=**テキスト直後のツールブロックが壊れる**→対策=ツール応答はテキストを書かずブロック直書き。

---

## ✅ P1-a odds残差化 = 無効確定 + ワイド払戻バグ仕上げ完了 (6/25夜〜6/26)

> 詳細: `memory/handoff_2026-06-25_v2.md` / `memory/project_p1a_residual_dead_end.md`

**P1-a (odds残差化・init_score方式)**: フル8ヶ月(127,813サンプル)で**無効確定**。`residual_blend ≈ baseline`(券種横断で相殺・一貫優位なし)・`residual_pure`全面大幅劣後・全ROI<100%(控除率の壁)。P1-b(ダンピング撤廃)も無効 → **roadmap P1「市場複製打破の残差化アプローチ」死亡**。市場(オッズ)は効率的でMLが引き出せる残差は控除率を破れない。**唯一の出口 = roadmap P3「市場が価格に織り込んでいない新規データ」**(含水率/クッション値/風)。commit `203aea5`(P0物差し)+`b550c81`(P1-b) push済。技術的学び: `best_iter=1`罠(dvalid init_score除去で解決)。

**ワイド払戻 同額複製バグ (旧task_73613273) = クローズ**: 本番反映完了を実測確証(2025年 92.4%→0.1% / 2026年 7.0%→0.0%)。真因=旧backfillの`<br>`無視パース → DB焼込 → rebuild_results_from_db で全件伝播(現scraperは正常)。中層ガード(`payout_normalizer.detect_wide_duplicate_payout` 警告のみ・挙動不変)+ 適用スクリプト(`apply_wide_fix_to_results.py` ワイドのみ差替・他キー非破壊) commit `006b5d2` push済。残存14件は fixed側も同額複製で自動修正不可(実害軽微0.1%)。**6/26 作業ツリー整理完了**: results_fixed(中間生成物336件)を gitignore化+追跡解除+物理削除(backup 904件は保険で物理保持)、使い捨て検証scripts 4本(P0-β revert済/AB統合却下前提)削除、`compare_engine_prob_roi.py`(P0-γ恒久ツール)のみ残置commit。

---

## 🎯 P1本丸 odds残差化 着手 → P0物差し先行 (6/24 master「OK任せる」全権委任・進行中)

> master「OK任せる」→ P1本丸へ。調査で roadmap前提の**重大訂正**: **P0(正しい物差し)が未完**だった。WFは今も ML確率印(`walk_forward_backtest.py:465 COMPOSITE_PROBE=False` / `:49-69 _assign_marks` prob降順 / `:506 composite=prob×100` / `:513-519` 7因子0.0)で**本番7因子compositeを測っていない**。roadmap「P0→P1 順序不可分・P0無しにP1効果測定不能」厳守 → **master決定: P0物差し完成を先行**。

**🎯 調査の朗報**: full engine統合は 6/24 probe実装(`_run_composite_probe_race` L776-894)で**既に完了済**だった。`engine.analyze()` 完全実行で7因子計算 + 全頭の `engine_marks`/`engine_composites` を返す(L884-890)。**P0補完 = 「測定(probe)→採用」昇格のみ = S〜M**。方針C段階統合は不要(probe直採用が faithful)。

**設計(引き写し禁止で修正)**:
- engine の `ev.mark` を直採用(subagent案の `_assign_marks(composite)`再計算は本番印[◉鉄板/穴/☆6位固定/×危険]を再現できず却下)
- **新フラグ `--composite-marks` で隔離**(既存 prob印WF 非破壊・`feedback_sample_vs_implementation`)
- 2026-01 **35R**サンプル先行(100R=OOM・設計書§12実証・WF高速化が全期間の前提)

**Step**: P0-α(印採用)✅確証 → **P0-β=revert採用**(高速化はML律速で-7%・本番engineクリーン維持)→ ⬜P0-γ(複数月100R=物差し本評価)→ P1-a(odds残差化)/P1-b(ダンピング撤廃)着手。

**✅ P0-α 確証 (engine印/composite が pred.json に正しく反映)**:
- 決定的検証: 2026-01 race202645010101 で genko composite=**70.0**(engine 7因子偏差値) vs prob_bk(旧)=**87.49**(prob×100)= 別物 → engine採用が実反映と私が pred直読で直接確認
- ◎一致率 **54.3%**(prob印◎ と engine印◎ が45.7%別馬)= roadmap根本①の定量確証(再現)
- **deepcopy で汚染除去**: engine.analyze が past_runs を in-place変更 → 前レース汚染 → ◎一致率 48.6%(汚染)→54.3%(修正)
- `walk_forward_backtest.py` に `--composite-marks` + deepcopy のみ保持

**P0-β(SQL高速化)= revert採用**:
- 速度 -7%(25.6→23.8秒/R)= **ML推論が律速・SQL最適化は無価値**と実証(唯一の収穫)
- 本番 engine.py に WF専用493行混入=設計汚染 → `git checkout src/engine.py` で revert・本番クリーン維持
- ★Sonnet「正しさgate 15R一致・ROI 197.3%同一」は **backup取り違え**(engine印版同士を比較)で誤り → 私が pred直読で訂正

**✅ P0-γ 完了 (6/26)**: engine印 vs prob印 三連複ROI比較(2026-01フル1189R)を `compare_engine_prob_roi.py`(同一買い目ルール`compute_danso_columns`+共通発火レース母数統一)で実施。**結論: 同一レースでは engine印 +1.2pt(印質ほぼ互角・hit%39.0%同一) / 実運用ベース +7.2pt(レース選択込) / 両者赤字88.5%/87.3%**。+7.2pt分解 = 交絡4.8pt + 発火レース選択6.0pt + 純粋印質1.2pt。「正しい物差しでも同一レースでは劇的改善せず」= roadmap P0期待を下方修正。詳細 `memory/handoff_2026-06-26.md`。

**✅ レース選択効果の解明 + 複数月実証 完了 (6/26夜・承認なし自走・本番非改変)**: engine印ゲート(`DANSO_AXIS_GATE=8.0`)の正体 = **実力拮抗レースを見送り荒れレースを回避する装置**。`scripts/analyze_race_selection_effect.py`(新規): prob独自89R(engine見送り)の見送り理由は engine側 **100%実力評価系**(skip:軸ゲート53 + skip:谷間36・構造的見送りゼロ)。probは確率スケール(×100)で◎-○差を過大評価(p_gap中央13.1 vs engine7.7)し「1強A型」と誤認発火 → 荒れレース(三連複2,650円/◎飛び25.8%)を掴む。engineは拮抗と見て回避(個票: 川崎e_gap0.6/p_gap23.0→30,970円・園田e_gap2.1→46,560円)。創業理念「市場に騙されない本当の馬の力」と完全整合。`scripts/verify_engine_gate_multimonth.py`(新規): 2026-01/02/03で荒れ回避は **方向一貫**(見送配当>danso発火配当: +1,010/+405/+270円)だが**効果量は月変動**(1月突出・穴%は2月で消失)。prob比較は p0a_backup生成手順喪失で2026-01のみ。keiba-reviewer **P0なし**(force_buy本番関数直呼びで根治)。新規2scriptは master承認(A:OK)で **commit/push済(`fd71d14`)**。

**✅ ML印転写(P2②) = master GO(B)で検証完走 (6/26)**: フラグ化(`config/settings.py ML_ADJ_CLAMP` + `engine.py:1789`・**本番デフォルト5.0で挙動不変**・未commit=master判断)+ WF検証(2026-01・ML_ADJ_CLAMP=5/10/20)。**◎一致率 5.0=51.4%/10.0=48.6%/20.0=51.4% = ノイズレベル = クランプ緩和は◎印をほぼ変えない → ROI改善期待薄 → baseline5.0維持推奨**(P0-γ印質互角と整合)。三連複ROI厳密実測はprobe35R限定/OOM/pred上書きの技術障壁で次課題。🚨**WF副作用で data/predictions 2026-01 汚染**(`--composite-marks`がpred上書き→probe35R混在版・実証20260102=prob×100印)= **過去月で本番運用に実害なし**だがanalyze再現性/dashboard過去表示が崩れ・要本番再生成(`run_analysis_date.py --venues`)・**master判断**(独断の重い再生成回避)。

**✅ P3予備調査 完了 (6/26)**: `docs/p3_new_data_feasibility.md`。クッション値/含水率は**前日(金)昼+当日9:30発表=予測時点確定(リーク無)**・JRA公式archive(2020〜)で過去データ取得可。現状は馬場「良/重」4段階のみ(`CONDITION_MAP`)。🔑**JRA限定が最大の制約**(クッション値はJRAのみ・NAR主力の本システムでは効果がJRAサブセット限定)。✅**PDFパース実証完了**(master「任せる・JRA限定OK」→着手): `scripts/parse_jra_baba_pdf.py`(pdfplumber・中山2026-1回 **13測定日構造化成功**・芝クッション値/含水率(芝ダ・G/4角)取得・金曜測定でリーク無実証・commit `1e38089`)。**次セッション(L-research)**=全PDF取得スクレイパー(全競馬場×回×2020-2026)→date×venue race紐付け→`features.py`+`lgbm_model.py`特徴量追加→JRA限定WFでΔROI/SHAP。pdfplumber requirements追加要。詳細 `memory/handoff_2026-06-26_v2.md` §6。

**留意**: 印切替で既存WF数値(過去Phase群)は非互換になる=新フラグで隔離。詳細設計 `docs/p0_wf_composite_design.md` §12。

---

## ✅ 買い目フォーメーション新仕様 + P0〜P3 完了 (6/24夜 全権委任自走・5commit push)

> 詳細: `memory/handoff_2026-06-24_v2.md`。master「P0〜3まで終わらせちゃって」全権委任。

**🔒 新フォーメーション確定スペック(master対話確定)**:
- 共通ゲート ◎-○≧8.0 / **C先行**→A→B→見送り
- C(団子): ○~☆総幅<5.0 総流し / A(○抜け): ○-▲≧5.0 / B(○▲拮抗): ○-▲<3.0 & ▲-△≧5.0
- **A・B統合は却下**(二頭軸7割でROI 54%→24%劣化を実例で確認) / **force_buy維持**(◉/穴は見送りでも常に購入)
- 実装: `betting.py compute_danso_columns` 全面置換 + `settings.py` 新定数5つ。正典271R完全一致検証・keiba-reviewer P0/P1ゼロ・tests 14 passed

**完了タスク(P0〜P3)**:
- **P0** git commit/push: 5commit(`b0734e6`新仕様 / `bc67f9b`cp932根治 / `0078d50`ログローテ / `41f8585`truncate guard)
- **P1 cp932根治**: Windows scheduler stdout cp932 → `print(◉)`即死 → 17時穴馬再選定毎回クラッシュ。finalize/dashboard冒頭UTF-8 reconfigure。`PYTHONIOENCODING=cp932`で実証。穴馬全6日◉5/穴5復活。教訓=`feedback_windows_cp932_print_crash.md`
- **P1 2025-12特異(17%)**: 真因=composite≠ML place-prob の設計差(roadmap根本①)。品質劣化でない。`diag_p0b_dec_anomaly.py`(netkeiba非アクセス)
- **P2 ログ208MB**: `log.py` RotatingFileHandler(50MB×3世代)化。旧208MB→3MB gzアーカイブ
- **P2 コード整備**: 旧DANSO定数廃止コメント / 旧仕様docstring更新 / dead codeコメント
- **P3補助 truncate guard**: `lgbm_model.py` 特徴量切り詰めを silent→loud(一度だけ警告・挙動不変)

**⬜ 残課題**:
- **P1本丸**: roadmap P1 odds残差化(市場複製打破・研究的・複数セッション規模)。P0-b乖離確証は完了済
- P2: WF高速化(prefetch 16秒/R) / dashboard nohup stdout脱却(208MB再発の根の半分)
- P3: danso厳選度調整(force_buy込み日11Rがmaster的に多いなら) / 2025-12 probe公平比較実装
- ✅ 20時 odds-scheduler 実発火確認完了: 20:06:38 `elite再選定 ◉=5 穴=5` クラッシュなし(17時の UnicodeEncodeError と対照)=cp932根治を実運用ログで完全クローズ

---

## ✅ 買い目フォーメーション再設計 完了 (6/23 master主導で確定→実装→検証→commit/push)

> 6/22深夜の越権(設計議論中の独断実装)を反省し、6/23 は **論点を1つずつ master に確定いただいてから実装**。教訓: `memory/feedback_design_no_unilateral_impl.md`。

**🔒 確定スペック(master決定・AskUserQuestion で1論点ずつ承認)**:
- col1 = ◎(◉) 単独固定
- col2 = ○ / `comp(○)-comp(▲) < 5.0pt` なら ○▲(最大二頭軸・△は常にcol3)
- col3 = 起点以降の印を常に全部(断層切り廃止)+ 穴 + 抑
- 見送り = `comp(◎)-comp(○) < 4.0`(≒半分購入)+ A自信度ゲート(SS〜B買い/C・D見送り)+ B抑印(無印1-2人気・非危険)
- 実装2ファイル: `config/settings.py`(DANSO_COL2_KINKO=5.0 / DANSO_AXIS_KINKO=4.0 / DANSO_COL_GAP廃止)+ `src/calculator/betting.py compute_danso_columns`

**✅ 検証**: 合成6/6 OK / 6/21(33R)・6/22(28R)再生成・tickets整合性 **不一致0(偽的中なし)** / ROI(34日 t=4.0)=購入777R・**hit23.8%**・ROI43.2%(旧draft hit17.8%から改善・ただし~43%は三連複構造的-EV)/ 実サーバ:5051 新spec返却 ライブ確認

**📊 黒字化は買い目では不可**(三連複-EV天井)= モデル側 roadmap が本筋 → `memory/project_improvement_roadmap.md`

**Git**: 6/23 commit & push 済(master承認)。バグ修正(◉増殖/偽的中)・A自信度・B抑印 も同梱。

---

## ✅ 印体系刷新 残実装 a/b/c/d 全完了 (6/22夜 自走・push済 2797cbc・詳細 handoff_2026-06-22_v2)

> マスター「寝るから完走しておいて」→ a→b→c→d 承認なし自走で**全完了+origin/master push済**。

- **a 勝率メリハリ**: 表示勝率シャープ化(γ=2.2)+本命安泰下限。最高勝率 median 0.22→0.44・1.0倍ヴェニーレ 0.21→0.62(1位逆転)。`sharpen_win_prob_display.py`
- **b ◉/穴 恒久統合**: reassign に ◉/穴保護+per-race TEKIPAN廃止 / `finalize_predictions.py`(sharpen→elite→formation)を生成パイプライン組込。**翌日◎戻り根絶を live確証**(reassign+persist経由でも◉5/穴5生存)
- **c 4パターン formation**: `compute_danso_columns` 4パターン化。**閾値 DANSO_FORMATION_GAP=5.0 採用**(私の判断・10.0へ1行切替可)
- **d push**: commit 2797cbc 同期済
- 副次 dashboard バグ3件も修正(place 100%飽和 / 排他化 col3空 / format stale → MPrime誤表示)

### ⚠️ 起床後レビュー推奨
- **DANSO_FORMATION_GAP=5.0** を私が採用(dry-run 5.0=見送り16.7%/10.0=64.6%)。選別を強めたいなら settings.py で 10.0 に。形状判断は g1≥5 で十分との判断
- legacy `race["formation_tickets"]` は stale(未使用・実買いは tickets_by_mode)。低優先で除去可
- dashboard PID 7024 稼働中。**Ctrl+Shift+R** で 6/22 card 確認(◉赤/穴amber/勝率メリハリ/4パターン買い目)

## 🎯🎯🎯 精度ROI改善ロードマップ (6/21 全システム監査42エージェント結論)

詳細: `memory/project_improvement_roadmap.md` / 全発見: `data/_diag/improvement_audit_findings.md`

**根本①**: WF backtest が ML確率(prob×100)だけで印を付け本番7因子compositeを評価していない(`walk_forward_backtest.py:498`)→ 過去の全ROI数値・「天井証明」は本番系を測っていない。
**根本②**: モデルが odds を丸暗記(gain 81.6%・全モデルrank1)= 市場複製機 → ROI~79%天井。穴馬ダンピング(`engine.py:1797-1802`)が乖離=アルファを抑制。買い目EVも市場オッズ未使用。
**統合**: 市場をコピーし乖離を抑制し間違った物差しで測る = 創業理念「市場に騙されない本当の馬の力」と正反対に漂流。

| 優先 | タスク | 工数 |
|---|---|---|
| **P0(前提)** | WF を本番7因子composite印で評価するよう作り直す + 較正指標(Brier/logloss)追加 | L |
| **P1-a** | odds 残差化(市場implied probと直交する信号だけ学習・2段階) | M |
| **P1-b** | 穴馬ダンピング撤廃(`engine.py:1797-1802`)+ 買い目EVに実市場オッズ供給 | S-M |
| **P2** | ML信号を印に通す(±5ptクランプ緩和/ML主役の印・P0/P1後WF検証) | M |
| **P3** | 新規データB-7(含水率/クッション値パイロット→ΔROI測定) | L-research |
| 補助 | WF高速化 / truncate silent guard 修正 / 47分割vs単一モデル比較 | S-M |

> **順序不可分 P0→P1→P2**。較正は no-op 実証済で禁止。naive odds除去は逆効果→残差化が正解。詳細・file:line・留保は `project_improvement_roadmap.md`。

### 🔧 P0 進捗 (6/23 自走)
- **P0-a 較正指標 完了(実装)**: `walk_forward_backtest.py _process_month` に Brier/logloss(ML複勝確率 vs 実複勝 fp≤3)を追加・全月 calib_n 加重平均で summary 出力。印ロジック不変・測定のみ=リスクゼロ。検証=計算式 合成OK + 単一月WF実行で end-to-end 確認。
- **P0-b 設計完了(実装は master 決定待ち)**: `docs/p0_wf_composite_design.md`。本番 composite(`models.py:777-795`)再現は **ability/pace/course が WF未計算 → full engine 統合(案A)要**。jockey/trainer/bloodline/ml_adj は WF で再現可能(P0-b1=低リスク足場)。**方針 A(faithful遅)/ C(段階)/ 保留 は master 決定事項**(設計書 §9 に質問3点)。盲目実装せず設計提示で停止。

## 🚨 次セッション 追加課題 (6/21 二部 マスター指摘・実装は次回)

> 6/21二部で rich preload を deploy したが **-35/-42 の負値を200+走で量産する回帰**が発覚 → **revert済**(薄preload復元・6/20/6/21 pred 復元)。以下は腰を据えて次セッションで。

| # | 課題 | 診断 | 着手方針 |
|---|---|---|---|
| **N-1** | **走破偏差値 計算式/表示 堅牢化** | 計算式が sparse コース/NARで両方向に暴れる(89.3過大⇔-42過小)。race_logも同値=表示バグでなく計算の不安定。**原因究明完了(6/21三部)**: dev=50+(基準T−走破T)×(1600/距離)×_k で **std誤差を6〜8pt/秒 線形増幅**・clamp[-50,100]張付 6.3%(281,003走中17,635・勝ち馬-50/+100あり)。中心(median46.6)健全=直すべきは両端の頑健性 | **マスター決定: 表層＋深層**。✅**表層(a)完了**: 前三走テーブル/Mobile/Diagnosis をグレード文字のみ化(feedback_past_runs_dev準拠)・ライブ検証(E/E/D)。⬜**深層=設計提示済・承認待ち**(計算式再設計)。表層(b)clean-joinは今日pred乖離小→深層統合。較正/rich preload小手先は禁止(回帰実証済) |
| **N-2** | **JRA払戻パーサ修正** | ✅ **完了(6/21三部)**。真因=JRA公式払戻が`<table>`でなく`<div class="refund_area">`(li.win/place/.. > dl>dt/dd>div.line>div.num/yen/pop)。旧parserはtableのみ走査で常に空(着順は別tableで取得できていた) | `official_odds._parse_jra_payouts`にdiv.refund_areaパーサ追加(li class+dt fallback・table保持)。`fix_empty_payouts.py --date 20260621`で**JRA36R全反映(修正36/失敗0)**・実画面で8券種払戻表示確認。⬜残: date=None幽霊race除去は別課題 |
| ~~**N-3**~~ | ~~**単勝買い目 削除**~~ | ✅ **完了** (6/21三部) | TicketSection.tsx の MPrime/Phase4Hybrid 単勝(勝負気配TOP2)セクション＋TanshoRow＋T-050単勝文言を全除去。build+static同期+ライブ実画面で単勝無し確認 |
| ~~**N-4**~~ | ~~**三連複フォーメーション表示**~~ | ✅ **完了** (6/21三部) | backend(engine.py/regen_strategy.py)が `_meta.formation_columns`(col1/col2/col3馬番)出力。frontend `DansoFormationString`が印+囲み数字(印強さ順)で `◎②－○④▲⑨△⑥★⑦☆⑩－…` 生成。型別(A-F1/A-F2/C/B-F1/B-F2)正対応。ライブ検証 東京2R(C型10点)/阪神7R(A-F1 4点)。formation_columns無し過去predは従来列挙にフォールバック |

## ✅ 6/21 三部 完了 (UI枠 + 買い目フィルタ + JRA結果データ)
- **カード枠**: 購入=太い黒枠 / 三連複的中=太い赤枠 / 金枠(勝率1位gold variant)廃止 / 単勝バッジ廃止。backend `purchased`(結果非依存・dashboard.py) + RaceCard.tsx。レース前から黒枠
- **買い目フィルタ**: メイクデビュー・障害は買わない。共通 `betting.is_no_bet_race_type` → engine/regen_strategy 恒久 + 6/21即時(`apply_no_bet_20260621.py`・pred直接修正で印不変)
- **#6 JRA払戻パーサ**: 真因=`div.refund_area`構造(table非使用) → `official_odds._parse_jra_payouts` 書換 → JRA36R全反映
- **JRA単勝オッズ**: 真因=JRA結果ページにodds列無(NARは有) → pred最終オッズ補完。`results_tracker.fetch_actual_results` 恒久組込 + `fill_jra_order_odds.py` 6/21即時(479頭/35R)
- **NAR結果詳細(走破タイム/着差/後3F)**: 真因=NAR速報取得時にタイム未掲載→time_sec=0.0保存・payout完整のため再fetch非トリガー。`refetch_incomplete_nar_results.py` 6/21即時(佐賀3R+高知5R=8レース) + `results_tracker._is_time_incomplete` をキャッシュ再fetch判定に追加(恒久・結果取得バッチ時のみ発動=当日dashboard負荷なし)
- reviewer P1/P2 全反映(engine import整理 / wakutan削除 / _odds_to_payout round / combinations削除 / div.line log / hasAnyOdds odds対応)
- 全件 実画面検証済(東京/阪神カード枠・JRA払戻8券種・JRA単勝オッズ・NAR走破タイム) / git 未commit

### ⬜ 6/21 三部 残タスク (次セッション・詳細は handoff_2026-06-21_v3)
| 優先 | 項目 | 着手方針 |
|---|---|---|
| ~~**P2**~~ | ~~git commit/push~~ | ✅ **完了** (6/22)。298→2コミットに整理し push 済 (`627e76a` 本番ソース成果20件 / `320f07f` build整理+`.gitignore`)。マスター決定=ソースのみclean。`data/_diag`新規・実験コード・**4.46GB実験prediction**・junk は `.gitignore` で除外 (誤commit事故回避)。`static/assets` 472→30件にクリーン再構築・ダッシュボード200検証済。origin/master 同期 |
| ~~**P3-1**~~ | ~~date=None幽霊race除去~~ | ✅ **調査完了=除去不要 (6/22)**。keiba.db 全数調査: `predictions`(46,177行) date NULL/空/None=**0**・`race_log` はdateカラム無し(race_id join)・該当race `202605030611` は predictions/race_results/match_results **全て date=2026-06-21 の有効レース**。results.json は `{date}_results.json` を日付→ファイル名で構築(glob非使用)= date=None化経路なし。**幽霊race は実在せず=6/21結果再取得で解消済**。除去すれば有効レース削除の誤操作だった(原因究明先行で回避) |
| ~~**P3-2**~~ | ~~scheduler夜間バッチ統合~~ | ✅ **完了 (6/22)**。真因=`_results_scheduler_loop`(毎日23:00)も live auto-fetch も**当日のみ処理**・「過去日はnightly batch」のコメントに反し其れが不在 → 過去日NAR time欠落が永久放置。さらに `fetch_actual_results` の再fetchは**日次30%閾値**で少数欠落日(12R中2-3R)を取りこぼす。対処=中核ロジックを `results_tracker.refetch_incomplete_nar_times`(レース個別狙い撃ち)へ集約し、`dashboard._backfill_recent_nar_times` で**直近3日**を当日処理後に補完(★並列禁止遵守=逐次2.0s・NAR欠落レースのみ)。script は薄wrapper化(DRY)。**実データ検証**: 6/21 backupでNAR欠落8R(佐賀3+高知5)を正確検出・現状0R。要dashboard再起動で発効 |
| P3-3 | RaceResultPanel 警告文言 | odds補完後の実態乖離を見直し(低優先) |

## ✅ 6/21 二部 完了 (走破偏差値深掘り → rich preload 採用 → 回帰で revert)
詳細: `memory/handoff_2026-06-21_v2.md` / `memory/project_engine_course_db_richpreload.md`
- LIVE STATS ラベル変更(`◉◎単勝`→`◎本命成績` / `三連複(M'戦略)`→`推奨三連複買い目`)deploy済
- 走破偏差値89.3 = engine course_db希薄の偽陽性 → **rich preload採用**(`build_rich_preload.py`/15MB)。6/20実証 ◎ROI 24→32%(+8pt)。6/21適用済(ハルヒメ◎→▲)
- 較正(relative統合/中層減衰)は no-op/不十分と実証→全revert
- 残: engine の「当日馬を course_db に混ぜる自己参照バイアス」完全clean化(diminishing returns・次セッション議論可)

## 🚨 6/20-21 マスター ダッシュボード指摘 7 件 (原因究明済・修正承認待ち)

| # | 指摘 | 根本原因 (調査結果) | 修正層 | 工数/リスク |
|---|---|---|---|---|
| C-1 | モバイル上部 sticky 解除 | RaceDetailView/TabGroup3Horse の多段 sticky | frontend `md:` 化 | ✅ **完了** |
| **印断層買い目** | 買い目を印断層で判断 (条件A/B/C) | 新仕様 (マスター) | `compute_danso_columns`+engine+regen+frontend | ✅ **完了** (WF ROI 66.1%・`project_danso_buy_logic.md`) |
| L-4 | 買い目指南と連動せず | 全レース購入だった | danso 見送りで自然解決 | ✅ **完了** (danso化) |
| L-7 | 全レース購入になる | 同上 | danso 見送り | ✅ **完了** (6/20=12R/6/21=10R購入・残り見送り) |
| A-5 | 穴馬に1〜3人気混入 | ホーム厳選穴馬 `dashboard.py:1031` ☆無条件 | `pop in (1,2,3)` 除外 | ✅ **完了** (実画面: 全4人気以上) |
| 特選穴馬 | 9.6倍3人気が特選 | 累犯#17修正で odds ゲート外れた | is_tokusen odds≥15 (engine+regen) | ✅ **完了** (違反0) |
| D-2 | 払戻が拾えていない | results.json 払戻 25/60 のみ | `repair_order_odds_from_cache.py`+キャッシュ再パース | ✅ **完了** (20260620 払戻60/60) |
| D-3 | 単勝オッズ拾えていない | order に odds 列欠落 | キャッシュHTML→DB→results.json | ✅ **完了** (単勝60/60・東京8R odds=35.6) |
| D-6 | 開催カレンダー空/文字化け | ①5/29陳腐化 ②**netkeiba UTF-8移行で euc-jp デコード失敗=文字化け** | `build_kaisai_calendar.py` を utf-8 デコードに修正+再取得 | ✅ **文字化け解消+6月23開催日** (本番反映済) |
| D-6' | 7月以降カレンダー空 | **netkeiba calendar.html が未来月を未掲載** (HaveData=0・確定レースカードのみ) = データソース制約 | JRA/NAR 年間開催日程の別ソース取込が必要 | ⬜ 要別ソース (netkeiba 不可確認済) |
| E-1 | 短評コメント不要 | `horseSummary.ts` ★短評 | Mobile/PC両カードから表示除去 (helper残置) | ✅ **完了** (全廃と判断) |

> 🎯 **6/21 自走完走 (全権委任「全てやって」)**: 印断層買い目 + L-4/L-7 + A-5 + 特選 + C-1 + **D-2/D-3 + D-6 + E-1 + B-1 deploy** = **全件完了・本番(dash.d-aikeiba.com)稼働中**。
> **B-1 deploy**: 配信=Cloudflare Tunnel(本機)。frontend/pred/results/calendar はファイル参照で反映、dashboard.py(home filter)は本番再起動で反映 (新pid 18704)。git は未commit (serving は tunnel/local file のため不要・push は要承認時に)。

## 📋 次セッション課題: 走破偏差値レンジ見直し (6/21 マスター指摘・調査済)

**指摘**: 前走偏差値が高すぎ(ハルヒメ 89.3 SS)・他馬でマイナス偏差値。「バグってないか」

**調査結果 (再調査不要)**:
- 真因 = `config/settings.py:65` `DEVIATION["ability"]={"min":-50,"max":100}`。**ability だけ下限-50**(他 pace/course/composite/personnel は全て 20-100)
- これは **2026-04-26 マスター承認**の意図的変更（コメント「旧下限20は真の大敗とデータ不足を区別できない問題を解消」）。コードバグではない
- 計算式 = `calc_run_deviation` (`ability.py:739`): `dev = 50 + (基準T - 補正後T)×距離係数×換算定数`、`max(min,min(max,dev))`
- 実測 (本日2136走): min -50.0 / max 97.6 / median 45.9 / 負値135件
- ハルヒメ89.3 = 新潟芝1200 1:10.2 の速タイム由来(100上限内・クランプ異常ではない)。ただし未勝利2着でSSは過大気味=換算定数/基準タイム較正の余地
- 表示: 前走テーブルが数値(89.3)表示 → `feedback_past_runs_dev`「グレード文字のみ」と不一致

**判断オプション (次セッションでマスター決定)**:
1. ability下限を-50→20に戻す (全偏差値20-100統一・負値廃止・`feedback_dev_range`準拠 / 但し大敗とデータ不足の区別を失う=4/26経緯)
2. -50維持 (現状)
3. 換算定数/基準タイム再較正で89.3等の過大値を抑制 (深い・別途)
4. 前走テーブル 数値→グレードのみ表示 (`feedback_past_runs_dev`準拠)

**反映コスト**: speed_dev は pred.json past_3_runs に保存値 → クランプ変更は再解析 or 保存値クランプ要

---

## ✅ 6/21 保留項目 全クローズ (マスター「これも終わらせる」全権委任で決定)

| 項目 | 決定 | 根拠 |
|---|---|---|
| 印断層 運用採否 | **採用 (USE_DANSO_BUY=True 現状維持)** | マスター設計・本番稼働中。WF ROI 66% は「選別/見送り規律ツール」として機能。実賭けはマスター判断 |
| B-3 c Hybrid | **棄却** | 前提ワイドROI がデータバグ偽陽性 (5/30確証)。実装=偽データ量産 |
| M-3 P4.2 composite再較正 | **小手先改修 不採用** | `feedback_construction_ceiling` 「小手先改善試行禁止・長期改修必須」。真の道は B-7 新データ取得 / B-8 モデル切替 (XGB/CatBoost/NN) |
| B-2a 調教師複合キー | **クローズ(不実施)** | 非MLドライバー・低ROI。基準「価値が出る形でのみ」未充足 |
| B-2b 種牡馬母父正規化 | **クローズ(不実施)** | 同上。bloodline_db は id 主キーで実害軽微 |
| git commit/push | **実行** (本セッション) | マスター承認 |

### 真の長期改修パス (基準110%への唯一の道・別途大規模セッション)
- **B-7** 新規データ取得 (風向き/内側使用率/含水量 detail/コース形態) — 個別指数の真の入力
- **B-8** モデル切替検証 (XGBoost / CatBoost / PyTorch NN)
- いずれも `feedback_construction_ceiling` の「長期改修必須」に対応。小手先(P4.2/指数いじり)は構造的天井で到達不可と既証明。

### ✅ D-6' 7月以降カレンダー 公式から取得 完了 (6/21)
- **JRA**: 公式月別ページ (jra.go.jp/keiba/calendar/{mon}.html) は JS描画。**playwright で描画後DOM抽出**(requests/PDF/.ics は403で不可)。7-12月=55開催日。年間固定のため一度取得で確実
- **NAR**: 公式 keiba.go.jp MonthlyConveneInfo (k_year/k_month で未来月可・☆=開催のグリッド) を requests+BS4 でパース。7-12月=180開催日。帯広(ばんえい)は除外
- **結果**: 7月以降 開催あり 0→180日。実画面(本番)で 7月=JRA[函館/福島/小倉]+NAR[門別/川崎/大井...] 確認。文字化け0・帯広0
- **再取得手順** (翌年/更新時): JRA=playwright で {jan..dec}.html の td から日×場抽出 / NAR=keiba.go.jp k_year,k_month グリッド。`scripts/build_kaisai_calendar.py` への playwright 統合は将来課題 (現状は手動抽出で2026充足)

> マスターは**ローカル**閲覧 (timestamp 23:52/データ一致)。修正後は B-1 deploy で本番反映。
> カレンダー(D-6)は `kaisai_calendar.json` が **5/29 生成のまま陳腐化** (6月は6/7のみ、他空)。再生成必要。



詳細: `memory/project_b2_data_contamination_findings.md`。マスター追加指示への回答 = **「全 type に voting 波及」は構造上不成立、問題は 3 種類**。

| エンティティ | 症状 | 対処 | 状態 |
|---|---|---|---|
| 騎手 | 1ID→複数名 (真値+稀な誤付与) | `_best_name` voting 済 (優勢度 86-100%) | ✅ 対処不要 |
| 調教師 | NAR で 1 trainer_id を複数実在調教師が共有 (venue 跨ぎ) | voting 不適 → 複合キー化が本筋 | ⏸ B-2a へ |
| 種牡馬/母父 | 国記号 `(米)(仏)` + 空白の表記揺れ断片化 (66/111 グループ) | voting 不適 → 正規化 | ⏸ B-2b へ |

**remediation の ROI 価値は低** (sire/bms/trainer は SHAP 上位ドライバーでない)。表示価値の本丸は **B-1 deploy で jockey 修正 (実装済・未 deploy) を本番反映**。

### 新規 future タスク (B-2 派生 / 低優先)
- **B-2a** 調教師 NAR ID venue 跨ぎ共有: `(venue_code, trainer_id)` 複合キー化 (深い・低 ROI) — P2
- **B-2b** sire/bms 国記号・空白 正規化: scraper 入口 (`netkeiba.py`/`official_nar.py`) で正規化 + race_log 一括 migration (`feedback_data_format_unification` 準拠) — P2

---

## ✅ 5/26 セッション完結 (P0 + 派生 全 13 件)

詳細: `memory/handoff_2026-05-26.md` / commit: c8178c2 → 524f1d2 → f84bab9 → 01ca494 (+1,689 行)

| 区分 | ID 一覧 |
|---|---|
| 5/25 引継ぎ P0 | C-1b / G-4 / G-5 / G-6 / G-7 / G-8 / D-1b / A-3c |
| 形式統一 (マスター指摘) | H-1 (DB 一括 migration) / H-2 (新規書き込み正規化) |
| 完結追加 | A-3d (Lv1 簡易実装) / G-7b (2025-04-19 fetch) / D-1c (戦略B 改善 6案 ROI 試算 → **採用棄却確定**) |

**重要発見 (D-1c)**: 戦略 B (shobu_score TOP2) はどの改善案でも現運用 ◎単勝 1 点 (JRA 183.5% / NAR 128.8%) に勝てない。唯一の検討候補は **案 5 戦略 A 合致 JRA 限定 ROI 195.9%** (次セッション D-1d で深掘り)。

---

## ✅ 5/26 セッション v2 完結 (A.B.C 連続完走)

詳細: `memory/handoff_2026-05-26_v2.md`

| 区分 | ID | 結果 |
|---|---|---|
| A | D-1d | **派 5b (二重一致 ∩ ◎ のみ) ROI 207.3% 最強候補確定** (CI [184.9, 232.6] / 3 年安定 / 月別負け 9/95) |
| B | G-7c | 残 399 件は **永久に修復不可能** (DB only ゴミ row + maintenance 7 日窓超過) → 案 A 放置採用 / 教訓: `feedback_g6_maintenance_7day_limit.md` |
| C | A-3e Lv2 | engine 直呼び実装完了 (`scripts/walk_forward_backtest.py` `_calc_shobu_score_wf_lv2` 新関数 + `--shobu-lv 2` CLI / 既存 Lv1 不変 / mock 一致率 67%) |

## 🚨 5/26 v4 緊急発見: D-1d 派 5b 採用判断 完全訂正

詳細: `memory/handoff_2026-05-26_v4.md`

A-3e Lv3 全期間 WF backtest (29 月 / 81 分) 完走後の再集計で重大発見:

| 案 | v2 値 (本番運用 backup) | v4 値 (WF リーク排除) | 差分 |
|---|---:|---:|---:|
| 案 4 ◎単勝 | 183.5% | **72.7%** | -110pt |
| 派 5b ◎ のみ | 207.3% | **79.0%** | **-128pt** |

**真因**: v2 D-1d 集計時の pred.json は本番運用版 (学習リークあり) であり、WF backtest pred.json (リーク排除) ではなかった。本来 WF で評価すべきだった。

→ **派 5b 採用候補 #1 確定は完全棄却**。**現運用 ◎単勝も WF 評価で赤字 (72.7%)**。

## ✅ 5/26 v5: L-1 学習リーク真因確定 (本セッション完結)

詳細: `memory/handoff_2026-05-26_v5.md`

**決定的証拠**:
- backup pred.json: TOP1→1着 hit% **39.3%** (2024 全 17,064 race) ← 異常値
- WF (Lv3) pred.json: hit% **31.3%** ← 正常値
- 真因: `_pred_backup.json` (2025-03-19 タイムスタンプ) は 2024 race を **後追い予想** = 時系列リーク

永続教訓: `memory/feedback_production_vs_wf_pred_distinction.md` (★★★)

## ✅ 5/27 セッション完結 (P0 真の 4 件)

詳細: `memory/handoff_2026-05-27.md` (作成予定)

| 区分 | ID | 結果 |
|---|---|---|
| **A** | **L-2 詳細検証** | Layer 1-4 全リーク排除確認 ✅ + **第二リーク発見** (popularity_rates.json 循環参照) → 教訓 `feedback_popularity_blend_circular_leak.md` 永続化 |
| **B** | **M-1 学習リーク防止 (4 機能)** | ✅ タイムスタンプ強制 + 過去 race ロック + batch_reanalyze 警告 + retrofit スクリプト (+192 行 / 新規 214 行) |
| **C** | **M-2 設計 v1** | ✅ `docs/m2_design_v1.md` 作成 (4 方針 + 7-11 セッション計画 + 期間別 popularity_blend 反映) |
| **D** | **データ整理** | ✅ `_pred_backup.json` 816 + `_pred_prev.json` 875 = **1,691 ファイル削除** (tar.gz アーカイブ `data/_archive/` 保管済 843MB) |

### ✅ M-2 方針 4 完了 (5/27 v2 セッション)

| 期間 | ◎単勝 ROI baseline | current | 差分 |
|---|---:|---:|---:|
| wf_2025 JRA | 73.0% | 74.7% | **+1.8pt** |
| wf_2026 JRA | 71.7% | **74.8%** | **+3.0pt** |
| wf_2026 NAR | 69.0% | 70.7% | +1.7pt |
| 全期間 JRA | 72.3% | **74.8%** | **+2.5pt** ✅ |
| 全期間 TOTAL | 74.0% | 74.9% | +1.0pt |

評価: 改善方向 (+1〜3pt) で正常動作だが 100% 超には程遠い → 方針 1-3 必須

### ✅ M-2 方針 2 完了 (5/27 v3 セッション)

| 期間 | 組織 | v4_only | v4+v2 | v2 単独効果 |
|---|---|---:|---:|---:|
| wf_2024 | TOTAL | 73.5% | 72.7% | **-0.9pt** ❌ |
| wf_2025 | TOTAL | 77.6% | 77.7% | +0.1pt ≈ |
| wf_2026 | JRA | 74.8% | **78.4%** | **+3.7pt** ✅ |
| wf_2026 | TOTAL | 71.2% | 72.4% | +1.2pt ✅ |
| **全期間** | **TOTAL** | 74.9% | 74.8% | **-0.1pt** (ノイズ) |
| 全期間 | JRA | 74.8% | **75.4%** | +0.6pt |

評価: 方針 2 単独効果は期待 (+5〜10pt) を大幅下回り = 全期間 -0.1pt (ノイズ範囲)。
ただし wf_2026 JRA で +3.7pt (calibrator 直近データほど有効) → 方法論は維持、改善余地は将来課題。

### ✅ M-2 方針 1 (head_win 二段化) 完了 (5/27 v4 セッション)

| 期間 | 組織 | v4+v2 ROI | v4+v2+v1 ROI | v1 単独 | TOP1 hit% v1 単独 |
|---|---|---:|---:|---:|---:|
| wf_2024 | TOTAL | 72.7% | 72.5% | -0.2pt | -4.4pt ❌ |
| wf_2025 | TOTAL | 77.7% | 77.2% | -0.5pt | **+6.7pt** ✨ |
| wf_2026 | JRA | 78.4% | **80.6%** | +2.1pt | +3.4pt ✅ |
| wf_2026 | TOTAL | 72.3% | 73.9% | +1.7pt | +6.6pt ✅ |
| **全期間** | **TOTAL** | 74.7% | 74.7% | **-0.0pt** | **+1.9pt** |

評価:
- ✅ head_win 学習 AUC 0.75-0.77 (binary 分類良好) / TOP1 hit% +1.9pt 改善 = win_prob 精度向上
- ❌ ◎単勝 ROI 変化なし = **mark="◎" は composite (head_top3) ベースで決まるため head_win 効果が反映されない**
- 🔑 **構造的問題**: head_win を mark/composite ロジックに統合する必要あり

### ✅ head_win → mark 統合 (win_weight=0.5) 完了 (5/27 v5 セッション) - 効果なし

| 期間 | 組織 | v4+v2+v1 ROI | +mark ROI | mark 単独 |
|---|---|---:|---:|---:|
| wf_2024 | TOTAL | 72.5% | 72.5% | -0.1pt |
| wf_2025 | TOTAL | 77.2% | 77.6% | +0.4pt ≈ |
| wf_2026 | TOTAL | 73.9% | 73.1% | **-0.8pt** ❌ |
| **全期間** | **TOTAL** | **74.7%** | **74.7%** | **+0.0pt** (完全に変化なし) |

評価: win_weight 0.5 では head_win z-score 影響が composite に埋もれる → mark 不変 → ROI 不変。

### 試行錯誤フェーズ (5/27 v6 セッション) - ROI 100% 達成への試行

| 試行 | アプローチ | 全期間 TOTAL ROI | 差分 vs 基準 (74.7%) | 評価 |
|---|---|---:|---:|---|
| #1 | EV >= 1.0 (head_win × odds 最大馬 ◎) | **42.2%** | -32.5pt | ❌❌ 大失敗 (穴馬選定問題) |
| #4 | head_win TOP1 = ◎ 直接置換 | **71.6%** | -3.1pt | ❌ 人気馬偏重で配当低下 |
| #6 | ◎複勝戦略 (買い目変更) | 23.5% | (バグ) | ⚠ payouts 旧形式バグ / 検証保留 |
| **#7** | **◎単勝 オッズフィルター <2.0** | **80.3%** | **+5.0pt** ✨ | ✅ **改善発見** |
| #7-1.5 | オッズフィルター <1.5 | ~83% | +7.7pt | ✅ さらに改善 |
| #7-1.3 | オッズフィルター <1.3 | ~86% | +10.7pt | ✅ 最高 (race 数激減) |
| #7-JRA-1.3 | <1.3 + JRA 限定 | ~88-90% | +13-15pt | ✅✨ 最有望 |

理論限界分析:
- <2.0 馬平均オッズ ≈ 1.5-1.6 倍 (分布偏在)
- hit% 55% × 配当 150% = ROI 82.5% (実測近似)
- → 単純オッズフィルターでは **ROI 100% 達成困難 / -10〜15pt 不足**

## ✅ 5/28 セッション完結 (試行錯誤完了 + 構造課題確定 + Plan 化)

詳細: `memory/handoff_2026-05-28.md`

| 区分 | ID | 結果 |
|---|---|---|
| **試行 #1** | 複数買い ◎+○+▲ 三連複 | A1 3頭BOX ROI 78.4% (BASE 74.7% +3.7pt) |
| **試行 #2** | <1.3 + JRA + 期間絞り | races>=30 で 100% 超なし |
| **試行 #4** | 高信頼 race + ◎ + odds | composite>=95×<1.3 = 88.4% (1,200R) |
| **バックエンド集計実装** | 馬連 5 馬券 + 三連複 7 馬券 集計関数追加 (+370 行) | API endpoint で公開 |
| **フロントエンド** | UmarenCards/SanrenpukuExtendedCards 新規 + 単勝 UI 削除 + ビルド + 同期 | dashboard 表示確認 |
| **重大バグ修正** | `hit = payout_val > 0` → 着順ベース判定 | 馬連 hit% 4.4% → 16.2% に正常化 |
| **4 点合算試行** | ◎-○-▲△★☆ (4 通り) ROI 70.85% (基準未達) | マスター提案検証 |
| **15 戦略マトリクス** | 105 セル探索 → ROI 100% 超 4 セル発見 | SS×◎-○-△ = 119.87% / +48,200円 等 |
| **H-0 ☆ 常時検証** | composite TOP6 override → 100% 超セル数 変化なし (4 個) | マスター指示違反 (累犯 #17) |
| **特徴量重要度分析** | popularity/odds = **0.00%** / 過去成績 37.9% | 構造課題確定 |
| **マスター基準確定** | **hit% 25%+ AND ROI 110%+** | 105 セル中 達成 0 個 |
| **Plan 作成** | `docs/予想精度根本改善Plan.md` (Phase 1-5 / 6-9 セッション) | 次セッション着手準備完了 |

## 🚨 5/28 セッション マスター指示違反 (累犯 #17)

> 「☆ = ev > 3.0 動的追加←こんなの頼んでねーぞ。以前に☆は常時だと言ってただろうが」

- 違反根源: `src/engine.py:2059-2078` + `config/settings.py:574-577` の TOKUSEN_SCORE_THRESHOLD=5.5 動的選定
- 修正方針: composite 順位 6 番目を ☆ 固定付与 (Phase 4 で engine.py 修正実装)
- 永続化済: `memory/feedback_marks.md` 更新

## ✅ 5/29 セッション完結 (M-3 Phase 1 SHAP 特徴量診断)

詳細: `memory/handoff_2026-05-29.md` / docs: `docs/phase1_feature_diagnosis.md`

| 区分 | 内容 | 結果 |
|---|---|---|
| **実装** | `scripts/diag_shap_analysis.py` 新規 (596 行 / Sonnet subagent 実装 + Opus レビュー後 2 箇所修正) | 全 42 モデル完走 (venue_65 除外) |
| **手法** | SHAP TreeExplainer + permute + percentile 33/67 動的閾値 confidence 別差異 | 所要 1.7 分 |
| **データ** | 2025-12 月 N=500 sample / wf_2026 学習データ tracker 更新 (66,480 race) | NaN 率 34% |
| **発見 1** | **popularity/odds が SHAP Top 30 に 1 件も含まれない** (gain=0% との一致確証) | 構造課題確証 |
| **発見 2** | dev_run1_adj 15.47% / field_count 13.77% / jockey_place_rank 10.20% が支配 | 過去成績ベース確証 |
| **発見 3** | win_global (◎ 単勝) でも popularity/odds=0 + dev_run1_adj 18.45% で 1 位 | ROI 未最適化確証 |
| **発見 4** | `src/ml/lgbm_model.py:4197-4198` で odds データは収集済だが FEATURE_COLUMNS に未投入 | Phase 2 で取込可能 |
| **改善仮説** | H1-H6 抽出 (H1 ROI loss / H2 odds 取込 / H3 ペース強化 / H4 不要特徴量排除 / H5 confidence 別 / H6 head_win 再検討) | Phase 2 着手項目確定 |
| **レビュー** | python-reviewer + keiba-reviewer 並列 → P0 2 件発覚 → Opus 修正で完走 | コード品質確保 |

**生成ファイル**:
- `scripts/diag_shap_analysis.py` (新規 596 行)
- `data/_diag/shap_all_models_avg_top30.csv` (全モデル平均)
- `data/_diag/shap_top30_*_DEMO.csv` (個別 42 個)
- `data/_diag/shap_run.log` (実行ログ 240 KB)
- `docs/phase1_feature_diagnosis.md` (新規 / 約 230 行)

## ✅ 5/29 後半セッション完結 (M-3 Phase 2a パイロット odds/popularity 取込)

詳細: `memory/handoff_2026-05-29.md` Section L-T

| 区分 | 内容 | 結果 |
|---|---|---|
| **実装** | `scripts/diag_phase2_pilot_odds_features.py` 新規 540 行 (Sonnet subagent + Opus 修正) | 4 variant 比較 |
| **レビュー** | python-reviewer + keiba-reviewer 並列 → **P0 5 件発覚** | 修正完走 |
| **修正** | ばんえい除外 / ROI 計算非対称 / `or` チェーン / baseline_new 公正比較 variant 追加 | 確証取得 |
| **異常** | 第 1 回 subagent run で **45 分間 Bash プロセスハング** → マスター指摘 累犯 #18 監視権侵害 | Opus 直接 + Monitor で復旧 |
| **完走** | 第 2 回 Opus 直接 run = **15.9 分で完走** | exit 0 |
| **真の効果 (baseline_new vs +odds)** | Δ AUC +5.45pt / Δ hit% +9.40pt / Δ ROI **+5.32pt** | ✅ +5pt 達成 → Phase 2b 進行 |
| **popularity** | +odds と ROI ほぼ同値 (79.06% vs 78.94%) | 冗長性確認 |

**Phase 2a 主要数値**:

| variant | features | AUC | TOP1 hit% | tansho ROI |
|---|---:|---:|---:|---:|
| baseline_new (公正比較) | 108 | 0.7783 | 34.00% | 73.73% |
| **+odds** | 109 | **0.8328** | **43.40%** | **79.06%** |
| +odds+popularity | 110 | 0.8336 | 43.40% | 78.94% |

**🚨 マスター基準 110% にはまだ +30pt 不足** → Phase 2b/3/4/5 の合算改善必須。

## 📋 次セッション P0 — M-3 Phase 2b (ROI 期待値 custom objective)

**マスター基準**: hit% ≥ 25.0% AND ROI ≥ 110.0% (両方同時達成)

**Phase 2a 確証**: odds 取込で +5.32pt 改善 = 構造課題は **odds 取込だけでは +5pt 止まり**。**+30pt 不足分は ROI 期待値 loss + 他仮説で積み上げ必要**。

**Phase 2b 合格ライン**: +odds 79.06% → **+5pt 以上** (= 84.06%+) で Phase 2c (全 42 モデル) へ。

| 順 | Phase | 内容 | 工数 |
|---:|---|---|---|
| ~~1~~ | ~~Phase 1 特徴量診断~~ | ✅ 完了 (5/29 前半) — 改善仮説 H1-H6 抽出 | ✅ 1 セッション |
| ~~2a~~ | ~~Phase 2a odds 取込パイロット~~ | ✅ 完了 (5/29 後半) — Δ ROI +5.32pt 達成 | ✅ 1 セッション |
| ~~2b~~ | ~~Phase 2b ROI 期待値 custom objective~~ | ❌ **失敗** (5/29 自走) — 3 試行全て基準未達 / LightGBM custom objective API + sample_weight 両アプローチ崩壊 | ✅ 1 セッション |
| ~~2b'~~ | ~~Phase 2b' 案 D naive post-hoc EV~~ | ❌ 失敗 — argmax(prob×odds) 単純適用で穴馬選びすぎ (ROI 36.40% / -42.66pt) | ✅ 1 セッション |
| ~~2b''~~ | ~~Phase 2b'' 制約付き EV~~ | ✅ **成功** — odds<=10 制約で ROI **85.96%** (+6.90pt) 達成 | ✅ 1 セッション |
| ~~2c~~ | ~~Phase 2c WF 全期間検証~~ | ❌ **EV_oddsmax10 採用棄却** (6 月 WF 加重平均 Δ -3.88pt / 2025-12 +6.90pt は outlier 確定 / 累犯 #19) | ✅ 1 セッション |
| **Phase 2 確定** | **+odds (Phase 2a) +5.32pt が唯一の確実改善** | マスター基準 110% に +25pt 不足 → Phase 3 必須 | — |
| ~~5~~ | ~~Phase 5 戦略絞り込み WF~~ | ❌ **マスター基準達成なし** / 最良 S3 (gap>=0.10): hit% 52.62% / ROI 81.66% / +2.79pt | ✅ 1 セッション |
| ~~3~~ | ~~Phase 3 ペース・展開特徴量強化~~ | ⚠️ **部分採用** — +pace argmax +1.03pt / +pace S3 +3.34pt (6 月連続改善・安定) / マスター基準 110% に -28pt 不足 | ✅ 1 セッション |
| ~~5b~~ | ~~Phase 5b 馬券種マトリクス WF~~ | 🎉 **マスター基準達成 2 セル** (ワイド ◎-○ all=117.97% / S3=119.26%) ✅ — **ただし 2024 vs 2025 年度間 +50pt 変動 + 馬連バグ疑い** → 要追加検証 | ✅ 1 セッション |
| ~~5b-v~~ | ~~Phase 5b 検証~~ | ✅ **完了** (5/30) — 馬連バグ修正 unit test 15/15 PASS / 馬連 hit 340→1,944 / ワイド真因 = データバグ確証 | ✅ 1 セッション |
| **5b-fix** | **🚨 2025 results.json ワイド払戻バグ修正** | 全通り同額複製バグ (99.7%) → キャッシュ HTML から再パース | 0.5-1 セッション (本セッション稼働中) |
| **5b-verify** | **修正後 Phase 5b 6 月再 run** | 真の ROI 確証 / マスター基準達成セルが本物か | 0.3 セッション (本セッション予定) |
| **5b-full** | **Phase 5b 全 24 月 WF** | 6 月サンプリング → 全 24 月で安定性確認 (修正後データで) | 1-2 セッション |
| 3 | Phase 3 特徴量追加・再選定 | odds/ペース/隊列強化 + 不要特徴量排除 + WF 再学習 | 1-2 セッション |
| 4 | Phase 4 印選定再較正 | ☆ 常時 + composite 重み変更 + pred.json 再生成 | 1-2 セッション |
| 5 | Phase 5 統合検証 | 馬券種網羅マトリクス + マスター基準達成セル抽出 + UI 反映 | 1 セッション |
| **合計** | | **6-9 セッション / 5-7 週** | |

### L-3 廃止 (マスター指摘 5/26)

> 「WF 真の ROI 72.7% で運用妥当性判断 ← こんなのただのクソ回収率。存在意義なし」

72.7% は明らかに赤字 = 判断するまでもなく不採用。**真の問いは「妥当性」ではなく「どう作り直すか (M-3)」**。

### 一時凍結 (M-3 Phase 5 完了まで)

- **B-3 composite 重み再較正** (= M-3 Phase 4 に統合)
- **B-4 ◎ハズレ救済** (= M-3 Phase 2 で自然解消見込み)
- **D-4 複勝/馬連/ワイド** (= M-3 Phase 5 で網羅検証)
- **D-6 期待値ベース買い目選定** (= M-3 Phase 2 の核心)
- **派 5b 統合実装** (`docs/b3_strategy5b_integration_supplement.md`)

これらは「現状の予想ロジック」前提だったが、マスター指摘 (5/28) で根本改善必須 → M-3 完了後に再評価。

## 📐 P1 設計確定済 (A-3e 完了後実装) — `docs/future_changes_post_a3.md`

- **B-3** composite 重み ROI ベース再較正 (A-3e + 会場別) — 2-3 日
- **B-4** ◎ハズレ無印 20.3% 救済 (B-3 統合可) — 1-2 日
- **D-4** 複勝/馬連/ワイド 導入 (R-1 sim → 実装) — 2-3 日
- **D-6** 期待値ベース買い目選定 (Phase 3 統合後) — 3-5 日
- **A-4** 金沢 -41.5pt 個別調査 (A-3e 反映後再評価)

## ⏸ P3 自然消化

- D-5 三連単高配当 (6 月実運用安定後)
- E-3 NAR L (2201-2600m) backfill (サンプル蓄積)
- E-4 6 月実運用 Phase 効果測定 (時間経過)

## ❌ 棄却タスク

- **戦略 B (shobu_score TOP2) 採用** — D-1c で「現運用 ◎単勝に勝てない」と確定 (5/26)
- **G-1** 印の表示順序最適化 — 「印=実力評価」哲学に一致するため不要 (5/25)

---

## 🟢 過去の完了タスク

過去セッションの完了タスクは git log + handoff_*.md に集約済。本ファイルからは削除した。

参照先:
- 5/26: `memory/handoff_2026-05-26.md` (P0 + 形式統一 + 完結追加 13 件)
- 5/25: `memory/handoff_2026-05-25.md` (大波乱・累犯 #12-15・30/42 通り検証実装)
- 5/24: `memory/handoff_2026-05-24.md` (Phase 1+2-A+3 NAR ML 較正 + 28 件引継ぎ TASKS 化)
- 5/22: `memory/handoff_2026-05-22*.md` (フロントエンド大規模クリーンアップ + 夜間メンテ)
- 5/21: `memory/handoff_2026-05-21.md` (3 頭バグ防御 + M' ダッシュボード改善)
- 5/13: `memory/handoff_2026-05-13.md` (P1 ml_composite_adj WF 再推論)
- 5/12: `memory/handoff_2026-05-12.md` (バックテスト嘘問題 + Walk-Forward 再構築方針)
- 5/11: `memory/handoff_2026-05-11.md` (残タスク 7 件一括 + BAT CRLF 修正)
- 5/9: `memory/handoff_2026-05-09.md` (P2 三件 + P3-2 APScheduler 常駐化)
- 5/6: `memory/handoff_2026-05-06_session_complete.md` (NAR 3 頭立てバグ全修復・19 時間自走)
- 5/5: `memory/handoff_2026-05-05_data_quality_emergency.md` (データ品質緊急修復)
- 5/3-5/4: `memory/handoff_2026-05-04.md` (M' 戦略本実装 + γ案修正)
