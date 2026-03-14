# ダッシュボード精査レポート（隅々まで）

実施日: 2026-02-26  
対象: `src/dashboard.py` および関連API・output配下HTML・各タブの挙動

---

## 修正したバグ一覧

### 1. レース予想タブ：分析完了後の「結果を開く」リンクが未設定
- **現象**: 分析完了してもリンクが `#` のまま。クリックしてもどこにも飛ばない。
- **原因**: `pollAnalyze` で `resultEl.classList.add('show')` のみ行い、`analyze-result-link` の `href` を設定していなかった。
- **修正**: 完了時に `analyze_date` の値から `YYYYMMDD_全レース.html` のURLを組み立て、`href` と表示テキストを設定。あわせて `loadHomeRaces()` と `loadShareUrl()` を呼び出し、HOMEタブの表示も更新。

### 2. データ収集タブ：完了後も進捗エリアが表示されたまま
- **現象**: 収集が終わってもプログレスバーとステータスが残る。
- **原因**: `pollCollect` の `else` 分支で `collect-progress` の `display` を `none` にしていなかった。
- **修正**: 収集終了時に `document.getElementById('collect-progress').style.display='none'` を追加。

### 3. HOMEタブ：レースカードのグレード表示が地方対応していない
- **現象**: 地方のクラス（C3, B1, A1 等）がバッジとして表示されない。
- **原因**: `gCls` の条件が `['G1','G2','G3','L','OP']` のみで、NARクラスを考慮していなかった。
- **修正**: 上記以外の `r.grade` には `h-rc-nar` クラスを付与し、`.h-rc-nar` のCSSを追加。地方グレードもバッジ表示するように変更。

### 4. HOMEタブ：自信度の ⁺（Unicode）で色分けが効かない
- **現象**: データが「B⁺」のとき、JSの `conf==='B+'` が false になり、色がデフォルトになる。
- **原因**: 比較が ASCII の `+` のみで、Unicode U+207A（⁺）を考慮していなかった。
- **修正**: `confRaw` を表示用に残し、色用に `conf = confRaw.replace(/\u207a/g, '+')` で正規化してから比較。

### 5. 開催場フォールバック：会場コードの取得が逆
- **現象**: 予想データから会場を復元するとき、`VENUE_MAP` の key/value を逆に使い、code が空になる可能性があった。
- **原因**: `code = next((k for k, v in VENUE_MAP.items() if v == venue_name), "")` で、VENUE_MAP が name→code のため k が name になり誤り。
- **修正**: `VENUE_NAME_TO_CODE.get(venue_name, "")` でコードを取得するように変更。

### 6. VENUE_COORDS と venue_master の場コード不一致
- **現象**: 園田・盛岡・水沢・門別・帯広のコードがマスタとずれており、天気取得で座標が取れない場合があった。
- **原因**: ダッシュボード側の VENUE_COORDS が古い定義のまま（例: 盛岡36→正しくは35、水沢37→36、園田50→49）。
- **修正**: venue_master に合わせ、30=門別、35=盛岡、36=水沢、49=園田、52=帯広 を設定。重複していた 50 を削除。

### 7. レース予想タブ：開催場0件・取得エラー時のボタン状態
- **現象**: 「この日の開催情報が見つかりませんでした」や取得エラー時に、`loadingEl` の表示や `btn_analyze` の disabled が一貫していない。
- **修正**: 0件時も `loadingEl.style.display = 'block'` と `btnAnalyze.disabled = true` を明示。catch 時も同様に `loadingEl.style.display = 'block'` と `btnAnalyze.disabled = true` を設定。

### 8. 結果分析タブ：照合完了後の年フィルタがリセットされる
- **現象**: 照合後に `loadResultsSummary()` を引数なしで呼んでおり、常に「通算」で再取得されていた。
- **修正**: 照合完了時に、現在アクティブな `.sub-tab.active` の `data-year` を渡して `loadResultsSummary(activeYear.dataset.year)` を呼ぶように変更。

### 9. 結果分析タブ：着順照合後の進捗エリアが非表示にならない
- **現象**: 照合完了後も「着順を取得して照合」の進捗バーが残る。
- **修正**: 成功時・失敗時ともに `fetch-progress` の `display = 'none'` を設定。catch 内でも同様に非表示にした。

### 10. 結果分析タブ：日付別成績の roi 未定義時
- **現象**: `r.roi` が無い場合に「回収 undefined%」と表示される。
- **修正**: `roiStr = r.roi != null ? r.roi + '%' : '—'` とし、`r.hit_tickets` / `r.total_tickets` も `|| 0` でフォールバック。

### 11. 結果分析タブ：自信度別テーブルで API のキーが ⁺ の場合
- **現象**: バックエンドが「B⁺」で返すと、フロントの `confOrder` は `'B⁺'` だが、キー一致で見つからない可能性があった。
- **修正**: 表示順用に `norm(c) = c.replace(/\u207a/g, '+')` で正規化し、`confData` のキーと正規化比較。表示には実際のキー `key` を使用。

### 12. 結果分析タブ：サマリー取得エラー時の表示
- **現象**: `loadResultsSummary` の catch でコンソールだけエラーで、画面は前の状態のまま。
- **修正**: catch 内で `rs-nodata` を表示、`rs-stat-row` を非表示、`_renderResultStats(null, null)` を呼ぶようにした。

---

## 修正していないが確認した点（仕様・軽微）

- **ルート `/` と `/dash`**: どちらも同じ HTML を返す。ルールで 5051/dash が案内されているため問題なし。
- **api/portfolio**: `course_runs` / `last_date` は `_get_db_state()` から取得。`date_files` / `single_files` は `_scan_output()` で取得。整合性あり。
- **api/today_predictions**: キャッシュ TTL 300秒。`_scan_today_predictions` は `YYYYMMDD_場名XR.html` のパターンでスキャン。ファイル名と `_VENUE_PRIORITY` の順序で `order` を生成。
- **api/share_url**: 存在しない場合は `exists=false`、`size_kb=0`。`getsize` は exists 時のみ呼んでいるためエラーにならない。
- **output 配下 HTML**: `_parse_race_html_meta` は `race-meta` JSON を優先し、フォールバックで正規表現。旧HTML・新HTML 両対応。
- **ナビバー注入**: `_build_nav_bar` は `YYYYMMDD_場名XR.html` で同日・同場のファイルをスキャン。`_VENUE_PRIO_MAP` で場順を統一。ポート5051はハードコード。
- **結果分析の日付セレクト**: 初回のみ `_datesFetched` で `/api/results/dates` を取得。照合後に新しい日付が増えても、セレクトは次回の「結果分析」タブ表示まで更新されない。必要なら照合完了時に日付一覧を再取得する拡張が可能。

---

## ファイル変更まとめ

- **src/dashboard.py**
  - 上記 1〜12 の修正をすべて反映。
  - VENUE_COORDS の場コードを venue_master に合わせて修正（門別30、盛岡35、水沢36、園田49、帯広52 等）。
  - HOME レースカードのグレードに `.h-rc-nar` を追加。
  - 分析完了時の「結果を開く」リンク設定、収集完了時の進捗非表示、照合完了時の進捗非表示と年フィルタ維持。

---

## 今後の改善案（任意）

1. **レース予想タブ**: 分析開始時に `analyze_date` を readonly にするか、完了まで変更不可にして、結果リンクの日付とずれないようにする。
2. **結果分析タブ**: 照合完了後に `/api/results/dates` を再取得し、日付セレクトの option を更新する。
3. **HOME タブ**: レースカードから `/output/xxx` を開いたとき、同一ウィンドウで開くか `target="_blank"` を付けるか、設定またはユーザー要望に合わせて検討。
4. **api/home_info**: 天気の取得失敗時も `condition: "—"` で返しているため、フォールバックは問題なし。必要ならリトライやログを追加可能。

以上。
