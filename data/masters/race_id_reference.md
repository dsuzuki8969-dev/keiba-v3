# race_id 形式リファレンス

netkeiba と競馬ブックの race_id の作り方・違いをまとめる。調教・コメント取得時の変換に参照する。

---

## netkeiba（race.netkeiba.com / nar.netkeiba.com）

### 共通
- **桁数**: 12桁
- **取得元**: レース一覧リンク `race_id=(\d{12})` から取得

### 中央競馬（JRA）
| 位置 | 桁数 | 内容 | 例 |
|------|------|------|-----|
| [0:8] | 8桁 | 開催ID相当（日付・回次等を含む形式） | `20250105` |
| [8:10] | 2桁 | **venue_code（競馬場）** 01〜10 | `05`=東京 |
| [10:12] | 2桁 | レース番号 R | `11`=11R |

- venue_code: `data/masters/venue_master.py` の JRA_VENUES 参照
- 日付は race_id に直接入らない場合あり（HTML から取得）

### 地方競馬（NAR）
| 位置 | 桁数 | 内容 | 例 |
|------|------|------|-----|
| [0:4] | 4桁 | 年 YYYY | `2025` |
| [4:6] | 2桁 | **venue_code（競馬場）** | `44`=大井 |
| [6:8] | 2桁 | 月 MM | `01` |
| [8:10] | 2桁 | 日 DD | `22` |
| [10:12] | 2桁 | レース番号 R | `05`=5R |

- venue_code: `data/masters/venue_master.py` の NAR_VENUES 参照
- 判定: `[8:10]` が JRA_CODES に含まれる → JRA、そうでなければ `[4:6]` が venue

---

## 競馬ブック（s.keibabook.co.jp）

### 12桁形式（主に使用）
| 位置 | 桁数 | 内容 | 備考 |
|------|------|------|------|
| [0:8] | 8桁 | 日付 YYYYMMDD | |
| [8:10] | 2桁 | 場所コード PP | **netkeiba と異なる可能性あり** |
| [10:12] | 2桁 | レース番号 RR | |

- URL例（中央）: `https://s.keibabook.co.jp/cyuou/cyokyo/202601050701`
- URL例（danwa）: `https://s.keibabook.co.jp/cyuou/danwa/0/202601050701` （`/0/` が入る場合あり）

### その他の形式（参考）
- 8桁（旧）: `ppyyknrr` 場所+年下2桁+回+日+R
- 16桁（新）: より詳細な開催情報

---

## 変換: netkeiba → 競馬ブック

### 中央競馬
- 12桁の並びが同一の場合あり（要実機確認）
- 違いがあれば `netkeiba_to_keibabook_id()` でマッピング追加

### 地方競馬
- **場コードが異なる可能性が高い**
- netkeiba の jyo_cd（例: 44=大井）と競馬ブックの PP が一致しない場合、変換テーブルが必要

### 変換テーブル（要調査・追記）
```
# 判明次第追記
# netkeiba_venue → keibabook_place のマッピング
# 例: {"36": "xx", "44": "yy", ...}
```

---

## 参照関数

| 用途 | ファイル | 関数 |
|------|----------|------|
| venue_code 取得 | `venue_master.py` | `get_venue_code_from_race_id(race_id)` |
| JRA 判定 | `venue_master.py` | `is_jra(venue_code)` |
| netkeiba→競馬ブック | `keibabook_training.py` | `netkeiba_to_keibabook_id(netkeiba_race_id)` |
