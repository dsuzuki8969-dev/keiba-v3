"""
NAR scraper の _parse_race_mark_table メソッドのテスト

バグ再発防止: 以前 `if len(cells) < 12: continue` により 4着以降の馬が
全スキップされていた。修正後は `< 4` に緩和済み。
このファイルはその再発防止を含む全ケースを検証する。

ネットワーク不要で実行可能 (BeautifulSoup に HTML 文字列を直接渡す)。
"""

import pytest
from bs4 import BeautifulSoup

from src.scraper.official_nar import OfficialNARScraper


# ================================================================
# ヘルパー
# ================================================================

def _make_scraper() -> OfficialNARScraper:
    """ネットワーク接続なしでインスタンスを生成するヘルパー"""
    return OfficialNARScraper()


def _build_html(rows: list[list[str]]) -> str:
    """着順テーブル HTML を組み立てる。

    rows: 各行のセルテキストリスト。
    「着順」「馬番」を含むヘッダ行を先頭に付与し、
    _parse_race_mark_table がテーブルを特定できるようにする。
    """
    header = (
        "<tr>"
        "<th>着順</th><th>枠番</th><th>馬番</th><th>馬名</th>"
        "<th>所属</th><th>性齢</th><th>負担重量</th><th>騎手</th>"
        "<th>調教師</th><th>馬体重</th><th>差</th><th>タイム</th>"
        "<th>着差</th><th>上り3F</th><th>人気</th>"
        "</tr>"
    )
    body = ""
    for cells in rows:
        tds = "".join(f"<td>{c}</td>" for c in cells)
        body += f"<tr>{tds}</tr>"
    return f"<table>{header}{body}</table>"


def _soup(html: str) -> BeautifulSoup:
    """HTML 文字列から BeautifulSoup オブジェクトを生成する"""
    return BeautifulSoup(html, "html.parser")


# 15 列分のフルデータ行を生成するヘルパー
def _full_row(finish: int, horse_no: int, horse_name: str = "テスト馬",
              jockey: str = "テスト騎手", weight_kg: str = "55.0",
              horse_weight: str = "480(+4)", time_str: str = "1:23.5",
              last_3f: str = "36.8", popularity: str = "1",
              margin: str = "クビ") -> list[str]:
    """15 列の完全データ行を返す"""
    # 列順: 着順, 枠番, 馬番, 馬名, 所属, 性齢, 負担重量, 騎手, 調教師,
    #       馬体重, 差, タイム, 着差, 上り3F, 人気
    return [
        str(finish), "1", str(horse_no), horse_name,
        "栗東", "牡3", weight_kg, jockey,
        "テスト調教師", horse_weight, "0.0", time_str,
        margin, last_3f, popularity,
    ]


# ================================================================
# テストケース 1: 全列あり 15 セル — 1-3着の完全データ
# ================================================================

class TestFullRows:
    """15 列フルデータの正常パース"""

    def test_three_horses_full_fields(self):
        """1-3着馬がすべてのフィールドを正しく取得できること"""
        scraper = _make_scraper()
        rows = [
            _full_row(1, 3, "ダイワスカーレット", "武豊", "55.0",
                      "480(+4)", "1:23.5", "36.8", "1", ""),
            _full_row(2, 7, "ウオッカ", "四位洋文", "54.0",
                      "456(-2)", "1:23.8", "37.0", "2", "クビ"),
            _full_row(3, 1, "アドマイヤムーン", "岩田康誠", "56.0",
                      "500(0)", "1:24.0", "37.2", "3", "1/2"),
        ]
        html = _build_html(rows)
        results = scraper._parse_race_mark_table(_soup(html))

        assert len(results) == 3

        # 1着馬の検証
        r1 = results[0]
        assert r1["finish"] == 1
        assert r1["horse_no"] == 3
        assert r1["horse_name"] == "ダイワスカーレット"
        assert r1["jockey_name"] == "武豊"
        assert r1["weight_kg"] == 55.0
        assert r1["horse_weight"] == 480
        assert r1["time_sec"] == pytest.approx(83.5)  # 1*60 + 23.5
        assert r1["last_3f"] == pytest.approx(36.8)
        assert r1["popularity"] == 1
        assert r1["margin"] == ""

        # 2着馬の検証
        r2 = results[1]
        assert r2["finish"] == 2
        assert r2["horse_no"] == 7
        assert r2["horse_weight"] == 456
        assert r2["margin"] == "クビ"

        # 3着馬の検証
        r3 = results[2]
        assert r3["finish"] == 3
        assert r3["horse_no"] == 1
        assert r3["horse_weight"] == 500


# ================================================================
# テストケース 2: 最小 4 セル — 着順・枠番・馬番・馬名のみ
# ================================================================

class TestMinimalRow:
    """4 列最小構成のパース"""

    def test_four_cells_returns_horse_with_defaults(self):
        """4 セル行で horse_no・finish・horse_name が取れ、他はデフォルト値になること"""
        scraper = _make_scraper()
        # 着順, 枠番, 馬番, 馬名 の 4 列のみ
        rows = [["1", "2", "5", "ミニマム馬"]]
        html = _build_html(rows)
        results = scraper._parse_race_mark_table(_soup(html))

        assert len(results) == 1
        r = results[0]
        assert r["finish"] == 1
        assert r["horse_no"] == 5
        assert r["horse_name"] == "ミニマム馬"
        # 不足列はデフォルト値になること
        assert r["jockey_name"] == ""
        assert r["weight_kg"] == 55.0  # デフォルト
        assert r["horse_weight"] is None
        assert r["time_sec"] == 0.0
        assert r["last_3f"] == 0.0
        assert r["popularity"] is None
        assert r["margin"] == ""


# ================================================================
# テストケース 3: 混在 (15 セル + 4 セル) — バグ再発防止の核心テスト
# ================================================================

class TestMixedRows:
    """15 列と 4 列の混在テスト (バグ再発防止の核心)"""

    def test_five_horses_mixed_cell_counts(self):
        """3 頭が 15 セル、2 頭が 4 セルの混在で 5 頭全取得できること

        旧バグ: len(cells) < 12 でスキップしていたため、4 セル行は全部欠落。
        修正後: len(cells) < 4 に緩和し、全頭取込可能になった。
        """
        scraper = _make_scraper()
        # 1-3着: フル 15 列
        full_rows = [
            _full_row(1, 3, "フルデータ馬1"),
            _full_row(2, 7, "フルデータ馬2"),
            _full_row(3, 1, "フルデータ馬3"),
        ]
        # 4-5着: 最小 4 列 (旧バグでスキップされていた行)
        minimal_rows = [
            ["4", "3", "9", "最小データ馬4"],
            ["5", "4", "11", "最小データ馬5"],
        ]
        all_rows = full_rows + minimal_rows
        html = _build_html(all_rows)
        results = scraper._parse_race_mark_table(_soup(html))

        # 5 頭全取得が必須 (旧バグでは 3 頭しか取れなかった)
        assert len(results) == 5, (
            f"5 頭取得を期待したが {len(results)} 頭しか取れなかった。"
            "len(cells) < 12 でスキップするバグが再発している可能性あり。"
        )

        # 着順が 1-5 で揃っていること
        finishes = [r["finish"] for r in results]
        assert finishes == [1, 2, 3, 4, 5]

        # 4着馬 (最小 4 列) の horse_no と horse_name が取れていること
        r4 = results[3]
        assert r4["horse_no"] == 9
        assert r4["horse_name"] == "最小データ馬4"

        # 5着馬 (最小 4 列) の horse_no と horse_name が取れていること
        r5 = results[4]
        assert r5["horse_no"] == 11
        assert r5["horse_name"] == "最小データ馬5"


# ================================================================
# テストケース 4: 3 セル以下 — スキップされること
# ================================================================

class TestTooFewCells:
    """セル数不足の行はスキップされること"""

    def test_three_cells_skipped(self):
        """3 セルの行は len(cells) < 4 でスキップされること"""
        scraper = _make_scraper()
        rows = [["1", "2", "5"]]  # 3 セルのみ
        html = _build_html(rows)
        results = scraper._parse_race_mark_table(_soup(html))
        assert results == []

    def test_one_cell_skipped(self):
        """1 セルの行もスキップされること"""
        scraper = _make_scraper()
        rows = [["1"]]
        html = _build_html(rows)
        results = scraper._parse_race_mark_table(_soup(html))
        assert results == []


# ================================================================
# テストケース 5: ヘッダ行 — 着順が「着順」(非数字) → スキップ
# ================================================================

class TestHeaderRow:
    """ヘッダ行はスキップされること"""

    def test_header_row_skipped(self):
        """着順列が「着順」という文字列の場合はスキップされること"""
        scraper = _make_scraper()
        # th ではなく td でヘッダ行を作る場合のテスト
        # texts[0].isdigit() が False → スキップ
        rows = [
            ["着順", "枠番", "馬番", "馬名", "所属", "性齢", "負担重量", "騎手",
             "調教師", "馬体重", "差", "タイム", "着差", "上り3F", "人気"],
            _full_row(1, 3, "本命馬"),
        ]
        html = _build_html(rows)
        results = scraper._parse_race_mark_table(_soup(html))

        # ヘッダ行はスキップされ、データ行 1 行のみ取れること
        assert len(results) == 1
        assert results[0]["finish"] == 1


# ================================================================
# テストケース 6: 馬番 0 または非数字 — スキップ
# ================================================================

class TestInvalidHorseNo:
    """馬番が 0 または非数字の行はスキップされること"""

    def test_horse_no_zero_skipped(self):
        """馬番が 0 の行はスキップされること (horse_no = 0 は無効)"""
        scraper = _make_scraper()
        # 着順=1, 枠番=1, 馬番=0, 馬名=...
        rows = [["1", "1", "0", "馬番ゼロ馬", "栗東", "牡3", "55.0",
                 "騎手A", "調教師A", "480", "0.0", "1:23.5", "", "36.8", "1"]]
        html = _build_html(rows)
        results = scraper._parse_race_mark_table(_soup(html))
        assert results == []

    def test_horse_no_non_digit_skipped(self):
        """馬番が非数字の行はスキップされること"""
        scraper = _make_scraper()
        rows = [["1", "1", "abc", "非数字馬", "栗東", "牡3", "55.0",
                 "騎手A", "調教師A", "480", "0.0", "1:23.5", "", "36.8", "1"]]
        html = _build_html(rows)
        results = scraper._parse_race_mark_table(_soup(html))
        assert results == []

    def test_cancelled_horse_text_skipped(self):
        """着順列が「取消」(非数字) の行はスキップされること"""
        scraper = _make_scraper()
        rows = [["取消", "2", "8", "取消馬"]]
        html = _build_html(rows)
        results = scraper._parse_race_mark_table(_soup(html))
        assert results == []


# ================================================================
# テストケース 7: テーブルなし — 空リスト返却
# ================================================================

class TestNoTable:
    """テーブルが存在しない場合は空リストを返すこと"""

    def test_empty_html_returns_empty_list(self):
        """テーブルなし HTML → 空リスト"""
        scraper = _make_scraper()
        results = scraper._parse_race_mark_table(_soup("<html><body></body></html>"))
        assert results == []

    def test_table_without_header_returns_empty_list(self):
        """「着順」「馬番」ヘッダを持たないテーブルは無視されること"""
        scraper = _make_scraper()
        html = "<table><tr><td>foo</td><td>bar</td></tr></table>"
        results = scraper._parse_race_mark_table(_soup(html))
        assert results == []


# ================================================================
# テストケース 8: タイム "1:23.5" パース → 83.5 秒
# ================================================================

class TestTimeParsing:
    """タイム文字列のパース"""

    def test_time_mm_ss_format(self):
        """「M:SS.S」形式のタイムが秒数に変換されること"""
        scraper = _make_scraper()
        rows = [_full_row(1, 5, time_str="1:23.5")]
        html = _build_html(rows)
        results = scraper._parse_race_mark_table(_soup(html))
        assert len(results) == 1
        # 1*60 + 23.5 = 83.5 秒
        assert results[0]["time_sec"] == pytest.approx(83.5)

    def test_time_long_distance(self):
        """長距離「2:05.3」が正しく秒数変換されること"""
        scraper = _make_scraper()
        rows = [_full_row(1, 5, time_str="2:05.3")]
        html = _build_html(rows)
        results = scraper._parse_race_mark_table(_soup(html))
        # 2*60 + 5.3 = 125.3 秒
        assert results[0]["time_sec"] == pytest.approx(125.3)

    def test_time_missing_returns_zero(self):
        """タイム列が存在しない場合は 0.0 を返すこと"""
        scraper = _make_scraper()
        rows = [["1", "1", "3", "タイムなし馬"]]  # 4 列のみ
        html = _build_html(rows)
        results = scraper._parse_race_mark_table(_soup(html))
        assert results[0]["time_sec"] == 0.0


# ================================================================
# テストケース 9: 馬体重 "480(+4)" パース → 480
# ================================================================

class TestHorseWeightParsing:
    """馬体重文字列のパース"""

    def test_horse_weight_with_diff(self):
        """「480(+4)」から 480 が取れること"""
        scraper = _make_scraper()
        rows = [_full_row(1, 5, horse_weight="480(+4)")]
        html = _build_html(rows)
        results = scraper._parse_race_mark_table(_soup(html))
        assert results[0]["horse_weight"] == 480

    def test_horse_weight_negative_diff(self):
        """「456(-2)」から 456 が取れること"""
        scraper = _make_scraper()
        rows = [_full_row(1, 5, horse_weight="456(-2)")]
        html = _build_html(rows)
        results = scraper._parse_race_mark_table(_soup(html))
        assert results[0]["horse_weight"] == 456

    def test_horse_weight_plain(self):
        """差分なし「500」から 500 が取れること"""
        scraper = _make_scraper()
        rows = [_full_row(1, 5, horse_weight="500")]
        html = _build_html(rows)
        results = scraper._parse_race_mark_table(_soup(html))
        assert results[0]["horse_weight"] == 500

    def test_horse_weight_missing_returns_none(self):
        """馬体重列が存在しない場合は None を返すこと"""
        scraper = _make_scraper()
        rows = [["1", "1", "3", "体重なし馬"]]  # 4 列のみ
        html = _build_html(rows)
        results = scraper._parse_race_mark_table(_soup(html))
        assert results[0]["horse_weight"] is None

    def test_horse_weight_out_of_range_returns_none(self):
        """範囲外の馬体重 (200 未満 or 800 超) は None になること"""
        scraper = _make_scraper()
        # 100 kg は範囲外
        rows = [_full_row(1, 5, horse_weight="100")]
        html = _build_html(rows)
        results = scraper._parse_race_mark_table(_soup(html))
        assert results[0]["horse_weight"] is None


# ================================================================
# 追加: 人気・着差・上り3F のエッジケース
# ================================================================

class TestAdditionalFields:
    """人気・着差・上り3F の追加検証"""

    def test_popularity_valid(self):
        """人気が正しく取得されること"""
        scraper = _make_scraper()
        rows = [_full_row(1, 5, popularity="3")]
        html = _build_html(rows)
        results = scraper._parse_race_mark_table(_soup(html))
        assert results[0]["popularity"] == 3

    def test_popularity_out_of_range_returns_none(self):
        """人気が範囲外 (>50) の場合は None になること"""
        scraper = _make_scraper()
        rows = [_full_row(1, 5, popularity="99")]
        html = _build_html(rows)
        results = scraper._parse_race_mark_table(_soup(html))
        assert results[0]["popularity"] is None

    def test_last_3f_valid(self):
        """上り3F が正しく取得されること"""
        scraper = _make_scraper()
        rows = [_full_row(1, 5, last_3f="36.5")]
        html = _build_html(rows)
        results = scraper._parse_race_mark_table(_soup(html))
        assert results[0]["last_3f"] == pytest.approx(36.5)

    def test_last_3f_out_of_range_returns_zero(self):
        """上り3F が範囲外 (30 未満 or 50 超) の場合は 0.0 になること"""
        scraper = _make_scraper()
        rows = [_full_row(1, 5, last_3f="99.9")]
        html = _build_html(rows)
        results = scraper._parse_race_mark_table(_soup(html))
        assert results[0]["last_3f"] == 0.0

    def test_margin_captured(self):
        """着差が正しく取得されること"""
        scraper = _make_scraper()
        rows = [_full_row(2, 5, margin="1/2")]
        html = _build_html(rows)
        results = scraper._parse_race_mark_table(_soup(html))
        assert results[0]["margin"] == "1/2"
