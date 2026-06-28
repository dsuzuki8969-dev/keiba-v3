#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
JRA 馬場 archive PDF 取得スクリプト (P3 Phase A)

JRA公式アーカイブから馬場PDFを逐次ダウンロードし、
parse_baba_pdf() でパースして track_condition_daily.json に累積保存する。

使用例:
  python scripts/fetch_jra_baba_archive.py --year 2024 --venue nakayama
  python scripts/fetch_jra_baba_archive.py --year 2023 2024 2025
  python scripts/fetch_jra_baba_archive.py --year 2024 --out data/masters/track_condition_daily.json

制約:
  - JRA公式のみアクセス (netkeiba には一切接触しない)
  - 各PDFダウンロード後に time.sleep(2.0) 以上待機 (レート制限厳守)
  - 並列DL禁止・逐次処理のみ
  - git commit はしない (Opus が後で行う)
  - src/ 改変禁止・新規は scripts/ と data/ 配下のみ
"""

import argparse
import datetime
import json
import re
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional

import requests

# プロジェクトルートを sys.path に追加して scripts/parse_jra_baba_pdf.py をインポート可能にする
_PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))
sys.path.insert(0, str(_PROJECT_ROOT / "scripts"))

from parse_jra_baba_pdf import parse_baba_pdf
from data.masters.venue_master import get_venue_code

# ---------------------------------------------------------------------------
# 定数
# ---------------------------------------------------------------------------

# JRA公式インデックスURL
_INDEX_URL = "https://www.jra.go.jp/keiba/baba/archive/{year}.html"

# PDFリンクパターン (href から絶対URLを構築するため)
_PDF_HREF_RE = re.compile(
    r'href="(/keiba/baba/archive/\d+pdf/([a-z]+)(\d+)\.pdf)"',
    re.IGNORECASE
)

# JRA公式ベースURL
_JRA_BASE = "https://www.jra.go.jp"

# PDFキャッシュディレクトリ
_PDF_CACHE_DIR = _PROJECT_ROOT / "data" / "baba_pdf_cache"

# デフォルト出力先
_DEFAULT_OUT = _PROJECT_ROOT / "data" / "masters" / "track_condition_daily.json"

# User-Agent (JRA公式アクセス用)
_UA = "Mozilla/5.0 (compatible; D-AIKeiba/3.0; +https://d-aikeiba.example.com)"

_REQUEST_HEADERS = {
    "User-Agent": _UA,
    "Accept": "text/html,application/xhtml+xml,application/xhtml+xml,application/pdf,*/*",
    "Accept-Language": "ja,en;q=0.9",
}

# レート制限: 各PDFダウンロード後に最低2.0秒待機
_RATE_LIMIT_SLEEP = 2.0

# 10場の英名
_ALL_VENUES = [
    "sapporo", "hakodate", "fukushima", "niigata",
    "tokyo", "nakayama", "chukyo", "kyoto", "hanshin", "kokura"
]


# ---------------------------------------------------------------------------
# 2024年以前フォーマット向けフォールバックパーサ
# ---------------------------------------------------------------------------
# JRA馬場PDFには2種類のフォーマットが存在する:
#
# [新フォーマット: 2025-2026年確認]
#  「1行=1測定日」形式: 月 日 曜日 芝コース クッション値 含水率(芝G,芝4,ダG,ダ4)
#  → parse_jra_baba_pdf.py の parse_baba_pdf() で正常パース可
#
# [旧フォーマット: 2024年以前確認]
#  「グループ×横並び日付」形式:
#    第N日・第M日（YYYY年M月D日～D日）
#    金曜日 土曜日 日曜日（...）   ← 曜日ヘッダ
#    芝コースクッション値 V1 V2 V3  ← 値が横並び
#    場所 金曜日 土曜日 ...
#    芝コース含水率 ゴール前 V1 V2 ...
#    （パーセント） ４コーナー V1 V2 ...
#    ダートコース含水率 ゴール前 V1 V2 ...
#    （パーセント） ４コーナー V1 V2 ...
#  → parse_baba_pdf() が行数0を返すため、フォールバックパーサで対応

# 旧フォーマット: ヘッダ行「2024年 第1回中山競馬 含水率・クッション値」
_OLD_HEADER = re.compile(r"(\d{4})年\s*第(\d+)回\s*(\S+?)競馬")

# 旧フォーマット: グループ開始行「第N日・第M日（2024年1月5日～8日）」
_OLD_GROUP_RE = re.compile(
    r"第\d+日.*?（(\d{4})年(\d+)月(\d+)日[^）]*）"
)

# 旧フォーマット: 曜日ヘッダ行「金曜日 土曜日 日曜日（...）」
_OLD_DOW_ROW_RE = re.compile(r"^(?:金曜日|土曜日|日曜日|月曜日|火曜日|水曜日|木曜日)[\s　]+")

# 旧フォーマット: 数値列抽出
_NUMS_RE = re.compile(r"[\d]+\.[\d]+|[\d]+(?=\s|$)")

# 曜日→開始日からの日数オフセット (週明けの月曜まで対応)
_DOW_ORDER = {"金曜日": 0, "土曜日": 1, "日曜日": 2, "月曜日": 3, "火曜日": 4, "水曜日": 5, "木曜日": 6}

# 「第N日・第M日（2024年1月5日～8日）」の開始日: グループの最初の日付が金曜日に対応
# 金曜日が基準(offset=0)なので開始日 = 「月 day」がそのまま offset=0 の日
def _old_format_parse_baba_pdf(pdf_path: str) -> Optional[Dict]:
    """
    旧フォーマット (2024年以前) PDFをパースする。
    parse_baba_pdf() が行数0の場合のフォールバック。
    戻り値は parse_baba_pdf() と同じ {meta, rows} 形式。
    """
    try:
        import pdfplumber
    except ImportError:
        return None

    meta: Dict = {"year": None, "kai": None, "racecourse": None}
    rows: List[Dict] = []

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            txt = page.extract_text() or ""
            lines = txt.split("\n")

            # メタ抽出
            if meta["year"] is None:
                for line in lines:
                    hm = _OLD_HEADER.search(line.replace("　", " "))
                    if hm:
                        meta["year"] = int(hm.group(1))
                        meta["kai"] = int(hm.group(2))
                        meta["racecourse"] = hm.group(3)
                        break

            # グループパース: 状態機械
            i = 0
            while i < len(lines):
                line = lines[i].strip()

                # グループヘッダ検出: 「第N日・第M日（2024年M月D日～D日）」
                gm = _OLD_GROUP_RE.search(line)
                if gm:
                    grp_year = int(gm.group(1))
                    grp_month = int(gm.group(2))
                    grp_day_start = int(gm.group(3))  # 金曜日の日付

                    # 次行: 曜日ヘッダ「金曜日 土曜日 日曜日 ...」
                    i += 1
                    if i >= len(lines):
                        break
                    dow_line = lines[i].strip()

                    # 曜日を順序どおり抽出
                    dows = re.findall(
                        r"金曜日|土曜日|日曜日|月曜日|火曜日|水曜日|木曜日",
                        dow_line
                    )
                    n_days = len(dows)
                    if n_days == 0:
                        continue

                    # 各日の日付を計算 (金曜=offset 0, 土=1, 日=2, 月=3...)
                    base_date = datetime.date(grp_year, grp_month, grp_day_start)
                    # 最初の曜日が金曜日でない場合もあるため、dows[0]からoffset計算
                    first_offset = _DOW_ORDER.get(dows[0], 0)
                    dates = []
                    for j, dow in enumerate(dows):
                        offset = _DOW_ORDER.get(dow, j) - first_offset
                        d = base_date + datetime.timedelta(days=offset)
                        dates.append(d)

                    # 次行: 芝コースクッション値 V1 V2 ...
                    i += 1
                    if i >= len(lines):
                        break
                    cushion_line = lines[i].strip()

                    if "クッション値" not in cushion_line:
                        # 予期しない行: グループスキップ
                        continue

                    cushion_vals = re.findall(r"[\d]+\.[\d]+", cushion_line)

                    # 次行: 「場所 金曜日 ...」(含水率ヘッダ・スキップ)
                    i += 1

                    # 次行: 「芝コース含水率 ゴール前 V1 V2 ...」
                    i += 1
                    if i >= len(lines):
                        break
                    turf_g_line = lines[i].strip()
                    if "ゴール前" not in turf_g_line:
                        continue
                    turf_g_vals = re.findall(r"[\d]+\.[\d]+", turf_g_line)

                    # 次行: 「（パーセント） ４コーナー V1 V2 ...」
                    i += 1
                    if i >= len(lines):
                        break
                    turf_c_line = lines[i].strip()
                    turf_c_vals = re.findall(r"[\d]+\.[\d]+", turf_c_line)

                    # 次行: 「ダートコース含水率 ゴール前 V1 V2 ...」
                    i += 1
                    if i >= len(lines):
                        break
                    dirt_g_line = lines[i].strip()
                    if "ゴール前" not in dirt_g_line:
                        continue
                    dirt_g_vals = re.findall(r"[\d]+\.[\d]+", dirt_g_line)

                    # 次行: 「（パーセント） ４コーナー V1 V2 ...」
                    i += 1
                    if i >= len(lines):
                        break
                    dirt_c_line = lines[i].strip()
                    dirt_c_vals = re.findall(r"[\d]+\.[\d]+", dirt_c_line)

                    # 各日のrowを生成
                    for j in range(n_days):
                        try:
                            cushion = float(cushion_vals[j]) if j < len(cushion_vals) else 0.0
                            tg = float(turf_g_vals[j]) if j < len(turf_g_vals) else 0.0
                            tc = float(turf_c_vals[j]) if j < len(turf_c_vals) else 0.0
                            dg = float(dirt_g_vals[j]) if j < len(dirt_g_vals) else 0.0
                            dc = float(dirt_c_vals[j]) if j < len(dirt_c_vals) else 0.0
                            d = dates[j]
                            rows.append({
                                "month": d.month,
                                "day": d.day,
                                "dow": dows[j],
                                "turf_course": "",  # 旧フォーマットにはコース記号なし
                                "cushion_value": cushion,
                                "moist_turf_goal": tg,
                                "moist_turf_corner": tc,
                                "moist_dirt_goal": dg,
                                "moist_dirt_corner": dc,
                            })
                        except (IndexError, ValueError):
                            pass

                i += 1

    return {"meta": meta, "rows": rows}


# ---------------------------------------------------------------------------
# コア関数
# ---------------------------------------------------------------------------

def fetch_index_html(year: int) -> str:
    """インデックスHTMLをGETして文字列で返す。失敗時は例外。"""
    url = _INDEX_URL.format(year=year)
    print(f"[INFO] インデックスHTMLを取得中: {url}")
    resp = requests.get(url, headers=_REQUEST_HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.text


def extract_pdf_links(html: str, year: int, venue_filter: str | None = None) -> list[dict]:
    """
    HTMLからPDFリンクを抽出する。

    Args:
        html: インデックスHTMLテキスト
        year: 対象年
        venue_filter: 英名で絞り込む場合に指定 (例: 'nakayama')。Noneで全10場。

    Returns:
        [{"href": str, "venue": str, "kai": int, "url": str}, ...]
    """
    results = []
    seen = set()
    for m in _PDF_HREF_RE.finditer(html):
        href = m.group(1)
        venue_en = m.group(2).lower()
        kai_str = m.group(3)

        if href in seen:
            continue
        seen.add(href)

        # venue フィルタ適用
        if venue_filter and venue_en != venue_filter.lower():
            continue

        try:
            kai = int(kai_str)
        except ValueError:
            print(f"[WARN] kai パース失敗: {href} (スキップ)")
            continue

        url = _JRA_BASE + href
        results.append({
            "href": href,
            "venue": venue_en,
            "kai": kai,
            "url": url,
        })

    results.sort(key=lambda x: (x["venue"], x["kai"]))
    return results


def download_pdf(url: str, dest_path: Path, dry_run: bool = False) -> bool:
    """
    PDFをダウンロードして dest_path に保存する。
    既存ファイルがある場合はスキップ (再DL回避)。
    ダウンロード後に _RATE_LIMIT_SLEEP 秒待機。

    Returns:
        True=新規DL, False=キャッシュ利用
    """
    if dest_path.exists():
        print(f"  [CACHE] スキップ (既存): {dest_path.name}")
        return False

    print(f"  [DL] ダウンロード: {url}")
    try:
        resp = requests.get(url, headers=_REQUEST_HEADERS, timeout=60)
        if resp.status_code == 404:
            print(f"  [WARN] 404 Not Found (スキップ): {url}")
            return False
        resp.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(f"  [WARN] HTTPエラー {e} (スキップ): {url}")
        return False
    except requests.exceptions.RequestException as e:
        print(f"  [WARN] 接続エラー {e} (スキップ): {url}")
        return False

    dest_path.parent.mkdir(parents=True, exist_ok=True)
    dest_path.write_bytes(resp.content)
    print(f"  [DL] 保存完了: {dest_path.name} ({len(resp.content):,} bytes)")

    # レート制限: 必ず2.0秒以上待機
    print(f"  [RATE] {_RATE_LIMIT_SLEEP:.1f}秒待機中...")
    time.sleep(_RATE_LIMIT_SLEEP)
    return True


def parse_and_accumulate(
    pdf_path: Path,
    year: int,
    accumulator: dict,
) -> int:
    """
    PDFをパースして accumulator に結果を追加する。

    accumulator 構造:
        {
            "YYYY-MM-DD": {
                "<venue_code>": {
                    "cushion_value": float,
                    "moist_turf_goal": float,
                    "moist_turf_corner": float,
                    "moist_dirt_goal": float,
                    "moist_dirt_corner": float,
                    "turf_course": str,
                }
            }
        }

    Returns:
        パース成功した行数
    """
    try:
        result = parse_baba_pdf(str(pdf_path))
    except Exception as e:
        print(f"  [WARN] parse_baba_pdf() 失敗 {pdf_path.name}: {e}")
        result = {"meta": {}, "rows": []}

    meta = result.get("meta", {})
    rows = result.get("rows", [])

    # 新フォーマット(2025+)でパース失敗 → 旧フォーマット(2024以前)でフォールバック
    if not rows:
        print(f"  [INFO] parse_baba_pdf() 行数0 → 旧フォーマットフォールバック試行: {pdf_path.name}")
        fallback = _old_format_parse_baba_pdf(str(pdf_path))
        if fallback and fallback.get("rows"):
            result = fallback
            meta = result.get("meta", {})
            rows = result.get("rows", [])
            print(f"  [INFO] 旧フォーマットパース成功: {len(rows)} 行")
        else:
            print(f"  [WARN] 旧フォーマットでも行数0: {pdf_path.name} (スキップ)")

    racecourse_ja = meta.get("racecourse")
    result_year = meta.get("year")

    if not racecourse_ja:
        print(f"  [WARN] racecourse が取得できませんでした: {pdf_path.name}")
        return 0

    venue_code = get_venue_code(racecourse_ja)
    if not venue_code:
        print(f"  [WARN] venue_code が見つかりません: '{racecourse_ja}' (スキップ)")
        return 0

    # 年はmetaの値を優先、なければ引数のyearを使用
    use_year = result_year if result_year else year

    print(f"  [PARSE] {pdf_path.name}: 場={racecourse_ja}(code={venue_code}) "
          f"年={use_year} 開催={meta.get('kai')} 行数={len(rows)}")

    for row in rows:
        month = row["month"]
        day = row["day"]
        date_str = f"{use_year:04d}-{month:02d}-{day:02d}"

        entry = {
            "cushion_value": row["cushion_value"],
            "moist_turf_goal": row["moist_turf_goal"],
            "moist_turf_corner": row["moist_turf_corner"],
            "moist_dirt_goal": row["moist_dirt_goal"],
            "moist_dirt_corner": row["moist_dirt_corner"],
            "turf_course": row["turf_course"],
        }

        if date_str not in accumulator:
            accumulator[date_str] = {}

        # 同一date×venueは上書き(最新PDFの値を採用)
        accumulator[date_str][venue_code] = entry

    return len(rows)


def load_existing_json(out_path: Path) -> dict:
    """既存JSONがあればロードして返す。なければ空dictを返す。"""
    if out_path.exists():
        try:
            with open(out_path, encoding="utf-8") as f:
                data = json.load(f)
            print(f"[INFO] 既存JSON読み込み: {out_path} ({len(data)} 日付エントリ)")
            return data
        except Exception as e:
            print(f"[WARN] 既存JSON読み込みエラー ({e})。空dictで開始します。")
    return {}


def save_json(data: dict, out_path: Path) -> None:
    """JSONをソートして保存する。"""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    sorted_data = {k: data[k] for k in sorted(data.keys())}
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(sorted_data, f, ensure_ascii=False, indent=2)
    print(f"[INFO] JSON保存完了: {out_path} ({len(sorted_data)} 日付エントリ)")


# ---------------------------------------------------------------------------
# メイン処理
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="JRA馬場PDFを逐次DLしてtrack_condition_daily.jsonに累積保存する (P3 Phase A)"
    )
    parser.add_argument(
        "--year", type=int, nargs="+", required=True,
        help="取得対象年 (複数指定可。例: --year 2024 または --year 2023 2024 2025)"
    )
    parser.add_argument(
        "--venue", type=str, default=None,
        help=(
            "取得対象場 (英名・任意。例: --venue nakayama)。"
            f"未指定で全10場: {', '.join(_ALL_VENUES)}"
        )
    )
    parser.add_argument(
        "--out", type=str, default=str(_DEFAULT_OUT),
        help=f"出力JSONパス (デフォルト: {_DEFAULT_OUT})"
    )
    args = parser.parse_args()

    out_path = Path(args.out)
    venue_filter = args.venue

    # バリデーション: venue名チェック
    if venue_filter and venue_filter.lower() not in _ALL_VENUES:
        print(f"[ERROR] --venue に無効な値: '{venue_filter}'")
        print(f"  有効な英名: {', '.join(_ALL_VENUES)}")
        sys.exit(1)

    # 既存JSONをロード (複数年/複数場を累積するため)
    accumulator = load_existing_json(out_path)

    total_pdf_count = 0
    total_dl_count = 0
    total_cache_count = 0
    total_row_count = 0

    for year in sorted(set(args.year)):
        print(f"\n{'='*60}")
        print(f"[YEAR] {year}年 処理開始")
        print(f"{'='*60}")

        # インデックスHTMLからPDFリンク抽出
        try:
            html = fetch_index_html(year)
        except requests.exceptions.RequestException as e:
            print(f"[ERROR] インデックスHTML取得失敗 ({year}年): {e} (スキップ)")
            continue

        pdf_links = extract_pdf_links(html, year, venue_filter=venue_filter)

        if not pdf_links:
            print(f"[WARN] {year}年: PDFリンクが見つかりませんでした")
            if venue_filter:
                print(f"  (venue_filter='{venue_filter}' で絞り込み中)")
            continue

        print(f"[INFO] {year}年: {len(pdf_links)} 件のPDFリンクを抽出")

        for i, link in enumerate(pdf_links, start=1):
            venue_en = link["venue"]
            kai = link["kai"]
            url = link["url"]
            basename = f"{venue_en}{kai:02d}.pdf"

            cache_dir = _PDF_CACHE_DIR / str(year)
            dest_path = cache_dir / basename

            print(f"\n[PDF {i}/{len(pdf_links)}] {venue_en} 第{kai}回 ({year}年)")

            # DL or キャッシュ利用
            is_new_dl = download_pdf(url, dest_path)
            if is_new_dl:
                total_dl_count += 1
            else:
                # キャッシュの場合もファイルが存在するか確認
                if not dest_path.exists():
                    # 404等でスキップされた場合
                    continue
                total_cache_count += 1

            # パースして累積
            row_count = parse_and_accumulate(dest_path, year, accumulator)
            total_row_count += row_count
            total_pdf_count += 1

    # 結果保存
    print(f"\n{'='*60}")
    print(f"[SUMMARY] 処理完了")
    print(f"  処理PDF総数  : {total_pdf_count}")
    print(f"  新規DL       : {total_dl_count}")
    print(f"  キャッシュ利用: {total_cache_count}")
    print(f"  パース行数合計: {total_row_count}")
    print(f"  JSONエントリ数: {len(accumulator)} 日付")
    print(f"{'='*60}")

    if accumulator:
        save_json(accumulator, out_path)
    else:
        print("[WARN] データがありません。JSON保存をスキップします。")


if __name__ == "__main__":
    main()
