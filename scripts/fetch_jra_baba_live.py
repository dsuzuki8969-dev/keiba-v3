#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
fetch_jra_baba_live.py — JRA 当日馬場 (含水率・クッション値) ライブスクレイパー

【取得元 URL】
  https://www.jra.go.jp/keiba/baba/_data_cushion.html  (全開催会場のクッション値)
  https://www.jra.go.jp/keiba/baba/_data_moist.html    (全開催会場の含水率)
  https://www.jra.go.jp/keiba/baba/index{N}.html       (使用コース取得・N=''/'2'/'3'...)

【出力形式】(track_condition_daily.json と完全一致)
  {
    "YYYY-MM-DD": {
      "<venue_code>": {
        "cushion_value": 9.7,
        "moist_turf_goal": 10.3,
        "moist_turf_corner": 12.9,
        "moist_dirt_goal": 7.0,
        "moist_dirt_corner": 6.2,
        "turf_course": "A"
      }
    }
  }

【制約】
  - JRA公式のみアクセス (netkeiba 一切不可)
  - fetchは逐次 / time.sleep(2.0) 以上 / 並列禁止
  - 開発・検証はサンプルHTMLで行い、ライブ fetch は最終確認のみ
  - src/ 配下は一切変更しない
  - git commit しない (Opus が後で行う)

使用例:
  python scripts/fetch_jra_baba_live.py --dry-run           # ライブ取得して標準出力のみ
  python scripts/fetch_jra_baba_live.py                     # track_condition_daily.json に書き込む
  python scripts/fetch_jra_baba_live.py --date 2026-06-28   # 日付指定
  python scripts/fetch_jra_baba_live.py --out /tmp/test.json --dry-run
"""

from __future__ import annotations

import argparse
import datetime
import json
import re
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# cp932 クラッシュ対策 (Windows スケジューラ / CLI 経由)
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

import requests
from bs4 import BeautifulSoup

# プロジェクトルートを sys.path に追加
_PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

from data.masters.venue_master import get_venue_code  # noqa: E402

# ---------------------------------------------------------------------------
# 定数
# ---------------------------------------------------------------------------

_JRA_BASE = "https://www.jra.go.jp"

# 全開催会場のクッション値 / 含水率を一括取得する JRA 公式エンドポイント
_URL_CUSHION = f"{_JRA_BASE}/keiba/baba/_data_cushion.html"
_URL_MOIST = f"{_JRA_BASE}/keiba/baba/_data_moist.html"

# 使用コース取得用: index.html / index2.html / index3.html ...
# N='' が会場1、'2' が会場2、以下同様。404 または会場名が空で打ち切り
_URL_INDEX_TMPL = f"{_JRA_BASE}/keiba/baba/index{{suffix}}.html"

# 最大開催会場数 (通常 JRA は最大 3 場同時開催)
_MAX_VENUES = 5

# レート制限: fetch 間に最低 2.0 秒待機
_RATE_LIMIT_SLEEP = 2.0

# デフォルト出力先
_DEFAULT_OUT = _PROJECT_ROOT / "data" / "masters" / "track_condition_daily.json"

# User-Agent
_UA = "Mozilla/5.0 (compatible; D-AIKeiba/3.0; +https://d-aikeiba.example.com)"
_HEADERS = {
    "User-Agent": _UA,
    "Accept": "text/html,*/*",
    "Accept-Language": "ja,en;q=0.9",
    "Referer": f"{_JRA_BASE}/keiba/baba/",
}

# 日付文字列パターン: 「6月28日（日曜）」
_DATE_RE = re.compile(r"(\d+)月(\d+)日")

# ---------------------------------------------------------------------------
# HTTP ユーティリティ
# ---------------------------------------------------------------------------


def _get_html(url: str, encoding: str = "shift_jis") -> Optional[str]:
    """
    GET リクエストを送り Shift_JIS でデコードして返す。
    404/403 はコース数打ち切り判定用に None を返す (ログは INFO)。
    その他エラーも None を返す (ログは WARN)。
    """
    print(f"[INFO] GET {url}")
    try:
        r = requests.get(url, headers=_HEADERS, timeout=20)
        if r.status_code in (404, 403):
            print(f"  [INFO] {r.status_code} (開催なし/存在しない): {url}")
            return None
        r.raise_for_status()
        # JRA 公式は Shift_JIS だが、サンプルHTMLは UTF-8 保存済のためフォールバック
        try:
            return r.content.decode(encoding, errors="replace")
        except Exception:
            return r.text
    except requests.exceptions.RequestException as e:
        print(f"  [WARN] HTTP エラー ({url}): {e}")
        return None


def _parse_html_from_file(path: Path) -> Optional[BeautifulSoup]:
    """ローカルサンプルHTMLを UTF-8 で読み込んで BeautifulSoup を返す。"""
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
        return BeautifulSoup(text, "html.parser")
    except Exception as e:
        print(f"  [WARN] ファイル読み込みエラー ({path}): {e}")
        return None


def _parse_html_from_string(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "html.parser")


# ---------------------------------------------------------------------------
# パーサ: _data_cushion.html
# ---------------------------------------------------------------------------

def parse_cushion_html(html: str) -> Dict[str, float]:
    """
    _data_cushion.html をパースして {venue_ja: cushion_value} を返す。

    HTML 構造 (例):
      <div id="cushion_data_list">
        <div id="rcA" title="福島">
          <div class="unit">
            <div class="time">6月28日（日曜）7時00分</div>
            <div class="cushion">9.7</div>
          </div>
          ...
        </div>
        <div id="rcB" title="小倉">
          ...
        </div>
      </div>

    先頭の <div class="unit"> が当日の最新測定値。
    """
    soup = _parse_html_from_string(html)
    result: Dict[str, float] = {}

    container = soup.find("div", id="cushion_data_list")
    if not container:
        print("  [WARN] parse_cushion_html: #cushion_data_list が見つかりません")
        return result

    # 各会場ブロック: id="rcA", "rcB", ... / title="福島", "小倉", ...
    for venue_div in container.find_all("div", id=re.compile(r"^rc[A-Z]$")):
        venue_ja = venue_div.get("title", "").strip()
        if not venue_ja:
            continue

        # 最初の unit = 最新測定値
        first_unit = venue_div.find("div", class_="unit")
        if not first_unit:
            print(f"  [WARN] 会場 {venue_ja}: unit が見つかりません")
            continue

        cushion_div = first_unit.find("div", class_="cushion")
        if not cushion_div:
            print(f"  [WARN] 会場 {venue_ja}: cushion div が見つかりません")
            continue

        try:
            cushion_val = float(cushion_div.get_text(strip=True))
        except ValueError as e:
            print(f"  [WARN] 会場 {venue_ja}: クッション値パース失敗: {e}")
            continue

        # 測定時刻ログ
        time_div = first_unit.find("div", class_="time")
        time_str = time_div.get_text(strip=True) if time_div else "不明"
        print(f"  [CUSHION] {venue_ja}: {cushion_val} ({time_str})")
        result[venue_ja] = cushion_val

    return result


# ---------------------------------------------------------------------------
# パーサ: _data_moist.html
# ---------------------------------------------------------------------------

def parse_moist_html(html: str) -> Dict[str, Dict[str, float]]:
    """
    _data_moist.html をパースして
    {venue_ja: {moist_turf_goal, moist_turf_corner, moist_dirt_goal, moist_dirt_corner}} を返す。

    HTML 構造 (例):
      <div id="rcA" title="福島">
        <div class="unit">
          <div class="time">6月28日（日曜）6時00分</div>
          <div class="turf">
            <span class="mg" data-condition="hard">10.3</span>   <!-- ゴール前 -->
            <span class="m4c" data-condition="hard">12.9</span>  <!-- 4コーナー -->
          </div>
          <div class="dirt">
            <span class="mg" data-condition="hard">7.0</span>
            <span class="m4c" data-condition="hard">6.2</span>
          </div>
        </div>
        ...
      </div>

    mg = goal (ゴール前), m4c = corner (4コーナー)
    """
    soup = _parse_html_from_string(html)
    result: Dict[str, Dict[str, float]] = {}

    container = soup.find("div", id="moist_data_list")
    if not container:
        print("  [WARN] parse_moist_html: #moist_data_list が見つかりません")
        return result

    for venue_div in container.find_all("div", id=re.compile(r"^rc[A-Z]$")):
        venue_ja = venue_div.get("title", "").strip()
        if not venue_ja:
            continue

        first_unit = venue_div.find("div", class_="unit")
        if not first_unit:
            print(f"  [WARN] 会場 {venue_ja}: moist unit が見つかりません")
            continue

        turf_div = first_unit.find("div", class_="turf")
        dirt_div = first_unit.find("div", class_="dirt")

        if not turf_div or not dirt_div:
            print(f"  [WARN] 会場 {venue_ja}: turf/dirt div が見つかりません")
            continue

        def _get_val(parent, cls: str) -> Optional[float]:
            span = parent.find("span", class_=cls)
            if not span:
                return None
            try:
                return float(span.get_text(strip=True))
            except ValueError:
                return None

        tg = _get_val(turf_div, "mg")    # 芝ゴール前
        tc = _get_val(turf_div, "m4c")   # 芝4コーナー
        dg = _get_val(dirt_div, "mg")    # ダートゴール前
        dc = _get_val(dirt_div, "m4c")   # ダート4コーナー

        if any(v is None for v in [tg, tc, dg, dc]):
            print(f"  [WARN] 会場 {venue_ja}: 含水率の一部取得失敗: tg={tg} tc={tc} dg={dg} dc={dc}")
            continue

        time_div = first_unit.find("div", class_="time")
        time_str = time_div.get_text(strip=True) if time_div else "不明"
        print(f"  [MOIST] {venue_ja}: 芝G={tg} 芝4C={tc} ダートG={dg} ダート4C={dc} ({time_str})")

        result[venue_ja] = {
            "moist_turf_goal": tg,
            "moist_turf_corner": tc,
            "moist_dirt_goal": dg,
            "moist_dirt_corner": dc,
        }

    return result


# ---------------------------------------------------------------------------
# パーサ: index{N}.html → 馬場状態 (新規追加)
# ---------------------------------------------------------------------------

def parse_index_html_for_condition(html: str) -> dict:
    """
    index{N}.html から公式馬場状態・天候・時刻を抽出する。

    対象セクション:
      <div class="line condition" id="course_condition" ...>
        <div class="main"><h3>馬場状態<span class="time">（6月26日（金曜）正午現在）</span></h3>
        <div class="grid">
          <div class="cell weather">...<strong>曇</strong>...
          <div class="cell turf"><ul>...<p>稍重</p>...
          <div class="cell dirt"><ul>...<p>稍重</p>...

    Returns:
        {
          "condition_turf": str | None,   # 芝馬場状態 例: "稍重" / None (芝コース無)
          "condition_dirt": str | None,   # ダート馬場状態 例: "良" / None
          "condition_time": str | None,   # 時刻テキスト 例: "6月26日（金曜）正午現在"
          "weather": str | None,          # 天候 例: "曇" / "雨"
        }
    要素欠落時は None (堅牢に)。
    """
    soup = _parse_html_from_string(html)
    result: dict = {
        "condition_turf": None,
        "condition_dirt": None,
        "condition_time": None,
        "weather": None,
    }

    # 馬場状態セクション取得
    section = soup.find("div", id="course_condition")
    if not section:
        return result

    # 時刻: .main h3 span.time の全角括弧を除去
    time_span = section.select_one(".main h3 span.time")
    if time_span:
        raw_time = time_span.get_text(strip=True)
        # 全角括弧「（）」を外側のみ除去 (removeprefix/suffix で内側の（金曜）を保護)
        result["condition_time"] = raw_time.removeprefix("（").removesuffix("）")

    # 天候: .cell.weather strong
    weather_strong = section.select_one(".cell.weather strong")
    if weather_strong:
        result["weather"] = weather_strong.get_text(strip=True) or None

    # 芝: .cell.turf .content p
    turf_p = section.select_one(".cell.turf .content p")
    if turf_p:
        result["condition_turf"] = turf_p.get_text(strip=True) or None

    # ダート: .cell.dirt .content p
    dirt_p = section.select_one(".cell.dirt .content p")
    if dirt_p:
        result["condition_dirt"] = dirt_p.get_text(strip=True) or None

    return result


# ---------------------------------------------------------------------------
# パーサ: index{N}.html → 使用コース
# ---------------------------------------------------------------------------

def parse_index_html_for_course(html: str) -> Tuple[Optional[str], Optional[str]]:
    """
    index{N}.html から会場名と使用コースを抽出する。

    会場名: <title>馬場情報（福島競馬場）　JRA</title>
    使用コース: <div id="baba" data-current-course="A">

    Returns:
        (venue_ja, turf_course) または (None, None)
    """
    soup = _parse_html_from_string(html)

    # 会場名抽出: title タグ「馬場情報（XXX競馬場）　JRA」
    venue_ja: Optional[str] = None
    title_tag = soup.find("title")
    if title_tag:
        m = re.search(r"馬場情報（(.+?)競馬場）", title_tag.get_text())
        if m:
            venue_ja = m.group(1)

    if not venue_ja:
        print("  [WARN] index.html: 会場名を title タグから取得できませんでした")
        return None, None

    # 使用コース抽出: <div id="baba" data-current-course="A">
    turf_course: Optional[str] = None
    baba_div = soup.find("div", id="baba")
    if baba_div:
        course_attr = baba_div.get("data-current-course", "").strip()
        if course_attr:
            turf_course = course_attr

    print(f"  [INDEX] 会場={venue_ja} 使用コース={turf_course}")
    return venue_ja, turf_course


# ---------------------------------------------------------------------------
# ライブ取得: 使用コース
# ---------------------------------------------------------------------------

def fetch_turf_courses_live() -> Tuple[Dict[str, Optional[str]], Dict[str, dict]]:
    """
    index.html / index2.html / ... をGETして (course_map, condition_map) を返す。

    course_map:    {venue_ja: turf_course}    (既存互換)
    condition_map: {venue_ja: {condition_turf, condition_dirt, condition_time, weather}}
    404 または会場名が空になった時点で打ち切り。
    """
    course_result: Dict[str, Optional[str]] = {}
    cond_result: Dict[str, dict] = {}
    suffixes = ["", "2", "3", "4", "5"]  # 通常 3 場まで

    for i, suffix in enumerate(suffixes):
        url = _URL_INDEX_TMPL.format(suffix=suffix)
        html = _get_html(url)

        if html is None:
            # 404 = これ以上の会場なし
            print(f"  [INFO] index{suffix}.html: 取得失敗 → 打ち切り")
            break

        venue_ja, turf_course = parse_index_html_for_course(html)

        if not venue_ja:
            print(f"  [INFO] index{suffix}.html: 会場名なし → 打ち切り")
            break

        course_result[venue_ja] = turf_course

        # 馬場状態も抽出
        cond = parse_index_html_for_condition(html)
        cond_result[venue_ja] = cond
        print(
            f"  [COND] {venue_ja}: 芝={cond['condition_turf']} "
            f"ダート={cond['condition_dirt']} 天候={cond['weather']} "
            f"時刻={cond['condition_time']}"
        )

        # レート制限 (最後のループは待機不要)
        if i < len(suffixes) - 1:
            print(f"  [RATE] {_RATE_LIMIT_SLEEP:.1f} 秒待機中...")
            time.sleep(_RATE_LIMIT_SLEEP)

    return course_result, cond_result


# ---------------------------------------------------------------------------
# サンプルHTMLからの使用コース取得 (開発・テスト用)
# ---------------------------------------------------------------------------

def fetch_turf_courses_from_samples(
    sample_dir: Path,
) -> Tuple[Dict[str, Optional[str]], Dict[str, dict]]:
    """
    data/_diag/baba_live_sample/index{N}.html からコースと馬場状態を取得する (テスト用)。

    Returns:
        (course_map, condition_map)
        course_map:    {venue_ja: turf_course}    (既存互換)
        condition_map: {venue_ja: {condition_turf, condition_dirt, condition_time, weather}}
    """
    course_result: Dict[str, Optional[str]] = {}
    cond_result: Dict[str, dict] = {}
    for suffix in ["1", "2", "3", "4", "5"]:
        path = sample_dir / f"index{suffix}.html"
        if not path.exists():
            break
        html = path.read_text(encoding="utf-8", errors="replace")
        venue_ja, turf_course = parse_index_html_for_course(html)
        if not venue_ja:
            break
        course_result[venue_ja] = turf_course

        # 馬場状態も抽出
        cond = parse_index_html_for_condition(html)
        cond_result[venue_ja] = cond
        print(
            f"  [COND] {venue_ja}: 芝={cond['condition_turf']} "
            f"ダート={cond['condition_dirt']} 天候={cond['weather']} "
            f"時刻={cond['condition_time']}"
        )

    return course_result, cond_result


# ---------------------------------------------------------------------------
# データ統合
# ---------------------------------------------------------------------------

def build_result(
    date_str: str,
    cushion_map: Dict[str, float],
    moist_map: Dict[str, Dict[str, float]],
    course_map: Dict[str, Optional[str]],
    condition_map: Optional[Dict[str, dict]] = None,
) -> Dict:
    """
    クッション値・含水率・使用コース・馬場状態を統合して
    track_condition_daily.json 形式のエントリを作る。

    cushion_map:    {venue_ja: cushion_value}
    moist_map:      {venue_ja: {moist_turf_goal, ...}}
    course_map:     {venue_ja: turf_course}
    condition_map:  {venue_ja: {condition_turf, condition_dirt, condition_time, weather}}
                    None の場合は馬場状態フィールドを追加しない (後方互換)

    Returns:
        { "YYYY-MM-DD": { "<venue_code>": {...} } }
    """
    if condition_map is None:
        condition_map = {}

    venue_names = set(cushion_map.keys()) | set(moist_map.keys())
    date_entry: Dict = {}

    for venue_ja in sorted(venue_names):
        venue_code = get_venue_code(venue_ja)
        if not venue_code:
            print(f"  [WARN] venue_code 未定義: '{venue_ja}' (スキップ)")
            continue

        cushion = cushion_map.get(venue_ja)
        moist = moist_map.get(venue_ja, {})
        turf_course = course_map.get(venue_ja)
        cond = condition_map.get(venue_ja, {})

        if cushion is None:
            print(f"  [WARN] {venue_ja}: クッション値なし (スキップ)")
            continue
        if not moist:
            print(f"  [WARN] {venue_ja}: 含水率なし (スキップ)")
            continue

        entry = {
            "cushion_value": cushion,
            "moist_turf_goal": moist["moist_turf_goal"],
            "moist_turf_corner": moist["moist_turf_corner"],
            "moist_dirt_goal": moist["moist_dirt_goal"],
            "moist_dirt_corner": moist["moist_dirt_corner"],
            "turf_course": turf_course if turf_course else "",
            # 馬場状態 (index{N}.html から取得。取得できない場合は None)
            "condition_turf": cond.get("condition_turf"),
            "condition_dirt": cond.get("condition_dirt"),
            "condition_time": cond.get("condition_time"),
            "weather": cond.get("weather"),
        }
        date_entry[venue_code] = entry
        print(
            f"  [MERGE] {venue_ja}(code={venue_code}): "
            f"cushion={cushion} moist_tg={moist['moist_turf_goal']} "
            f"moist_tc={moist['moist_turf_corner']} "
            f"moist_dg={moist['moist_dirt_goal']} "
            f"moist_dc={moist['moist_dirt_corner']} "
            f"course={turf_course} "
            f"cond_turf={cond.get('condition_turf')} "
            f"cond_dirt={cond.get('condition_dirt')} "
            f"weather={cond.get('weather')}"
        )

    return {date_str: date_entry}


# ---------------------------------------------------------------------------
# JSON I/O
# ---------------------------------------------------------------------------

def load_existing_json(out_path: Path) -> Dict:
    """既存 JSON ロード (なければ空 dict)。"""
    if not out_path.exists():
        return {}
    try:
        with open(out_path, encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"[WARN] 既存 JSON 読み込みエラー ({e}) → 空で開始")
        return {}


def save_json(data: Dict, out_path: Path) -> None:
    """ソートして JSON 保存。"""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    sorted_data = {k: data[k] for k in sorted(data.keys())}
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(sorted_data, f, ensure_ascii=False, indent=2)
    print(f"[INFO] 保存完了: {out_path} ({len(sorted_data)} 日付エントリ)")


# ---------------------------------------------------------------------------
# メイン処理
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="JRA 当日馬場 (含水率・クッション値) をライブ取得して JSON に保存する"
    )
    parser.add_argument(
        "--date", type=str, default=None,
        help="対象日付 YYYY-MM-DD (省略時 today)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="JSON に書かず標準出力のみ"
    )
    parser.add_argument(
        "--out", type=str, default=str(_DEFAULT_OUT),
        help=f"出力 JSON パス (デフォルト: {_DEFAULT_OUT})"
    )
    parser.add_argument(
        "--sample", action="store_true",
        help=(
            "サンプルHTMLで動作検証 (JRA にアクセスしない)。"
            "data/_diag/baba_live_sample/ の index*.html / _data_cushion.html / _data_moist.html を使用"
        )
    )
    args = parser.parse_args()

    # 日付決定
    if args.date:
        try:
            target_date = datetime.date.fromisoformat(args.date)
        except ValueError:
            print(f"[ERROR] --date の形式が不正です: {args.date} (YYYY-MM-DD 形式で指定)")
            sys.exit(1)
    else:
        target_date = datetime.date.today()

    date_str = target_date.strftime("%Y-%m-%d")
    out_path = Path(args.out)

    print(f"[START] JRA 当日馬場ライブ取得: 日付={date_str}")
    print(f"  dry-run={args.dry_run}, out={out_path}, sample={args.sample}")
    print()

    # ---- サンプルモード ----
    if args.sample:
        sample_dir = _PROJECT_ROOT / "data" / "_diag" / "baba_live_sample"
        print(f"[SAMPLE] サンプルHTMLディレクトリ: {sample_dir}")

        # クッション値
        cushion_path = sample_dir / "_data_cushion.html"
        if not cushion_path.exists():
            print(f"[ERROR] サンプルファイルが存在しません: {cushion_path}")
            sys.exit(1)
        cushion_html = cushion_path.read_text(encoding="utf-8", errors="replace")

        # 含水率
        moist_path = sample_dir / "_data_moist.html"
        if not moist_path.exists():
            print(f"[ERROR] サンプルファイルが存在しません: {moist_path}")
            sys.exit(1)
        moist_html = moist_path.read_text(encoding="utf-8", errors="replace")

        # 使用コース + 馬場状態 (index1.html / index2.html / index3.html)
        course_map, condition_map = fetch_turf_courses_from_samples(sample_dir)

    # ---- ライブモード ----
    else:
        # クッション値取得
        print("[STEP 1/3] クッション値取得")
        cushion_html = _get_html(_URL_CUSHION)
        if not cushion_html:
            print("[ERROR] クッション値データの取得に失敗しました")
            sys.exit(1)
        print(f"  [RATE] {_RATE_LIMIT_SLEEP:.1f} 秒待機中...")
        time.sleep(_RATE_LIMIT_SLEEP)

        # 含水率取得
        print("[STEP 2/3] 含水率取得")
        moist_html = _get_html(_URL_MOIST)
        if not moist_html:
            print("[ERROR] 含水率データの取得に失敗しました")
            sys.exit(1)
        print(f"  [RATE] {_RATE_LIMIT_SLEEP:.1f} 秒待機中...")
        time.sleep(_RATE_LIMIT_SLEEP)

        # 使用コース + 馬場状態取得 (index.html / index2.html / ...)
        print("[STEP 3/3] 使用コース・馬場状態取得 (index.html 系)")
        course_map, condition_map = fetch_turf_courses_live()

    print()
    print("[PARSE] クッション値パース中...")
    cushion_map = parse_cushion_html(cushion_html)

    print()
    print("[PARSE] 含水率パース中...")
    moist_map = parse_moist_html(moist_html)

    print()
    print("[PARSE] 使用コース結果:")
    for vja, tc in course_map.items():
        print(f"  {vja}: コース={tc}")

    print()
    print("[MERGE] データ統合中...")
    result = build_result(date_str, cushion_map, moist_map, course_map, condition_map)

    # サマリー表示
    print()
    print("[SUMMARY] 抽出結果:")
    day_entry = result.get(date_str, {})
    if not day_entry:
        print("  (データなし)")
    else:
        for code, entry in sorted(day_entry.items()):
            from data.masters.venue_master import get_venue_name
            vname = get_venue_name(code) or "?"
            print(
                f"  {vname}(code={code}): "
                f"cushion={entry['cushion_value']} "
                f"芝G={entry['moist_turf_goal']} 芝4C={entry['moist_turf_corner']} "
                f"ダートG={entry['moist_dirt_goal']} ダート4C={entry['moist_dirt_corner']} "
                f"コース={entry['turf_course']} "
                f"芝状態={entry.get('condition_turf')} "
                f"ダート状態={entry.get('condition_dirt')} "
                f"天候={entry.get('weather')}"
            )

    # dry-run: JSON 出力のみ
    if args.dry_run:
        print()
        print("[DRY-RUN] JSON 出力 (ファイル書き込みなし):")
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return

    # JSON に書き込む
    print()
    existing = load_existing_json(out_path)
    # 既存 date_str エントリに上書きマージ (既存会場を保持しつつ今回分で上書き)
    if date_str in existing:
        existing[date_str].update(day_entry)
    else:
        existing[date_str] = day_entry
    save_json(existing, out_path)
    print("[DONE] 完了")


if __name__ == "__main__":
    main()
