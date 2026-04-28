"""
開催カレンダービルダー — JRA + NAR 統合
scripts/build_kaisai_calendar.py

netkeiba.com のカレンダーページから JRA・NAR 各月の開催日 × 会場を取得し、
data/masters/kaisai_calendar.json に統合マスタとして保存する。

使用方法:
  python scripts/build_kaisai_calendar.py --start 2022 --end 2026
  python scripts/build_kaisai_calendar.py --year 2026        # 単年
  python scripts/build_kaisai_calendar.py --year 2026 --month 1  # 単月テスト
  python scripts/build_kaisai_calendar.py --refresh           # キャッシュ無視で再取得
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Optional

# Windows console の cp932 問題対策
if sys.stdout.encoding and sys.stdout.encoding.lower() in ("cp932", "mbcs"):
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

try:
    import requests
    from bs4 import BeautifulSoup
    HAS_DEPS = True
except ImportError:
    HAS_DEPS = False
    print("ERROR: requests と beautifulsoup4 が必要です: pip install requests beautifulsoup4 lxml")
    sys.exit(1)

try:
    import lz4.frame as _lz4
    HAS_LZ4 = True
except ImportError:
    HAS_LZ4 = False

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False

# ============================================================
# パス設定
# ============================================================

# プロジェクトルートを sys.path に追加
_SCRIPT_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT = _SCRIPT_DIR.parent
sys.path.insert(0, str(_PROJECT_ROOT))

_CACHE_DIR = _PROJECT_ROOT / "data" / "cache"
_MASTERS_DIR = _PROJECT_ROOT / "data" / "masters"
_OUTPUT_FILE = _MASTERS_DIR / "kaisai_calendar.json"

_CACHE_DIR.mkdir(parents=True, exist_ok=True)
_MASTERS_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================
# 定数
# ============================================================

NAR_CALENDAR_URL = "https://nar.netkeiba.com/top/calendar.html"
JRA_CALENDAR_URL = "https://race.netkeiba.com/top/calendar.html"

REQUEST_INTERVAL = 1.2  # 秒（礼儀あるスクレイピング）
MAX_RETRY = 3           # 最大リトライ回数
RETRY_WAIT = 3.0        # リトライ間隔（秒）

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ja-JP,ja;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# JRA 会場コード → 正式名称（参照用）
JRA_VENUES = {
    "札幌", "函館", "福島", "新潟", "東京", "中山", "中京", "京都", "阪神", "小倉"
}

# NAR 会場名（JyoName で出現するテキスト → 正規化）
_NAR_NORMALIZE = {
    "帯広ば": "帯広",  # 「帯広ばんえい」の省略形
    "帯広ばんえい": "帯広",
}

# ============================================================
# キャッシュ操作
# ============================================================

def _cache_path(kind: str, year: int, month: int) -> Path:
    """キャッシュファイルパスを返す"""
    return _CACHE_DIR / f"netkeiba_calendar_{kind}_{year}_{month:02d}.html"


def _read_cache(path: Path) -> Optional[str]:
    """lz4 → plain HTML の順でキャッシュを読む"""
    lz4_path = Path(str(path) + ".lz4")
    if HAS_LZ4 and lz4_path.exists():
        try:
            with open(lz4_path, "rb") as f:
                return _lz4.decompress(f.read()).decode("euc-jp", errors="replace")
        except Exception:
            pass
    if path.exists():
        try:
            with open(path, "r", encoding="euc-jp", errors="replace") as f:
                return f.read()
        except Exception:
            pass
    return None


def _write_cache(path: Path, content: bytes) -> None:
    """lz4 が利用可能なら圧縮保存、なければ plain 保存（bytes をそのまま）"""
    if HAS_LZ4:
        lz4_path = Path(str(path) + ".lz4")
        with open(lz4_path, "wb") as f:
            f.write(_lz4.compress(content))
    else:
        with open(path, "wb") as f:
            f.write(content)

# ============================================================
# HTTP 取得（レート制限 + リトライ付き）
# ============================================================

_session = requests.Session()
_session.headers.update(HEADERS)
_last_request_time: float = 0.0


def _fetch_html(url: str, params: dict, kind: str) -> Optional[bytes]:
    """URL を取得して raw bytes を返す。失敗時は None。"""
    global _last_request_time

    # レート制限
    elapsed = time.time() - _last_request_time
    if elapsed < REQUEST_INTERVAL:
        time.sleep(REQUEST_INTERVAL - elapsed)

    headers = dict(HEADERS)
    if "nar.netkeiba.com" in url:
        headers["Referer"] = "https://nar.netkeiba.com/"
    else:
        headers["Referer"] = "https://race.netkeiba.com/"

    for attempt in range(1, MAX_RETRY + 1):
        try:
            _last_request_time = time.time()
            resp = _session.get(url, params=params, headers=headers, timeout=15)

            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", 60))
                print(f"  [WARN] HTTP 429 - {retry_after}秒 cooldown後リトライ (試行{attempt}/{MAX_RETRY})")
                time.sleep(retry_after)
                continue

            if resp.status_code == 503:
                print(f"  [WARN] HTTP 503 - 60秒 cooldown後リトライ (試行{attempt}/{MAX_RETRY})")
                time.sleep(60)
                continue

            resp.raise_for_status()
            return resp.content

        except requests.RequestException as e:
            print(f"  [WARN] 取得失敗 (試行{attempt}/{MAX_RETRY}): {e}")
            if attempt < MAX_RETRY:
                time.sleep(RETRY_WAIT * attempt)

    return None

# ============================================================
# NAR カレンダーパーサ
# ============================================================

def fetch_nar_calendar(year: int, month: int, refresh: bool = False) -> Dict[str, List[str]]:
    """
    NAR カレンダーを取得し {日付文字列: [会場名, ...]} を返す。

    HTML 構造:
      <td class="RaceCellBox HaveData">
        <div class="RaceKaisaiBox">
          <p><span class="Day">1</span></p>
          <div class="kaisai_1">
            <a href="...kaisai_date=20260101..."><span class="JyoName">川崎</span></a>
          </div>
          ...
        </div>
      </td>
    """
    cache_p = _cache_path("nar", year, month)
    html_bytes: Optional[bytes] = None

    # キャッシュ確認（refresh=False のとき）
    if not refresh:
        cached = _read_cache(cache_p)
        if cached is not None:
            html_bytes = cached.encode("euc-jp", errors="replace")
            # デコード済みとして扱う
            text = cached
        else:
            html_bytes = None

    if html_bytes is None or refresh:
        raw = _fetch_html(NAR_CALENDAR_URL, {"year": year, "month": month}, "nar")
        if raw is None:
            print(f"  [SKIP] NAR {year}-{month:02d}: 取得失敗（推定埋め禁止）")
            return {}
        _write_cache(cache_p, raw)
        text = raw.decode("euc-jp", errors="replace")

    soup = BeautifulSoup(text, "html.parser")
    result: Dict[str, List[str]] = {}

    for td in soup.select("td.RaceCellBox.HaveData"):
        day_span = td.select_one("span.Day")
        if not day_span:
            continue
        day_text = day_span.get_text(strip=True)
        if not day_text.isdigit():
            continue
        day = int(day_text)
        date_str = f"{year}-{month:02d}-{day:02d}"

        venues = []
        for jyo_span in td.select("span.JyoName"):
            raw_name = jyo_span.get_text(strip=True)
            # 正規化（帯広ば → 帯広 など）
            name = _NAR_NORMALIZE.get(raw_name, raw_name)
            if name and name not in venues:
                venues.append(name)

        if venues:
            result[date_str] = venues

    return result

# ============================================================
# JRA カレンダーパーサ
# ============================================================

def fetch_jra_calendar(year: int, month: int, refresh: bool = False) -> Dict[str, List[str]]:
    """
    JRA カレンダーを取得し {日付文字列: [会場名, ...]} を返す。

    HTML 構造（NAR と異なる点）:
      <td class="RaceCellBox">
        <a href="...kaisai_date=20260104...">
          <div class="RaceKaisaiBox HaveData">
            <p><span class="Day">4</span></p>
            <p><span class="JyoName">中山</span>...</p>
          </div>
        </a>
      </td>

    開催なし:
      <td class="RaceCellBox">
        <div class="RaceKaisaiBox">   ← HaveData クラスなし
          <p><span class="Day">1</span></p>
        </div>
      </td>
    """
    cache_p = _cache_path("jra", year, month)
    html_bytes: Optional[bytes] = None
    text: Optional[str] = None

    if not refresh:
        cached = _read_cache(cache_p)
        if cached is not None:
            text = cached

    if text is None:
        raw = _fetch_html(JRA_CALENDAR_URL, {"year": year, "month": month}, "jra")
        if raw is None:
            print(f"  [SKIP] JRA {year}-{month:02d}: 取得失敗（推定埋め禁止）")
            return {}
        _write_cache(cache_p, raw)
        text = raw.decode("euc-jp", errors="replace")

    soup = BeautifulSoup(text, "html.parser")
    result: Dict[str, List[str]] = {}

    # JRA: HaveData は td 内の a タグ配下の div に付く
    for td in soup.select("td.RaceCellBox"):
        hd = td.select_one("div.RaceKaisaiBox.HaveData")
        if not hd:
            continue
        day_span = hd.select_one("span.Day")
        if not day_span:
            continue
        day_text = day_span.get_text(strip=True)
        if not day_text.isdigit():
            continue
        day = int(day_text)
        date_str = f"{year}-{month:02d}-{day:02d}"

        venues = []
        for jyo_span in hd.select("span.JyoName"):
            name = jyo_span.get_text(strip=True)
            if name and name not in venues:
                venues.append(name)

        if venues:
            result[date_str] = venues

    return result

# ============================================================
# 全期間取得 + 統合
# ============================================================

def build_calendar(
    start_year: int,
    end_year: int,
    start_month: int = 1,
    end_month: int = 12,
    refresh: bool = False,
) -> dict:
    """
    指定期間の JRA + NAR カレンダーを取得し、統合 dict を構築して返す。

    戻り値:
    {
        "version": "1.0",
        "generated_at": "...",
        "period": {"start": "...", "end": "..."},
        "source": "netkeiba.com calendar",
        "stats": {...},
        "days": {
            "2026-01-01": {"jra": [], "nar": ["川崎", "名古屋", "高知"]},
            ...
        }
    }
    """
    # 対象 (year, month) のリスト生成
    months_to_fetch = []
    for y in range(start_year, end_year + 1):
        m_start = start_month if y == start_year else 1
        m_end = end_month if y == end_year else 12
        for m in range(m_start, m_end + 1):
            months_to_fetch.append((y, m))

    total = len(months_to_fetch)
    print(f"対象: {total} ヶ月 × 2 (JRA + NAR) = {total * 2} リクエスト")

    # days 集約辞書
    days: Dict[str, Dict[str, List[str]]] = {}

    # 統計用カウンタ
    jra_skip_count = 0
    nar_skip_count = 0

    # イテレータ（tqdm があれば表示）
    iterator = months_to_fetch
    if HAS_TQDM:
        iterator = tqdm(months_to_fetch, desc="カレンダー取得", unit="ヶ月")

    for y, m in iterator:
        label = f"{y}-{m:02d}"
        if HAS_TQDM:
            iterator.set_postfix({"月": label})  # type: ignore
        else:
            pct = months_to_fetch.index((y, m)) / total * 100
            print(f"[{'█' * int(pct / 5):<20}] {pct:4.0f}% — {label}", flush=True)

        # JRA 取得
        jra_data = fetch_jra_calendar(y, m, refresh=refresh)
        if not jra_data and y >= 2022:
            jra_skip_count += 1

        # NAR 取得
        nar_data = fetch_nar_calendar(y, m, refresh=refresh)
        if not nar_data and y >= 2022:
            nar_skip_count += 1

        # 統合: JRA 分
        for date_str, venues in jra_data.items():
            if date_str not in days:
                days[date_str] = {"jra": [], "nar": []}
            days[date_str]["jra"] = venues

        # 統合: NAR 分
        for date_str, venues in nar_data.items():
            if date_str not in days:
                days[date_str] = {"jra": [], "nar": []}
            days[date_str]["nar"] = venues

    # 期間内の全日付について "jra"/"nar" キーが必ずあるよう補完
    start_date = date(start_year, start_month, 1)
    end_date = date(end_year, end_month, 31 if end_month == 12 else end_month * 30)
    # 実際の末日を使う
    import calendar as _cal
    last_day = _cal.monthrange(end_year, end_month)[1]
    end_date = date(end_year, end_month, last_day)

    current = start_date
    while current <= end_date:
        ds = current.strftime("%Y-%m-%d")
        if ds not in days:
            days[ds] = {"jra": [], "nar": []}
        else:
            if "jra" not in days[ds]:
                days[ds]["jra"] = []
            if "nar" not in days[ds]:
                days[ds]["nar"] = []
        from datetime import timedelta
        current += timedelta(days=1)

    # ソート
    days_sorted = dict(sorted(days.items()))

    # 統計計算
    open_days = sum(
        1 for v in days_sorted.values() if v["jra"] or v["nar"]
    )
    jra_days = sum(1 for v in days_sorted.values() if v["jra"])
    nar_days = sum(1 for v in days_sorted.values() if v["nar"])
    total_days = len(days_sorted)

    # 生成日時
    generated_at = datetime.now().isoformat()

    result = {
        "version": "1.0",
        "generated_at": generated_at,
        "period": {
            "start": f"{start_year}-{start_month:02d}-01",
            "end": end_date.strftime("%Y-%m-%d"),
        },
        "source": "netkeiba.com calendar",
        "stats": {
            "total_days": total_days,
            "open_days": open_days,
            "jra_days": jra_days,
            "nar_days": nar_days,
            "jra_skip_months": jra_skip_count,
            "nar_skip_months": nar_skip_count,
        },
        "days": days_sorted,
    }

    return result

# ============================================================
# アトミック書き込み
# ============================================================

def save_calendar(data: dict, output_path: Path) -> None:
    """アトミック書き込み (.tmp → replace)
    Windows では Path.rename() は既存ファイルに上書きできないため replace() を使用。
    """
    tmp_path = output_path.with_suffix(".json.tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    # Path.replace() は Windows でも既存ファイルに上書き可能
    tmp_path.replace(output_path)
    print(f"[OK] 保存完了: {output_path}")

# ============================================================
# 整合性検証: race_id_date_master.json との突合
# ============================================================

def verify_against_race_id_master(calendar_data: dict) -> None:
    """
    data/masters/race_id_date_master.json と kaisai_calendar を突合し、
    不整合をレポートする。
    """
    race_id_master_path = _MASTERS_DIR / "race_id_date_master.json"
    if not race_id_master_path.exists():
        print("[SKIP] race_id_date_master.json が存在しないため整合性検証をスキップ")
        return

    with open(race_id_master_path, "r", encoding="utf-8") as f:
        race_id_master = json.load(f)

    # venue_master から JRA コードと名称のマッピングを取得
    try:
        from data.masters.venue_master import VENUE_MAP, JRA_CODES
        _venue_code_to_name = {code: name for code, name in VENUE_MAP.items()}
    except ImportError:
        _venue_code_to_name = {}
        JRA_CODES = set()

    days = calendar_data.get("days", {})
    total = len(race_id_master)
    mismatch = 0
    mismatch_examples = []

    for race_id, entry in race_id_master.items():
        if not isinstance(entry, dict):
            continue
        race_date = entry.get("date", "")
        if not race_date or len(race_id) < 6:
            continue

        venue_code = race_id[4:6]
        venue_name = _venue_code_to_name.get(venue_code, "")
        is_jra = venue_code in JRA_CODES if JRA_CODES else False

        if race_date not in days:
            # カレンダー期間外は無視
            continue

        day_entry = days[race_date]
        venues_on_day = day_entry.get("jra", []) if is_jra else day_entry.get("nar", [])

        if not venues_on_day:
            # カレンダーに開催がない日に race_id が存在 → 不整合
            mismatch += 1
            if len(mismatch_examples) < 10:
                mismatch_examples.append({
                    "race_id": race_id,
                    "date": race_date,
                    "venue_code": venue_code,
                    "venue_name": venue_name,
                    "kind": "jra" if is_jra else "nar",
                })
        elif venue_name and venue_name not in venues_on_day:
            # 会場名不一致
            mismatch += 1
            if len(mismatch_examples) < 10:
                mismatch_examples.append({
                    "race_id": race_id,
                    "date": race_date,
                    "venue_code": venue_code,
                    "venue_name": venue_name,
                    "calendar_venues": venues_on_day,
                    "kind": "jra" if is_jra else "nar",
                })

    match_rate = (total - mismatch) / total * 100 if total > 0 else 0.0
    print(f"\n=== race_id_date_master.json 整合性検証 ===")
    print(f"  総エントリ数: {total}")
    print(f"  不整合件数  : {mismatch}")
    print(f"  一致率      : {match_rate:.1f}%")

    if mismatch_examples:
        print("  不整合例 (最大10件):")
        for ex in mismatch_examples:
            print(f"    {ex}")

# ============================================================
# Sanity Check
# ============================================================

def sanity_check(days: dict) -> None:
    """主要日付のチェック"""
    checks = [
        ("2026-01-01", "元旦: JRA=空 / NAR=川崎+名古屋+高知", lambda d: not d["jra"] and set(d["nar"]) >= {"川崎", "名古屋", "高知"}),
        ("2026-01-04", "1/4: JRA=中山+京都 / NAR あり", lambda d: "中山" in d["jra"] and "京都" in d["jra"]),
        ("2024-12-28", "年末: JRA or NAR いずれか開催あり", lambda d: bool(d["jra"] or d["nar"])),
    ]

    print("\n=== Sanity Check ===")
    for date_str, desc, check_fn in checks:
        if date_str not in days:
            print(f"  [?] {date_str} ({desc}): データなし")
            continue
        d = days[date_str]
        ok = check_fn(d)
        mark = "OK" if ok else "NG"
        print(f"  [{mark}] {date_str} ({desc})")
        print(f"       JRA={d['jra']}")
        print(f"       NAR={d['nar']}")

# ============================================================
# メイン
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="開催カレンダービルダー (JRA + NAR)")
    parser.add_argument("--start", type=int, default=2022, help="開始年 (例: 2022)")
    parser.add_argument("--end",   type=int, default=2026, help="終了年 (例: 2026)")
    parser.add_argument("--year",  type=int, default=None, help="単年指定 (--start/--end より優先)")
    parser.add_argument("--month", type=int, default=None, help="単月指定 (--year と併用)")
    parser.add_argument("--refresh", action="store_true", help="キャッシュ無視で再取得")
    parser.add_argument("--verify-only", action="store_true", help="既存 JSON の整合性検証のみ実行")
    args = parser.parse_args()

    # 単年 / 単月 指定の処理
    if args.year is not None:
        start_year = end_year = args.year
        start_month = args.month if args.month else 1
        end_month   = args.month if args.month else 12
    else:
        start_year, end_year = args.start, args.end
        start_month, end_month = 1, 12

    # --verify-only: 既存 JSON を読み込んで検証だけ行う
    if args.verify_only:
        if not _OUTPUT_FILE.exists():
            print(f"ERROR: {_OUTPUT_FILE} が存在しません。先にビルドしてください。")
            sys.exit(1)
        with open(_OUTPUT_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        verify_against_race_id_master(data)
        sanity_check(data.get("days", {}))
        return

    print(f"=== 開催カレンダービルダー 開始 ===")
    print(f"  期間: {start_year}-{start_month:02d} 〜 {end_year}-{end_month:02d}")
    print(f"  キャッシュ: {'再取得 (--refresh)' if args.refresh else '利用'}")
    print()

    t_start = time.time()
    data = build_calendar(
        start_year=start_year,
        end_year=end_year,
        start_month=start_month,
        end_month=end_month,
        refresh=args.refresh,
    )
    elapsed = time.time() - t_start

    # 統計表示
    stats = data["stats"]
    print(f"\n=== 取得統計 ===")
    print(f"  総日数      : {stats['total_days']}")
    print(f"  開催あり日  : {stats['open_days']}")
    print(f"  JRA 開催日数: {stats['jra_days']}")
    print(f"  NAR 開催日数: {stats['nar_days']}")
    print(f"  JRA スキップ月数: {stats['jra_skip_months']}")
    print(f"  NAR スキップ月数: {stats['nar_skip_months']}")
    print(f"  経過時間    : {elapsed:.1f}秒")

    # 保存
    save_calendar(data, _OUTPUT_FILE)

    # Sanity Check
    sanity_check(data["days"])

    # race_id_date_master との整合性検証
    verify_against_race_id_master(data)

    print(f"\n=== 完了 ===")


if __name__ == "__main__":
    main()
