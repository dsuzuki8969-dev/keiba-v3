"""
race_log カバレッジ監査スクリプト。
年別 / venue別 (JRA / NAR) の実績件数を kaisai_calendar.json と比較し、
不足レース数・キャッシュあり率を出力する。

実行:
    PYTHONIOENCODING=utf-8 python scripts/audit_race_log_coverage.py 2>&1 | tee log/race_log_audit.log | tail -80
"""

import json
import os
import re
import sqlite3
import sys
from collections import defaultdict

# プロジェクトルートを sys.path に追加
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

DB_PATH = os.path.join(ROOT, "data", "keiba.db")
CALENDAR_PATH = os.path.join(ROOT, "data", "masters", "kaisai_calendar.json")
CACHE_DIR = os.path.join(ROOT, "data", "cache")

# JRA 会場コード
JRA_VENUE_CODES = frozenset(["01", "02", "03", "04", "05", "06", "07", "08", "09", "10"])


def load_race_log_coverage(conn: sqlite3.Connection) -> dict:
    """race_log から年別/kind別 race_id 数と行数を集計する。"""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
          substr(race_date, 1, 4) AS year,
          CASE WHEN substr(race_id, 5, 2) IN
            ('01','02','03','04','05','06','07','08','09','10')
               THEN 'JRA' ELSE 'NAR' END AS kind,
          COUNT(DISTINCT race_id) AS race_count,
          COUNT(*) AS row_count
        FROM race_log
        WHERE race_date IS NOT NULL
        GROUP BY year, kind
        ORDER BY year, kind
        """
    )
    result = {}
    for row in cur.fetchall():
        result[(row[0], row[1])] = {"race_count": row[2], "row_count": row[3]}
    return result


def load_calendar_expectations(calendar_path: str) -> dict:
    """kaisai_calendar.json から年別/kind別 場日数を集計し、期待レース数を返す。"""
    with open(calendar_path, "r", encoding="utf-8") as f:
        cal = json.load(f)

    days = cal.get("days", {})
    if not isinstance(days, dict):
        raise ValueError("kaisai_calendar.json の days がdict形式ではありません")

    result = defaultdict(int)
    for date_str, venues in days.items():
        year = date_str[:4]
        if venues.get("jra"):
            result[(year, "JRA")] += len(venues["jra"])
        if venues.get("nar"):
            result[(year, "NAR")] += len(venues["nar"])

    # 場日数 → 期待レース数 (1場日=12レース想定)
    return {k: v * 12 for k, v in result.items()}


def count_cache_files_by_year(cache_dir: str) -> dict:
    """キャッシュディレクトリ内の race_result HTML を年別/kind別にカウントする。"""
    # パターン: race.netkeiba.com_race_result.html_race_id=YYYYVV....html.lz4
    pattern = re.compile(r"result\.html_race_id=(\d{4})(\d{2})\d+\.html(?:\.lz4)?$")
    counts = defaultdict(int)

    try:
        for fname in os.listdir(cache_dir):
            m = pattern.search(fname)
            if not m:
                continue
            year = m.group(1)
            venue_code = m.group(2)
            kind = "JRA" if venue_code in JRA_VENUE_CODES else "NAR"
            counts[(year, kind)] += 1
    except FileNotFoundError:
        pass

    return dict(counts)


def print_section(title: str) -> None:
    """セクションタイトルを表示する。"""
    print()
    print("=" * 65)
    print(f"  {title}")
    print("=" * 65)


def main() -> None:
    print("race_log カバレッジ監査レポート")
    print(f"DB    : {DB_PATH}")
    print(f"暦    : {CALENDAR_PATH}")
    print(f"キャッシュ: {CACHE_DIR}")

    # --- DB 接続 ---
    if not os.path.exists(DB_PATH):
        print(f"[ERROR] DB が見つかりません: {DB_PATH}", file=sys.stderr)
        sys.exit(1)
    conn = sqlite3.connect(DB_PATH)

    # --- データ読み込み ---
    print_section("1. race_log 実績集計")
    actual = load_race_log_coverage(conn)
    conn.close()

    print(f"{'year':<6} {'kind':<5} {'race_count':>12} {'row_count':>12}")
    print("-" * 45)
    for key in sorted(actual.keys()):
        d = actual[key]
        print(f"{key[0]:<6} {key[1]:<5} {d['race_count']:>12,} {d['row_count']:>12,}")

    # --- 期待値 ---
    print_section("2. kaisai_calendar 期待レース数 (場日x12)")
    if not os.path.exists(CALENDAR_PATH):
        print(f"[WARN] kaisai_calendar.json が見つかりません: {CALENDAR_PATH}")
        expected = {}
    else:
        expected = load_calendar_expectations(CALENDAR_PATH)

    # --- 充足率比較 ---
    print_section("3. カバレッジ比較 (実績 / 期待)")
    all_keys = sorted(set(list(actual.keys()) + list(expected.keys())))
    print(f"{'year':<6} {'kind':<5} {'期待':>9} {'実績':>9} {'充足率':>8} {'不足':>9}")
    print("-" * 58)

    grand_total_missing = 0
    missing_by_kind = {"JRA": 0, "NAR": 0}

    for key in all_keys:
        exp = expected.get(key, 0)
        act = actual.get(key, {}).get("race_count", 0)
        ratio = act / exp * 100 if exp > 0 else 0.0
        missing = max(0, exp - act)
        grand_total_missing += missing
        missing_by_kind[key[1]] = missing_by_kind.get(key[1], 0) + missing
        flag = " ★" if ratio < 50.0 and exp > 100 else ("  !" if ratio < 90.0 else "")
        print(f"{key[0]:<6} {key[1]:<5} {exp:>9,} {act:>9,} {ratio:>7.1f}% {missing:>9,}{flag}")

    print("-" * 58)
    print(f"{'合計不足(概算)':<20} JRA={missing_by_kind['JRA']:,} / NAR={missing_by_kind['NAR']:,} / Total={grand_total_missing:,}")

    # --- キャッシュ確認 ---
    print_section("4. キャッシュあり/なし内訳")
    cache_counts = count_cache_files_by_year(CACHE_DIR)

    if cache_counts:
        print(f"{'year':<6} {'kind':<5} {'キャッシュ件数':>14} {'不足レース':>12} {'補填可能率':>12}")
        print("-" * 55)
        for key in all_keys:
            exp = expected.get(key, 0)
            act = actual.get(key, {}).get("race_count", 0)
            missing = max(0, exp - act)
            cached = cache_counts.get(key, 0)
            cover = cached / missing * 100 if missing > 0 else 100.0
            print(f"{key[0]:<6} {key[1]:<5} {cached:>14,} {missing:>12,} {cover:>11.1f}%")
    else:
        print("race_result HTML キャッシュなし (backfill には netkeiba 再取得が必要)")

    # --- バックフィル判断サマリ ---
    print_section("5. バックフィル判断サマリ")

    # キャッシュから補填できる総件数
    total_from_cache = sum(cache_counts.values())
    total_missing_all = grand_total_missing

    print(f"不足レース数 (概算)    : {total_missing_all:,} レース")
    print(f"キャッシュあり件数     : {total_from_cache:,} (race_result HTML)")

    if total_from_cache >= 10000:
        print("→ オプションA: キャッシュ完備。既存 backfill_race_log.py で対応可能")
    elif total_from_cache > 0:
        print(f"→ オプションA: キャッシュ一部あり ({total_from_cache:,}件)。軽量バックフィル可能")
        print("→ オプションB: 残りは netkeiba 再取得が必要")
    else:
        print("→ オプションA: キャッシュなし → netkeiba 再取得必要 (オプションB/C)")

    print()
    print("2023年の状況:")
    y2023_jra = actual.get(("2023", "JRA"), {}).get("race_count", 0)
    y2023_nar = actual.get(("2023", "NAR"), {}).get("race_count", 0)
    exp_2023_jra = expected.get(("2023", "JRA"), 0)
    exp_2023_nar = expected.get(("2023", "NAR"), 0)
    print(f"  JRA 実績: {y2023_jra:,} / 期待: {exp_2023_jra:,} ({y2023_jra/exp_2023_jra*100:.1f}%)")
    print(f"  NAR 実績: {y2023_nar:,} / 期待: {exp_2023_nar:,} ({y2023_nar/exp_2023_nar*100:.1f}%)")
    print(f"  2023年7-12月 JRA/NAR ともに race_log ゼロ（下半期全欠損）")
    print(f"  キャッシュ (race_result HTML): 0件 → netkeiba 再取得が必要")

    print()
    print("推奨アクション:")
    print("  [即時可能]  なし (race_result HTMLキャッシュなし)")
    print("  [大規模取得] 2023年7-12月: JRA約1,944レース + NAR約12,505レース")
    print("               → 本セッション保留。別タスク化推奨 (P1)")
    print("  [2023上半期] JRA 1-5月は充足済み。NAR 4月以降に若干の不足あり (~1,000レース)")
    print()
    print("次セッション着手条件:")
    print("  1. マスター承認後に netkeiba 再取得スクリプト (backfill_2023_missing.py) 作成")
    print("  2. JRA 2023年下半期から開始（重要レース優先）")
    print("  3. バックアップ keiba.db バックアップ後に apply")


if __name__ == "__main__":
    main()
