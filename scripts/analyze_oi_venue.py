"""
大井競馬場（venue_code: 44）ベースライン分析スクリプト

race_log から大井の実データを集計し、以下を出力:
1. 距離別の脚質別複勝率
2. 距離別の枠番別複勝率
3. 距離別の前半3F分布（H/M/S境界値の決定用）
4. 馬場状態別のタイム差
"""

import sqlite3
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import DATABASE_PATH

VENUE_CODE = "44"
VENUE_NAME = "大井"


def get_conn():
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def section(title: str):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


def analyze_basic_stats(conn):
    """基本統計"""
    section(f"{VENUE_NAME}競馬場 基本統計")
    row = conn.execute(
        "SELECT COUNT(*) as total, COUNT(DISTINCT race_id) as races, "
        "MIN(race_date) as earliest, MAX(race_date) as latest "
        "FROM race_log WHERE venue_code = ?",
        (VENUE_CODE,),
    ).fetchone()
    print(f"  走行データ: {row['total']:,} 走")
    print(f"  レース数:   {row['races']:,} R")
    print(f"  期間:       {row['earliest']} 〜 {row['latest']}")

    # 距離別
    rows = conn.execute(
        "SELECT distance, COUNT(*) as runs, COUNT(DISTINCT race_id) as races "
        "FROM race_log WHERE venue_code = ? "
        "GROUP BY distance ORDER BY distance",
        (VENUE_CODE,),
    ).fetchall()
    print(f"\n  {'距離':>6s}  {'走数':>7s}  {'レース数':>7s}")
    print(f"  {'-'*6}  {'-'*7}  {'-'*7}")
    for r in rows:
        print(f"  {r['distance']:>5d}m  {r['runs']:>7,d}  {r['races']:>7,d}")


def analyze_running_style(conn):
    """距離別の脚質別複勝率"""
    section("距離別 脚質別複勝率")
    rows = conn.execute(
        """
        SELECT
            distance,
            running_style,
            COUNT(*) AS total,
            SUM(CASE WHEN finish_pos = 1 THEN 1 ELSE 0 END) AS win,
            SUM(CASE WHEN finish_pos <= 3 THEN 1 ELSE 0 END) AS place3
        FROM race_log
        WHERE venue_code = ? AND running_style IS NOT NULL
          AND finish_pos IS NOT NULL AND finish_pos > 0
        GROUP BY distance, running_style
        ORDER BY distance, running_style
        """,
        (VENUE_CODE,),
    ).fetchall()

    current_dist = None
    for r in rows:
        if r["distance"] != current_dist:
            current_dist = r["distance"]
            print(f"\n  --- {current_dist}m ---")
            print(f"  {'脚質':>6s}  {'走数':>6s}  {'勝率':>6s}  {'複勝率':>6s}")
        style = r["running_style"] or "不明"
        total = r["total"]
        win_rate = r["win"] / total * 100 if total else 0
        place_rate = r["place3"] / total * 100 if total else 0
        print(f"  {style:>6s}  {total:>6,d}  {win_rate:>5.1f}%  {place_rate:>5.1f}%")


def analyze_gate_bias(conn):
    """距離別の枠番別複勝率"""
    section("距離別 枠番別複勝率")

    distances = [
        r["distance"]
        for r in conn.execute(
            "SELECT DISTINCT distance FROM race_log WHERE venue_code = ? ORDER BY distance",
            (VENUE_CODE,),
        ).fetchall()
    ]

    for dist in distances:
        rows = conn.execute(
            """
            SELECT
                gate_no,
                COUNT(*) AS total,
                SUM(CASE WHEN finish_pos <= 3 THEN 1 ELSE 0 END) AS place3
            FROM race_log
            WHERE venue_code = ? AND distance = ?
              AND gate_no IS NOT NULL AND gate_no BETWEEN 1 AND 8
              AND finish_pos IS NOT NULL AND finish_pos > 0
              AND field_count >= 8
            GROUP BY gate_no
            ORDER BY gate_no
            """,
            (VENUE_CODE, dist),
        ).fetchall()
        if not rows:
            continue

        # 全枠平均
        total_all = sum(r["total"] for r in rows)
        place_all = sum(r["place3"] for r in rows)
        avg_rate = place_all / total_all * 100 if total_all else 0

        print(f"\n  --- {dist}m (平均複勝率: {avg_rate:.1f}%, n={total_all:,}) ---")
        print(f"  {'枠':>3s}  {'走数':>6s}  {'複勝率':>6s}  {'差分':>6s}")
        for r in rows:
            rate = r["place3"] / r["total"] * 100 if r["total"] else 0
            diff = rate - avg_rate
            mark = "★" if abs(diff) >= 3.0 else ""
            print(
                f"  {r['gate_no']:>3d}  {r['total']:>6,d}  {rate:>5.1f}%  {diff:>+5.1f}% {mark}"
            )


def analyze_pace_distribution(conn):
    """距離別の前半3F分布"""
    section("距離別 前半3F分布（H/M/S境界決定用）")

    distances = [
        r["distance"]
        for r in conn.execute(
            "SELECT DISTINCT distance FROM race_log WHERE venue_code = ? ORDER BY distance",
            (VENUE_CODE,),
        ).fetchall()
    ]

    for dist in distances:
        rows = conn.execute(
            """
            SELECT first_3f_sec
            FROM race_log
            WHERE venue_code = ? AND distance = ?
              AND first_3f_sec IS NOT NULL AND first_3f_sec > 0
              AND finish_pos = 1
            ORDER BY first_3f_sec
            """,
            (VENUE_CODE, dist),
        ).fetchall()
        if not rows:
            continue

        values = [r["first_3f_sec"] for r in rows]
        n = len(values)
        if n < 5:
            continue

        avg = sum(values) / n
        p25 = values[int(n * 0.25)]
        p33 = values[int(n * 0.33)]
        p50 = values[int(n * 0.50)]
        p67 = values[int(n * 0.67)]
        p75 = values[int(n * 0.75)]
        mn = values[0]
        mx = values[-1]

        print(f"\n  --- {dist}m (n={n}) ---")
        print(f"    最速: {mn:.1f}s  最遅: {mx:.1f}s  平均: {avg:.1f}s")
        print(f"    P25: {p25:.1f}s  P33(H境界候補): {p33:.1f}s")
        print(f"    P50(中央): {p50:.1f}s")
        print(f"    P67(S境界候補): {p67:.1f}s  P75: {p75:.1f}s")

        # 現行閾値との比較
        if dist < 1400:
            bucket = "sprint"
            h_th, s_th = 35.0, 37.5
        elif dist < 1800:
            bucket = "mile"
            h_th, s_th = 35.5, 38.0
        elif dist < 2200:
            bucket = "middle"
            h_th, s_th = 36.5, 39.0
        else:
            bucket = "long"
            h_th, s_th = 37.5, 40.0

        h_count = sum(1 for v in values if v < h_th)
        m_count = sum(1 for v in values if h_th <= v < s_th)
        s_count = sum(1 for v in values if v >= s_th)
        print(f"    現行閾値({bucket}): H<{h_th}s / M:{h_th}-{s_th}s / S>={s_th}s")
        print(
            f"    現行分布: H={h_count}({h_count/n*100:.0f}%) / "
            f"M={m_count}({m_count/n*100:.0f}%) / S={s_count}({s_count/n*100:.0f}%)"
        )
        # 理想は H:M:S = 30:40:30 程度
        print(f"    推奨H境界: {p33:.1f}s / 推奨S境界: {p67:.1f}s")


def analyze_condition_time(conn):
    """馬場状態別の勝ちタイム"""
    section("距離別 馬場状態別 勝ちタイム")

    rows = conn.execute(
        """
        SELECT
            distance, condition,
            COUNT(*) AS wins,
            AVG(finish_time_sec) AS avg_time,
            MIN(finish_time_sec) AS best_time
        FROM race_log
        WHERE venue_code = ? AND finish_pos = 1
          AND finish_time_sec IS NOT NULL AND finish_time_sec > 0
          AND condition IS NOT NULL
        GROUP BY distance, condition
        ORDER BY distance, condition
        """,
        (VENUE_CODE,),
    ).fetchall()

    current_dist = None
    baseline = {}
    for r in rows:
        if r["distance"] != current_dist:
            current_dist = r["distance"]
            baseline = {}
            print(f"\n  --- {current_dist}m ---")
            print(f"  {'状態':>4s}  {'勝数':>5s}  {'平均タイム':>8s}  {'最速':>8s}  {'良比差':>6s}")
        if r["condition"] == "良":
            baseline[current_dist] = r["avg_time"]
        base = baseline.get(current_dist, r["avg_time"])
        diff = r["avg_time"] - base
        print(
            f"  {r['condition']:>4s}  {r['wins']:>5,d}  "
            f"{r['avg_time']:>8.1f}s  {r['best_time']:>8.1f}s  {diff:>+5.1f}s"
        )


def analyze_position_by_distance(conn):
    """距離別の4角位置別勝率（前残り度合い）"""
    section("距離別 4角位置別勝率（前残り傾向分析）")

    distances = [
        r["distance"]
        for r in conn.execute(
            "SELECT DISTINCT distance FROM race_log WHERE venue_code = ? ORDER BY distance",
            (VENUE_CODE,),
        ).fetchall()
    ]

    for dist in distances:
        rows = conn.execute(
            """
            SELECT
                CASE
                    WHEN position_4c <= 3 THEN '先頭(1-3)'
                    WHEN position_4c <= 6 THEN '中団(4-6)'
                    WHEN position_4c <= 9 THEN '後方(7-9)'
                    ELSE '殿(10+)'
                END AS pos_group,
                COUNT(*) AS total,
                SUM(CASE WHEN finish_pos = 1 THEN 1 ELSE 0 END) AS win,
                SUM(CASE WHEN finish_pos <= 3 THEN 1 ELSE 0 END) AS place3
            FROM race_log
            WHERE venue_code = ? AND distance = ?
              AND position_4c IS NOT NULL AND position_4c > 0
              AND finish_pos IS NOT NULL AND finish_pos > 0
            GROUP BY pos_group
            ORDER BY pos_group
            """,
            (VENUE_CODE, dist),
        ).fetchall()
        if not rows:
            continue
        print(f"\n  --- {dist}m ---")
        print(f"  {'4角位置':>12s}  {'走数':>6s}  {'勝率':>6s}  {'複勝率':>6s}")
        for r in rows:
            total = r["total"]
            win_r = r["win"] / total * 100 if total else 0
            place_r = r["place3"] / total * 100 if total else 0
            print(f"  {r['pos_group']:>12s}  {total:>6,d}  {win_r:>5.1f}%  {place_r:>5.1f}%")


def main():
    print(f"{'#'*60}")
    print(f"  {VENUE_NAME}競馬場 ベースライン分析レポート")
    print(f"{'#'*60}")

    conn = get_conn()
    try:
        analyze_basic_stats(conn)
        analyze_running_style(conn)
        analyze_gate_bias(conn)
        analyze_pace_distribution(conn)
        analyze_condition_time(conn)
        analyze_position_by_distance(conn)
    finally:
        conn.close()

    print(f"\n{'='*60}")
    print("  分析完了")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
