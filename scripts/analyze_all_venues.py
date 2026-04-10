"""
全25競馬場 一括分析スクリプト

race_log から各場の実データを集計し、以下を出力:
1. 基本統計（走行数、レース数、距離別）
2. 脚質別複勝率（距離別）
3. 4角位置別勝率（前残り傾向）
4. 前半3F分布 → ペース閾値の自動提案
5. 問題点サマリ＆VENUE_PACE_THRESHOLDS生成コード

Usage:
  python scripts/analyze_all_venues.py              # 全場レポート
  python scripts/analyze_all_venues.py --pace-code  # ペース閾値Pythonコード出力
  python scripts/analyze_all_venues.py --venue 大井  # 1場のみ詳細
"""

import argparse
import sqlite3
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import DATABASE_PATH

# 汎用ペース閾値（pace_inference.pyと同じ）
DEFAULT_THRESHOLDS = {
    "sprint": {"芝": (34.5, 36.5), "ダート": (35.0, 37.5)},
    "mile":   {"芝": (35.0, 37.0), "ダート": (35.5, 38.0)},
    "middle": {"芝": (36.0, 38.0), "ダート": (36.5, 39.0)},
    "long":   {"芝": (37.0, 39.0), "ダート": (37.5, 40.0)},
}


def get_conn():
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def distance_bucket(d):
    if d < 1400: return "sprint"
    if d < 1800: return "mile"
    if d < 2200: return "middle"
    return "long"


def get_venues(conn):
    """venue_code → venue_name マッピング"""
    rows = conn.execute(
        "SELECT DISTINCT venue_code, "
        "CASE venue_code "
        "  WHEN '01' THEN '福島' WHEN '02' THEN '新潟' WHEN '03' THEN '札幌' "
        "  WHEN '04' THEN '函館' WHEN '05' THEN '東京' WHEN '06' THEN '中山' "
        "  WHEN '07' THEN '中京' WHEN '08' THEN '京都' WHEN '09' THEN '阪神' "
        "  WHEN '10' THEN '小倉' WHEN '30' THEN '門別' WHEN '35' THEN '盛岡' "
        "  WHEN '36' THEN '水沢' WHEN '42' THEN '浦和' WHEN '43' THEN '船橋' "
        "  WHEN '44' THEN '大井' WHEN '45' THEN '川崎' WHEN '46' THEN '金沢' "
        "  WHEN '47' THEN '笠松' WHEN '48' THEN '名古屋' WHEN '50' THEN '園田' "
        "  WHEN '51' THEN '姫路' WHEN '54' THEN '高知' WHEN '55' THEN '佐賀' "
        "  WHEN '65' THEN '帯広' ELSE venue_code END as venue_name "
        "FROM race_log WHERE venue_code IS NOT NULL "
        "GROUP BY venue_code HAVING COUNT(*) >= 50 "
        "ORDER BY venue_code"
    ).fetchall()
    return [(r["venue_code"], r["venue_name"]) for r in rows]


def analyze_pace_distribution(conn, venue_code):
    """距離別の前半3F分布を分析し、カスタム閾値候補を返す"""
    distances = conn.execute(
        "SELECT DISTINCT distance FROM race_log "
        "WHERE venue_code = ? AND race_first_3f > 0 ORDER BY distance",
        (venue_code,),
    ).fetchall()

    results = []
    for d_row in distances:
        dist = d_row["distance"]
        rows = conn.execute(
            "SELECT race_first_3f FROM race_log "
            "WHERE venue_code = ? AND distance = ? AND race_first_3f > 0 AND finish_pos = 1 "
            "ORDER BY race_first_3f",
            (venue_code, dist),
        ).fetchall()

        vals = [r["race_first_3f"] for r in rows]
        n = len(vals)
        if n < 30:
            continue

        # 馬場判定（大半がダートかチェック）
        surf_row = conn.execute(
            "SELECT surface, COUNT(*) as c FROM race_log "
            "WHERE venue_code = ? AND distance = ? GROUP BY surface ORDER BY c DESC LIMIT 1",
            (venue_code, dist),
        ).fetchone()
        surface = surf_row["surface"] if surf_row else "ダート"
        surf_key = "芝" if "芝" in surface else "ダート"

        p33 = vals[int(n * 0.33)]
        p67 = vals[int(n * 0.67)]

        bucket = distance_bucket(dist)
        default_h, default_s = DEFAULT_THRESHOLDS[bucket].get(surf_key, (35.0, 37.0))

        h_diff = p33 - default_h
        s_diff = p67 - default_s

        results.append({
            "distance": dist,
            "surface": surf_key,
            "bucket": bucket,
            "n": n,
            "p33": p33,
            "p67": p67,
            "default_h": default_h,
            "default_s": default_s,
            "h_diff": h_diff,
            "s_diff": s_diff,
            "needs_custom": abs(h_diff) >= 0.5 or abs(s_diff) >= 0.5,
        })
    return results


def analyze_style_bias(conn, venue_code):
    """脚質別複勝率の分析"""
    rows = conn.execute(
        """
        SELECT distance, running_style,
            COUNT(*) AS total,
            SUM(CASE WHEN finish_pos <= 3 THEN 1 ELSE 0 END) AS place3
        FROM race_log
        WHERE venue_code = ? AND running_style IS NOT NULL
          AND finish_pos IS NOT NULL AND finish_pos > 0
        GROUP BY distance, running_style
        ORDER BY distance, running_style
        """,
        (venue_code,),
    ).fetchall()
    return rows


def analyze_position_bias(conn, venue_code):
    """4角位置別の勝率分析"""
    rows = conn.execute(
        """
        SELECT distance,
            CASE WHEN position_4c <= 3 THEN 'front'
                 WHEN position_4c <= 6 THEN 'mid'
                 WHEN position_4c <= 9 THEN 'rear'
                 ELSE 'tail' END AS pos_group,
            COUNT(*) AS total,
            SUM(CASE WHEN finish_pos <= 3 THEN 1 ELSE 0 END) AS place3
        FROM race_log
        WHERE venue_code = ? AND position_4c IS NOT NULL AND position_4c > 0
          AND finish_pos IS NOT NULL AND finish_pos > 0
        GROUP BY distance, pos_group
        ORDER BY distance, pos_group
        """,
        (venue_code,),
    ).fetchall()
    return rows


def print_venue_report(conn, venue_code, venue_name, verbose=False):
    """1場の分析レポートを出力"""
    # 基本統計
    stats = conn.execute(
        "SELECT COUNT(*) as total, COUNT(DISTINCT race_id) as races "
        "FROM race_log WHERE venue_code = ?",
        (venue_code,),
    ).fetchone()

    print(f"\n{'='*50}")
    print(f"  {venue_name} (code: {venue_code}) - {stats['total']:,}走 / {stats['races']:,}R")
    print(f"{'='*50}")

    # ペース分布
    pace_data = analyze_pace_distribution(conn, venue_code)
    custom_needed = [p for p in pace_data if p["needs_custom"]]

    if custom_needed:
        print(f"\n  [!] ペース閾値カスタム推奨: {len(custom_needed)}距離")
        for p in custom_needed:
            print(f"      {p['distance']}m({p['surface']},{p['bucket']}): "
                  f"推奨H<{p['p33']:.1f}/S>={p['p67']:.1f} "
                  f"(汎用H<{p['default_h']}/S>={p['default_s']} "
                  f"差H{p['h_diff']:+.1f}/S{p['s_diff']:+.1f})")
    else:
        print(f"\n  ペース閾値: 汎用閾値で適正（乖離<0.5s）")

    # 4角位置別の前残り傾向
    pos_data = analyze_position_bias(conn, venue_code)
    if verbose:
        current_dist = None
        for r in pos_data:
            if r["distance"] != current_dist:
                current_dist = r["distance"]
                print(f"\n  --- {current_dist}m 4角位置別 ---")
            rate = r["place3"] / r["total"] * 100 if r["total"] > 0 else 0
            print(f"    {r['pos_group']:>5s}: {rate:>5.1f}% ({r['total']:,}走)")

    # 前残りスコア算出（全距離平均）
    front_total = sum(r["total"] for r in pos_data if r["pos_group"] == "front")
    front_place = sum(r["place3"] for r in pos_data if r["pos_group"] == "front")
    tail_total = sum(r["total"] for r in pos_data if r["pos_group"] == "tail")
    tail_place = sum(r["place3"] for r in pos_data if r["pos_group"] == "tail")

    front_rate = front_place / front_total * 100 if front_total > 0 else 0
    tail_rate = tail_place / tail_total * 100 if tail_total > 0 else 0
    print(f"\n  前残り度: 先頭(1-3)複勝率={front_rate:.1f}%, 殿(10+)複勝率={tail_rate:.1f}%, "
          f"差={front_rate - tail_rate:.1f}pt")

    return pace_data


def generate_pace_threshold_code(all_pace_data):
    """全場のペース閾値Pythonコードを生成"""
    print("\n" + "=" * 60)
    print("  自動生成: VENUE_PACE_THRESHOLDS / _VENUE_DISTANCE_OVERRIDES")
    print("=" * 60)

    # 場ごとにバケット別閾値を集約
    venue_thresholds = {}  # {venue_code: {bucket: {surface: (h, s)}}}
    venue_dist_overrides = {}  # {(venue_code, dist): {surface: (h, s)}}

    for venue_code, pace_list in all_pace_data.items():
        custom = [p for p in pace_list if p["needs_custom"]]
        if not custom:
            continue

        # バケット別に集約
        bucket_data = {}
        for p in custom:
            key = (p["bucket"], p["surface"])
            if key not in bucket_data:
                bucket_data[key] = []
            bucket_data[key].append(p)

        # バケット内に複数距離がある場合、距離別overrideを使う
        for (bucket, surface), items in bucket_data.items():
            if len(items) == 1:
                # 1距離のみ → バケット閾値として設定
                p = items[0]
                if venue_code not in venue_thresholds:
                    venue_thresholds[venue_code] = {}
                if bucket not in venue_thresholds[venue_code]:
                    venue_thresholds[venue_code][bucket] = {}
                venue_thresholds[venue_code][bucket][surface] = (
                    round(p["p33"], 1), round(p["p67"], 1)
                )
            else:
                # 複数距離 → 距離別override
                for p in items:
                    venue_dist_overrides[(venue_code, p["distance"])] = {
                        surface: (round(p["p33"], 1), round(p["p67"], 1))
                    }

    # VENUE_PACE_THRESHOLDS コード生成
    print("\nVENUE_PACE_THRESHOLDS = {")
    for vc in sorted(venue_thresholds.keys()):
        buckets = venue_thresholds[vc]
        print(f'    "{vc}": {{')
        for bucket in ["sprint", "mile", "middle", "long"]:
            if bucket in buckets:
                for surf, (h, s) in buckets[bucket].items():
                    print(f'        "{bucket}": {{"{surf}": ({h}, {s})}},')
        print(f'    }},')
    print("}")

    # _VENUE_DISTANCE_OVERRIDES コード生成
    print("\n_VENUE_DISTANCE_OVERRIDES = {")
    for (vc, dist) in sorted(venue_dist_overrides.keys()):
        for surf, (h, s) in venue_dist_overrides[(vc, dist)].items():
            print(f'    ("{vc}", {dist}): {{"{surf}": ({h}, {s})}},')
    print("}")


def main():
    parser = argparse.ArgumentParser(description="全25競馬場 一括分析")
    parser.add_argument("--venue", help="特定場のみ（場名指定）")
    parser.add_argument("--pace-code", action="store_true", help="ペース閾値Pythonコード出力")
    parser.add_argument("--verbose", "-v", action="store_true", help="詳細出力")
    args = parser.parse_args()

    conn = get_conn()
    venues = get_venues(conn)

    if args.venue:
        venues = [(vc, vn) for vc, vn in venues if vn == args.venue]
        if not venues:
            print(f"エラー: '{args.venue}' が見つかりません")
            return

    print(f"{'#' * 60}")
    print(f"  全競馬場 一括分析レポート ({len(venues)}場)")
    print(f"{'#' * 60}")

    all_pace_data = {}
    summary = []

    for venue_code, venue_name in venues:
        pace_data = print_venue_report(conn, venue_code, venue_name, verbose=args.verbose)
        all_pace_data[venue_code] = pace_data

        custom_count = sum(1 for p in pace_data if p["needs_custom"])
        summary.append((venue_name, venue_code, custom_count, len(pace_data)))

    # サマリ
    print(f"\n{'=' * 60}")
    print(f"  サマリ: ペース閾値カスタム推奨場")
    print(f"{'=' * 60}")
    print(f"  {'場名':>6s}  {'code':>4s}  {'要カスタム':>8s}  {'全距離':>6s}")
    for vn, vc, custom, total in sorted(summary, key=lambda x: -x[2]):
        mark = "★" if custom > 0 else ""
        print(f"  {vn:>6s}  {vc:>4s}  {custom:>5d}距離  {total:>5d}  {mark}")

    total_custom = sum(c for _, _, c, _ in summary)
    print(f"\n  合計: {total_custom}距離でカスタム閾値が推奨")

    if args.pace_code:
        generate_pace_threshold_code(all_pace_data)

    conn.close()


if __name__ == "__main__":
    main()
