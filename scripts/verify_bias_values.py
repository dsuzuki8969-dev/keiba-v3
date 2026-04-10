"""
全競馬場×全距離のバイアス補正値 根拠検証レポート

race_logの実績データから、現在設定されている補正値が妥当かを検証:
1. コーナータイプ別 × 脚質別複勝率（全場×全距離）
2. 坂タイプ別 逃げ馬成績
3. 直線距離帯別 前残り度
4. ペース閾値 H:M:S分布（全場×全距離）
5. 初角距離帯別 ペース傾向
"""

import sqlite3
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import DATABASE_PATH
from data.masters.course_master import ALL_COURSES, get_all_courses
from src.utils.pace_inference import infer_pace_from_first3f


def get_conn():
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def section(title):
    print(f"\n{'=' * 70}")
    print(f"  {title}")
    print(f"{'=' * 70}")


def verify_corner_type_bias(conn):
    """コーナータイプ別 脚質別複勝率"""
    section("1. コーナータイプ別 脚質別複勝率")
    print("  補正値: 小回り逃げ先行+2, スパイラル差し追込+1.5, 大回り差し追込+1.0")

    corner_map = {
        "小回り": ("04", "36", "42", "44", "45", "46", "47", "51", "54", "55"),
        "スパイラル": ("01", "06", "07", "10", "43", "48", "50"),
        "大回り": ("02", "03", "05", "08", "09", "30", "35"),
    }
    # 大井は内(1600)が小回り、外がスパイラルだが、全体としてはスパイラル寄りなので除外

    for ct, venues in corner_map.items():
        placeholders = ",".join("?" * len(venues))
        rows = conn.execute(
            f"""SELECT running_style,
                COUNT(*) AS total,
                SUM(CASE WHEN finish_pos <= 3 THEN 1 ELSE 0 END) AS place3
            FROM race_log
            WHERE venue_code IN ({placeholders})
              AND running_style IS NOT NULL AND finish_pos > 0
            GROUP BY running_style ORDER BY running_style""",
            venues,
        ).fetchall()
        print(f"\n  [{ct}] ({len(venues)}場)")
        for r in rows:
            rate = r["place3"] / r["total"] * 100 if r["total"] > 0 else 0
            print(f"    {r['running_style']:>4s}: {rate:>5.1f}% ({r['total']:>7,d}走)")


def verify_slope_bias(conn):
    """坂タイプ別の逃げ馬成績"""
    section("2. 坂タイプ別 逃げ馬成績")
    print("  補正値: 急坂->逃先-2, 軽坂->逃先-1, 坂なし->0")

    slope_map = {
        "急坂": ("05", "06", "07", "09"),
        "軽坂": ("01", "04", "10", "35"),
        "坂なし": ("02", "03", "08", "42", "43", "44", "45", "46", "47", "48", "50", "51", "54", "55"),
    }
    for slope, venues in slope_map.items():
        placeholders = ",".join("?" * len(venues))
        r = conn.execute(
            f"""SELECT
                SUM(CASE WHEN running_style='逃げ' THEN 1 ELSE 0 END) as n_total,
                SUM(CASE WHEN running_style='逃げ' AND finish_pos=1 THEN 1 ELSE 0 END) as n_win,
                SUM(CASE WHEN running_style='逃げ' AND finish_pos<=3 THEN 1 ELSE 0 END) as n_place
            FROM race_log
            WHERE venue_code IN ({placeholders}) AND running_style IS NOT NULL AND finish_pos > 0""",
            venues,
        ).fetchone()
        wr = r["n_win"] / r["n_total"] * 100 if r["n_total"] > 0 else 0
        pr = r["n_place"] / r["n_total"] * 100 if r["n_total"] > 0 else 0
        print(f"  {slope}({len(venues)}場): 逃げ勝率={wr:.1f}%, 逃げ複勝率={pr:.1f}% (n={r['n_total']:,})")


def verify_straight_bias(conn):
    """直線距離帯別の前残り度"""
    section("3. 直線距離帯別 前残り度")
    print("  補正値: <=300m先行+3/追込-2, >=420m差し+3")

    idx = get_all_courses()
    # 全コースを直線距離帯別に分類
    venue_straight = {}
    for c in ALL_COURSES:
        if c.venue_code not in venue_straight:
            venue_straight[c.venue_code] = []
        venue_straight[c.venue_code].append(c.straight_m)
    venue_avg = {k: sum(v) / len(v) for k, v in venue_straight.items()}

    bands = [
        ("<=230m", 0, 230),
        ("231-300m", 231, 300),
        ("301-400m", 301, 400),
        (">=401m", 401, 9999),
    ]

    for label, lo, hi in bands:
        vcs = tuple(vc for vc, avg in venue_avg.items() if lo <= avg <= hi)
        if not vcs:
            continue
        placeholders = ",".join("?" * len(vcs))
        r = conn.execute(
            f"""SELECT
                SUM(CASE WHEN position_4c<=3 AND finish_pos<=3 THEN 1 ELSE 0 END) as fp,
                SUM(CASE WHEN position_4c<=3 THEN 1 ELSE 0 END) as ft,
                SUM(CASE WHEN position_4c>=10 AND finish_pos<=3 THEN 1 ELSE 0 END) as tp,
                SUM(CASE WHEN position_4c>=10 THEN 1 ELSE 0 END) as tt
            FROM race_log
            WHERE venue_code IN ({placeholders}) AND position_4c>0 AND finish_pos>0""",
            vcs,
        ).fetchone()
        fr = r["fp"] / r["ft"] * 100 if r["ft"] > 0 else 0
        tr = r["tp"] / r["tt"] * 100 if r["tt"] > 0 else 0
        print(f"  直線{label}: 先頭複勝率={fr:.1f}%, 殿複勝率={tr:.1f}%, 差={fr - tr:.1f}pt ({len(vcs)}場)")


def verify_pace_thresholds(conn):
    """全場×全距離のペース閾値 H:M:S分布"""
    section("4. ペース閾値 H:M:S分布（全場x全距離, 理想 30:40:30)")

    idx = get_all_courses()
    results = []

    # 全場×全距離をループ
    rows = conn.execute(
        """SELECT venue_code, distance, surface, COUNT(*) as n
        FROM race_log
        WHERE race_first_3f >= 33 AND finish_pos = 1
        GROUP BY venue_code, distance, surface
        HAVING n >= 30
        ORDER BY venue_code, distance"""
    ).fetchall()

    problem_count = 0
    for row in rows:
        vc, dist, surf = row["venue_code"], row["distance"], row["surface"]
        data = conn.execute(
            "SELECT race_first_3f FROM race_log "
            "WHERE venue_code=? AND distance=? AND surface=? AND race_first_3f>=33 AND finish_pos=1",
            (vc, dist, surf),
        ).fetchall()

        n = len(data)
        gen = {"H": 0, "M": 0, "S": 0}
        cus = {"H": 0, "M": 0, "S": 0}
        for d in data:
            g = infer_pace_from_first3f(dist, surf, d["race_first_3f"], venue_code=None)
            c = infer_pace_from_first3f(dist, surf, d["race_first_3f"], venue_code=vc)
            if g:
                gen[g.value] += 1
            if c:
                cus[c.value] += 1

        # 偏りチェック: どれかが60%以上なら問題
        cus_pcts = {k: v / n * 100 for k, v in cus.items()}
        gen_pcts = {k: v / n * 100 for k, v in gen.items()}
        is_problem = any(v >= 60 for v in cus_pcts.values())

        # 汎用からの改善度
        gen_max = max(gen_pcts.values())
        cus_max = max(cus_pcts.values())
        improved = cus_max < gen_max

        results.append({
            "vc": vc, "dist": dist, "surf": surf, "n": n,
            "gen": gen_pcts, "cus": cus_pcts,
            "is_problem": is_problem, "improved": improved,
        })

    # サマリ
    total = len(results)
    problems = sum(1 for r in results if r["is_problem"])
    improved = sum(1 for r in results if r["improved"])

    print(f"\n  分析対象: {total}コース (30走以上)")
    print(f"  カスタム閾値で改善: {improved}コース")
    print(f"  依然偏り(60%+)あり: {problems}コース")

    # 問題あるコースを表示
    if problems > 0:
        print(f"\n  --- 偏りが残るコース ---")
        for r in results:
            if r["is_problem"]:
                g = r["gen"]
                c = r["cus"]
                mark = "改善" if r["improved"] else "未改善"
                print(
                    f"    {r['vc']}_{r['surf']}_{r['dist']}: "
                    f"汎用H{g['H']:.0f}:M{g['M']:.0f}:S{g['S']:.0f} -> "
                    f"固有H{c['H']:.0f}:M{c['M']:.0f}:S{c['S']:.0f} [{mark}] (n={r['n']})"
                )


def verify_first_corner_effect(conn):
    """初角距離帯別のペース傾向"""
    section("5. 初角距離帯別のペース傾向")
    print("  補正値: <=150m->+3pt, <=200m->+1pt, >=500m->-2pt")

    idx = get_all_courses()
    # コース別の初角距離→ペース傾向を集計
    bands = [
        ("<=150m(+3pt)", 1, 150),
        ("151-200m(+1pt)", 151, 200),
        ("201-400m(0pt)", 201, 400),
        ("401-499m(0pt)", 401, 499),
        (">=500m(-2pt)", 500, 9999),
    ]

    for label, lo, hi in bands:
        # この初角距離帯に該当するコースを抽出
        matching = [c for c in ALL_COURSES if lo <= c.first_corner_m <= hi]
        if not matching:
            continue

        # race_logから逃げ馬複勝率を集計
        total_nige = 0
        total_place = 0
        for c in matching:
            r = conn.execute(
                """SELECT COUNT(*) as n,
                    SUM(CASE WHEN finish_pos<=3 THEN 1 ELSE 0 END) as p
                FROM race_log
                WHERE venue_code=? AND distance=? AND running_style='逃げ' AND finish_pos>0""",
                (c.venue_code, c.distance),
            ).fetchone()
            total_nige += r["n"] or 0
            total_place += r["p"] or 0

        rate = total_place / total_nige * 100 if total_nige > 0 else 0
        print(f"  初角{label}: 逃げ複勝率={rate:.1f}% ({total_nige:,}走, {len(matching)}コース)")


def main():
    print("#" * 70)
    print("  全競馬場×全距離 バイアス補正値 根拠検証レポート")
    print("#" * 70)

    conn = get_conn()
    try:
        verify_corner_type_bias(conn)
        verify_slope_bias(conn)
        verify_straight_bias(conn)
        verify_pace_thresholds(conn)
        verify_first_corner_effect(conn)
    finally:
        conn.close()

    print(f"\n{'=' * 70}")
    print("  検証完了")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()
