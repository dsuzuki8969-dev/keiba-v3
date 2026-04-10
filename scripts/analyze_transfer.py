"""
NAR会場間移籍・遠征の成績変化分析スクリプト

race_logテーブルから複数会場で出走した馬を抽出し、
会場間移籍パターンごとに成績変化を統計分析する。
"""
import sqlite3
import os
import re
import sys
from collections import defaultdict
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

DB_PATH = PROJECT_ROOT / "data" / "keiba.db"
OUTPUT_PATH = PROJECT_ROOT / "data" / "analysis" / "transfer_analysis.txt"

# 会場コード → 名称
VENUE_NAMES = {
    "01": "札幌", "02": "函館", "03": "福島", "04": "新潟",
    "05": "東京", "06": "中山", "07": "中京", "08": "京都",
    "09": "阪神", "10": "小倉",
    "30": "門別", "35": "盛岡", "36": "水沢",
    "42": "浦和", "43": "船橋", "44": "大井", "45": "川崎",
    "46": "金沢", "47": "笠松", "48": "名古屋",
    "50": "園田", "51": "姫路",
    "54": "高知", "55": "佐賀", "65": "帯広",
}

JRA_CODES = {"01", "02", "03", "04", "05", "06", "07", "08", "09", "10"}
NAR_CODES = {"30", "35", "36", "42", "43", "44", "45", "46", "47", "48", "50", "51", "54", "55", "65"}

# 南関東
MINAMI_KANTO = {"42", "43", "44", "45"}
# 地方主要会場グループ
VENUE_GROUPS = {
    "南関東": MINAMI_KANTO,
    "東海": {"47", "48"},
    "兵庫": {"50", "51"},
    "北海道": {"30"},
    "東北": {"35", "36"},
    "北陸": {"46"},
    "四国": {"54"},
    "九州": {"55"},
}


def vname(vc):
    return VENUE_NAMES.get(vc, f"不明({vc})")


def is_jra(vc):
    return vc in JRA_CODES


def load_data(conn):
    """race_logから有効データを取得"""
    cur = conn.cursor()
    cur.execute("""
        SELECT horse_id, venue_code, race_date, finish_pos, field_count,
               grade, is_jra, race_name, surface, distance
        FROM race_log
        WHERE horse_id != '' AND horse_id IS NOT NULL
          AND finish_pos > 0 AND field_count > 0
          AND venue_code != ''
        ORDER BY horse_id, race_date
    """)
    rows = cur.fetchall()
    cols = ["horse_id", "venue_code", "race_date", "finish_pos", "field_count",
            "grade", "is_jra", "race_name", "surface", "distance"]
    return [dict(zip(cols, r)) for r in rows]


def group_by_horse(data):
    """馬ごとにレース履歴をグループ化"""
    horses = defaultdict(list)
    for row in data:
        horses[row["horse_id"]].append(row)
    return horses


def relative_pos(finish_pos, field_count):
    """相対着順 (0=1着, 1=最下位)"""
    if field_count <= 1:
        return 0.0
    return (finish_pos - 1) / (field_count - 1)


# NARクラスをrace_nameから抽出する正規表現
_NAR_CLASS_RE = re.compile(
    r"^(S1|A1|A2|A3|B1|B2|B3|C1|C2|C3|D1|D2|D3|"
    r"2歳|3歳|2歳新馬|3歳新馬|新馬|未勝利|"
    r"OP|オープン|重賞|交流重賞|G[1-3])"
)

# JRAクラスの正規化
_JRA_CLASS_MAP = {
    "新馬": "新馬",
    "未勝利": "未勝利",
    "1勝": "1勝",
    "2勝": "2勝",
    "OP": "OP",
    "交流重賞": "交流重賞",
    "G1": "G1", "G2": "G2", "G3": "G3",
}


def extract_nar_class(race_name, grade):
    """race_nameからNARクラスを抽出"""
    if not race_name:
        return grade or "不明"

    # JRAグレードはそのまま
    if grade in _JRA_CLASS_MAP:
        return _JRA_CLASS_MAP[grade]

    name = race_name.strip()

    # 末尾の括弧内にクラス表記がある場合: "浦和800ラウンド(C3)" → C3
    m_paren = re.search(r"\(([A-DSa-ds][1-3])\)\s*$", name)
    if m_paren:
        return m_paren.group(1).upper()

    # 末尾の括弧内に"3歳"等: "(3歳)" → 3歳
    m_paren2 = re.search(r"\(([23]歳)\)\s*$", name)
    if m_paren2:
        return m_paren2.group(1)

    # race_nameの先頭からクラス名を抽出
    m = _NAR_CLASS_RE.match(name)
    if m:
        return m.group(1)

    # C311 → C3, B2xxx → B2 のパターン
    m2 = re.match(r"^([A-DSa-ds][1-3])", name)
    if m2:
        return m2.group(1).upper()

    # 2歳/3歳パターン（先頭）
    m3 = re.match(r"^([23]歳)", name)
    if m3:
        return m3.group(1)

    # "3歳六" → 3歳
    m4 = re.match(r"^([23]歳)", name)
    if m4:
        return m4.group(1)

    # 交流重賞
    if grade == "交流重賞":
        return "交流重賞"

    # 数字のみのrace_name（例: "1R"）→ grade使用
    if grade and grade != "その他":
        return grade

    return "その他"


def detect_transfers(horse_races):
    """馬の出走履歴から移籍（異なる会場への移動）を検出"""
    transfers = []
    for i in range(1, len(horse_races)):
        prev = horse_races[i - 1]
        curr = horse_races[i]
        if prev["venue_code"] != curr["venue_code"]:
            transfers.append((prev, curr))
    return transfers


def analyze_venue_pair_transfers(horses):
    """会場ペアごとの移籍成績を集計"""
    # (from_vc, to_vc) → [{"before": [...], "after": [...]}]
    pair_stats = defaultdict(lambda: {
        "count": 0,
        "before_rel_pos": [],
        "after_rel_pos": [],
        "before_finish": [],
        "after_finish": [],
        "grade_transitions": [],
    })

    for hid, races in horses.items():
        if len(races) < 2:
            continue
        venues = set(r["venue_code"] for r in races)
        if len(venues) < 2:
            continue

        # 各会場ごとの成績をまとめる
        venue_runs = defaultdict(list)
        for r in races:
            venue_runs[r["venue_code"]].append(r)

        # 連続した会場変更を検出
        transfers = detect_transfers(races)
        for prev_race, curr_race in transfers:
            from_vc = prev_race["venue_code"]
            to_vc = curr_race["venue_code"]
            key = (from_vc, to_vc)

            # 移籍前の同一会場での直近成績（最大5走）
            before_runs = [r for r in races if r["venue_code"] == from_vc
                           and r["race_date"] <= prev_race["race_date"]][-5:]
            # 移籍後の同一会場での成績（最大5走）
            after_runs = [r for r in races if r["venue_code"] == to_vc
                          and r["race_date"] >= curr_race["race_date"]][:5]

            if not before_runs or not after_runs:
                continue

            before_rel = sum(relative_pos(r["finish_pos"], r["field_count"]) for r in before_runs) / len(before_runs)
            after_rel = sum(relative_pos(r["finish_pos"], r["field_count"]) for r in after_runs) / len(after_runs)
            before_fin = sum(r["finish_pos"] for r in before_runs) / len(before_runs)
            after_fin = sum(r["finish_pos"] for r in after_runs) / len(after_runs)

            stats = pair_stats[key]
            stats["count"] += 1
            stats["before_rel_pos"].append(before_rel)
            stats["after_rel_pos"].append(after_rel)
            stats["before_finish"].append(before_fin)
            stats["after_finish"].append(after_fin)
            stats["grade_transitions"].append((prev_race["grade"], curr_race["grade"]))

    return pair_stats


def analyze_class_transitions(horses):
    """クラス変動の追跡（race_nameからクラスを抽出）"""
    # (from_vc, to_vc, from_class, to_class) → count
    class_trans = defaultdict(int)
    for hid, races in horses.items():
        if len(races) < 2:
            continue
        transfers = detect_transfers(races)
        for prev_race, curr_race in transfers:
            from_cls = extract_nar_class(prev_race["race_name"], prev_race["grade"])
            to_cls = extract_nar_class(curr_race["race_name"], curr_race["grade"])
            if from_cls and to_cls:
                key = (prev_race["venue_code"], curr_race["venue_code"],
                       from_cls, to_cls)
                class_trans[key] += 1
    return class_trans


def compute_level_coefficients(pair_stats):
    """会場ペアの相対レベル係数を算出"""
    # 移籍前後の相対着順差から算出
    # 正の値 = 移籍先の方がレベルが低い（成績向上）
    coefficients = {}
    for (from_vc, to_vc), stats in pair_stats.items():
        if stats["count"] < 5:
            continue
        before_avg = sum(stats["before_rel_pos"]) / len(stats["before_rel_pos"])
        after_avg = sum(stats["after_rel_pos"]) / len(stats["after_rel_pos"])
        diff = before_avg - after_avg  # 正=成績向上（移籍先の方が楽）
        coefficients[(from_vc, to_vc)] = {
            "count": stats["count"],
            "before_rel": before_avg,
            "after_rel": after_avg,
            "diff": diff,
        }
    return coefficients


def analyze_jra_to_nar(horses):
    """JRA→NAR移籍の詳細分析"""
    # JRAのクラス別に、各NAR会場での成績を集計
    # jra_grade → nar_vc → stats
    jra_nar_stats = defaultdict(lambda: defaultdict(lambda: {
        "count": 0,
        "wins": 0,
        "place3": 0,
        "avg_finish": [],
        "avg_rel_pos": [],
        "nar_grades": [],
    }))

    for hid, races in horses.items():
        jra_races = [r for r in races if is_jra(r["venue_code"])]
        nar_races = [r for r in races if not is_jra(r["venue_code"]) and r["venue_code"] in NAR_CODES]

        if not jra_races or not nar_races:
            continue

        # JRAの最後のレースのグレードを取得
        last_jra = jra_races[-1]
        jra_grade = last_jra["grade"]
        if not jra_grade:
            continue

        # JRA最終走の日付以降のNARレース
        after_nar = [r for r in nar_races if r["race_date"] > last_jra["race_date"]]
        if not after_nar:
            continue

        for r in after_nar[:5]:  # 移籍後最大5走
            nar_vc = r["venue_code"]
            s = jra_nar_stats[jra_grade][nar_vc]
            s["count"] += 1
            if r["finish_pos"] == 1:
                s["wins"] += 1
            if r["finish_pos"] <= 3:
                s["place3"] += 1
            s["avg_finish"].append(r["finish_pos"])
            s["avg_rel_pos"].append(relative_pos(r["finish_pos"], r["field_count"]))
            if r["grade"]:
                s["nar_grades"].append(r["grade"])

    return jra_nar_stats


def analyze_monbetsu_transfers(horses):
    """門別2歳 → 各地への移籍分析"""
    stats = defaultdict(lambda: {
        "count": 0,
        "wins": 0,
        "place3": 0,
        "avg_finish": [],
        "avg_rel_pos": [],
        "monbetsu_grade": [],
        "dest_grade": [],
    })

    for hid, races in horses.items():
        mon_races = [r for r in races if r["venue_code"] == "30"]
        other_races = [r for r in races if r["venue_code"] != "30" and r["venue_code"] in NAR_CODES]

        if not mon_races or not other_races:
            continue

        # 門別での最終走が2歳シーズン（概ね11月以前）
        last_mon = mon_races[-1]
        # 門別後の各地での成績
        after_races = [r for r in other_races if r["race_date"] > last_mon["race_date"]]
        if not after_races:
            continue

        for r in after_races[:5]:
            vc = r["venue_code"]
            s = stats[vc]
            s["count"] += 1
            if r["finish_pos"] == 1:
                s["wins"] += 1
            if r["finish_pos"] <= 3:
                s["place3"] += 1
            s["avg_finish"].append(r["finish_pos"])
            s["avg_rel_pos"].append(relative_pos(r["finish_pos"], r["field_count"]))
            if last_mon["grade"]:
                s["monbetsu_grade"].append(last_mon["grade"])
            if r["grade"]:
                s["dest_grade"].append(r["grade"])

    return stats


def format_report(pair_stats, class_trans, coefficients, jra_nar, monbetsu, horses):
    """レポートをテキスト形式でフォーマット"""
    lines = []
    lines.append("=" * 80)
    lines.append("NAR会場間移籍・遠征 成績変化分析レポート")
    lines.append("=" * 80)

    # --- 基本統計 ---
    lines.append("\n" + "=" * 60)
    lines.append("1. 基本統計")
    lines.append("=" * 60)
    total_horses = len(horses)
    multi_venue = sum(1 for hid, races in horses.items()
                      if len(set(r["venue_code"] for r in races)) >= 2)
    lines.append(f"  総馬数（race_log内）: {total_horses:,}")
    lines.append(f"  複数会場出走馬数:     {multi_venue:,} ({100*multi_venue/total_horses:.1f}%)")

    # --- 会場ペア別移籍統計 ---
    lines.append("\n" + "=" * 60)
    lines.append("2. 主要会場ペア別 移籍前後の成績変化")
    lines.append("   (相対着順 = (着順-1)/(頭数-1), 0=1着, 1=最下位)")
    lines.append("=" * 60)

    # 指定パターン
    target_patterns = [
        ("大井→園田/姫路", [("44", "50"), ("44", "51")]),
        ("大井→名古屋/笠松", [("44", "48"), ("44", "47")]),
        ("大井→高知/佐賀", [("44", "54"), ("44", "55")]),
        ("船橋→各地", [(f, t) for f in ["43"] for t in NAR_CODES - {"43"}]),
        ("川崎→各地", [(f, t) for f in ["45"] for t in NAR_CODES - {"45"}]),
        ("浦和→各地", [(f, t) for f in ["42"] for t in NAR_CODES - {"42"}]),
    ]

    for label, pairs in target_patterns:
        lines.append(f"\n  --- {label} ---")
        found_any = False
        for from_vc, to_vc in sorted(pairs):
            key = (from_vc, to_vc)
            if key not in pair_stats or pair_stats[key]["count"] < 3:
                continue
            s = pair_stats[key]
            n = s["count"]
            b_rel = sum(s["before_rel_pos"]) / n
            a_rel = sum(s["after_rel_pos"]) / n
            b_fin = sum(s["before_finish"]) / n
            a_fin = sum(s["after_finish"]) / n
            diff = b_rel - a_rel
            direction = "↑成績向上" if diff > 0.05 else ("↓成績低下" if diff < -0.05 else "→変化なし")
            lines.append(f"    {vname(from_vc)}→{vname(to_vc)}: "
                         f"n={n:4d}  移籍前相対着順={b_rel:.3f} → 移籍後={a_rel:.3f} "
                         f"(差={diff:+.3f} {direction})  "
                         f"平均着順: {b_fin:.1f}→{a_fin:.1f}")
            found_any = True
        if not found_any:
            lines.append("    該当データなし（サンプル数3未満）")

    # 全ペアのサマリ（上位30）
    lines.append(f"\n  --- 移籍件数上位30ペア ---")
    lines.append(f"    {'移籍元':>6s}→{'移籍先':<6s}  件数  前相対着  後相対着  差分     方向           前平均着 後平均着")
    sorted_pairs = sorted(pair_stats.items(), key=lambda x: x[1]["count"], reverse=True)[:30]
    for (from_vc, to_vc), s in sorted_pairs:
        n = s["count"]
        if n < 3:
            continue
        b_rel = sum(s["before_rel_pos"]) / n
        a_rel = sum(s["after_rel_pos"]) / n
        b_fin = sum(s["before_finish"]) / n
        a_fin = sum(s["after_finish"]) / n
        diff = b_rel - a_rel
        direction = "↑向上" if diff > 0.05 else ("↓低下" if diff < -0.05 else "→同等")
        lines.append(f"    {vname(from_vc):>6s}→{vname(to_vc):<6s}  {n:5d}  {b_rel:.3f}     {a_rel:.3f}     {diff:+.3f}    {direction:10s}  {b_fin:5.1f}    {a_fin:5.1f}")

    # --- クラス変動 ---
    lines.append("\n" + "=" * 60)
    lines.append("3. クラス変動の追跡（主要パターン）")
    lines.append("=" * 60)

    # 主要移籍元会場
    focus_from_venues = ["44", "43", "45", "42", "30"]  # 大井, 船橋, 川崎, 浦和, 門別
    for from_vc in focus_from_venues:
        relevant = {k: v for k, v in class_trans.items()
                    if k[0] == from_vc and v >= 3}
        if not relevant:
            continue
        lines.append(f"\n  --- {vname(from_vc)}からの移籍 ---")
        # グループ: 移籍先会場ごと
        by_dest = defaultdict(list)
        for (fv, tv, fg, tg), cnt in sorted(relevant.items(), key=lambda x: -x[1]):
            by_dest[tv].append((fg, tg, cnt))

        for tv in sorted(by_dest.keys()):
            lines.append(f"    → {vname(tv)}:")
            for fg, tg, cnt in sorted(by_dest[tv], key=lambda x: -x[2])[:10]:
                lines.append(f"      {fg} → {tg}: {cnt}件")

    # --- レベル係数 ---
    lines.append("\n" + "=" * 60)
    lines.append("4. 会場間相対レベル係数")
    lines.append("   (正の値=移籍先の方がレベルが低い、負=移籍先の方がレベルが高い)")
    lines.append("=" * 60)

    # 双方向ペアの平均を使って各会場のレーティングを推定
    # まず主要ペアの係数を表示
    lines.append("\n  --- 主要ペアの係数 ---")
    lines.append(f"    {'ペア':<20s}  件数   移籍前   移籍後   差分(レベル係数)")

    # NARのみのペアを抽出
    nar_pairs = {k: v for k, v in coefficients.items()
                 if k[0] in NAR_CODES and k[1] in NAR_CODES and v["count"] >= 10}
    for (from_vc, to_vc), c in sorted(nar_pairs.items(), key=lambda x: -x[1]["count"])[:40]:
        lines.append(f"    {vname(from_vc)}→{vname(to_vc):<8s}  {c['count']:5d}  "
                     f"{c['before_rel']:.3f}    {c['after_rel']:.3f}    {c['diff']:+.3f}")

    # 会場レーティング推定（Bradley-Terry風の簡易推定）
    lines.append("\n  --- NAR会場レーティング推定 ---")
    lines.append("   （相対レベル係数の平均を利用した簡易推定）")

    # 各会場の「移籍先としての成績改善度」の平均
    venue_rating = defaultdict(lambda: {"as_from": [], "as_to": []})
    for (fv, tv), c in coefficients.items():
        if fv in NAR_CODES and tv in NAR_CODES and c["count"] >= 5:
            venue_rating[fv]["as_from"].append(c["diff"])
            venue_rating[tv]["as_to"].append(-c["diff"])

    # レーティング = as_from の平均（正=そこから出ると成績上がる=強い会場）
    ratings = {}
    for vc in NAR_CODES:
        vals = venue_rating[vc]["as_from"] + venue_rating[vc]["as_to"]
        if len(vals) >= 3:
            ratings[vc] = sum(vals) / len(vals)

    if ratings:
        # 正規化: 大井を100とする
        base = ratings.get("44", 0)
        lines.append(f"    {'会場':>6s}  レーティング  サンプル数  (大井=100基準)")
        for vc in sorted(ratings.keys(), key=lambda x: -ratings[x]):
            n_samples = len(venue_rating[vc]["as_from"]) + len(venue_rating[vc]["as_to"])
            # レーティング: diffが大きい=強い会場
            raw = ratings[vc]
            normalized = 100 + (raw - base) * 200  # スケーリング
            lines.append(f"    {vname(vc):>6s}  {normalized:8.1f}      {n_samples:4d}")

    # --- JRA→NAR ---
    lines.append("\n" + "=" * 60)
    lines.append("5. JRA→NAR移籍の成績分析")
    lines.append("=" * 60)

    jra_grade_order = ["新馬", "未勝利", "1勝", "2勝", "OP", "交流重賞", "G3", "G2", "G1"]
    for jg in jra_grade_order:
        if jg not in jra_nar:
            continue
        nar_data = jra_nar[jg]
        total_n = sum(s["count"] for s in nar_data.values())
        if total_n < 3:
            continue
        lines.append(f"\n  --- JRA {jg}クラス → NAR各会場 ---")
        lines.append(f"    {'会場':>6s}  件数  勝率    三連率  平均着順  平均相対着順  主なクラス")
        for vc in sorted(nar_data.keys(), key=lambda x: -nar_data[x]["count"]):
            s = nar_data[vc]
            if s["count"] < 2:
                continue
            win_rate = s["wins"] / s["count"] * 100
            place_rate = s["place3"] / s["count"] * 100
            avg_fin = sum(s["avg_finish"]) / len(s["avg_finish"])
            avg_rel = sum(s["avg_rel_pos"]) / len(s["avg_rel_pos"])
            # 主なクラス
            grade_counts = defaultdict(int)
            for g in s["nar_grades"]:
                grade_counts[g] += 1
            top_grades = sorted(grade_counts.items(), key=lambda x: -x[1])[:3]
            grade_str = ", ".join(f"{g}({c})" for g, c in top_grades)
            lines.append(f"    {vname(vc):>6s}  {s['count']:4d}  {win_rate:5.1f}%  {place_rate:5.1f}%  "
                         f"{avg_fin:6.1f}    {avg_rel:.3f}        {grade_str}")

    # --- 門別→各地 ---
    lines.append("\n" + "=" * 60)
    lines.append("6. 門別 → 各地への移籍分析")
    lines.append("=" * 60)

    if monbetsu:
        lines.append(f"    {'移籍先':>6s}  件数  勝率    三連率  平均着順  平均相対着順  主な移籍後クラス")
        for vc in sorted(monbetsu.keys(), key=lambda x: -monbetsu[x]["count"]):
            s = monbetsu[vc]
            if s["count"] < 3:
                continue
            win_rate = s["wins"] / s["count"] * 100
            place_rate = s["place3"] / s["count"] * 100
            avg_fin = sum(s["avg_finish"]) / len(s["avg_finish"])
            avg_rel = sum(s["avg_rel_pos"]) / len(s["avg_rel_pos"])
            grade_counts = defaultdict(int)
            for g in s["dest_grade"]:
                grade_counts[g] += 1
            top_grades = sorted(grade_counts.items(), key=lambda x: -x[1])[:3]
            grade_str = ", ".join(f"{g}({c})" for g, c in top_grades)
            lines.append(f"    {vname(vc):>6s}  {s['count']:4d}  {win_rate:5.1f}%  {place_rate:5.1f}%  "
                         f"{avg_fin:6.1f}    {avg_rel:.3f}        {grade_str}")
    else:
        lines.append("    該当データなし")

    # --- まとめ ---
    lines.append("\n" + "=" * 60)
    lines.append("7. サマリ・考察")
    lines.append("=" * 60)

    # NAR会場レベルランキング
    if ratings:
        sorted_venues = sorted(ratings.keys(), key=lambda x: -ratings[x])
        lines.append("\n  NAR会場レベル推定ランキング（移籍成績ベース）:")
        for i, vc in enumerate(sorted_venues, 1):
            base = ratings.get("44", 0)
            normalized = 100 + (ratings[vc] - base) * 200
            lines.append(f"    {i:2d}. {vname(vc):>6s}  (レーティング: {normalized:.1f})")

    return "\n".join(lines)


def main():
    print("データベース接続...")
    conn = sqlite3.connect(str(DB_PATH))

    print("データ読み込み...")
    data = load_data(conn)
    print(f"  有効レコード数: {len(data):,}")

    print("馬ごとにグループ化...")
    horses = group_by_horse(data)
    print(f"  馬数: {len(horses):,}")

    print("会場ペア別移籍分析...")
    pair_stats = analyze_venue_pair_transfers(horses)
    print(f"  会場ペア数: {len(pair_stats)}")

    print("クラス変動追跡...")
    class_trans = analyze_class_transitions(horses)
    print(f"  クラス移行パターン数: {len(class_trans)}")

    print("レベル係数算出...")
    coefficients = compute_level_coefficients(pair_stats)
    print(f"  有効係数ペア数: {len(coefficients)}")

    print("JRA→NAR移籍分析...")
    jra_nar = analyze_jra_to_nar(horses)

    print("門別→各地移籍分析...")
    monbetsu = analyze_monbetsu_transfers(horses)

    print("レポート生成...")
    report = format_report(pair_stats, class_trans, coefficients, jra_nar, monbetsu, horses)

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        f.write(report)

    print(f"\n完了: {OUTPUT_PATH}")
    print(f"レポート行数: {len(report.splitlines())}")

    conn.close()


if __name__ == "__main__":
    main()
