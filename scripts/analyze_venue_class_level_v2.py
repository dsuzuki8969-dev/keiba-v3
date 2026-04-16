#!/usr/bin/env python
"""
NAR各会場×クラスの世代別JRA相当値分析 + 突出馬検出 + 交流重賞成績。

v1（analyze_venue_class_level.py）を拡張:
  - 分析1: 世代別（2歳/3歳/古馬）のクラスレベル比較
  - 分析2: 突出馬の検出（Z-score < -2.0）
  - 分析3: 交流重賞での会場別成績

出力: data/analysis/venue_class_level_v2.txt
"""

import sqlite3
import sys
import os
import re
import statistics
from collections import defaultdict
from pathlib import Path

# プロジェクトルートをパスに追加
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

DB_PATH = PROJECT_ROOT / "data" / "keiba.db"
OUTPUT_PATH = PROJECT_ROOT / "data" / "analysis" / "venue_class_level_v2.txt"

# --- 定数 ---

VENUE_CODE_TO_NAME = {
    "01": "札幌", "02": "函館", "03": "福島", "04": "新潟",
    "05": "東京", "06": "中山", "07": "中京", "08": "京都",
    "09": "阪神", "10": "小倉",
    "30": "門別", "35": "盛岡", "36": "水沢", "42": "浦和",
    "43": "船橋", "44": "大井", "45": "川崎", "46": "金沢",
    "47": "笠松", "48": "名古屋", "50": "園田", "51": "姫路",
    "54": "高知", "55": "佐賀", "65": "帯広",
}

JRA_VENUE_CODES = frozenset(["01", "02", "03", "04", "05", "06", "07", "08", "09", "10"])

# JRAグレードのスコア（1勝=0基準）
JRA_GRADE_SCORE = {
    "新馬": -2.0, "未勝利": -1.0, "1勝": 0.0, "500万": 0.0,
    "2勝": 1.0, "1000万": 1.0, "3勝": 2.0, "1600万": 2.0,
    "OP": 3.0, "L": 3.0, "G3": 4.0, "G2": 5.0, "G1": 6.0,
}

# NAR主要会場（表示順）
MAIN_NAR_VENUES = [
    "大井", "船橋", "川崎", "浦和", "園田", "姫路",
    "名古屋", "笠松", "金沢", "門別", "盛岡", "水沢", "高知", "佐賀",
]

JRA_VENUE_NAMES = frozenset(["札幌", "函館", "福島", "新潟", "東京", "中山", "中京", "京都", "阪神", "小倉"])


# --- NARクラスパーサー ---

def parse_nar_class(race_name: str) -> str:
    """race_nameからNARクラスを抽出"""
    cn = race_name.strip()
    if not cn:
        return ""

    # 重賞系
    if re.search(r"Jpn\s*[123]|JPN|JpnI", cn, re.IGNORECASE):
        return "交流重賞"
    if re.search(r"重賞", cn):
        return "重賞"

    # A系
    if "A1" in cn or "Ａ１" in cn:
        return "A1"
    if re.search(r"A[2-4]|Ａ[２-４]", cn):
        return "A2"
    if re.search(r"A[5-9]|Ａ[５-９]", cn):
        return "A2"
    if re.search(r"\(A\)|\bA\b級|Aクラス", cn) or cn.endswith("(A)"):
        return "A2"

    # B系
    if "B1" in cn or "Ｂ１" in cn:
        return "B1"
    if "B2" in cn or "Ｂ２" in cn:
        return "B2"
    if "B3" in cn or "Ｂ３" in cn:
        return "B3"
    if re.search(r"B[4-9]|Ｂ[４-９]", cn):
        return "B3"
    if re.search(r"\(B\)|\bB\b級|Bクラス", cn) or cn.endswith("(B)"):
        return "B2"

    # C系
    if re.search(r"C[4-9][^0-9]|C[4-9]$|Ｃ[４-９]", cn):
        return "C4"
    if re.search(r"C1[ーー\-\s]?\d|C1[^\d]|C1$|Ｃ１", cn):
        return "C1"
    if re.search(r"C2[ーー\-\s]?\d|C2[^\d]|C2$|Ｃ２", cn):
        return "C2"
    if re.search(r"C3[ーー\-\s]?\d|C3[^\d]|C3$|Ｃ３", cn):
        return "C3"
    if re.search(r"\(C\)|\bC\b級|Cクラス", cn) or cn.endswith("(C)"):
        return "C2"

    # 混合クラス
    mixed = re.search(r"\(([ABC]\d)([ABC]\d)\)", cn)
    if mixed:
        return mixed.group(1)

    # OP
    if re.search(r"\bOP\b|オープン|OP$", cn):
        return "OP"

    # 新馬/未勝利
    if "新馬" in cn or "デビュー" in cn:
        return "新馬"
    if "未勝利" in cn or "未格付" in cn:
        return "未勝利"

    # 世代戦（クラス指定なし）
    if re.search(r"[23]歳", cn):
        return "3歳"

    return ""


def classify_generation(race_name: str, age: int, race_date: str) -> str:
    """レースの世代カテゴリを判別。

    返り値: "2歳", "3歳", "古馬", ""（判別不能）
    """
    cn = race_name.strip()

    # 2歳戦: race_nameに"2歳"を含む or ageが2
    if "2歳" in cn:
        return "2歳"
    if age == 2:
        return "2歳"

    # 3歳限定戦: "(3歳)"を含むか、"3歳"を含みつつ"以上"を含まない
    if "(3歳)" in cn:
        return "3歳"
    if "3歳" in cn and "以上" not in cn:
        # "3歳C1", "3歳B1" 等
        return "3歳"

    # "3歳以上" → 古馬戦
    # クラス指定 (A/B/C系、OP、重賞) → 古馬戦
    nar_class = parse_nar_class(cn)
    if nar_class in ("A1", "A2", "B1", "B2", "B3", "C1", "C2", "C3", "C4",
                      "OP", "重賞", "交流重賞"):
        return "古馬"

    if "以上" in cn:
        return "古馬"

    # 新馬/未勝利 → 世代混合として扱う
    if nar_class in ("新馬", "未勝利"):
        if age == 2:
            return "2歳"
        elif age == 3:
            return "3歳"
        return "古馬"

    return ""


# NARクラスのソート順序
NAR_CLASS_ORDER = ["重賞", "交流重賞", "OP", "A1", "A2", "B1", "B2", "B3",
                   "C1", "C2", "C3", "C4", "3歳", "新馬", "未勝利"]


def nar_class_sort_key(cls: str) -> int:
    if cls in NAR_CLASS_ORDER:
        return NAR_CLASS_ORDER.index(cls)
    return 99


def score_to_jra_label(score: float) -> str:
    """スコアをJRA相当クラスラベルに変換"""
    if score >= 5.5:
        return "G1"
    elif score >= 4.5:
        return "G2"
    elif score >= 3.5:
        return "G3"
    elif score >= 2.5:
        return "OP"
    elif score >= 1.5:
        return "3勝"
    elif score >= 0.5:
        return "2勝"
    elif score >= -0.5:
        return "1勝"
    elif score >= -1.5:
        return "未勝利"
    else:
        return "新馬"


def score_to_jra_range(score: float) -> str:
    """スコアをJRA相当クラスの範囲ラベルに変換"""
    if score >= 5.0:
        return "G1~G2"
    elif score >= 4.0:
        return "G2~G3"
    elif score >= 3.0:
        return "G3~OP"
    elif score >= 2.5:
        return "OP"
    elif score >= 2.0:
        return "OP~3勝"
    elif score >= 1.5:
        return "3勝~2勝"
    elif score >= 1.0:
        return "2勝"
    elif score >= 0.5:
        return "2勝~1勝"
    elif score >= 0.0:
        return "1勝"
    elif score >= -0.5:
        return "1勝~未勝利"
    elif score >= -1.0:
        return "未勝利"
    elif score >= -1.5:
        return "未勝利~新馬"
    else:
        return "新馬以下"


def reliability_mark(n: int) -> str:
    if n >= 100:
        return "◎"
    elif n >= 50:
        return "○"
    elif n >= 20:
        return "△"
    return "×"


# =======================================================================
# メイン
# =======================================================================

def main():
    print("=" * 70)
    print("NAR会場×クラス 世代別JRA相当値分析 + 突出馬検出")
    print("=" * 70)

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    lines = []

    def w(text=""):
        lines.append(text)
        print(text)

    w("=" * 100)
    w("NAR会場×クラス 世代別JRA相当値分析レポート (v2)")
    w("=" * 100)

    # --- 分析1: 世代別クラスレベル ---
    print("\n[分析1] 世代別クラスレベル分析中...")
    gen_results = analyze_generation_class_level(conn)
    output_generation_results(w, gen_results)

    # --- 分析2: 突出馬の検出 ---
    print("\n[分析2] 突出馬検出中...")
    outlier_results = analyze_outlier_horses(conn)
    output_outlier_results(w, outlier_results, conn)

    # --- 分析3: 交流重賞での会場別成績 ---
    print("\n[分析3] 交流重賞での会場別成績分析中...")
    exchange_results = analyze_exchange_graded(conn)
    output_exchange_results(w, exchange_results)

    # ファイル出力
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    conn.close()
    print(f"\n完了: {OUTPUT_PATH}")


# =======================================================================
# 分析1: 世代別クラスレベル
# =======================================================================

def analyze_generation_class_level(conn):
    """世代別（2歳/3歳/古馬）にJRA↔NAR移籍馬の成績を分析。"""
    cur = conn.cursor()

    # JRA/NAR両走馬を取得
    cur.execute("""
        SELECT DISTINCT rl_nar.horse_id
        FROM race_log rl_nar
        INNER JOIN race_log rl_jra ON rl_nar.horse_id = rl_jra.horse_id
        WHERE rl_nar.is_jra = 0
          AND rl_jra.is_jra = 1
          AND rl_nar.horse_id != ''
          AND rl_nar.finish_pos > 0
          AND rl_jra.finish_pos > 0
    """)
    transfer_ids = [r[0] for r in cur.fetchall()]
    print(f"  移籍馬数: {len(transfer_ids)}")

    if not transfer_ids:
        return {}

    # NAR成績取得
    nar_records = []
    jra_records = defaultdict(list)

    batch_size = 999
    for i in range(0, len(transfer_ids), batch_size):
        batch = transfer_ids[i:i + batch_size]
        ph = ",".join(["?"] * len(batch))

        cur.execute(f"""
            SELECT horse_id, venue_code, race_name, finish_pos, field_count,
                   race_date, age
            FROM race_log
            WHERE is_jra = 0
              AND horse_id IN ({ph})
              AND finish_pos > 0
              AND field_count > 0
              AND status IS NULL
        """, batch)
        nar_records.extend(cur.fetchall())

        cur.execute(f"""
            SELECT horse_id, grade, finish_pos, field_count, race_date
            FROM race_log
            WHERE is_jra = 1
              AND horse_id IN ({ph})
              AND finish_pos > 0
              AND field_count > 0
              AND grade != ''
              AND status IS NULL
        """, batch)
        for r in cur.fetchall():
            jra_records[r[0]].append((r[1], r[2], r[3], r[4]))

    print(f"  NARレコード数: {len(nar_records)}, JRA馬数: {len(jra_records)}")

    # 世代別に集計
    # {generation: {(venue, nar_class): {horse_id: {"jra_grades": [], ...}}}}
    gen_data = defaultdict(lambda: defaultdict(lambda: defaultdict(
        lambda: {"jra_grades": [], "jra_rel_pos": []})))

    for rec in nar_records:
        horse_id = rec[0]
        venue_code = rec[1]
        race_name = rec[2]
        age = rec[6] if rec[6] else 0
        race_date = rec[5] or ""

        nar_class = parse_nar_class(race_name)
        if not nar_class:
            continue
        venue = VENUE_CODE_TO_NAME.get(venue_code, venue_code)
        if venue == "帯広":
            continue

        gen = classify_generation(race_name, age, race_date)
        if not gen:
            continue

        jra_runs = jra_records.get(horse_id, [])
        if not jra_runs:
            continue

        hdata = gen_data[gen][(venue, nar_class)][horse_id]
        if not hdata["jra_grades"]:
            for jra_grade, jra_pos, jra_fc, jra_date in jra_runs:
                score = JRA_GRADE_SCORE.get(jra_grade)
                if score is not None:
                    hdata["jra_grades"].append(score)
                    hdata["jra_rel_pos"].append(jra_pos / jra_fc)

    # 集計
    results = {}  # {generation: {(venue, cls): {score, n_horses, ...}}}
    for gen, venue_cls_horses in gen_data.items():
        gen_summary = {}
        for (venue, cls), horses_data in venue_cls_horses.items():
            horse_avg_grades = []
            for horse_id, hdata in horses_data.items():
                if hdata["jra_grades"]:
                    horse_avg_grades.append(statistics.mean(hdata["jra_grades"]))

            n_horses = len(horse_avg_grades)
            if n_horses < 3:
                continue

            avg_grade = statistics.mean(horse_avg_grades)
            gen_summary[(venue, cls)] = {
                "score": avg_grade,
                "n_horses": n_horses,
            }
        results[gen] = gen_summary

    return results


def output_generation_results(w, gen_results):
    """分析1の出力: セクション1A + セクション1B"""

    w("\n" + "=" * 100)
    w("セクション1A: 世代別クラスレベル")
    w("=" * 100)

    for gen_name in ["2歳", "3歳", "古馬"]:
        summary = gen_results.get(gen_name, {})
        if not summary:
            w(f"\n【{gen_name}戦】 データなし")
            continue

        w(f"\n【{gen_name}戦】")
        w(f"{'会場':<6} {'クラス':<12} {'スコア':>6} {'JRA相当':<14} {'サンプル':>8} {'信頼性':<6}")
        w("-" * 70)

        # 会場ごと
        venues_in = sorted(set(v for v, c in summary.keys()))
        nar_venues = [v for v in MAIN_NAR_VENUES if v in set(venues_in)]

        for venue in nar_venues:
            items = [(k, v) for k, v in summary.items() if k[0] == venue]
            items.sort(key=lambda x: nar_class_sort_key(x[0][1]))
            for (v, cls), data in items:
                score = data["score"]
                n = data["n_horses"]
                jra = score_to_jra_range(score)
                rel = reliability_mark(n)
                w(f"{v:<6} {cls:<12} {score:>+6.2f} {jra:<14} {n:>8} {rel:<6}")
            if items:
                w()

    # --- セクション1B: 世代別クロス比較 ---
    w("\n" + "=" * 100)
    w("セクション1B: 世代別クロス比較")
    w("=" * 100)

    for gen_name in ["古馬", "3歳", "2歳"]:
        summary = gen_results.get(gen_name, {})
        if not summary:
            continue

        # このカテゴリ内のクラスを収集
        classes = sorted(set(c for v, c in summary.keys()), key=nar_class_sort_key)

        for cls in classes:
            cls_items = [(k, v) for k, v in summary.items() if k[1] == cls]
            if len(cls_items) < 2:
                continue

            cls_items.sort(key=lambda x: x[1]["score"], reverse=True)

            w(f"\n【{gen_name} {cls}クラス比較】")
            w(f"{'会場':<8} {'スコア':>8} {'JRA相当':<14} {'サンプル':>8}")
            w("-" * 50)

            for (venue, _), data in cls_items:
                score = data["score"]
                n = data["n_horses"]
                jra = score_to_jra_range(score)
                w(f"{venue:<8} {score:>+8.2f} {jra:<14} {n:>8}")


# =======================================================================
# 分析2: 突出馬の検出
# =======================================================================

def analyze_outlier_horses(conn):
    """各会場×クラスでZ-score < -2.0の突出馬を検出（過去3年）。"""
    cur = conn.cursor()

    # 過去3年のNAR成績を取得
    cur.execute("""
        SELECT horse_id, horse_name, venue_code, race_name, finish_pos,
               field_count, race_date
        FROM race_log
        WHERE is_jra = 0
          AND finish_pos > 0
          AND field_count > 0
          AND race_date >= '2023-01-01'
          AND status IS NULL
          AND venue_code != '65'
    """)
    nar_rows = cur.fetchall()
    print(f"  NARレコード数(3年): {len(nar_rows)}")

    # 各会場×クラスごとの馬別成績
    # {(venue, cls): {horse_id: {"name": str, "rel_positions": [float], "races": int}}}
    venue_cls_horses = defaultdict(lambda: defaultdict(
        lambda: {"name": "", "rel_positions": [], "races": 0, "wins": 0}))

    for row in nar_rows:
        horse_id = row[0]
        horse_name = row[1] or horse_id
        venue_code = row[2]
        race_name = row[3]
        finish_pos = row[4]
        field_count = row[5]

        cls = parse_nar_class(race_name)
        if not cls:
            continue
        venue = VENUE_CODE_TO_NAME.get(venue_code, venue_code)

        d = venue_cls_horses[(venue, cls)][horse_id]
        d["name"] = horse_name
        rel = finish_pos / field_count
        d["rel_positions"].append(rel)
        d["races"] += 1
        if finish_pos == 1:
            d["wins"] += 1

    # Z-score計算して突出馬を検出
    # 最低3走以上の馬のみ対象
    outliers = defaultdict(list)  # venue -> [(horse_id, name, cls, avg_rel, z, win_rate, races)]

    for (venue, cls), horses in venue_cls_horses.items():
        # このクラスの全馬の平均相対着順を集める（最低3走のみ）
        horse_avgs = []
        horse_info = []
        for hid, d in horses.items():
            if d["races"] >= 3:
                avg = statistics.mean(d["rel_positions"])
                horse_avgs.append(avg)
                horse_info.append((hid, d["name"], avg, d["wins"], d["races"]))

        if len(horse_avgs) < 10:
            continue

        mean_val = statistics.mean(horse_avgs)
        std_val = statistics.stdev(horse_avgs)
        if std_val < 0.01:
            continue

        for hid, name, avg, wins, races in horse_info:
            z = (avg - mean_val) / std_val
            if z < -2.0:
                win_rate = wins / races if races > 0 else 0
                outliers[venue].append({
                    "horse_id": hid,
                    "name": name,
                    "cls": cls,
                    "avg_rel": avg,
                    "z": z,
                    "win_rate": win_rate,
                    "races": races,
                    "wins": wins,
                })

    return outliers


def output_outlier_results(w, outliers, conn):
    """分析2の出力: セクション2 + セクション3"""
    cur = conn.cursor()

    w("\n" + "=" * 100)
    w("セクション2: 各会場の突出馬（過去3年、相対着順 Z < -2.0）")
    w("=" * 100)

    # 突出馬のJRA/NAR他会場成績を取得するための準備
    all_outlier_ids = set()
    for venue, horses in outliers.items():
        for h in horses:
            all_outlier_ids.add(h["horse_id"])

    # JRA成績
    jra_perf = defaultdict(list)
    # NAR他会場成績
    nar_other_perf = defaultdict(list)

    if all_outlier_ids:
        id_list = list(all_outlier_ids)
        for i in range(0, len(id_list), 999):
            batch = id_list[i:i + 999]
            ph = ",".join(["?"] * len(batch))

            cur.execute(f"""
                SELECT horse_id, grade, finish_pos, field_count, race_name
                FROM race_log
                WHERE is_jra = 1
                  AND horse_id IN ({ph})
                  AND finish_pos > 0
                  AND field_count > 0
                  AND grade != ''
                  AND status IS NULL
            """, batch)
            for r in cur.fetchall():
                jra_perf[r[0]].append({
                    "grade": r[1], "pos": r[2], "fc": r[3], "name": r[4]
                })

            cur.execute(f"""
                SELECT horse_id, venue_code, race_name, finish_pos, field_count
                FROM race_log
                WHERE is_jra = 0
                  AND horse_id IN ({ph})
                  AND finish_pos > 0
                  AND field_count > 0
                  AND status IS NULL
                  AND race_date >= '2023-01-01'
            """, batch)
            for r in cur.fetchall():
                venue_name = VENUE_CODE_TO_NAME.get(r[1], r[1])
                nar_other_perf[r[0]].append({
                    "venue": venue_name, "race_name": r[2],
                    "pos": r[3], "fc": r[4]
                })

    # 岩手（盛岡/水沢）はまとめて表示
    venue_groups = {
        "岩手（盛岡/水沢）": ["盛岡", "水沢"],
    }
    # その他の会場はそのまま
    all_venues = set()
    for v in outliers.keys():
        all_venues.add(v)
    grouped_venues = set()
    for g, vs in venue_groups.items():
        for v in vs:
            grouped_venues.add(v)

    display_order = []
    for v in MAIN_NAR_VENUES:
        if v in grouped_venues:
            continue
        if v in all_venues:
            display_order.append(("■ " + v, [v]))
    # 岩手グループ
    if "盛岡" in all_venues or "水沢" in all_venues:
        display_order.insert(
            next((i for i, (label, _) in enumerate(display_order)
                  if "金沢" in label), len(display_order)),
            ("■ 岩手（盛岡/水沢）", ["盛岡", "水沢"])
        )

    for label, venue_list in display_order:
        # この会場グループの突出馬を集める
        group_horses = []
        for v in venue_list:
            if v in outliers:
                group_horses.extend(outliers[v])

        if not group_horses:
            continue

        # Z-scoreの小さい順（より突出している順）
        group_horses.sort(key=lambda x: x["z"])

        w(f"\n{label}")
        for h in group_horses[:10]:  # 上位10頭まで
            win_pct = h["win_rate"] * 100
            line = (f"  {h['name']}: {h['cls']} 相対着順{h['avg_rel']:.2f}"
                    f"(Z={h['z']:.1f}), 勝率{win_pct:.0f}%({h['wins']}勝/{h['races']}走)")

            # JRA成績
            jra = jra_perf.get(h["horse_id"], [])
            if jra:
                # 最高グレードでの成績
                grade_order = ["G1", "G2", "G3", "OP", "2勝", "1勝", "未勝利", "新馬"]
                best_grade = None
                best_pos = None
                for go in grade_order:
                    grade_runs = [r for r in jra if r["grade"] == go]
                    if grade_runs:
                        best_grade = go
                        best_pos = min(r["pos"] for r in grade_runs)
                        break
                if best_grade:
                    line += f", JRA: {best_grade} {best_pos}着"

            # NAR他会場成績（突出馬の所属会場以外で交流重賞等）
            nar_other = nar_other_perf.get(h["horse_id"], [])
            other_venues = set()
            for nr in nar_other:
                if nr["venue"] not in venue_list:
                    rn_cls = parse_nar_class(nr["race_name"])
                    if rn_cls in ("交流重賞", "重賞"):
                        other_venues.add(f"{nr['venue']}{rn_cls} {nr['pos']}着")
            if other_venues:
                line += ", " + " / ".join(list(other_venues)[:3])

            w(line)

    # --- セクション3: 突出馬統計サマリ ---
    w("\n" + "=" * 100)
    w("セクション3: 突出馬統計サマリ")
    w("=" * 100)
    w(f"{'会場':<8} {'クラス':<8} {'突出馬数':>8}  突出馬名（上位3頭）")
    w("-" * 80)

    # 会場×クラスごとに集計
    venue_cls_outliers = defaultdict(list)
    for venue, horses in outliers.items():
        for h in horses:
            venue_cls_outliers[(venue, h["cls"])].append(h)

    for venue in MAIN_NAR_VENUES:
        items = [(k, v) for k, v in venue_cls_outliers.items() if k[0] == venue]
        items.sort(key=lambda x: nar_class_sort_key(x[0][1]))
        for (v, cls), horses in items:
            horses_sorted = sorted(horses, key=lambda x: x["z"])
            top3 = ", ".join(h["name"] for h in horses_sorted[:3])
            w(f"{v:<8} {cls:<8} {len(horses):>8}  {top3}")


# =======================================================================
# 分析3: 交流重賞での会場別成績
# =======================================================================

def analyze_exchange_graded(conn):
    """交流重賞での各会場馬の成績を集計。

    会場判定は、その馬が直近でどの会場を主戦場としているかを
    race_logのNARレコードから推定する。
    """
    cur = conn.cursor()

    # 交流重賞レースを取得
    cur.execute("""
        SELECT horse_id, horse_name, venue_code, finish_pos, field_count,
               race_name, race_date, is_jra
        FROM race_log
        WHERE (grade = '交流重賞' OR race_name LIKE '%Jpn%')
          AND finish_pos > 0
          AND field_count > 0
          AND status IS NULL
    """)
    exchange_rows = cur.fetchall()
    print(f"  交流重賞レコード数: {len(exchange_rows)}")

    if not exchange_rows:
        return {}

    # 各馬の主戦場を判定
    # JRA出走数とNAR出走数を比較し、JRA主体ならJRA所属と判定
    horse_ids = list(set(r[0] for r in exchange_rows if r[0]))
    horse_home = {}  # horse_id -> venue_name

    for i in range(0, len(horse_ids), 999):
        batch = horse_ids[i:i + 999]
        ph = ",".join(["?"] * len(batch))

        # JRA出走数を取得
        cur.execute(f"""
            SELECT horse_id, COUNT(*) as cnt
            FROM race_log
            WHERE horse_id IN ({ph})
              AND is_jra = 1
              AND finish_pos > 0
            GROUP BY horse_id
        """, batch)
        jra_counts = {r[0]: r[1] for r in cur.fetchall()}

        # NAR出走（交流重賞を除くNAR条件戦）の会場分布を取得
        cur.execute(f"""
            SELECT horse_id, venue_code, race_date
            FROM race_log
            WHERE horse_id IN ({ph})
              AND is_jra = 0
              AND venue_code != '65'
              AND finish_pos > 0
              AND grade NOT IN ('交流重賞')
            ORDER BY race_date DESC
        """, batch)

        horse_venue_counts = defaultdict(lambda: defaultdict(int))
        horse_nar_total = defaultdict(int)
        horse_venue_limit = defaultdict(int)
        for r in cur.fetchall():
            hid = r[0]
            if horse_venue_limit[hid] >= 30:
                continue
            horse_venue_limit[hid] += 1
            horse_nar_total[hid] += 1
            vc = VENUE_CODE_TO_NAME.get(r[1], r[1])
            horse_venue_counts[hid][vc] += 1

        for hid in batch:
            jra_n = jra_counts.get(hid, 0)
            nar_n = horse_nar_total.get(hid, 0)

            # JRA出走がNAR条件戦の2倍以上 → JRA所属
            if jra_n > 0 and (nar_n == 0 or jra_n >= nar_n * 2):
                horse_home[hid] = "JRA"
            elif hid in horse_venue_counts and horse_venue_counts[hid]:
                horse_home[hid] = max(horse_venue_counts[hid],
                                       key=horse_venue_counts[hid].get)
            elif jra_n > 0:
                horse_home[hid] = "JRA"

    # 会場別に集計
    venue_stats = defaultdict(lambda: {"runs": 0, "wins": 0, "top3": 0, "pos_sum": 0})

    for r in exchange_rows:
        hid = r[0]
        pos = r[3]
        fc = r[4]

        home = horse_home.get(hid, "不明")
        d = venue_stats[home]
        d["runs"] += 1
        d["pos_sum"] += pos
        if pos == 1:
            d["wins"] += 1
        if pos <= 3:
            d["top3"] += 1

    return venue_stats


def output_exchange_results(w, exchange_results):
    """分析3の出力"""
    w("\n" + "=" * 100)
    w("セクション4: 交流重賞での会場別成績")
    w("=" * 100)
    w(f"{'会場':<10} {'出走数':>8} {'勝率':>8} {'三連率':>8} {'平均着順':>8}")
    w("-" * 55)

    # 出走数の多い順にソート（JRAは先頭に）
    items = sorted(exchange_results.items(),
                   key=lambda x: (0 if x[0] == "JRA" else 1, -x[1]["runs"]))

    for venue, d in items:
        if d["runs"] < 5 or venue == "不明":
            continue
        win_rate = d["wins"] / d["runs"] * 100
        top3_rate = d["top3"] / d["runs"] * 100
        avg_pos = d["pos_sum"] / d["runs"]
        w(f"{venue:<10} {d['runs']:>8} {win_rate:>7.1f}% {top3_rate:>7.1f}% {avg_pos:>8.1f}")


if __name__ == "__main__":
    main()
