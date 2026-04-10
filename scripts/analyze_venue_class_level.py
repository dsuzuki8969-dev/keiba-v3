#!/usr/bin/env python
"""
NAR各会場×クラスのJRA相当値を実データから分析するスクリプト。

分析方法:
  1. JRA↔NAR移籍馬のクラス別成績比較
  2. NAR会場間移籍の同一馬クラス別成績比較
  3. 走破タイムの標準化比較（同距離・同面）

出力: data/analysis/venue_class_level.txt
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
OUTPUT_PATH = PROJECT_ROOT / "data" / "analysis" / "venue_class_level.txt"

# --- 定数 ---

VENUE_CODE_TO_NAME = {
    "01": "福島", "02": "新潟", "03": "札幌", "04": "函館",
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

# NARクラスの大分類マッピング
def parse_nar_class(race_name: str) -> str:
    """race_nameからNARクラスを抽出。返せない場合は空文字"""
    cn = race_name.strip()
    if not cn:
        return ""

    # 重賞系を先にチェック
    if re.search(r"重賞", cn):
        return "重賞"
    if re.search(r"Jpn\s*[123]|JPN|交流", cn, re.IGNORECASE):
        return "交流重賞"

    # A系
    if "A1" in cn or "Ａ１" in cn:
        return "A1"
    if re.search(r"A[2-4]|Ａ[２-４]", cn):
        return "A2"
    if re.search(r"A[5-9]|Ａ[５-９]", cn):
        return "A2"  # A5以上もA2相当にまとめる
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
        return "B3"  # B4以下もB3相当
    if re.search(r"\(B\)|\bB\b級|Bクラス", cn) or cn.endswith("(B)"):
        return "B2"  # B指定のみはB2相当

    # C系 — 組番号に注意（C13=C1の3組、C15=C1の5組）
    # 先にC4以上を判定
    if re.search(r"C[4-9][^0-9]|C[4-9]$|Ｃ[４-９]", cn):
        return "C4"
    # C1の組番号パターン: C1ー5組, C1-11組 等
    if re.search(r"C1[ーー\-\s]?\d|C1[^\d]|C1$|Ｃ１", cn):
        return "C1"
    if re.search(r"C2[ーー\-\s]?\d|C2[^\d]|C2$|Ｃ２", cn):
        return "C2"
    if re.search(r"C3[ーー\-\s]?\d|C3[^\d]|C3$|Ｃ３", cn):
        return "C3"
    if re.search(r"\(C\)|\bC\b級|Cクラス", cn) or cn.endswith("(C)"):
        return "C2"

    # 混合クラス: (B2B3), (B2C1) 等 → 上位クラスを採用
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


# NARクラスのソート順序
NAR_CLASS_ORDER = ["重賞", "交流重賞", "OP", "A1", "A2", "B1", "B2", "B3",
                   "C1", "C2", "C3", "C4", "3歳", "新馬", "未勝利"]

def nar_class_sort_key(cls: str) -> int:
    if cls in NAR_CLASS_ORDER:
        return NAR_CLASS_ORDER.index(cls)
    return 99


def main():
    print("=" * 70)
    print("NAR会場×クラス JRA相当値分析")
    print("=" * 70)

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    # --- 方法1: JRA↔NAR移籍馬のクラス別成績 ---
    print("\n[方法1] JRA↔NAR移籍馬の成績分析中...")
    method1_results = analyze_transfer_horses(conn)

    # --- 方法2: NAR会場間移籍の同一馬成績 ---
    print("\n[方法2] NAR会場間移籍の成績分析中...")
    method2_results = analyze_nar_transfer(conn)

    # --- 方法3: 走破タイムの標準化比較 ---
    print("\n[方法3] 走破タイムの標準化比較中...")
    method3_results = analyze_time_comparison(conn)

    # --- 結果統合 ---
    print("\n結果を統合中...")
    merged = merge_results(method1_results, method2_results, method3_results)

    # --- 出力 ---
    output_results(merged, method1_results, method2_results, method3_results)

    conn.close()
    print(f"\n完了: {OUTPUT_PATH}")


def analyze_transfer_horses(conn):
    """
    方法1: JRA↔NAR移籍馬のクラス別成績分析。
    同一horse_idがJRAとNARの両方で走っている馬を対象に、
    NARの各会場×クラスごとに、その馬がJRAで走った時のグレード分布と相対着順を集計。
    """
    cur = conn.cursor()

    # JRA/NAR両走馬のhorse_idを取得
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
    transfer_horse_ids = [r[0] for r in cur.fetchall()]
    print(f"  移籍馬数: {len(transfer_horse_ids)}")

    if not transfer_horse_ids:
        return {}

    # NARレース成績を取得（バッチ処理）
    placeholders = ",".join(["?"] * min(len(transfer_horse_ids), 999))
    nar_records = []  # (horse_id, venue_code, race_name, finish_pos, field_count, race_date)
    jra_records = defaultdict(list)  # horse_id -> [(grade, finish_pos, field_count, race_date)]

    batch_size = 999
    for i in range(0, len(transfer_horse_ids), batch_size):
        batch = transfer_horse_ids[i:i+batch_size]
        ph = ",".join(["?"] * len(batch))

        # NAR成績
        cur.execute(f"""
            SELECT horse_id, venue_code, race_name, finish_pos, field_count, race_date
            FROM race_log
            WHERE is_jra = 0
              AND horse_id IN ({ph})
              AND finish_pos > 0
              AND field_count > 0
              AND status IS NULL
        """, batch)
        nar_records.extend(cur.fetchall())

        # JRA成績
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

    print(f"  NARレコード数: {len(nar_records)}, JRAレコード数: {sum(len(v) for v in jra_records.values())}")

    # NAR各会場×クラスごとに、その馬のJRA成績を集計
    # 馬ごとにJRA平均グレードを計算し、それを会場×クラスで集約
    # (1頭のJRA成績が多い馬の影響が過大にならないようにする)
    # {(venue_name, nar_class): {horse_id: {"jra_grades": [], "jra_rel_pos": []}}}
    horse_level_data = defaultdict(lambda: defaultdict(lambda: {"jra_grades": [], "jra_rel_pos": []}))

    for horse_id, venue_code, race_name, finish_pos, field_count, race_date in nar_records:
        nar_class = parse_nar_class(race_name)
        if not nar_class:
            continue
        venue_name = VENUE_CODE_TO_NAME.get(venue_code, venue_code)
        if venue_name == "帯広":  # ばんえい除外
            continue

        # この馬のJRA成績を取得
        jra_runs = jra_records.get(horse_id, [])
        if not jra_runs:
            continue

        key = (venue_name, nar_class)
        hdata = horse_level_data[key][horse_id]
        if not hdata["jra_grades"]:  # 初回のみJRA成績を追加（重複防止）
            for jra_grade, jra_pos, jra_fc, jra_date in jra_runs:
                score = JRA_GRADE_SCORE.get(jra_grade)
                if score is not None:
                    hdata["jra_grades"].append(score)
                    hdata["jra_rel_pos"].append(jra_pos / jra_fc)

    # 集計: 馬ごとの平均 → 全体平均
    summary = {}
    for (venue, cls), horses_data in horse_level_data.items():
        horse_avg_grades = []
        horse_avg_rel_pos = []
        for horse_id, hdata in horses_data.items():
            if hdata["jra_grades"]:
                horse_avg_grades.append(statistics.mean(hdata["jra_grades"]))
                horse_avg_rel_pos.append(statistics.mean(hdata["jra_rel_pos"]))

        n_horses = len(horse_avg_grades)
        if n_horses < 5:
            continue

        avg_grade = statistics.mean(horse_avg_grades)
        med_grade = statistics.median(horse_avg_grades)
        avg_rel_pos = statistics.mean(horse_avg_rel_pos)
        summary[(venue, cls)] = {
            "n_horses": n_horses,
            "n_samples": n_horses,  # 馬数ベースに変更
            "avg_jra_grade": avg_grade,
            "med_jra_grade": med_grade,
            "avg_jra_rel_pos": avg_rel_pos,
        }

    return summary


def analyze_nar_transfer(conn):
    """
    方法2: NAR会場間移籍の同一馬成績比較。
    異なるNAR会場で走った同一馬について、移籍前後の相対着順変化を分析。
    """
    cur = conn.cursor()

    # NAR複数会場で走った馬
    cur.execute("""
        SELECT horse_id, COUNT(DISTINCT venue_code) as n_venues
        FROM race_log
        WHERE is_jra = 0 AND horse_id != '' AND finish_pos > 0
          AND venue_code NOT IN ('65')
        GROUP BY horse_id
        HAVING n_venues >= 2
    """)
    multi_venue_horses = [r[0] for r in cur.fetchall()]
    print(f"  NAR複数会場走行馬: {len(multi_venue_horses)}")

    if not multi_venue_horses:
        return {}

    # 全NAR成績を取得
    all_nar = defaultdict(list)
    batch_size = 999
    for i in range(0, len(multi_venue_horses), batch_size):
        batch = multi_venue_horses[i:i+batch_size]
        ph = ",".join(["?"] * len(batch))
        cur.execute(f"""
            SELECT horse_id, venue_code, race_name, finish_pos, field_count, race_date
            FROM race_log
            WHERE is_jra = 0
              AND horse_id IN ({ph})
              AND finish_pos > 0
              AND field_count > 0
              AND venue_code != '65'
              AND status IS NULL
        """, batch)
        for r in cur.fetchall():
            all_nar[r[0]].append({
                "venue_code": r[1], "race_name": r[2],
                "finish_pos": r[3], "field_count": r[4], "race_date": r[5],
            })

    # 各馬の会場×クラスごとの平均相対着順を計算
    # horse_perf[horse_id][(venue, cls)] = avg_rel_pos
    horse_perf = {}
    for horse_id, runs in all_nar.items():
        venue_cls_runs = defaultdict(list)
        for run in runs:
            cls = parse_nar_class(run["race_name"])
            if not cls:
                continue
            venue = VENUE_CODE_TO_NAME.get(run["venue_code"], run["venue_code"])
            venue_cls_runs[(venue, cls)].append(run["finish_pos"] / run["field_count"])
        if len(venue_cls_runs) >= 2:
            horse_perf[horse_id] = {k: statistics.mean(v) for k, v in venue_cls_runs.items()}

    print(f"  分析対象馬: {len(horse_perf)}")

    # ペア比較: 同一馬が (会場A, クラスX) と (会場B, クラスY) で走った場合、
    # 相対着順の差からクラスレベル差を推定
    # 基準: 大井C1 での相対着順を0として、他の会場×クラスとの差を計算
    pair_diffs = defaultdict(list)  # (venue, cls) -> [相対着順差 from 大井C1]

    for horse_id, perfs in horse_perf.items():
        keys = list(perfs.keys())
        for i in range(len(keys)):
            for j in range(i + 1, len(keys)):
                k1, k2 = keys[i], keys[j]
                # 相対着順の差を記録（各ペアに対して）
                diff = perfs[k1] - perfs[k2]
                pair_diffs[(k1, k2)].append(diff)
                pair_diffs[(k2, k1)].append(-diff)

    # 各会場×クラスの平均相対着順を計算（全体平均からの偏差）
    venue_cls_avg_pos = defaultdict(list)
    for horse_id, perfs in horse_perf.items():
        for k, v in perfs.items():
            venue_cls_avg_pos[k].append(v)

    summary = {}
    for key, positions in venue_cls_avg_pos.items():
        if len(positions) < 5:
            continue
        summary[key] = {
            "avg_rel_pos": statistics.mean(positions),
            "n_horses": len(positions),
        }

    return summary


def analyze_time_comparison(conn):
    """
    方法3: 走破タイムの標準化比較。
    主要距離（ダート1200m, 1400m, 1600m, 1800m, 2000m）の
    各会場×クラスの勝ち馬タイムを比較。
    """
    cur = conn.cursor()

    # 主要距離リスト
    target_distances = [1200, 1400, 1600, 1800, 2000]

    # JRA各クラスの基準タイム（ダート・良馬場・勝ち馬）
    jra_baseline = {}  # (distance, grade) -> avg_time
    for dist in target_distances:
        cur.execute("""
            SELECT grade, AVG(finish_time_sec) as avg_time, COUNT(*) as cnt
            FROM race_log
            WHERE is_jra = 1
              AND surface = 'ダート'
              AND distance = ?
              AND finish_pos = 1
              AND finish_time_sec > 0
              AND condition IN ('良', '')
              AND grade IN ('新馬', '未勝利', '1勝', '2勝', '3勝', 'OP')
            GROUP BY grade
            HAVING cnt >= 10
        """, (dist,))
        for r in cur.fetchall():
            jra_baseline[(dist, r[0])] = r[1]

    # NAR各会場×クラスの勝ち馬タイム
    nar_times = defaultdict(lambda: defaultdict(list))  # (venue, cls) -> {distance: [times]}

    cur.execute("""
        SELECT venue_code, race_name, distance, finish_time_sec, condition
        FROM race_log
        WHERE is_jra = 0
          AND surface = 'ダート'
          AND finish_pos = 1
          AND finish_time_sec > 0
          AND condition IN ('良', '')
          AND venue_code != '65'
          AND distance IN (1200, 1400, 1600, 1800, 2000)
    """)

    for vc, rn, dist, time_sec, cond in cur.fetchall():
        cls = parse_nar_class(rn)
        if not cls:
            continue
        venue = VENUE_CODE_TO_NAME.get(vc, vc)
        nar_times[(venue, cls)][dist].append(time_sec)

    # 各会場×クラスの JRA相当値を推定
    summary = {}
    for (venue, cls), dist_times in nar_times.items():
        jra_equiv_scores = []
        total_samples = 0
        dist_details = {}

        for dist, times in dist_times.items():
            if len(times) < 3:
                continue
            avg_time = statistics.mean(times)
            total_samples += len(times)

            # JRA各グレードとの距離を計算
            best_grade = None
            best_diff = float("inf")

            for grade in ["新馬", "未勝利", "1勝", "2勝", "3勝", "OP"]:
                jra_time = jra_baseline.get((dist, grade))
                if jra_time is None:
                    continue
                diff = abs(avg_time - jra_time)
                if diff < best_diff:
                    best_diff = diff
                    best_grade = grade

            # JRA基準間の補間でスコアを推定
            if best_grade:
                base_score = JRA_GRADE_SCORE[best_grade]
                jra_time = jra_baseline[(dist, best_grade)]
                time_diff = avg_time - jra_time  # 正=遅い=弱い

                # 隣接グレードとの差分で補間
                # 大体1クラス差で1-2秒の差がある
                # 1秒差 ≈ 0.5-1.0スコア差と推定
                score = base_score - time_diff * 0.5
                jra_equiv_scores.append(score)
                dist_details[dist] = {
                    "avg_time": avg_time,
                    "n": len(times),
                    "closest_jra": best_grade,
                    "time_diff": time_diff,
                    "score": score,
                }

        if jra_equiv_scores and total_samples >= 5:
            summary[(venue, cls)] = {
                "avg_score": statistics.mean(jra_equiv_scores),
                "n_distances": len(jra_equiv_scores),
                "n_samples": total_samples,
                "dist_details": dist_details,
            }

    return summary


def merge_results(method1, method2, method3):
    """3つの分析結果を統合してスコアを算出。

    方法1（移籍馬JRA成績）を主軸とする。
    方法3（タイム比較）はNAR/JRAの馬場差が大きすぎるため、
    方法1がない場合のフォールバックとしてのみ使用。
    方法3のタイムスコアは馬場差補正（各会場の最上位クラスを基準に
    相対化）を適用する。
    """
    all_keys = set()
    all_keys.update(method1.keys())
    all_keys.update(method2.keys())
    all_keys.update(method3.keys())

    # 方法3の馬場差補正: 各会場の上位クラスのM1-M3差分を計算し、
    # タイムスコアにオフセットを加える
    venue_time_offset = {}  # 会場 -> タイムスコアへのオフセット
    for key in all_keys:
        venue, cls = key
        if key in method1 and key in method3:
            m1_score = method1[key]["avg_jra_grade"]
            m3_score = method3[key]["avg_score"]
            if venue not in venue_time_offset:
                venue_time_offset[venue] = []
            venue_time_offset[venue].append(m1_score - m3_score)
    # 各会場の中央値をオフセットとする
    venue_offset_final = {}
    for venue, offsets in venue_time_offset.items():
        if len(offsets) >= 3:
            venue_offset_final[venue] = statistics.median(offsets)

    merged = {}
    for key in all_keys:
        venue, cls = key
        scores = []
        weights = []
        details = {}

        # 方法1: 移籍馬のJRAグレード平均（最も直接的・信頼性が高い）
        if key in method1:
            m1 = method1[key]
            scores.append(m1["avg_jra_grade"])
            w = min(m1["n_samples"] / 50, 5.0)  # サンプル数に応じた重み（最大5）
            weights.append(w)
            details["method1"] = m1

        # 方法2: 記録のみ（統合スコアには含めない）
        if key in method2:
            details["method2"] = method2[key]

        # 方法3: タイム比較（馬場差補正後、方法1がない場合のフォールバック）
        if key in method3:
            m3 = method3[key]
            offset = venue_offset_final.get(venue, 0)
            corrected_score = m3["avg_score"] + offset
            m3_copy = dict(m3)
            m3_copy["corrected_score"] = corrected_score
            m3_copy["offset"] = offset
            details["method3"] = m3_copy

            if key not in method1:
                # 方法1がない場合のみ方法3を使用
                scores.append(corrected_score)
                w = min(m3["n_samples"] / 30, 1.0)  # 低い重み
                weights.append(w)

        if scores and weights:
            total_weight = sum(weights)
            weighted_score = sum(s * w for s, w in zip(scores, weights)) / total_weight
            total_samples = sum(
                details.get(f"method{i}", {}).get("n_samples", 0) for i in [1, 3]
            )
            merged[key] = {
                "score": weighted_score,
                "total_samples": total_samples,
                "details": details,
            }

    return merged


def score_to_jra_label(score: float) -> str:
    """スコアをJRA相当クラスのラベルに変換"""
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


def output_results(merged, method1, method2, method3):
    """結果をファイルに出力"""
    lines = []

    def w(text=""):
        lines.append(text)
        print(text)

    w("=" * 90)
    w("NAR各会場×クラス JRA相当値分析レポート")
    w("=" * 90)

    # --- セクション1: NAR各会場×クラスのJRA相当値 ---
    w("\n" + "=" * 90)
    w("セクション1: NAR各会場×クラスのJRA相当値")
    w("=" * 90)
    w(f"{'会場':<6} {'クラス':<8} {'スコア':>6} {'JRA相当':<14} {'サンプル':>8} {'信頼性':<6} {'根拠'}")
    w("-" * 90)

    # 会場ごとにソート
    venues_in_data = sorted(set(v for v, c in merged.keys()))
    # NAR会場のみ
    nar_venues = [v for v in venues_in_data if v not in {"札幌","函館","福島","新潟","東京","中山","中京","京都","阪神","小倉"}]

    for venue in nar_venues:
        venue_items = [(k, v) for k, v in merged.items() if k[0] == venue]
        venue_items.sort(key=lambda x: nar_class_sort_key(x[0][1]))
        for (v, cls), data in venue_items:
            score = data["score"]
            n = data["total_samples"]
            jra_label = score_to_jra_range(score)
            reliability = "◎" if n >= 100 else ("○" if n >= 50 else ("△" if n >= 20 else "×"))

            # 根拠の詳細
            basis_parts = []
            d = data["details"]
            if "method1" in d:
                m1 = d["method1"]
                basis_parts.append(f"移籍馬{m1['n_horses']}頭/JRA成績{m1['n_samples']}件(avg={m1['avg_jra_grade']:.2f})")
            if "method3" in d:
                m3 = d["method3"]
                basis_parts.append(f"タイム{m3['n_samples']}件(score={m3['avg_score']:.2f})")
            basis = " + ".join(basis_parts)

            w(f"{v:<6} {cls:<8} {score:>+6.2f} {jra_label:<14} {n:>8} {reliability:<6} {basis}")
        if venue_items:
            w()

    # --- セクション2: 会場間同一クラス比較 ---
    w("\n" + "=" * 90)
    w("セクション2: 会場間同一クラス比較（JRA1勝=0基準のスコア）")
    w("=" * 90)

    # 全NARクラスを収集
    all_classes = sorted(set(c for v, c in merged.keys() if v in set(nar_venues)),
                        key=nar_class_sort_key)
    # 主要クラスのみ
    main_classes = [c for c in all_classes if c in {"A1","A2","B1","B2","B3","C1","C2","C3","C4","重賞","OP","3歳","新馬","未勝利"}]

    # 主要会場
    main_venues = ["大井","船橋","川崎","浦和","園田","姫路","名古屋","笠松","金沢","門別","盛岡","水沢","高知","佐賀"]
    main_venues = [v for v in main_venues if v in set(nar_venues)]

    header = f"{'クラス':<8}" + "".join(f"{v:>8}" for v in main_venues)
    w(header)
    w("-" * len(header))

    for cls in main_classes:
        row = f"{cls:<8}"
        for venue in main_venues:
            key = (venue, cls)
            if key in merged:
                score = merged[key]["score"]
                n = merged[key]["total_samples"]
                mark = "*" if n < 50 else ""
                row += f"{score:>+7.2f}{mark}"
            else:
                row += f"{'---':>8}"
            row = row  # keep alignment
        w(row)

    w("\n※ * はサンプル50件未満（参考値）")

    # --- セクション3: VENUE_CLASS_SCOREテーブル ---
    w("\n" + "=" * 90)
    w("セクション3: VENUE_CLASS_SCOREテーブル（実装用Python辞書）")
    w("=" * 90)
    w("# ability.py の CLASS_SCORE を補完するための会場別クラススコア")
    w("# スコア基準: JRA 1勝クラス = 0.0")
    w("VENUE_CLASS_SCORE = {")

    for venue in main_venues:
        venue_items = [(k, v) for k, v in merged.items() if k[0] == venue]
        venue_items.sort(key=lambda x: nar_class_sort_key(x[0][1]))
        for (v, cls), data in venue_items:
            score = data["score"]
            n = data["total_samples"]
            comment = f"# n={n}, {score_to_jra_range(score)}"
            w(f'    ("{v}", "{cls}"): {score:.1f},  {comment}')

    w("}")

    # --- セクション4: 検証用統計 ---
    w("\n" + "=" * 90)
    w("セクション4: 検証用統計")
    w("=" * 90)

    w(f"\n{'会場':<6} {'クラス':<8} {'スコア':>6} {'M1件数':>8} {'M1馬数':>8} {'M3件数':>8} {'M3距離数':>8} {'十分?':<6} {'95%CI'}")
    w("-" * 100)

    for venue in nar_venues:
        venue_items = [(k, v) for k, v in merged.items() if k[0] == venue]
        venue_items.sort(key=lambda x: nar_class_sort_key(x[0][1]))
        for (v, cls), data in venue_items:
            score = data["score"]
            d = data["details"]
            m1_n = d.get("method1", {}).get("n_samples", 0)
            m1_h = d.get("method1", {}).get("n_horses", 0)
            m3_n = d.get("method3", {}).get("n_samples", 0)
            m3_d = d.get("method3", {}).get("n_distances", 0)
            sufficient = "Yes" if data["total_samples"] >= 50 else "No"

            # 簡易95%CI（方法1と方法3のスコア差から推定）
            ci_str = "---"
            score_list = []
            if "method1" in d:
                score_list.append(d["method1"]["avg_jra_grade"])
            if "method3" in d:
                score_list.append(d["method3"]["avg_score"])
            if len(score_list) >= 2:
                spread = abs(score_list[0] - score_list[1])
                ci_str = f"±{spread/2:.2f}"
            elif data["total_samples"] >= 20:
                ci_str = f"±{1.5/((data['total_samples']/20)**0.5):.2f}"

            w(f"{v:<6} {cls:<8} {score:>+6.2f} {m1_n:>8} {m1_h:>8} {m3_n:>8} {m3_d:>8} {sufficient:<6} {ci_str}")

    # --- 追加: 現行CLASS_SCOREとの比較 ---
    w("\n" + "=" * 90)
    w("参考: 現行ability.py CLASS_SCOREとの比較")
    w("=" * 90)

    current_scores = {
        "A1": 3.0, "A2": 2.0, "B1": 1.5, "B2": 1.0, "B3": 0.5,
        "C1": 0.0, "C2": -0.5, "C3": -1.0, "C4": -1.5,
        "重賞": 3.0, "交流重賞": 4.0,
    }

    w(f"{'クラス':<8} {'現行値':>8} {'大井':>8} {'船橋':>8} {'川崎':>8} {'浦和':>8} {'園田':>8} {'名古屋':>8} {'高知':>8} {'佐賀':>8}")
    w("-" * 88)

    for cls in ["A1","A2","B1","B2","B3","C1","C2","C3","C4"]:
        row = f"{cls:<8}"
        cur_val = current_scores.get(cls, 0)
        row += f"{cur_val:>+8.1f}"
        for venue in ["大井","船橋","川崎","浦和","園田","名古屋","高知","佐賀"]:
            key = (venue, cls)
            if key in merged:
                val = merged[key]["score"]
                row += f"{val:>+8.2f}"
            else:
                row += f"{'---':>8}"
        w(row)

    # ファイル出力
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    w(f"\n出力完了: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
