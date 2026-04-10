# -*- coding: utf-8 -*-
"""
競馬場×クラス×距離帯の走破タイム統計分析
race_logテーブルから徹底分析し、data/analysis/venue_class_times.txt に出力
"""
import sqlite3
import sys
import re
import statistics
from collections import defaultdict
from pathlib import Path

# プロジェクトルート
ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "keiba.db"
OUT_PATH = ROOT / "data" / "analysis" / "venue_class_times.txt"
OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

# 会場コード → 名前
VENUE_MAP = {
    "01": "札幌", "02": "函館", "03": "福島", "04": "新潟",
    "05": "東京", "06": "中山", "07": "中京", "08": "京都",
    "09": "阪神", "10": "小倉",
    "30": "門別", "35": "盛岡", "36": "水沢",
    "42": "浦和", "43": "船橋", "44": "大井", "45": "川崎",
    "46": "名古屋", "47": "笠松", "48": "園田", "50": "姫路",
    "51": "高知", "54": "佐賀", "55": "金沢", "65": "帯広",
}

NAR_VENUES = {"30", "35", "36", "42", "43", "44", "45", "46", "47", "48", "50", "51", "54", "55"}
JRA_VENUES = {"01", "02", "03", "04", "05", "06", "07", "08", "09", "10"}

MIN_SAMPLES = 50


def extract_nar_class(race_name, grade):
    """race_nameからNARクラスを抽出"""
    if not race_name:
        return None
    # 交流重賞
    if grade in ("交流重賞", "G1", "G2", "G3"):
        return "重賞"
    # OP
    if grade == "OP":
        return "OP"
    # 新馬
    if "新馬" in race_name:
        return "新馬"
    # 未勝利/未受賞
    if "未勝利" in race_name or "未受賞" in race_name:
        return "未勝利"
    # 括弧内のクラス (C1) (A2) 等
    m = re.search(r'\(([A-D]\d)\)', race_name)
    if m:
        return m.group(1)
    # 先頭のクラス C311 → C3
    m = re.match(r'^([A-D])(\d)', race_name)
    if m:
        return m.group(1) + m.group(2)
    # 2歳/3歳
    if race_name.startswith("2歳"):
        return "2歳"
    if race_name.startswith("3歳"):
        return "3歳"
    return None


def extract_jra_class(grade):
    """JRAのgradeをクラス名に変換"""
    mapping = {
        "新馬": "新馬",
        "未勝利": "未勝利",
        "1勝": "1勝",
        "2勝": "2勝",
        "OP": "OP",
        "G3": "G3",
        "G2": "G2",
        "G1": "G1",
        "交流重賞": "重賞",
    }
    return mapping.get(grade, None)


def fmt_time(sec):
    """秒→分:秒.X形式"""
    if sec is None or sec <= 0:
        return "---"
    m = int(sec) // 60
    s = sec - m * 60
    return f"{m}:{s:04.1f}"


def main():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # 全データ取得（finish_time_sec > 0, status is null or empty=完走）
    print("データ読み込み中...")
    cur.execute("""
        SELECT venue_code, is_jra, surface, distance, finish_time_sec,
               finish_pos, race_name, grade, condition
        FROM race_log
        WHERE finish_time_sec > 10
          AND (status IS NULL OR status = '' OR status = '完走')
          AND distance >= 800
          AND distance <= 3600
    """)
    rows = cur.fetchall()
    print(f"  有効レコード数: {len(rows)}")

    # データ構造: {(venue_code, class, surface, distance): [times]}
    all_data = defaultdict(list)         # 全馬
    winner_data = defaultdict(list)      # 勝ち馬のみ
    condition_data = defaultdict(list)   # 馬場状態別

    for r in rows:
        vc = r["venue_code"]
        is_jra = r["is_jra"]
        surface = r["surface"]
        dist = r["distance"]
        time_sec = r["finish_time_sec"]
        fpos = r["finish_pos"]
        rname = r["race_name"]
        grade = r["grade"]
        cond = r["condition"] or ""

        if is_jra:
            cls = extract_jra_class(grade)
        else:
            cls = extract_nar_class(rname, grade)

        if cls is None:
            continue

        key = (vc, cls, surface, dist)
        all_data[key].append(time_sec)
        if fpos == 1:
            winner_data[key].append(time_sec)
        condition_data[(vc, cls, surface, dist, cond)].append(time_sec)

    # ---- 出力 ----
    lines = []
    def add(s=""):
        lines.append(s)

    add("=" * 120)
    add("競馬場×クラス×距離帯 走破タイム統計分析")
    add(f"データベース: {DB_PATH}")
    add(f"有効レコード数: {len(rows)}")
    add(f"最小サンプル数: {MIN_SAMPLES}")
    add("=" * 120)

    # ======== セクション1: 会場×クラス×距離別統計 ========
    add("")
    add("=" * 120)
    add("【1】会場×クラス×距離別 走破タイム統計（全馬）")
    add("=" * 120)

    # NARとJRA分けて出力
    for label, venue_set in [("NAR（地方）", NAR_VENUES), ("JRA（中央）", JRA_VENUES)]:
        add(f"\n{'─' * 100}")
        add(f"  {label}")
        add(f"{'─' * 100}")

        # 会場ごと
        for vc in sorted(venue_set):
            venue_name = VENUE_MAP.get(vc, vc)
            venue_keys = [(k, v) for k, v in all_data.items()
                          if k[0] == vc and len(v) >= MIN_SAMPLES]
            if not venue_keys:
                continue

            add(f"\n  ■ {venue_name} ({vc})")
            add(f"  {'クラス':<8} {'馬場':<6} {'距離':>6} {'サンプル':>8} {'平均':>10} {'中央値':>10} {'最速':>10} {'最遅':>10} {'標準偏差':>8}")
            add(f"  {'─' * 90}")

            for key, times in sorted(venue_keys, key=lambda x: (x[0][2], x[0][1], x[0][3])):
                _, cls, srf, dist = key
                n = len(times)
                avg = statistics.mean(times)
                med = statistics.median(times)
                mn = min(times)
                mx = max(times)
                std = statistics.stdev(times) if n > 1 else 0
                add(f"  {cls:<8} {srf:<6} {dist:>5}m {n:>8} {fmt_time(avg):>10} {fmt_time(med):>10} {fmt_time(mn):>10} {fmt_time(mx):>10} {std:>8.2f}")

    # ======== セクション2: 勝ち馬のみの統計 ========
    add("")
    add("=" * 120)
    add("【2】勝ち馬（1着）のみの走破タイム統計")
    add("=" * 120)

    for label, venue_set in [("NAR（地方）", NAR_VENUES), ("JRA（中央）", JRA_VENUES)]:
        add(f"\n{'─' * 100}")
        add(f"  {label}")
        add(f"{'─' * 100}")

        for vc in sorted(venue_set):
            venue_name = VENUE_MAP.get(vc, vc)
            venue_keys = [(k, v) for k, v in winner_data.items()
                          if k[0] == vc and len(v) >= (MIN_SAMPLES // 5)]  # 勝ち馬は少ないので閾値下げる
            if not venue_keys:
                continue

            add(f"\n  ■ {venue_name} ({vc})")
            add(f"  {'クラス':<8} {'馬場':<6} {'距離':>6} {'サンプル':>8} {'平均':>10} {'中央値':>10} {'最速':>10} {'最遅':>10} {'標準偏差':>8}")
            add(f"  {'─' * 90}")

            for key, times in sorted(venue_keys, key=lambda x: (x[0][2], x[0][1], x[0][3])):
                _, cls, srf, dist = key
                n = len(times)
                if n < 10:
                    continue
                avg = statistics.mean(times)
                med = statistics.median(times)
                mn = min(times)
                mx = max(times)
                std = statistics.stdev(times) if n > 1 else 0
                add(f"  {cls:<8} {srf:<6} {dist:>5}m {n:>8} {fmt_time(avg):>10} {fmt_time(med):>10} {fmt_time(mn):>10} {fmt_time(mx):>10} {std:>8.2f}")

    # ======== セクション3: 同一距離での会場間比較 ========
    add("")
    add("=" * 120)
    add("【3】同一距離での会場間比較（ダート主要距離×クラス別 平均走破タイム）")
    add("=" * 120)

    target_distances = [1000, 1200, 1400, 1500, 1600, 1800, 2000]
    # NARクラス比較用
    nar_classes = ["C3", "C2", "C1", "B3", "B2", "B1", "A2", "A1", "OP", "重賞"]

    for dist in target_distances:
        add(f"\n  ■ ダート {dist}m")
        # ヘッダ
        header = f"  {'会場':<10}"
        for cls in nar_classes:
            header += f" {cls:>10}"
        add(header)
        add(f"  {'─' * (10 + 11 * len(nar_classes))}")

        for vc in sorted(NAR_VENUES):
            venue_name = VENUE_MAP.get(vc, vc)
            row_str = f"  {venue_name:<10}"
            has_data = False
            for cls in nar_classes:
                key = (vc, cls, "ダート", dist)
                times = all_data.get(key, [])
                if len(times) >= MIN_SAMPLES:
                    avg = statistics.mean(times)
                    row_str += f" {fmt_time(avg):>10}"
                    has_data = True
                else:
                    row_str += f" {'---':>10}"
            if has_data:
                add(row_str)

    # JRA同一距離比較
    add(f"\n  ■ JRA会場間比較（ダート主要距離×クラス別）")
    jra_classes = ["未勝利", "1勝", "2勝", "OP"]
    jra_dists = [1200, 1400, 1600, 1800, 2000, 2100]

    for dist in jra_dists:
        add(f"\n  ダート {dist}m")
        header = f"  {'会場':<10}"
        for cls in jra_classes:
            header += f" {cls:>10}"
        add(header)
        add(f"  {'─' * (10 + 11 * len(jra_classes))}")

        for vc in sorted(JRA_VENUES):
            venue_name = VENUE_MAP.get(vc, vc)
            row_str = f"  {venue_name:<10}"
            has_data = False
            for cls in jra_classes:
                key = (vc, cls, "ダート", dist)
                times = all_data.get(key, [])
                if len(times) >= MIN_SAMPLES:
                    avg = statistics.mean(times)
                    row_str += f" {fmt_time(avg):>10}"
                    has_data = True
                else:
                    row_str += f" {'---':>10}"
            if has_data:
                add(row_str)

    # ======== セクション4: JRA vs NAR比較 ========
    add("")
    add("=" * 120)
    add("【4】JRA vs NAR 同一距離でのタイム比較")
    add("    JRA各クラスとNAR各クラスの平均走破タイムを横並びで比較")
    add("=" * 120)

    common_dists = [1200, 1400, 1600, 1800, 2000]

    for dist in common_dists:
        add(f"\n  ■ ダート {dist}m — 全馬平均タイム")
        add(f"  {'─' * 80}")
        add(f"  [JRA]")
        for vc in sorted(JRA_VENUES):
            venue_name = VENUE_MAP.get(vc, vc)
            for cls in ["未勝利", "1勝", "2勝", "OP"]:
                key = (vc, cls, "ダート", dist)
                times = all_data.get(key, [])
                if len(times) >= MIN_SAMPLES:
                    avg = statistics.mean(times)
                    n = len(times)
                    add(f"    {venue_name:<6} {cls:<6} n={n:>5}  平均={fmt_time(avg)}  勝馬平均={fmt_time(statistics.mean(winner_data[key]) if len(winner_data.get(key, [])) >= 10 else 0)}")

        add(f"  [NAR]")
        for vc in sorted(NAR_VENUES):
            venue_name = VENUE_MAP.get(vc, vc)
            for cls in ["C1", "B1", "A1", "OP"]:
                key = (vc, cls, "ダート", dist)
                times = all_data.get(key, [])
                if len(times) >= MIN_SAMPLES:
                    avg = statistics.mean(times)
                    n = len(times)
                    add(f"    {venue_name:<6} {cls:<6} n={n:>5}  平均={fmt_time(avg)}  勝馬平均={fmt_time(statistics.mean(winner_data[key]) if len(winner_data.get(key, [])) >= 10 else 0)}")

    # ======== セクション5: 勝ち馬のみ会場間比較 ========
    add("")
    add("=" * 120)
    add("【5】勝ち馬のみ — 同一距離での会場間比較（ダート主要距離）")
    add("=" * 120)

    for dist in target_distances:
        add(f"\n  ■ ダート {dist}m （勝ち馬のみ）")
        header = f"  {'会場':<10}"
        for cls in nar_classes:
            header += f" {cls:>10}"
        add(header)
        add(f"  {'─' * (10 + 11 * len(nar_classes))}")

        for vc in sorted(NAR_VENUES):
            venue_name = VENUE_MAP.get(vc, vc)
            row_str = f"  {venue_name:<10}"
            has_data = False
            for cls in nar_classes:
                key = (vc, cls, "ダート", dist)
                times = winner_data.get(key, [])
                if len(times) >= 10:
                    avg = statistics.mean(times)
                    row_str += f" {fmt_time(avg):>10}"
                    has_data = True
                else:
                    row_str += f" {'---':>10}"
            if has_data:
                add(row_str)

    # ======== サマリー統計 ========
    add("")
    add("=" * 120)
    add("【補足】全体サマリー")
    add("=" * 120)

    # 会場別データ件数
    add(f"\n  ■ 会場別有効レコード数")
    venue_counts = defaultdict(int)
    for key, times in all_data.items():
        vc = key[0]
        venue_counts[vc] += len(times)
    for vc in sorted(venue_counts.keys()):
        add(f"    {VENUE_MAP.get(vc, vc):<8} ({vc}): {venue_counts[vc]:>8}")

    # クラス別データ件数
    add(f"\n  ■ クラス別有効レコード数")
    class_counts = defaultdict(int)
    for key, times in all_data.items():
        cls = key[1]
        class_counts[cls] += len(times)
    for cls, cnt in sorted(class_counts.items(), key=lambda x: -x[1]):
        add(f"    {cls:<8}: {cnt:>8}")

    # 出力
    output = "\n".join(lines)
    OUT_PATH.write_text(output, encoding="utf-8")
    print(f"\n結果を {OUT_PATH} に保存しました")
    print(f"出力行数: {len(lines)}")

    conn.close()


if __name__ == "__main__":
    main()
