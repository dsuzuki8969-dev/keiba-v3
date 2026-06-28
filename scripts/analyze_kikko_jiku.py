"""
analyze_kikko_jiku.py
======================
「拮抗(波乱注意)」閾値を 軸馬度差(jiku_gap) で再検証するスクリプト。

各レースで軸馬度(jiku_score)を計算し、
◎(=composite 1位)の複勝率を jiku_gap 帯別に集計する。

jiku_score の計算式:
    N = 有効頭数(composite 有効馬)
    comp_rank = composite 降順順位(1始まり)
    jiku_score = 0.40 * clamp((composite-20)/0.8, 0, 100)
               + 0.30 * clamp(place3_prob*100, 0, 100)
               + 0.30 * ((N - comp_rank) / (N - 1) * 100)

jiku_gap = jiku_score 1位 - jiku_score 2位

本番ファイル非改変・git commit不要。
"""

import sys
import io
import sqlite3
import json
from collections import defaultdict

# Windows cp932対策: stdout を UTF-8 に再設定
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

DB_PATH = "data/keiba.db"

# jiku_gap 帯定義 (左閉右開, 最後は open-ended)
BANDS = [
    (0.0, 3.0),
    (3.0, 6.0),
    (6.0, 10.0),
    (10.0, 15.0),
    (15.0, float("inf")),
]

BAND_LABELS = ["0-3", "3-6", "6-10", "10-15", "15+"]

# 参考: top3_range 版の帯
TOP3_BANDS = [
    (0.0, 2.0),
    (2.0, 4.0),
    (4.0, 6.0),
    (6.0, 8.0),
    (8.0, 10.0),
    (10.0, 15.0),
    (15.0, float("inf")),
]

TOP3_BAND_LABELS = ["0-2", "2-4", "4-6", "6-8", "8-10", "10-15", "15+"]


def clamp(val: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, val))


def calc_jiku_score(composite: float, place3_prob: float, comp_rank: int, n: int) -> float:
    """軸馬度を計算する"""
    if n <= 1:
        return 100.0  # 1頭だけ → 最大値
    term_comp = 0.40 * clamp((composite - 20) / 0.8, 0.0, 100.0)
    term_place = 0.30 * clamp(place3_prob * 100, 0.0, 100.0)
    term_rank = 0.30 * ((n - comp_rank) / (n - 1) * 100.0)
    return term_comp + term_place + term_rank


def jiku_band_label(val: float) -> str:
    for (lo, hi), label in zip(BANDS, BAND_LABELS):
        if lo <= val < hi:
            return label
    return "15+"


def top3_band_label(val: float) -> str:
    for (lo, hi), label in zip(TOP3_BANDS, TOP3_BAND_LABELS):
        if lo <= val < hi:
            return label
    return "15+"


def build_finish_map(conn: sqlite3.Connection) -> dict:
    """race_id -> {horse_no: finish_pos} のマップを構築"""
    cur = conn.cursor()
    cur.execute(
        "SELECT race_id, horse_no, finish_pos FROM race_log WHERE finish_pos IS NOT NULL"
    )
    result: dict[str, dict[int, int]] = defaultdict(dict)
    for race_id, horse_no, finish_pos in cur.fetchall():
        result[race_id][int(horse_no)] = int(finish_pos)
    return result


class BandCounter:
    """帯別集計用カウンタ"""

    def __init__(self, labels):
        # {band_label: {"n": int, "win": int, "place": int}}
        self.labels = labels
        self.data: dict[str, dict] = {
            lb: {"n": 0, "win": 0, "place": 0} for lb in labels
        }

    def add(self, band: str, win: bool, place: bool):
        if band not in self.data:
            return
        self.data[band]["n"] += 1
        if win:
            self.data[band]["win"] += 1
        if place:
            self.data[band]["place"] += 1


def print_table_jiku(title: str, counter: BandCounter):
    """jiku_gap 帯別の表を出力"""
    print(f"\n{'=' * 65}")
    print(f"  {title}  [jiku_gap 帯別]")
    print(f"{'=' * 65}")
    print(f"{'jiku_gap帯':>10}  {'レース数':>8}  {'勝率':>8}  {'複勝率':>8}")
    print("-" * 44)
    total_n, total_win, total_place = 0, 0, 0
    for lb in counter.labels:
        d = counter.data[lb]
        n = d["n"]
        if n == 0:
            print(f"{lb:>10}  {'0':>8}  {'—':>8}  {'—':>8}")
            continue
        win_r = d["win"] / n * 100
        place_r = d["place"] / n * 100
        print(f"{lb:>10}  {n:>8,}  {win_r:>7.1f}%  {place_r:>7.1f}%")
        total_n += n
        total_win += d["win"]
        total_place += d["place"]
    print("-" * 44)
    if total_n:
        print(
            f"{'合計':>10}  {total_n:>8,}  "
            f"{total_win/total_n*100:>7.1f}%  {total_place/total_n*100:>7.1f}%"
        )


def print_comparison_table(title: str, jiku_counter: BandCounter, top3_counter: BandCounter):
    """top3_range と jiku_gap の比較表を出力"""
    print(f"\n{'=' * 65}")
    print(f"  {title}  [top3_range 参照帯別]")
    print(f"{'=' * 65}")
    print(f"{'top3_range帯':>12}  {'レース数':>8}  {'勝率':>8}  {'複勝率':>8}")
    print("-" * 46)
    total_n, total_win, total_place = 0, 0, 0
    for lb in top3_counter.labels:
        d = top3_counter.data[lb]
        n = d["n"]
        if n == 0:
            print(f"{lb:>12}  {'0':>8}  {'—':>8}  {'—':>8}")
            continue
        win_r = d["win"] / n * 100
        place_r = d["place"] / n * 100
        print(f"{lb:>12}  {n:>8,}  {win_r:>7.1f}%  {place_r:>7.1f}%")
        total_n += n
        total_win += d["win"]
        total_place += d["place"]
    print("-" * 46)
    if total_n:
        print(
            f"{'合計':>12}  {total_n:>8,}  "
            f"{total_win/total_n*100:>7.1f}%  {total_place/total_n*100:>7.1f}%"
        )


JRA_VENUES = {"東京", "中山", "阪神", "京都", "中京", "小倉", "函館", "札幌", "福島", "新潟"}


def main():
    conn = sqlite3.connect(DB_PATH)

    print("=== 分析開始: 拮抗閾値 jiku_gap 版 ===")
    print("DBロード中...")

    finish_map = build_finish_map(conn)
    print(f"  race_log マップ構築完了: {len(finish_map):,} レース")

    cur = conn.cursor()
    cur.execute(
        """
        SELECT race_id, horses_json, venue
        FROM predictions
        WHERE race_id != '' AND horses_json != '[]' AND horses_json IS NOT NULL
        ORDER BY race_id
        """
    )
    rows = cur.fetchall()
    print(f"  predictions ロード完了: {len(rows):,} 件")

    # 集計カウンタ: jiku_gap 版 (ALL / JRA / NAR)
    jiku_counters = {
        "ALL": BandCounter(BAND_LABELS),
        "JRA": BandCounter(BAND_LABELS),
        "NAR": BandCounter(BAND_LABELS),
    }
    # 集計カウンタ: top3_range 参照版 (ALL / JRA / NAR)
    top3_counters = {
        "ALL": BandCounter(TOP3_BAND_LABELS),
        "JRA": BandCounter(TOP3_BAND_LABELS),
        "NAR": BandCounter(TOP3_BAND_LABELS),
    }

    skipped = 0
    matched = 0

    for race_id, horses_json, venue in rows:
        # race_log と突合できないレースはスキップ
        if race_id not in finish_map:
            skipped += 1
            continue

        try:
            horses = json.loads(horses_json)
        except (json.JSONDecodeError, TypeError):
            skipped += 1
            continue

        if not isinstance(horses, list) or len(horses) < 3:
            skipped += 1
            continue

        # composite が有効な馬のみ抽出
        horses_valid = [
            h for h in horses
            if h.get("composite") is not None and isinstance(h.get("composite"), (int, float))
        ]
        if len(horses_valid) < 3:
            skipped += 1
            continue

        # composite 降順ソート
        horses_sorted = sorted(horses_valid, key=lambda h: h["composite"], reverse=True)
        n = len(horses_sorted)

        # jiku_score を各馬に計算
        jiku_scores = []
        for rank_0based, h in enumerate(horses_sorted):
            comp_rank = rank_0based + 1  # 1始まり
            composite = float(h["composite"])
            # place3_prob: None / 欠損対応
            p3 = h.get("place3_prob")
            place3_prob = float(p3) if isinstance(p3, (int, float)) else 0.0
            score = calc_jiku_score(composite, place3_prob, comp_rank, n)
            jiku_scores.append((score, h))

        # jiku_score 降順ソート
        jiku_scores.sort(key=lambda x: x[0], reverse=True)

        if len(jiku_scores) < 2:
            skipped += 1
            continue

        jiku_gap = jiku_scores[0][0] - jiku_scores[1][0]

        # ◎ = composite 1位
        top1 = horses_sorted[0]
        horse_no_1 = top1.get("horse_no")
        if horse_no_1 is None:
            skipped += 1
            continue

        fp_map = finish_map[race_id]
        try:
            horse_no_1 = int(horse_no_1)
        except (ValueError, TypeError):
            skipped += 1
            continue

        if horse_no_1 not in fp_map:
            skipped += 1
            continue

        finish_pos = fp_map[horse_no_1]
        is_win = (finish_pos == 1)
        is_place = (finish_pos <= 3)

        # top3_range (参照: 既存スクリプトと同様)
        composite_1 = horses_sorted[0]["composite"]
        composite_3 = horses_sorted[2]["composite"]
        top3_range = composite_1 - composite_3

        # JRA/NAR 判定
        is_jra = venue in JRA_VENUES
        key = "JRA" if is_jra else "NAR"

        # jiku_gap 帯別集計
        jb = jiku_band_label(jiku_gap)
        jiku_counters["ALL"].add(jb, is_win, is_place)
        jiku_counters[key].add(jb, is_win, is_place)

        # top3_range 帯別集計(参照)
        t3b = top3_band_label(top3_range)
        top3_counters["ALL"].add(t3b, is_win, is_place)
        top3_counters[key].add(t3b, is_win, is_place)

        matched += 1

    conn.close()

    print(f"\n  突合完了: {matched:,} レース / スキップ: {skipped:,} 件")
    print()

    # === 結果出力: jiku_gap 版 ===
    print_table_jiku("ALL (JRA + NAR)", jiku_counters["ALL"])
    print_table_jiku("JRA のみ", jiku_counters["JRA"])
    print_table_jiku("NAR のみ", jiku_counters["NAR"])

    # === 結果出力: top3_range 参照版 ===
    print_comparison_table("ALL (JRA + NAR)", top3_counters["ALL"], top3_counters["ALL"])
    print_comparison_table("JRA のみ", top3_counters["JRA"], top3_counters["JRA"])
    print_comparison_table("NAR のみ", top3_counters["NAR"], top3_counters["NAR"])

    # === 閾値所見 ===
    print(f"\n{'=' * 65}")
    print("  閾値所見: jiku_gap 版 vs top3_range 版")
    print(f"{'=' * 65}")
    print()
    print("【jiku_gap 版】")
    all_jiku = jiku_counters["ALL"]
    for lb in BAND_LABELS:
        d = all_jiku.data[lb]
        n = d["n"]
        if n == 0:
            print(f"  jiku_gap {lb:>6}: レース数    0  複勝率    —")
        else:
            place_r = d["place"] / n * 100
            print(f"  jiku_gap {lb:>6}: レース数 {n:>6,}  複勝率 {place_r:>5.1f}%")

    print()
    print("【top3_range 版 (参照)】")
    all_top3 = top3_counters["ALL"]
    for lb in TOP3_BAND_LABELS:
        d = all_top3.data[lb]
        n = d["n"]
        if n == 0:
            print(f"  top3_range {lb:>6}: レース数    0  複勝率    —")
        else:
            place_r = d["place"] / n * 100
            print(f"  top3_range {lb:>6}: レース数 {n:>6,}  複勝率 {place_r:>5.1f}%")

    print()
    print("現状 top3_range の閾値: top3 < 6 (ALL) / top3 < 8 (JRA)")
    print()
    print("=== 分析終了 ===")


if __name__ == "__main__":
    main()
