"""
analyze_kikko_jiku3.py
======================
「拮抗(波乱注意)」閾値を jiku_gap3(軸馬度1位-3位差) で再検証するスクリプト。

マスター指摘: 複勝(3着以内)の波乱は上位3頭の混戦で決まる。
→ jiku_gap(1位-2位差) とは別軸として jiku_gap3(1位-3位差) を集計する。

jiku_score の計算式:
    N = 有効頭数(composite 有効馬)
    comp_rank = composite 降順順位(1始まり)
    jiku_score = 0.40 * clamp((composite-20)/0.8, 0, 100)
               + 0.30 * clamp(place3_prob*100, 0, 100)
               + 0.30 * ((N - comp_rank) / (N - 1) * 100)

jiku_gap3 = jiku_score 1位 - jiku_score 3位  (3頭未満は999で除外)

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

# jiku_gap3 帯定義 (左閉右開, 最後は open-ended)
BANDS = [
    (0.0, 4.0),
    (4.0, 8.0),
    (8.0, 12.0),
    (12.0, 18.0),
    (18.0, float("inf")),
]
BAND_LABELS = ["0-4", "4-8", "8-12", "12-18", "18+"]

# 細粒度帯 (閾値探索用)
FINE_BANDS = [
    (0.0, 2.0),
    (2.0, 4.0),
    (4.0, 6.0),
    (6.0, 8.0),
    (8.0, 10.0),
    (10.0, 13.0),
    (13.0, 16.0),
    (16.0, 20.0),
    (20.0, float("inf")),
]
FINE_LABELS = ["0-2", "2-4", "4-6", "6-8", "8-10", "10-13", "13-16", "16-20", "20+"]

JRA_VENUES = {"東京", "中山", "阪神", "京都", "中京", "小倉", "函館", "札幌", "福島", "新潟"}


def clamp(val: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, val))


def calc_jiku_score(composite: float, place3_prob: float, comp_rank: int, n: int) -> float:
    """軸馬度を計算する"""
    if n <= 1:
        return 100.0
    term_comp = 0.40 * clamp((composite - 20) / 0.8, 0.0, 100.0)
    term_place = 0.30 * clamp(place3_prob * 100, 0.0, 100.0)
    term_rank = 0.30 * ((n - comp_rank) / (n - 1) * 100.0)
    return term_comp + term_place + term_rank


def band_label(val: float, bands, labels) -> str:
    for (lo, hi), label in zip(bands, labels):
        if lo <= val < hi:
            return label
    return labels[-1]


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


def print_table(title: str, counter: BandCounter, col_header: str = "jiku_gap3帯"):
    """帯別の表を出力"""
    print(f"\n{'=' * 68}")
    print(f"  {title}")
    print(f"{'=' * 68}")
    print(f"{col_header:>12}  {'レース数':>8}  {'勝率':>8}  {'複勝率':>8}")
    print("-" * 46)
    total_n, total_win, total_place = 0, 0, 0
    for lb in counter.labels:
        d = counter.data[lb]
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


def main():
    conn = sqlite3.connect(DB_PATH)

    print("=== 分析開始: 拮抗閾値 jiku_gap3 (軸馬度1位-3位差) 版 ===")
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

    # 集計カウンタ: jiku_gap3 版 (ALL / JRA / NAR)
    counters = {
        "ALL": BandCounter(BAND_LABELS),
        "JRA": BandCounter(BAND_LABELS),
        "NAR": BandCounter(BAND_LABELS),
    }
    # 細粒度版
    fine_counters = {
        "ALL": BandCounter(FINE_LABELS),
        "JRA": BandCounter(FINE_LABELS),
        "NAR": BandCounter(FINE_LABELS),
    }

    # 参考: jiku_gap(1位-2位) 版も同時集計
    GAP2_BANDS = [(0.0, 3.0), (3.0, 6.0), (6.0, 10.0), (10.0, 15.0), (15.0, float("inf"))]
    GAP2_LABELS = ["0-3", "3-6", "6-10", "10-15", "15+"]
    gap2_counters = {
        "ALL": BandCounter(GAP2_LABELS),
        "JRA": BandCounter(GAP2_LABELS),
        "NAR": BandCounter(GAP2_LABELS),
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
            comp_rank = rank_0based + 1
            composite = float(h["composite"])
            p3 = h.get("place3_prob")
            place3_prob = float(p3) if isinstance(p3, (int, float)) else 0.0
            score = calc_jiku_score(composite, place3_prob, comp_rank, n)
            jiku_scores.append((score, h))

        # jiku_score 降順ソート
        jiku_scores.sort(key=lambda x: x[0], reverse=True)

        if len(jiku_scores) < 3:
            skipped += 1
            continue

        # jiku_gap3: 1位-3位差
        jiku_gap3 = jiku_scores[0][0] - jiku_scores[2][0]
        # jiku_gap: 1位-2位差(参考)
        jiku_gap2 = jiku_scores[0][0] - jiku_scores[1][0]

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

        # JRA/NAR 判定
        is_jra = venue in JRA_VENUES
        key = "JRA" if is_jra else "NAR"

        # jiku_gap3 帯別集計
        lb3 = band_label(jiku_gap3, BANDS, BAND_LABELS)
        counters["ALL"].add(lb3, is_win, is_place)
        counters[key].add(lb3, is_win, is_place)

        # 細粒度版
        flb3 = band_label(jiku_gap3, FINE_BANDS, FINE_LABELS)
        fine_counters["ALL"].add(flb3, is_win, is_place)
        fine_counters[key].add(flb3, is_win, is_place)

        # jiku_gap(1位-2位)参考
        lb2 = band_label(jiku_gap2, GAP2_BANDS, GAP2_LABELS)
        gap2_counters["ALL"].add(lb2, is_win, is_place)
        gap2_counters[key].add(lb2, is_win, is_place)

        matched += 1

    conn.close()

    print(f"\n  突合完了: {matched:,} レース / スキップ: {skipped:,} 件")

    # === jiku_gap3 メイン表 ===
    print_table("ALL (JRA + NAR) — jiku_gap3 帯別", counters["ALL"], "jiku_gap3帯")
    print_table("JRA のみ — jiku_gap3 帯別", counters["JRA"], "jiku_gap3帯")
    print_table("NAR のみ — jiku_gap3 帯別", counters["NAR"], "jiku_gap3帯")

    # === 細粒度版 ===
    print_table("ALL (細粒度) — jiku_gap3 帯別", fine_counters["ALL"], "jiku_gap3帯")
    print_table("JRA (細粒度) — jiku_gap3 帯別", fine_counters["JRA"], "jiku_gap3帯")
    print_table("NAR (細粒度) — jiku_gap3 帯別", fine_counters["NAR"], "jiku_gap3帯")

    # === 参考: jiku_gap(1位-2位) ===
    print_table("【参考】ALL — jiku_gap(1位-2位) 帯別", gap2_counters["ALL"], "jiku_gap帯")
    print_table("【参考】JRA — jiku_gap(1位-2位) 帯別", gap2_counters["JRA"], "jiku_gap帯")

    # === 閾値所見サマリ ===
    print(f"\n{'=' * 68}")
    print("  閾値所見サマリ")
    print(f"{'=' * 68}")
    print()
    print("【jiku_gap3 (1位-3位差) — 複勝率推移 ALL】")
    all_c = counters["ALL"]
    for lb in BAND_LABELS:
        d = all_c.data[lb]
        n = d["n"]
        if n == 0:
            print(f"  jiku_gap3 {lb:>6}: レース数     0  複勝率    —")
        else:
            place_r = d["place"] / n * 100
            print(f"  jiku_gap3 {lb:>6}: レース数 {n:>6,}  複勝率 {place_r:>5.1f}%")

    print()
    print("【jiku_gap3 (細粒度) — ALL】")
    fall_c = fine_counters["ALL"]
    for lb in FINE_LABELS:
        d = fall_c.data[lb]
        n = d["n"]
        if n == 0:
            print(f"  jiku_gap3 {lb:>6}: レース数     0  複勝率    —")
        else:
            place_r = d["place"] / n * 100
            print(f"  jiku_gap3 {lb:>6}: レース数 {n:>6,}  複勝率 {place_r:>5.1f}%")

    print()
    print("【参考: jiku_gap(1位-2位) — ALL (現行閾値 <6)】")
    agap2 = gap2_counters["ALL"]
    for lb in GAP2_LABELS:
        d = agap2.data[lb]
        n = d["n"]
        if n == 0:
            print(f"  jiku_gap  {lb:>6}: レース数     0  複勝率    —")
        else:
            place_r = d["place"] / n * 100
            print(f"  jiku_gap  {lb:>6}: レース数 {n:>6,}  複勝率 {place_r:>5.1f}%")

    print()
    print("=== 分析終了 ===")


if __name__ == "__main__":
    main()
