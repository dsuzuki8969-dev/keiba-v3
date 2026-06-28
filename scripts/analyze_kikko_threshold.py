"""
analyze_kikko_threshold.py
==========================
「拮抗(波乱注意)」レースの閾値をデータで決める分析スクリプト。

各レースの composite 降順上位N頭の指数差(top3_range / top5_range)を計算し、
帯別に◎の複勝率・勝率・レース数を集計。
JRA / NAR / ALL 別に出力する。

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

# 指数差の帯定義 (左閉右開, 最後は open-ended)
BANDS = [
    (0.0, 2.0),
    (2.0, 4.0),
    (4.0, 6.0),
    (6.0, 8.0),
    (8.0, 10.0),
    (10.0, 15.0),
    (15.0, float("inf")),
]

BAND_LABELS = ["0-2", "2-4", "4-6", "6-8", "8-10", "10-15", "15+"]


def band_label(val: float) -> str:
    for (lo, hi), label in zip(BANDS, BAND_LABELS):
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


def build_is_jra_map(conn: sqlite3.Connection) -> dict:
    """race_id -> is_jra(bool) のマップ。race_log から取得"""
    cur = conn.cursor()
    cur.execute(
        "SELECT race_id, MAX(is_jra) FROM race_log GROUP BY race_id"
    )
    return {race_id: bool(is_jra) for race_id, is_jra in cur.fetchall()}


# 帯別集計用カウンタ
class BandCounter:
    def __init__(self):
        # {band_label: {"n": int, "win": int, "place": int}}
        self.data: dict[str, dict] = {
            lb: {"n": 0, "win": 0, "place": 0} for lb in BAND_LABELS
        }

    def add(self, band: str, win: bool, place: bool):
        self.data[band]["n"] += 1
        if win:
            self.data[band]["win"] += 1
        if place:
            self.data[band]["place"] += 1


def print_table(title: str, counter_top5: BandCounter, counter_top3: BandCounter):
    """帯別の表を出力"""
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")

    # top5_range 表
    print("\n--- top5_range (1位-5位 指数差) 帯別 ---")
    print(f"{'帯':>8}  {'レース数':>8}  {'勝率':>8}  {'複勝率':>8}")
    print("-" * 42)
    total_n, total_win, total_place = 0, 0, 0
    for lb in BAND_LABELS:
        d = counter_top5.data[lb]
        n = d["n"]
        if n == 0:
            print(f"{lb:>8}  {'0':>8}  {'—':>8}  {'—':>8}")
            continue
        win_r = d["win"] / n * 100
        place_r = d["place"] / n * 100
        print(f"{lb:>8}  {n:>8,}  {win_r:>7.1f}%  {place_r:>7.1f}%")
        total_n += n
        total_win += d["win"]
        total_place += d["place"]
    print("-" * 42)
    if total_n:
        print(
            f"{'合計':>8}  {total_n:>8,}  "
            f"{total_win/total_n*100:>7.1f}%  {total_place/total_n*100:>7.1f}%"
        )

    # top3_range 表
    print("\n--- top3_range (1位-3位 指数差) 帯別 ---")
    print(f"{'帯':>8}  {'レース数':>8}  {'勝率':>8}  {'複勝率':>8}")
    print("-" * 42)
    total_n, total_win, total_place = 0, 0, 0
    for lb in BAND_LABELS:
        d = counter_top3.data[lb]
        n = d["n"]
        if n == 0:
            print(f"{lb:>8}  {'0':>8}  {'—':>8}  {'—':>8}")
            continue
        win_r = d["win"] / n * 100
        place_r = d["place"] / n * 100
        print(f"{lb:>8}  {n:>8,}  {win_r:>7.1f}%  {place_r:>7.1f}%")
        total_n += n
        total_win += d["win"]
        total_place += d["place"]
    print("-" * 42)
    if total_n:
        print(
            f"{'合計':>8}  {total_n:>8,}  "
            f"{total_win/total_n*100:>7.1f}%  {total_place/total_n*100:>7.1f}%"
        )


def main():
    conn = sqlite3.connect(DB_PATH)

    print("=== 分析開始: 拮抗(波乱注意)閾値の実証分析 ===")
    print("DBロード中...")

    finish_map = build_finish_map(conn)
    is_jra_map = build_is_jra_map(conn)
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

    # 集計カウンタ (ALL / JRA / NAR)
    counters = {
        "ALL": {"top5": BandCounter(), "top3": BandCounter()},
        "JRA": {"top5": BandCounter(), "top3": BandCounter()},
        "NAR": {"top5": BandCounter(), "top3": BandCounter()},
    }

    # 頭数帯別カウンタ (参考)
    fc_counters: dict[str, dict] = {
        "9以下": {"top5": BandCounter(), "top3": BandCounter()},
        "10-13": {"top5": BandCounter(), "top3": BandCounter()},
        "14以上": {"top5": BandCounter(), "top3": BandCounter()},
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
        except json.JSONDecodeError:
            skipped += 1
            continue

        if not isinstance(horses, list) or len(horses) < 3:
            skipped += 1
            continue

        # composite 降順ソート。composite が None の馬は除外
        horses_valid = [
            h for h in horses
            if h.get("composite") is not None and isinstance(h.get("composite"), (int, float))
        ]
        if len(horses_valid) < 3:
            skipped += 1
            continue

        horses_sorted = sorted(horses_valid, key=lambda h: h["composite"], reverse=True)
        field_count = len(horses_sorted)

        # ◎ = composite 1位
        top1 = horses_sorted[0]
        horse_no_1 = top1.get("horse_no")
        if horse_no_1 is None:
            skipped += 1
            continue

        fp_map = finish_map[race_id]
        horse_no_1 = int(horse_no_1)
        if horse_no_1 not in fp_map:
            skipped += 1
            continue

        finish_pos = fp_map[horse_no_1]
        is_win = (finish_pos == 1)
        is_place = (finish_pos <= 3)

        # 指数差計算
        composite_1 = horses_sorted[0]["composite"]

        # top3_range: 1位 - 3位 (最低3頭必要)
        composite_3 = horses_sorted[2]["composite"]
        top3_range = composite_1 - composite_3

        # top5_range: 1位 - 5位 (5頭未満は3頭で代用 or スキップ)
        if len(horses_sorted) >= 5:
            composite_5 = horses_sorted[4]["composite"]
            top5_range = composite_1 - composite_5
        else:
            # 5頭未満: 最下位との差を使う
            composite_last = horses_sorted[-1]["composite"]
            top5_range = composite_1 - composite_last

        # JRA/NAR 判定 (predictions.venue から)
        jra_venues = {"東京", "中山", "阪神", "京都", "中京", "小倉", "函館", "札幌", "福島", "新潟"}
        is_jra = venue in jra_venues

        band5 = band_label(top5_range)
        band3 = band_label(top3_range)

        # ALL 集計
        counters["ALL"]["top5"].add(band5, is_win, is_place)
        counters["ALL"]["top3"].add(band3, is_win, is_place)

        # JRA/NAR 集計
        key = "JRA" if is_jra else "NAR"
        counters[key]["top5"].add(band5, is_win, is_place)
        counters[key]["top3"].add(band3, is_win, is_place)

        # 頭数帯別
        if field_count <= 9:
            fc_key = "9以下"
        elif field_count <= 13:
            fc_key = "10-13"
        else:
            fc_key = "14以上"
        fc_counters[fc_key]["top5"].add(band5, is_win, is_place)
        fc_counters[fc_key]["top3"].add(band3, is_win, is_place)

        matched += 1

    conn.close()

    print(f"\n  突合完了: {matched:,} レース / スキップ: {skipped:,} 件")

    # === 結果出力 ===
    print_table("ALL (JRA + NAR)", counters["ALL"]["top5"], counters["ALL"]["top3"])
    print_table("JRA のみ", counters["JRA"]["top5"], counters["JRA"]["top3"])
    print_table("NAR のみ", counters["NAR"]["top5"], counters["NAR"]["top3"])

    print(f"\n{'=' * 60}")
    print("  頭数帯別 (top5_range / ALL)")
    print(f"{'=' * 60}")
    for fc_key in ["9以下", "10-13", "14以上"]:
        c5 = fc_counters[fc_key]["top5"]
        c3 = fc_counters[fc_key]["top3"]
        print_table(f"頭数帯: {fc_key}", c5, c3)

    print("\n=== 分析終了 ===")


if __name__ == "__main__":
    main()
