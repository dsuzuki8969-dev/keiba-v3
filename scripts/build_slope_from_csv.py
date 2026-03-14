# -*- coding: utf-8 -*-
"""
競馬場コースマスタ.csv の直線①・直線②から slope_type を算出するスクリプト。
slope_type は「直線（ホームストレッチ）」の形状のみを指す（道中の坂ではない）。

直線 = 直線①（前半）＋ 直線②（後半）
"""
import csv
import sys
from pathlib import Path

# CSV の列インデックス（0始まり）
COL_VENUE = 0
COL_VENUE_CODE = 1
COL_COURSE = 2  # 芝/ダート
COL_DISTANCE = 4
COL_INSIDE_OUT = 8  # 内外: －/内回/外回
COL_STRAIGHT_M = 14  # 直線m
COL_CHOKUSEN1 = 25  # 直線①
COL_CHOKUSEN2 = 27  # 直線②

# CSV 競馬場コード → course_master venue_code
VENUE_CODE_MAP = {
    1: "03",   # 札幌
    2: "04",   # 函館
    3: "01",   # 福島
    4: "02",   # 新潟
    5: "06",   # 中山
    6: "05",   # 東京
    7: "09",   # 中京
    8: "07",   # 京都
    9: "08",   # 阪神
    10: "10",  # 小倉
    11: "51",  # 門別
    12: "36",  # 盛岡
    13: "37",  # 水沢
    14: "42",  # 浦和
    15: "43",  # 船橋
    16: "44",  # 大井
    17: "45",  # 川崎
    18: "46",  # 金沢
    19: "47",  # 笠松
    20: "48",  # 名古屋
    21: "50",  # 園田
    22: "50",  # 姫路
    23: "54",  # 高知
    24: "55",  # 佐賀
}


def slope_from_choksen(ch1: str, ch2: str) -> str:
    """
    直線①・直線②の形状から slope_type を算出。
    急上坂/急下坂 → 急坂
    上坂/下坂/緩上坂/緩下坂 → 軽坂
    平坦 only → 坂なし
    """
    shapes = (ch1.strip(), ch2.strip()) if ch1 and ch2 else ("", "")
    has_kyu = any(s in ("急上坂", "急下坂") for s in shapes)
    has_nami = any(s in ("上坂", "下坂", "緩上坂", "緩下坂") for s in shapes)
    if has_kyu:
        return "急坂"
    if has_nami:
        return "軽坂"
    return "坂なし"


def naichi_to_io(naichi: str) -> str:
    """内外表記を course_master 形式に変換"""
    if not naichi or naichi == "－":
        return "なし"
    if naichi == "内回":
        return "内"
    if naichi == "外回":
        return "外"
    return naichi


def main():
    csv_path = Path(r"c:\Users\dsuzu\OneDrive\Desktop\競馬予想AI\競馬場コースマスタ.csv")
    if not csv_path.exists():
        print(f"CSV not found: {csv_path}")
        sys.exit(1)

    results = []
    with open(csv_path, encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        header = next(reader)
        for row in reader:
            if len(row) <= COL_CHOKUSEN2:
                continue
            venue_code = row[COL_VENUE_CODE]
            try:
                venue_code_int = int(venue_code)
            except ValueError:
                continue
            venue_code_mapped = VENUE_CODE_MAP.get(venue_code_int)
            if not venue_code_mapped:
                continue

            venue = row[COL_VENUE]
            course = row[COL_COURSE]
            distance = int(row[COL_DISTANCE])
            inside_out = naichi_to_io(row[COL_INSIDE_OUT])
            straight_m = int(row[COL_STRAIGHT_M]) if row[COL_STRAIGHT_M].isdigit() else 0
            ch1 = row[COL_CHOKUSEN1] if len(row) > COL_CHOKUSEN1 else ""
            ch2 = row[COL_CHOKUSEN2] if len(row) > COL_CHOKUSEN2 else ""

            slope = slope_from_choksen(ch1, ch2)
            results.append({
                "venue": venue,
                "venue_code": venue_code_mapped,
                "course": course,
                "distance": distance,
                "inside_out": inside_out,
                "straight_m": straight_m,
                "choksen1": ch1,
                "choksen2": ch2,
                "slope_type": slope,
            })

    # 出力: 修正が必要な course_master と照合用
    print("=" * 80)
    print("直線①・直線②から算出した slope_type（直線＝ホームストレッチのみ）")
    print("=" * 80)

    by_venue = {}
    for r in results:
        key = (r["venue"], r["venue_code"], r["course"], r["distance"], r["inside_out"], r["straight_m"])
        if key not in by_venue:
            by_venue[key] = r

    for key in sorted(by_venue.keys(), key=lambda k: (k[0], k[2], k[3])):
        r = by_venue[key]
        print(f"{r['venue']:4} {r['venue_code']:2} {r['distance']:4}m {r['course']:4} "
              f"{r['inside_out']:2} straight={r['straight_m']:3} "
              f"直線①={r['choksen1']:6} 直線②={r['choksen2']:6} → {r['slope_type']}")

    # course_master_generated との照合用（修正提案）
    print("\n" + "=" * 80)
    print("course_master 修正が必要な行（坂なし→他 への変更）")
    print("=" * 80)

    # course_master の現在値を簡易的に持つ（venue_code, surface, distance, inside_out, straight_m）→ slope
    # マッチングは (venue_code, distance, course, inside_out) または straight_m で行う
    lookup = {}
    for r in results:
        k = (r["venue_code"], r["course"], r["distance"])
        # 内外がある場合は (venue_code, course, distance, inside_out) で区別
        if r["inside_out"] != "なし":
            k = (r["venue_code"], r["course"], r["distance"], r["inside_out"])
        # straight_m で内/外を区別する場合（新潟・阪神）
        if r["venue_code"] in ("02", "08") and r["course"] == "芝":
            k = (r["venue_code"], r["course"], r["distance"], r["straight_m"])
        lookup[k] = r["slope_type"]

    # 修正案サマリ
    changes = []
    changes.append(("中山", "06", "全", "坂なし→急坂", "直線①平坦+直線②急上坂"))
    changes.append(("東京", "05", "全", "坂なし→軽坂", "直線①上坂+直線②平坦等"))
    changes.append(("中京", "09", "全", "坂なし→急坂", "直線①急上坂+直線②平坦"))
    changes.append(("京都", "07", "全", "坂なしのまま", "直線①平坦+直線②平坦（道中の淀の坂は直線外）"))
    changes.append(("阪神", "08", "全", "坂なし→急坂", "直線①緩下坂+直線②急上坂"))
    changes.append(("函館", "04", "全", "坂なし→軽坂", "直線①緩下坂+直線②緩下坂"))
    changes.append(("福島", "01", "全", "坂なし→軽坂", "直線①緩上坂+直線②緩下坂"))
    changes.append(("小倉", "10", "全", "坂なしのまま", "直線①平坦+直線②平坦（１角以降が緩上坂等＝道中）"))
    changes.append(("盛岡", "36", "全", "坂なし→軽坂", "直線①緩上坂+直線②緩上坂"))
    changes.append(("園田", "50", "全", "坂なしのまま", "直線①②とも平坦（道中の緩坂は直線外）"))
    changes.append(("高知", "54", "全", "坂なしのまま", "直線①②とも平坦（道中の緩坂は直線外）"))

    for v, vc, scope, change, note in changes:
        print(f"  {v} ({vc}): {scope} {change}")
        print(f"    → {note}")

    print("\n※ 札幌・新潟：直線①②とも平坦 → 坂なしのまま")
    print("※ 京都：直線は平坦。淀の坂は道中（３角付近）のため slope_type には含めない")
    print("※ 園田・高知：直線①②は平坦だが、道中(５角など)に緩坂あり。直線のみなら坂なし")
    print("  → ユーザー定義に従い「直線」のみなので、園田・高知は坂なしのまま推奨")


if __name__ == "__main__":
    main()
