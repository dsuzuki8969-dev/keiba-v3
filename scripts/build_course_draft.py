"""
競馬場コースマスタCSV → たたき台変換
AI/プログラムが読みやすいJSON形式で出力する

使い方:
  python scripts/build_course_draft.py [CSVパス]
  CSVパス省略時: 競馬予想AIフォルダのCSVを参照
"""

import csv
import json
import os
import sys

# venue_master から直接インポート（JRA場コードのハードコード重複を排除）
from data.masters.venue_master import VENUE_NAME_TO_CODE

# コーナー角度の正規化（CSV→CourseMaster）
CORNER_MAP = {
    "大回": "大回り",
    "小回": "小回り",
    "ス曲": "スパイラル",
    "平曲": "大回り",
    "急曲": "小回り",
    "－": "大回り",
}

# 道中から slope_type を推定
def infer_slope_type(row: dict) -> str:
    keys = [f"道中{i}" for i in range(1, 9)]
    vals = [row.get(k, "") for k in keys if k in row]
    text = "".join(str(v) for v in vals)
    if "急上坂" in text or "下坂" in text or "上坂" in text:
        return "急坂"
    if "緩上坂" in text or "緩下坂" in text:
        return "軽坂"
    return "坂なし"

# 内外
def norm_inside_outside(val: str) -> str:
    if not val or val == "－":
        return "なし"
    if "内" in val:
        return "内"
    if "外" in val:
        return "外"
    return "なし"

def main():
    default_csv = r"C:\Users\dsuzu\OneDrive\Desktop\競馬予想AI\競馬場コースマスタ.csv"
    csv_path = sys.argv[1] if len(sys.argv) > 1 else default_csv
    if not os.path.exists(csv_path):
        print(f"[ERROR] CSVが見つかりません: {csv_path}")
        sys.exit(1)

    out_dir = os.path.join(os.path.dirname(__file__), "..", "data", "masters")
    out_path = os.path.join(out_dir, "course_master_draft.json")

    with open(csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    courses = []
    for r in rows:
        venue = r.get("競馬場", "").strip()
        if not venue:
            continue
        venue_code = VENUE_NAME_TO_CODE.get(venue, "")
        if not venue_code:
            continue
        course_val = r.get("コース") or r.get("コース(芝/ダート)", "")
        surface = "芝" if "芝" in course_val or course_val == "芝" else "ダート"
        try:
            distance = int(r.get("距離", 0))
        except (ValueError, TypeError):
            continue
        if distance <= 0:
            continue

        direction = "右" if "右" in (r.get("周回") or "") else "左"
        try:
            straight_m = int(r.get("直線ｍ", 0))
        except (ValueError, TypeError):
            straight_m = 300
        corner_raw = r.get("コーナー角度", "大回")
        corner_type = CORNER_MAP.get(corner_raw, "大回り")
        try:
            corner_count = int(r.get("コーナー回数", 2))
        except (ValueError, TypeError):
            corner_count = 2
        start_dist = r.get("スタート⇒コーナー距離", "平均")
        first_corner = start_dist if start_dist and start_dist != "－" else "平均"
        slope_type = infer_slope_type(r)
        inside_outside = norm_inside_outside(r.get("内外", ""))

        is_jra = venue_code in ("01", "02", "03", "04", "05", "06", "07", "08", "09", "10")

        course_id = f"{venue_code}_{surface}_{distance}"
        if inside_outside != "なし":
            course_id += f"_{inside_outside}"

        courses.append({
            "course_id": course_id,
            "venue": venue,
            "venue_code": venue_code,
            "distance": distance,
            "surface": surface,
            "direction": direction,
            "straight_m": straight_m,
            "corner_count": corner_count,
            "corner_type": corner_type,
            "first_corner": first_corner,
            "slope_type": slope_type,
            "inside_outside": inside_outside,
            "is_jra": is_jra,
            "_raw_corner": corner_raw,
        })

    os.makedirs(out_dir, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(courses, f, ensure_ascii=False, indent=2)

    print(f"[OK] {len(courses)}件 → {out_path}")

if __name__ == "__main__":
    main()
