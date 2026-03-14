"""
course_master_draft.json → course_master.py の ALL_COURSES 用Pythonコード生成

使い方:
  python scripts/draft_to_course_master.py

出力:
  data/masters/course_master_generated.py に ALL_COURSES 部分を生成
  ※既存 course_master.py を丸ごと置き換えるのではなく、
    生成された行をコピーして差し替える想定
"""

import json
import os

DRAFT_PATH = os.path.join(
    os.path.dirname(__file__), "..", "data", "masters", "course_master_draft.json"
)
OUT_PATH = os.path.join(
    os.path.dirname(__file__), "..", "data", "masters", "course_master_generated.py"
)


def main():
    with open(DRAFT_PATH, encoding="utf-8") as f:
        courses = json.load(f)

    lines = [
        "# たたき台(course_master_draft.json)から生成",
        "# この ALL_COURSES を course_master.py にコピーして差し替えてください",
        "",
        "from src.models import CourseMaster",
        "",
        "ALL_COURSES: list[CourseMaster] = [",
        "",
    ]

    prev_venue = ""
    for c in courses:
        venue = c["venue"]
        if venue != prev_venue:
            lines.append(f"    # ---- {venue} (venue_code: {c['venue_code']}) ----")
            prev_venue = venue

        # CourseMaster(venue, venue_code, distance, surface, direction,
        #   straight_m, corner_count, corner_type,
        #   first_corner, slope_type, inside_outside, is_jra)
        fc = c.get("first_corner") or str(c.get("first_corner_m", "平均"))
        line = (
            f"    CourseMaster("
            f'"{c["venue"]}", "{c["venue_code"]}", {c["distance"]}, '
            f'"{c["surface"]}", "{c["direction"]}", '
            f"{c['straight_m']}, {c['corner_count']}, "
            f'"{c["corner_type"]}", "{fc}", '
            f'"{c["slope_type"]}", "{c["inside_outside"]}", '
            f"{str(c['is_jra'])}"
            f"),"
        )
        lines.append(line)

    lines.append("]")
    lines.append("")

    body = "\n".join(lines)
    # 冒頭の typo 修正
    body = body.replace('"\"', '"""', 1)

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        f.write(body)

    print(f"[OK] {len(courses)}件 → {OUT_PATH}")
    print("  ※ venue_code は draft=venue_master の netkeiba 形式を使用")
    print("  ※ 既存 course_master.py の 地方 venue_code(19-22,30,35-37,41-42,46-47) とは異なります")


if __name__ == "__main__":
    main()
