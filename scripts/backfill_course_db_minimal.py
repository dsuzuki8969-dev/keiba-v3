"""
race_log から NAR 指定 venue×distance の course_db を補完するスクリプト。

対象 venue×distance（Section 6 修正 3）:
  42（浦和） ダート 1600
  48（名古屋） ダート 800 / 1600 / 1800 / 1900

実行:
  python scripts/backfill_course_db_minimal.py
  python scripts/backfill_course_db_minimal.py --dry-run
"""
import json
import os
import sys
import argparse
import sqlite3

# プロジェクトルートをパスに追加
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database import get_db, set_course_db, transaction

# 補完対象の venue_code × surface × distance
TARGETS = [
    ("42", "ダート", 1600),
    ("48", "ダート",  800),
    ("48", "ダート", 1600),
    ("48", "ダート", 1800),
    ("48", "ダート", 1900),
]


def fetch_race_log_entries(conn: sqlite3.Connection, venue_code: str, surface: str, distance: int) -> list:
    """
    race_log から指定コースの全走行レコードを course_db フォーマットで取得する。
    finish_time_sec > 0 かつ finish_pos < 90（除外馬以外）のみ対象。
    """
    course_id = f"{venue_code}_{surface}_{distance}"
    rows = conn.execute(
        """
        SELECT
            race_date, venue_code, race_id, distance, surface,
            condition, race_name, grade, field_count,
            gate_no, horse_no, jockey_name, jockey_id,
            trainer_id, weight_kg, position_4c,
            positions_corners, finish_pos, finish_time_sec,
            last_3f_sec, first_3f_sec, margin_ahead, margin_behind,
            race_pace, is_generation
        FROM race_log
        WHERE venue_code = ?
          AND surface = ?
          AND distance = ?
          AND finish_time_sec > 0
          AND finish_pos < 90
          AND (status IS NULL OR status = '')
        ORDER BY race_date, race_id, finish_pos
        """,
        (venue_code, surface, distance),
    ).fetchall()

    entries = []
    for r in rows:
        entries.append({
            "race_date":       r[0],
            "venue":           r[1],
            "course_id":       course_id,
            "distance":        r[3],
            "surface":         r[4],
            "condition":       r[5],
            "class_name":      r[6],
            "grade":           r[7] or "その他",
            "field_count":     r[8] or 0,
            "gate_no":         r[9] or 0,
            "horse_no":        r[10] or 0,
            "jockey":          r[11] or "",
            "jockey_id":       r[12] or "",
            "trainer_id":      r[13] or "",
            "weight_kg":       r[14] or 0.0,
            "position_4c":     r[15] or 0,
            "positions_corners": (
                json.loads(r[16]) if r[16] and r[16] not in ("{}", "[]", "") else []
            ),
            "finish_pos":      r[17],
            "finish_time_sec": r[18],
            "last_3f_sec":     r[19] or 0.0,
            "first_3f_sec":    r[20],
            "margin_ahead":    r[21] or 0.0,
            "margin_behind":   r[22] or 0.0,
            "pace":            r[23],
            "is_generation":   bool(r[24]),
        })
    return entries


def main():
    ap = argparse.ArgumentParser(description="NAR course_db 補完スクリプト（race_log → course_db）")
    ap.add_argument("--dry-run", action="store_true", help="DB 書き込みをしない（確認のみ）")
    args = ap.parse_args()

    conn = get_db()

    # 既存 course_db の course_key 一覧を取得
    existing_keys = set(
        row[0] for row in conn.execute("SELECT course_key FROM course_db").fetchall()
    )

    new_entries: dict = {}
    update_entries: dict = {}

    for venue_code, surface, distance in TARGETS:
        course_key = f"{venue_code}_{surface}_{distance}"
        entries = fetch_race_log_entries(conn, venue_code, surface, distance)

        if not entries:
            print(f"[SKIP] {course_key}: race_log にデータなし")
            continue

        if course_key in existing_keys:
            # 既存あり → 上書き更新（全件置換）
            update_entries[course_key] = entries
            print(f"[UPDATE] {course_key}: {len(entries)} 件（既存を更新）")
        else:
            # 新規 → 挿入
            new_entries[course_key] = entries
            print(f"[INSERT] {course_key}: {len(entries)} 件（新規追加）")

    total = len(new_entries) + len(update_entries)
    if total == 0:
        print("追加/更新するコースなし。終了。")
        return

    if args.dry_run:
        print(f"\n[DRY-RUN] {len(new_entries)} 件新規 / {len(update_entries)} 件更新（書き込みなし）")
        return

    # DB 書き込み
    all_entries = {**new_entries, **update_entries}
    set_course_db(all_entries)

    # 確認
    total_rows_added = sum(len(v) for v in all_entries.values())
    print(f"\n完了: {len(all_entries)} コース追加/更新 ({total_rows_added} 走)")

    # 確認クエリ
    for key in sorted(all_entries.keys()):
        row = conn.execute(
            "SELECT course_key, updated_at FROM course_db WHERE course_key = ?", (key,)
        ).fetchone()
        if row:
            data = json.loads(
                conn.execute(
                    "SELECT data_json FROM course_db WHERE course_key = ?", (key,)
                ).fetchone()[0]
            )
            print(f"  {key}: {len(data)} 件 (updated_at: {row[1]})")


if __name__ == "__main__":
    main()
