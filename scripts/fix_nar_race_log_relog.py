"""
fix_nar_race_log_relog.py — race_results.order_json から race_log を再構築

fix_nar_3horse_bug.py が race_results.order_json を更新済みなので、
それを元に race_log の 3 頭立て行を DELETE → 全頭 INSERT。

netkeiba 不使用・既存 DB 内のみで完結。
"""
import sys, os, json, sqlite3, time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

DB = 'data/keiba.db'
conn = sqlite3.connect(DB)
c = conn.cursor()

# 5 頭未満 NAR レース全件抽出 (race_log)
c.execute("""
SELECT race_id, race_date FROM race_log
WHERE is_jra = 0
GROUP BY race_id HAVING COUNT(*) < 5
""")
TARGETS = [(r[0], r[1]) for r in c.fetchall()]
print(f"[開始] race_log 再構築 対象 NAR レース: {len(TARGETS)} 件")

fixed = 0
skipped = 0

for i, (rid, rdate) in enumerate(TARGETS, 1):
    # race_results.order_json から最新データ取得
    c.execute("SELECT order_json FROM race_results WHERE race_id = ?", (rid,))
    row = c.fetchone()
    if not row or not row[0]:
        skipped += 1
        continue
    try:
        order = json.loads(row[0])
    except Exception:
        skipped += 1
        continue
    if not order or len(order) < 5:
        skipped += 1
        continue

    # race_log の既存 row を取得 (template として残す)
    c.execute("""
        SELECT venue_code, surface, distance, field_count, is_jra, course_id, race_name, grade, weather, condition, direction
        FROM race_log WHERE race_id = ? LIMIT 1
    """, (rid,))
    template = c.fetchone()
    if not template:
        skipped += 1
        continue
    venue_code, surface, distance, _old_fc, is_jra, course_id, race_name, grade, weather, condition, direction = template

    # 既存 race_log row を削除
    c.execute("DELETE FROM race_log WHERE race_id = ?", (rid,))

    # 新規 INSERT (order_json の各 horse)
    new_field_count = len(order)
    for h in order:
        try:
            horse_no = int(h.get('horse_no', 0))
            finish = int(h.get('finish_pos') or h.get('finish') or 0)
            time_sec = float(h.get('finish_time_sec') or h.get('time_sec') or 0)
            last_3f = float(h.get('last_3f') or h.get('last_3f_sec') or 0)
            corners_json = json.dumps(h.get('corners', []))
            position_4c = h.get('corners')[-1] if h.get('corners') else None
            weight_kg = float(h.get('weight_kg', 55.0))
            horse_weight = h.get('horse_weight')

            c.execute("""
                INSERT INTO race_log (
                    race_id, race_date, venue_code, surface, distance,
                    horse_no, finish_pos, finish_time_sec, last_3f_sec,
                    field_count, is_jra, course_id, race_name, grade,
                    weather, condition, direction,
                    weight_kg, horse_weight, positions_corners, position_4c
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                rid, rdate, venue_code, surface, distance,
                horse_no, finish, time_sec, last_3f,
                new_field_count, is_jra, course_id, race_name, grade,
                weather, condition, direction,
                weight_kg, horse_weight, corners_json, position_4c
            ))
        except Exception as e:
            print(f"  [{i}/{len(TARGETS)}] {rid} hno={h.get('horse_no')} INSERT err: {e}")

    fixed += 1
    if i % 20 == 0 or i == len(TARGETS):
        print(f"  [{i}/{len(TARGETS)}] race_log 再構築 (新 {new_field_count} 頭)")
        conn.commit()

conn.commit()
print()
print(f"[完了] race_log 再構築: {fixed} 件 / skip {skipped} 件")
conn.close()
