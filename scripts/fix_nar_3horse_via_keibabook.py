"""
fix_nar_3horse_via_keibabook.py — keibabook 経由で 5 頭未満レースを再取得

NAR 公式 get_result が失敗するため keibabook fetch_result を使用。
動作確認済 (scripts/check_3horse_races.py で 5/5 5 件取得成功)。

netkeiba 不使用 (★★ 累犯 2 回・5/5 厳禁) / keibabook + 既存 DB のみ。
レート制限 2.0 秒/件以上。
"""
import sys, os, json, sqlite3, time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.scraper.keibabook_training import KeibabookClient
from src.scraper.keibabook_result import KeibabookResultScraper

DB = 'data/keiba.db'
RATE = 2.0

kb = KeibabookClient()
if not kb.ensure_login():
    print("[ERR] keibabook login failed")
    sys.exit(1)

scraper = KeibabookResultScraper(kb)

conn = sqlite3.connect(DB)
c = conn.cursor()

c.execute("""
SELECT race_id, race_date FROM race_log
WHERE is_jra = 0
GROUP BY race_id HAVING COUNT(*) < 5
ORDER BY race_date DESC
""")
TARGETS = c.fetchall()
print(f"[開始] keibabook 再取得 対象: {len(TARGETS)} 件 (推定 {len(TARGETS) * RATE:.0f}秒)")

fixed = 0
fail = 0
t_start = time.time()

for i, (rid, rdate) in enumerate(TARGETS, 1):
    try:
        time.sleep(RATE)
        result = scraper.fetch_result(netkeiba_race_id=rid, race_date=rdate)
        if not result:
            fail += 1
            if i % 10 == 0:
                print(f"  [{i}/{len(TARGETS)}] {rid} → 取得失敗")
            continue
        order = result.get('order') or []
        if len(order) < 5:
            fail += 1
            continue
        # race_results.order_json 更新
        c.execute(
            "UPDATE race_results SET order_json = ?, fetched_at = datetime('now', 'localtime') WHERE race_id = ?",
            (json.dumps(order, ensure_ascii=False), rid)
        )
        # race_log 再構築 (DELETE → INSERT)
        c.execute("""
            SELECT venue_code, surface, distance, is_jra, course_id, race_name, grade,
                   weather, condition, direction
            FROM race_log WHERE race_id = ? LIMIT 1
        """, (rid,))
        template = c.fetchone()
        if not template:
            continue
        venue_code, surface, distance, is_jra, course_id, race_name, grade, weather, condition, direction = template
        c.execute("DELETE FROM race_log WHERE race_id = ?", (rid,))
        new_field_count = len(order)
        for h in order:
            try:
                horse_no = int(h.get('horse_no', 0))
                if not horse_no:
                    continue
                finish = int(h.get('finish_pos') or h.get('finish') or 0)
                time_sec = float(h.get('finish_time_sec') or h.get('time_sec') or 0)
                last_3f = float(h.get('last_3f') or h.get('last_3f_sec') or 0)
                weight_kg = float(h.get('weight_kg', 55.0))
                horse_weight = h.get('horse_weight')
                corners_list = h.get('corners') or []
                corners_json = json.dumps(corners_list)
                position_4c = corners_list[-1] if corners_list else None
                horse_name = h.get('horse_name', '')
                jockey_name = h.get('jockey_name', '')
                tansho_odds = h.get('odds') or h.get('win_odds')
                popularity = h.get('popularity')

                c.execute("""
                    INSERT INTO race_log (
                        race_id, race_date, venue_code, surface, distance,
                        horse_no, finish_pos, finish_time_sec, last_3f_sec,
                        field_count, is_jra, course_id, race_name, grade,
                        weather, condition, direction,
                        weight_kg, horse_weight, positions_corners, position_4c,
                        horse_name, jockey_name, tansho_odds, win_odds, popularity
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    rid, rdate, venue_code, surface, distance,
                    horse_no, finish, time_sec, last_3f,
                    new_field_count, is_jra, course_id, race_name, grade,
                    weather, condition, direction,
                    weight_kg, horse_weight, corners_json, position_4c,
                    horse_name, jockey_name, tansho_odds, tansho_odds, popularity
                ))
            except Exception as e:
                pass
        fixed += 1
        if i % 10 == 0 or i == len(TARGETS):
            elapsed = time.time() - t_start
            print(f"  [{i}/{len(TARGETS)}] OK ({nh_old:=0} → {new_field_count} 頭) elapsed={elapsed:.0f}s fixed={fixed} fail={fail}")
            conn.commit()
    except Exception as e:
        fail += 1
        print(f"  [{i}/{len(TARGETS)}] {rid} ERR: {e}")

conn.commit()
elapsed = time.time() - t_start
print()
print(f"[完了] {len(TARGETS)} 件 / 経過 {elapsed:.0f}秒")
print(f"  修復成功: {fixed}")
print(f"  失敗: {fail}")
conn.close()
