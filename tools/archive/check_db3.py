import sys
sys.path.insert(0, '.')
import json
from src.scraper.course_db_collector import load_preload_course_db
from config.settings import COURSE_DB_PRELOAD_PATH

print("course_db 読み込み...")
course_db = load_preload_course_db(COURSE_DB_PRELOAD_PATH)
total = sum(len(v) for v in course_db.values())
print(f"コース数: {len(course_db)}, 総走数: {total:,}")

# personnel_db から騎手IDを取得
with open('data/personnel_db.json', 'r', encoding='utf-8') as f:
    pdb = json.load(f)

# 最初の数騎手IDを確認
jids = list(pdb['jockeys'].keys())[:5]
tids = list(pdb['trainers'].keys())[:5]

print("\n--- 騎手 ---")
for jid in jids:
    count = sum(1 for runs in course_db.values() for r in runs if r.jockey_id == jid)
    wins  = sum(1 for runs in course_db.values() for r in runs if r.jockey_id == jid and r.finish_pos == 1)
    pop_data = sum(1 for runs in course_db.values() for r in runs if r.jockey_id == jid and getattr(r, 'popularity', None) is not None)
    name = pdb['jockeys'][jid].get('jockey_name', jid)
    print(f"  {name:12s} ({jid}): {count}走 {wins}勝  popularity有り:{pop_data}")

print("\n--- 調教師 ---")
for tid in tids:
    count = sum(1 for runs in course_db.values() for r in runs if r.trainer_id == tid)
    wins  = sum(1 for runs in course_db.values() for r in runs if r.trainer_id == tid and r.finish_pos == 1)
    name = pdb['trainers'][tid].get('trainer_name', tid)
    print(f"  {name:12s} ({tid}): {count}走 {wins}勝")
