import sys
sys.path.insert(0, '.')

from config.settings import COURSE_DB_PRELOAD_PATH
from src.scraper.course_db_collector import load_preload_course_db
from src.scraper.personnel import build_jockey_stats_from_course_db, build_trainer_stats_from_course_db
import json

# personnel_db から今日の出走馬の騎手・調教師IDを取得
with open('data/personnel_db.json', 'r', encoding='utf-8') as f:
    pdb = json.load(f)

print("course_db 読み込み中...")
course_db = load_preload_course_db(COURSE_DB_PRELOAD_PATH)
total = sum(len(v) for v in course_db.values())
print(f"総走数: {total:,}")

# 全騎手の course_db 集計
print("\n--- 騎手 course_db フォールバック結果 ---")
for jid, d in list(pdb['jockeys'].items())[:8]:
    name = d.get('jockey_name', jid)
    stats = build_jockey_stats_from_course_db(jid, name, course_db)
    
    # 生の集計数確認
    upper_r = sum(1 for runs in course_db.values() for r in runs 
                  if r.jockey_id == jid and (getattr(r, 'popularity', None) or 99) <= 3)
    total_r = sum(1 for runs in course_db.values() for r in runs if r.jockey_id == jid)
    print(f"  {name:10s}: total={total_r} upper_ninki={upper_r} → dev={stats.upper_long_dev:.1f}")

print("\n--- 調教師 course_db フォールバック結果 ---")
for tid, d in list(pdb['trainers'].items())[:8]:
    name = d.get('trainer_name', tid)
    stats = build_trainer_stats_from_course_db(tid, name, course_db)
    t_runs = sum(1 for runs in course_db.values() for r in runs if r.trainer_id == tid)
    t_wins = sum(1 for runs in course_db.values() for r in runs if r.trainer_id == tid and r.finish_pos == 1)
    print(f"  {name:10s}: {t_runs}走 {t_wins}勝 → rank={stats.rank.value}")
