import sys, io
sys.path.insert(0, '.')
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

from src.scraper.course_db_collector import load_preload_course_db
from config.settings import COURSE_DB_PRELOAD_PATH
from src.scraper.race_results import Last3FDBBuilder

preload = load_preload_course_db(COURSE_DB_PRELOAD_PATH)

# 東京ダート関連のcourse_idを探す
tokyo_dirt = {cid: runs for cid, runs in preload.items() if cid.startswith('05') and 'D' in cid.upper()}
print(f"東京ダートコースID一覧 ({len(tokyo_dirt)}件):")
for cid, runs in sorted(tokyo_dirt.items())[:20]:
    print(f"  {cid}: {len(runs)}走")

# last3f_dbの中身
l3f_db = Last3FDBBuilder().build(preload)
print(f"\npace_last3f_db キー数: {len(l3f_db)}")
print("サンプル:")
for cid in list(l3f_db.keys())[:5]:
    print(f"  {cid}: {dict(list(l3f_db[cid].items())[:2])}")

# 東京ダート1600のkey
for cid in l3f_db:
    if '05' in cid and 'D' in cid.upper() and '1600' in cid:
        print(f"\n東京D1600: {cid} -> {l3f_db[cid]}")
        break
