import sys, io
sys.path.insert(0, '.')
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

from src.scraper.course_db_collector import load_preload_course_db
from src.scraper.race_results import Last3FDBBuilder
from config.settings import COURSE_DB_PRELOAD_PATH

preload = load_preload_course_db(COURSE_DB_PRELOAD_PATH)

# 東京ダート関連
tokyo_keys = [cid for cid in preload if cid.startswith('05')]
print("東京(05)コースID一覧:")
for k in sorted(tokyo_keys):
    print(f"  {k}: {len(preload[k])}走")

l3f_db = Last3FDBBuilder().build(preload)

print("\n東京(05)のlast3f_db:")
for k in l3f_db:
    if k.startswith('05'):
        for pace, times in l3f_db[k].items():
            valid = [t for t in times if 30 < t < 45]
            avg = sum(valid)/len(valid) if valid else None
            print(f"  {k}[{pace}]: 有効{len(valid)}/{len(times)}件 avg={avg:.2f}秒" if avg else f"  {k}[{pace}]: 有効{len(valid)}/{len(times)}件")
