"""
course_db中のjockey_id設定状況を確認
"""
import sys, io
sys.path.insert(0, '.')
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

from src.scraper.course_db_collector import load_preload_course_db
from config.settings import COURSE_DB_PRELOAD_PATH

preload = load_preload_course_db(COURSE_DB_PRELOAD_PATH)

total = 0
with_jockey = 0
sample_jockeys = set()

for cid, runs in preload.items():
    for r in runs:
        total += 1
        if r.jockey_id:
            with_jockey += 1
            sample_jockeys.add(r.jockey_id)
            if len(sample_jockeys) >= 5:
                break
    if len(sample_jockeys) >= 5:
        break

print(f"総走: {total}")
print(f"jockey_idあり: {with_jockey}")
print(f"サンプルjockey_id: {list(sample_jockeys)[:5]}")

# 全データで確認
print("\n-- 全データ確認 --")
total2 = 0
has_j = 0
for cid, runs in preload.items():
    for r in runs:
        total2 += 1
        if r.jockey_id:
            has_j += 1
print(f"全走: {total2}, jockey_idあり: {has_j} ({has_j/total2*100:.1f}%)")
