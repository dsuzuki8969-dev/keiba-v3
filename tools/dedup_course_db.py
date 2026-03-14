"""
course_db の重複走を除去する
重複判定キー: course_id + race_date + finish_pos + horse_no + finish_time_sec
実行: python dedup_course_db.py
"""
import json, sys
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')

DB_PATH = Path(r'c:\Users\dsuzu\keiba\keiba-v3\data\course_db_preload.json')

print("読み込み中...")
with open(DB_PATH, 'r', encoding='utf-8') as f:
    data = json.load(f)

db = data.get('course_db', {})
total_before = sum(len(v) for v in db.values())
print(f"  除去前: {total_before:,}走")

# 重複除去
db_clean = {}
dup_total = 0
year_before = {}
year_after = {}

for cid, runs in db.items():
    seen = set()
    unique_runs = []
    for r in runs:
        # 重複判定キー
        key = (
            r.get('race_date', ''),
            r.get('finish_pos', 0),
            r.get('horse_no', 0),
            round(r.get('finish_time_sec', 0), 1),
        )
        # 年別カウント（除去前）
        yr = r.get('race_date', '')[:4]
        year_before[yr] = year_before.get(yr, 0) + 1

        if key not in seen:
            seen.add(key)
            unique_runs.append(r)
            year_after[yr] = year_after.get(yr, 0) + 1
        else:
            dup_total += 1

    if unique_runs:
        db_clean[cid] = unique_runs

total_after = sum(len(v) for v in db_clean.values())
print(f"  重複除去: {dup_total:,}走")
print(f"  除去後  : {total_after:,}走")

print("\n=== 年別走数（除去前 → 除去後）===")
for yr in sorted(set(list(year_before.keys()) + list(year_after.keys()))):
    b = year_before.get(yr, 0)
    a = year_after.get(yr, 0)
    diff = b - a
    print(f"  {yr}: {b:6,}走 → {a:6,}走  (重複{diff:,}走)")

print("\n保存中...")
tmp_path = str(DB_PATH) + ".tmp"
with open(tmp_path, 'w', encoding='utf-8') as f:
    json.dump({"version": 1, "course_db": db_clean}, f, ensure_ascii=False, indent=0)

import os
os.replace(tmp_path, str(DB_PATH))
print("完了。")
