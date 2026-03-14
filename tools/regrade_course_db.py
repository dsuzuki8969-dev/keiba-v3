"""
course_db_preload.json のグレード再計算（帯広除外は完了済み前提）
改善した _infer_grade_nar / _infer_grade / _is_generation_race を再適用する。
実行: python regrade_course_db.py
"""
import json, sys
from pathlib import Path
from collections import Counter

sys.stdout.reconfigure(encoding='utf-8')

from src.scraper.course_db_collector import (
    _infer_grade, _infer_grade_nar, _is_generation_race, JRA_CODES_SET
)

DB_PATH = Path(r'c:\Users\dsuzu\keiba\keiba-v3\data\course_db_preload.json')

print("読み込み中...")
with open(DB_PATH, 'r', encoding='utf-8') as f:
    data = json.load(f)

db = data.get('course_db', {})
total = sum(len(v) for v in db.values())
print(f"  {len(db)}コースID, {total:,}走")

grade_changes = 0
gen_changes = 0
processed = 0

for cid, runs in db.items():
    vc = cid.split('_')[0]
    is_jra = vc in JRA_CODES_SET
    for r in runs:
        cn = r.get('class_name', '')
        old_g = r.get('grade', '')
        new_g = _infer_grade(cn) if is_jra else _infer_grade_nar(cn)
        new_gen = _is_generation_race(cn)
        if old_g != new_g:
            grade_changes += 1
        r['grade'] = new_g
        r['is_generation'] = new_gen
        processed += 1
        if processed % 20000 == 0:
            print(f"  {processed:,}/{total:,}走...")

print(f"  grade変更: {grade_changes:,}走")
print("保存中...")
data['course_db'] = db
with open(DB_PATH, 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=0)

print("=== グレード分布 ===")
grade_counter = Counter()
gen_counter = Counter()
for cid, runs in db.items():
    for r in runs:
        grade_counter[r['grade']] += 1
        key = '世代戦' if r.get('is_generation') else '古馬・混合'
        gen_counter[key] += 1

for g, c in grade_counter.most_common():
    pct = c / total * 100
    print(f"  {g:12s}: {c:7,}走 ({pct:4.1f}%)")

print(f"\n  古馬・混合戦: {gen_counter['古馬・混合']:,}走")
g_cnt = gen_counter['世代戦']
print(f"  世代限定戦  : {g_cnt:,}走 ({g_cnt/total*100:.1f}%)")
print("\n完了。")
