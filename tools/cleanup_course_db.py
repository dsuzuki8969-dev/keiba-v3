"""
既存 course_db_preload.json のクリーンアップ＋再グレード付け
  1. 帯広（vc='65'）を除外
  2. 全runのgradeを改善ロジックで再計算
  3. is_generation フラグを付与
実行: python cleanup_course_db.py
"""
import json, sys, shutil, os
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')

# 改善済みロジックをインポート
from src.scraper.course_db_collector import (
    _infer_grade, _infer_grade_nar, _is_generation_race, JRA_CODES_SET
)

DB_PATH = Path(r'c:\Users\dsuzu\keiba\keiba-v3\data\course_db_preload.json')
BACKUP_PATH = DB_PATH.with_suffix('.json.bak')

print("=== course_db クリーンアップ開始 ===")
print(f"バックアップ作成: {BACKUP_PATH}")
shutil.copy2(DB_PATH, BACKUP_PATH)

print("JSONを読み込み中...")
with open(DB_PATH, 'r', encoding='utf-8') as f:
    data = json.load(f)

db_orig = data.get('course_db', {})
orig_count = sum(len(v) for v in db_orig.values())
orig_keys = len(db_orig)
print(f"  元データ: {orig_keys}コースID, {orig_count:,}走")

# --- 1. 帯広（vc='65'）除外 ---
banei_removed = 0
db_clean = {}
for cid, runs in db_orig.items():
    vc = cid.split('_')[0]
    if vc == '65':
        banei_removed += len(runs)
        continue
    db_clean[cid] = runs

print(f"  帯広除外: {banei_removed:,}走 ({len(db_orig) - len(db_clean)}コースID削除)")

# --- 2. grade再計算 + is_generation 付与 ---
grade_changes = 0
gen_flagged = 0
total = sum(len(v) for v in db_clean.values())
processed = 0

for cid, runs in db_clean.items():
    vc = cid.split('_')[0]
    is_jra = vc in JRA_CODES_SET
    for r in runs:
        cn = r.get('class_name', '')
        old_grade = r.get('grade', '')
        new_grade = _infer_grade(cn) if is_jra else _infer_grade_nar(cn)
        is_gen = _is_generation_race(cn)

        if old_grade != new_grade:
            grade_changes += 1
        if is_gen:
            gen_flagged += 1

        r['grade'] = new_grade
        r['is_generation'] = is_gen
        processed += 1
        if processed % 10000 == 0:
            print(f"  処理中: {processed:,}/{total:,}走...")

print(f"  grade変更: {grade_changes:,}走")
print(f"  世代戦フラグ付与: {gen_flagged:,}走 ({gen_flagged/total*100:.1f}%)")

# --- 3. 保存 ---
new_count = sum(len(v) for v in db_clean.values())
print(f"\n保存中... ({new_count:,}走)")
data['course_db'] = db_clean
with open(DB_PATH, 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=0)

print(f"\n=== 完了 ===")
print(f"  元: {orig_count:,}走 → 後: {new_count:,}走 (削減: {orig_count - new_count:,}走)")
print(f"  バックアップ: {BACKUP_PATH}")

# --- 4. クリーンアップ後の確認サマリー ---
print("\n=== クリーンアップ後 グレード分布（上位20） ===")
from collections import Counter
grade_counter = Counter()
gen_counter = Counter()
for cid, runs in db_clean.items():
    for r in runs:
        grade_counter[r.get('grade', '?')] += 1
        if r.get('is_generation'):
            gen_counter['世代戦'] += 1
        else:
            gen_counter['古馬・混合'] += 1

for g, c in grade_counter.most_common(20):
    print(f"  {g:12s}: {c:7,}走")

print(f"\n  古馬・混合戦: {gen_counter['古馬・混合']:,}走")
print(f"  世代限定戦  : {gen_counter['世代戦']:,}走 ({gen_counter['世代戦']/(gen_counter['世代戦']+gen_counter['古馬・混合'])*100:.1f}%)")
