#!/bin/bash
# 9レースの壊れたキャッシュを再生成するための最小再実行
set -e
LOG="scripts/fix_9races.log"
echo "=== 9レース修復開始 $(date +%H:%M:%S) ===" | tee "$LOG"

for dt in 2026-04-26 2026-04-28 2026-05-01 2026-05-05; do
    echo "--- $dt 開始 $(date +%H:%M:%S) ---" | tee -a "$LOG"
    python -X utf8 run_analysis_date.py "$dt" --race-ids-from-pred --no-html --force 2>&1 | tee -a "$LOG"
    echo "--- $dt 完了 $(date +%H:%M:%S) ---" | tee -a "$LOG"
done

echo "=== 全完了 $(date +%H:%M:%S) ===" | tee -a "$LOG"

# 検証
echo "--- 検証 ---" | tee -a "$LOG"
python -X utf8 -c "
import json, os
dates = ['20260426','20260428','20260501','20260505']
for dt in dates:
    fp = f'data/predictions/{dt}_pred.json'
    with open(fp,'r',encoding='utf-8') as f:
        data = json.load(f)
    races = data.get('races',[])
    broken = [r for r in races if len(r.get('horses',[])) <= 3]
    print(f'{dt}: {len(races)}R, 3頭以下={len(broken)}')
    for b in broken:
        print(f'  → {b.get(\"venue\",\"?\")} {b.get(\"race_no\",\"?\")}R: {len(b.get(\"horses\",[]))}頭')
" 2>&1 | tee -a "$LOG"
