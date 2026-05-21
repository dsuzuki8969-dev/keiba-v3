#!/bin/bash
# 3頭バグ修復: pred.json 全4日分再生成 + results修復 + 検証
# PID 14116 (5/5) 完了待ち → 4/26 → 4/28 → 5/1 → results修復 → 検証
set -e
cd "$(dirname "$0")/.."

LOG="scripts/regen_all_broken.log"
echo "=== 3頭バグ完全修復開始 $(date '+%Y-%m-%d %H:%M:%S') ===" | tee "$LOG"

# Step 1: PID 14116 (5/5) 完了待ち
echo "[1/6] PID 14116 (5/5 再生成) 完了待ち..." | tee -a "$LOG"
while wmic process where "ProcessId=14116" get ProcessId 2>/dev/null | grep -q "14116"; do
    sz=$(wc -c < data/predictions/20260505_pred.json 2>/dev/null || echo 0)
    echo "  $(date '+%H:%M:%S') 5/5 waiting... size=${sz}" | tee -a "$LOG"
    sleep 30
done
echo "  ✅ PID 14116 完了 $(date '+%H:%M:%S')" | tee -a "$LOG"
sleep 5

# Step 2: 4/26 再生成
echo "[2/6] 4/26 再生成開始 $(date '+%H:%M:%S')" | tee -a "$LOG"
python -X utf8 run_analysis_date.py 2026-04-26 --race-ids-from-db --no-html --force 2>&1 | tee -a "$LOG"
sz426=$(wc -c < data/predictions/20260426_pred.json 2>/dev/null || echo 0)
echo "  ✅ 4/26 完了: ${sz426} bytes $(date '+%H:%M:%S')" | tee -a "$LOG"
sleep 5

# Step 3: 4/28 再生成
echo "[3/6] 4/28 再生成開始 $(date '+%H:%M:%S')" | tee -a "$LOG"
python -X utf8 run_analysis_date.py 2026-04-28 --race-ids-from-db --no-html --force 2>&1 | tee -a "$LOG"
sz428=$(wc -c < data/predictions/20260428_pred.json 2>/dev/null || echo 0)
echo "  ✅ 4/28 完了: ${sz428} bytes $(date '+%H:%M:%S')" | tee -a "$LOG"
sleep 5

# Step 4: 5/1 再生成
echo "[4/6] 5/1 再生成開始 $(date '+%H:%M:%S')" | tee -a "$LOG"
python -X utf8 run_analysis_date.py 2026-05-01 --race-ids-from-db --no-html --force 2>&1 | tee -a "$LOG"
sz501=$(wc -c < data/predictions/20260501_pred.json 2>/dev/null || echo 0)
echo "  ✅ 5/1 完了: ${sz501} bytes $(date '+%H:%M:%S')" | tee -a "$LOG"
sleep 5

# Step 5: results.json 修復 (215 broken entries)
echo "[5/6] results.json 修復開始 $(date '+%H:%M:%S')" | tee -a "$LOG"
python -X utf8 scripts/fix_results_refetch.py 2>&1 | tee -a "$LOG"
echo "  ✅ results修復完了 $(date '+%H:%M:%S')" | tee -a "$LOG"

# Step 6: 検証
echo "[6/6] 検証開始 $(date '+%H:%M:%S')" | tee -a "$LOG"
python -X utf8 scripts/verify_3head_fix.py 2>&1 | tee -a "$LOG"

# サマリ
echo "" | tee -a "$LOG"
echo "=== 全工程完了 $(date '+%Y-%m-%d %H:%M:%S') ===" | tee -a "$LOG"
echo "pred.json sizes:" | tee -a "$LOG"
for dt in 20260426 20260428 20260501 20260505; do
    sz=$(wc -c < "data/predictions/${dt}_pred.json" 2>/dev/null || echo 0)
    echo "  ${dt}: ${sz} bytes" | tee -a "$LOG"
done

# 3頭レース最終チェック
python -X utf8 -c "
import json
for dt in ['20260426','20260428','20260501','20260505']:
    with open(f'data/predictions/{dt}_pred.json','r',encoding='utf-8') as f:
        d = json.load(f)
    small = [r for r in d.get('races',[]) if len([h for h in r.get('horses',[]) if not h.get('is_scratched')]) <= 3]
    status = '✅' if len(small) == 0 else '❌'
    print(f'{status} {dt}: 3頭以下レース={len(small)}')
" 2>&1 | tee -a "$LOG"

echo "ALL_REGEN_DONE $(date '+%H:%M:%S')" | tee -a "$LOG"
