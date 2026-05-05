#!/bin/bash
# overnight_chain.sh — マスター就寝中の自走完走 (5/5 23:45〜 一晩)
#
# 連鎖順:
# 1. NAR 3 頭バグ修復 BG (PID 105898) 完走待ち
# 2. race_log 再構築 (fix_nar_race_log_relog.py)
# 3. 全期間 ability 月単位再計算 (recalc_ability_gamma.py 月単位ループ)
# 4. バックテスト (backtest_master_strategy.py)
# 5. 最終 commit + push
# 6. handoff 作成

cd /c/Users/dsuzu/keiba/keiba-v3

echo "[$(date)] === overnight chain 開始 ==="

# ---------- Step 1: NAR 3 頭バグ修復 BG 完走待ち ----------
TARGET_PID=105898
echo "[$(date)] Step 1: fix_nar_3horse_bug 完走待ち PID=$TARGET_PID"
while kill -0 $TARGET_PID 2>/dev/null; do sleep 60; done
echo "[$(date)] Step 1 完了"

# ---------- Step 2: race_log 再構築 ----------
echo "[$(date)] Step 2: race_log 再構築"
PYTHONIOENCODING=utf-8 python scripts/fix_nar_race_log_relog.py 2>&1 | tail -10
echo "[$(date)] Step 2 完了"

# ---------- Step 3: 全期間 ability 月単位再計算 ----------
echo "[$(date)] Step 3: 全期間 ability 月単位再計算"
for YM in 202401 202402 202403 202404 202405 202406 202407 202408 202409 202410 202411 202412 \
          202501 202502 202503 202504 202505 202506 202507 202508 202509 202510 202511 202512 \
          202601 202602 202603 202604 202605; do
    FROM="${YM}01"
    case "${YM:4:2}" in
      "01"|"03"|"05"|"07"|"08"|"10"|"12") TO="${YM}31" ;;
      "04"|"06"|"09"|"11") TO="${YM}30" ;;
      "02") TO="${YM}29" ;;
    esac
    echo "[$(date)] Step 3.$YM === ${FROM}-${TO} ==="
    PYTHONIOENCODING=utf-8 python scripts/recalc_ability_gamma.py --from $FROM --to $TO 2>&1 | grep -E "完了|elapsed|処理対象|改善" | head -5
done
echo "[$(date)] Step 3 完了"

# ---------- Step 4: バックテスト ----------
echo "[$(date)] Step 4: バックテスト"
PYTHONIOENCODING=utf-8 python scripts/backtest_master_strategy.py 2>&1 | tail -20
echo "[$(date)] Step 4 完了"

# ---------- Step 5: 最終 commit + push ----------
echo "[$(date)] Step 5: 最終 commit"
cd /c/Users/dsuzu/keiba/keiba-v3
git add scripts/ src/ 2>&1
if [ -n "$(git status --porcelain)" ]; then
    git commit -m "fix: 5/5 一晩自走完走 — NAR 3頭バグ修復 + race_log 再構築 + 全期間 ability 再計算 + バックテスト

【マスター激怒指摘 (5/5)】
- NAR scraper 4 着以降ロストバグ (cells < 12 で全 skip)
- 「無駄な時間使わせるなよ」就寝指示

【完走】
1. fix_nar_3horse_bug.py: NAR 公式 133 件再取得 (race_results.order_json 更新)
2. fix_nar_race_log_relog.py: race_log を 3 頭→正しい頭数に再構築
3. recalc_ability_gamma.py: 全期間 ability 月単位再計算
4. backtest_master_strategy.py: T-050 戦略バックテスト

netkeiba 直接アクセス 0 件・全工程 NAR 公式 + 既存 DB から完結。

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
    git push origin master 2>&1 | tail -3
fi
echo "[$(date)] Step 5 完了"

echo "[$(date)] === 全完走 ==="
