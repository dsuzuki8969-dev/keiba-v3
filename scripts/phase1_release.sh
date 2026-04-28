#!/usr/bin/env bash
# Phase 1 買い目指南リリース手順（paraphrase 完了後に実行）
#
# 使い方:
#   bash scripts/phase1_release.sh
#
# 手順:
#   1. run_analysis_date.py で 2026-04-19 を再分析（Phase 1 データ投入）
#   2. verify_tickets.py で検収
#   3. evaluate_tickets_by_mode.py で 3モード KPI 実測（結果照合可能な過去日）
#   4. ダッシュボード再起動
#   5. 目視確認用 URL 提示

set -e
cd "$(dirname "$0")/.."

TARGET_DATE="${1:-2026-04-19}"
YMD=$(echo "$TARGET_DATE" | tr -d '-')

echo "=========================================="
echo "  Phase 1 買い目指南 リリース検証"
echo "  対象日: $TARGET_DATE"
echo "=========================================="
echo

# 1. run_analysis_date.py 実行
echo "[1/5] run_analysis_date.py を実行中..."
python run_analysis_date.py "$TARGET_DATE" --no-html 2>&1 | tail -20 \
    || { echo "[ERROR] 分析に失敗"; exit 1; }
echo

# 2. verify_tickets.py 検収
echo "[2/5] verify_tickets.py で検収..."
python scripts/verify_tickets.py "data/predictions/${YMD}_pred.json" \
    || echo "[WARN] verify_tickets.py でクリティカル違反検出"
echo

# 3. 過去7日間で 3モード KPI 実測（2026-04-19 はまだ結果未確定のためスキップ）
echo "[3/5] 過去 14 日の 3モード KPI 評価（結果照合可能分）..."
python scripts/evaluate_tickets_by_mode.py --after "2026-04-05" --end "2026-04-18" \
    || echo "[WARN] 3モード KPI 評価失敗（データ未投入の可能性あり）"
echo

# 4. フロント ビルド & ダッシュボード再起動
echo "[4/5] フロントビルド確認..."
if [ -d "frontend/dist/assets" ]; then
    echo "  frontend/dist が存在。src/static へ同期..."
    rm -rf src/static && cp -r frontend/dist src/static
    echo "  同期完了"
else
    echo "  [WARN] frontend/dist が無い → npm run build を実行"
    (cd frontend && npm run build)
    rm -rf src/static && cp -r frontend/dist src/static
fi

echo "  ダッシュボード再起動..."
schtasks //End //TN "DAI_Keiba_Dashboard" 2>/dev/null || true
sleep 2
schtasks //Run //TN "DAI_Keiba_Dashboard"
sleep 3
echo

# 5. 目視確認 URL
echo "[5/5] 目視確認チェックリスト:"
echo "  http://127.0.0.1:5051/  → TodayPage"
echo "  - 中山 11R（皐月賞 GⅠ）  : A〜SS 級想定、3モード同時表示 & 点数差あり"
echo "  - C 信頼度の NAR 未勝利戦: 「買わない」メッセージ + 参考ヒモ3点グレー表示"
echo "  - モバイル 375px         : 3モードが縦スタック"
echo "  - 実オッズ反映           : 「実」バッジ(緑)が表示される"
echo
echo "=========================================="
echo "  Phase 1 リリース検証 完了"
echo "=========================================="
