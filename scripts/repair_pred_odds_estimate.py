"""
pred.json の補完馬 (scrape_failed=True) に odds 基準で勝率推定 + 印再割り振り。

【背景】
race_log から馬名のみ補完した馬は win_prob=0 / mark="" で
LIVE STATS 集計や印付けで実質「予想なし」状態。マスター画面で「意味ない」表示。

【方針】
1. pred.json の各 race で「scrape_failed=True or win_prob=0」馬を検出
2. odds (単勝) から win_prob 推定: p ≈ 0.80 / odds
3. レース全体で正規化 (sum win_prob = 1.0)
4. composite を win_prob × 100 で簡易設定
5. 印を上位 5 頭に再割り振り (◉=1位 ◎=2位 ○=3位 ▲=4位 △=5位)

【使い方】
    python scripts/repair_pred_odds_estimate.py 2026-05-05
    python scripts/repair_pred_odds_estimate.py 2026-05-05 --dry-run

【注意】
これは ML 推論ではなくオッズ逆数による簡易推定。
本来の M' 戦略 ROI 計算には不適切だが、表示画面で「予想なし」を回避。
LIVE STATS 集計は scrape_failed フラグで除外推奨。
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# 印 (上位 5 頭)
MARKS = ["◉", "◎", "○", "▲", "△"]


def estimate_race(race: dict) -> int:
    """1 レース内の win_prob/mark を odds から推定。

    Returns:
        修正した馬数
    """
    horses = race.get("horses", [])
    if len(horses) < 2:
        return 0

    # 全頭の odds を取得 (None / 0 は除外)
    valid = []
    for h in horses:
        odds = h.get("odds") or h.get("tansho_odds")
        if odds and odds > 0:
            # 逆数で raw 勝率 (0.80 を係数として控除率反映)
            raw = 0.80 / float(odds)
            valid.append((h, float(odds), raw))

    if len(valid) < 2:
        return 0

    # 正規化 (sum=1.0)
    total_raw = sum(r for _, _, r in valid)
    if total_raw <= 0:
        return 0

    # win_prob 設定
    fixed = 0
    for h, odds, raw in valid:
        wp = raw / total_raw
        # win_prob=0 だった馬のみ更新 (元データ尊重)
        if (h.get("win_prob") or 0) <= 0:
            h["win_prob"] = wp
            h["place2_prob"] = min(0.95, wp * 2.5)
            h["place3_prob"] = min(0.99, wp * 4.0)
            h["composite"] = round(wp * 100, 2)
            h["odds_estimate_repair"] = True
            fixed += 1

    if fixed == 0:
        return 0

    # 印再割り振り (composite or win_prob 降順 上位 5)
    # ただし scrape_failed の馬は印「-」として明示
    for h in horses:
        h["mark"] = ""

    # 印付与: 全馬 (補完馬含む) の win_prob 降順
    sorted_h = sorted(
        horses,
        key=lambda h: (h.get("win_prob") or 0),
        reverse=True,
    )
    for i, h in enumerate(sorted_h[:5]):
        # scrape_failed の馬は印「-」
        if h.get("scrape_failed") or h.get("repair_source"):
            h["mark"] = "-"  # 補完馬は印なし表示
        else:
            h["mark"] = MARKS[i]

    return fixed


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("date", help="YYYY-MM-DD")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    date_key = args.date.replace("-", "")
    pred_path = PROJECT_ROOT / "data" / "predictions" / f"{date_key}_pred.json"
    if not pred_path.exists():
        print(f"[ERR] pred.json なし: {pred_path}")
        return

    with open(pred_path, encoding="utf-8") as f:
        pred = json.load(f)

    total_fixed = 0
    total_races_fixed = 0
    for race in pred.get("races", []):
        if not isinstance(race, dict):
            continue
        n = estimate_race(race)
        if n > 0:
            total_fixed += n
            total_races_fixed += 1

    print(f"[INFO] {args.date}: 修正 {total_races_fixed} レース / {total_fixed} 頭")

    if args.dry_run:
        print("[DRY-RUN] 書き込みなし")
        return

    if total_races_fixed > 0:
        pred["odds_estimate_repaired_at"] = datetime.now().isoformat()
        with open(pred_path, "w", encoding="utf-8") as f:
            json.dump(pred, f, ensure_ascii=False, indent=2)
        print(f"[DONE] {pred_path}: 保存完了")


if __name__ == "__main__":
    main()
