# -*- coding: utf-8 -*-
"""A-3e Lv3 検証: 現 pred.json (Lv3 WF backtest) vs _pred_backup.json (3/19 本番運用) 比較

handoff v2 D-1d 集計時の Lv1 数字 (案 4 183.5%) と、Lv3 WF backtest 集計値 (案 4 72.7%) の
乖離が大きすぎる問題を究明するため、両方の pred.json で同じ集計コードを走らせて比較する。

- _pred.json: 現在 (Lv3 WF backtest --shobu-lv 3 --force 直後)
- _pred_backup.json: 2026-03-19 時点の backup (本番運用 run_analysis_date.py 出力と推定)

仮説: _pred_backup.json は本番運用版 (full engine + 学習リークあり可能性) で ROI が高く、
_pred.json は WF backtest 版 (リーク排除) で ROI が低い (現実的な値)。
"""
from __future__ import annotations
import json
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.payout_normalizer import normalize_payouts
from data.masters.venue_master import JRA_CODES

PRED_DIR = PROJECT_ROOT / "data" / "predictions"
RESULTS_DIR = PROJECT_ROOT / "data" / "results"

MARKED = {"◉", "◎", "○", "▲", "△", "★", "☆"}
HONMEI = {"◉", "◎"}


def _get_tansho_payout(payouts: dict, winner_hno: int) -> int:
    normalized = normalize_payouts(payouts)
    tansho_list = normalized.get("tansho", []) or normalized.get("単勝", [])
    if isinstance(tansho_list, dict):
        tansho_list = [tansho_list]
    for entry in tansho_list:
        combo = str(entry.get("combo", ""))
        try:
            combo_int = int(combo)
        except (ValueError, TypeError):
            continue
        if combo_int == winner_hno:
            return int(entry.get("payout", 0) or 0)
    return 0


def _race_picks(race: dict) -> dict:
    """各案で picks を計算 (active filter は緩和: is_scratched のみ除外)"""
    horses = race.get("horses", [])
    active = [h for h in horses if not h.get("is_scratched")]
    if len(active) < 2:
        return {}

    by_shobu = sorted(active, key=lambda h: h.get("shobu_score", 0) or 0, reverse=True)
    by_comp = sorted(active, key=lambda h: h.get("composite", 0) or 0, reverse=True)
    shobu_top2 = by_shobu[:2]
    comp_top2_nos = {int(h.get("horse_no", 0)) for h in by_comp[:2]}

    honmei = next((h for h in active if h.get("mark") in HONMEI), None)
    overlap = [h for h in shobu_top2 if int(h.get("horse_no", 0)) in comp_top2_nos]
    overlap_honmei = next((h for h in overlap if h.get("mark") in HONMEI), None)

    return {
        "plan4_honmei": [honmei] if honmei else [],
        "plan5_overlap": overlap,
        "plan5b_overlap_honmei": [overlap_honmei] if overlap_honmei else [],
    }


def collect(suffix: str, label: str) -> dict:
    """suffix = '' (current) or '_backup' (3/19 backup) で集計"""
    print(f"\n--- {label} (suffix={suffix!r}) 集計中 ---", file=sys.stderr)
    stats = {p: [0, 0, 0, 0] for p in ["plan4_honmei", "plan5_overlap", "plan5b_overlap_honmei"]}
    n_files = 0
    n_races = 0

    pattern = f"*_pred{suffix}.json"
    pred_files = sorted(PRED_DIR.glob(pattern))
    print(f"  対象ファイル: {len(pred_files)}", file=sys.stderr)

    for pf in pred_files:
        date_str = pf.name.replace(f"_pred{suffix}.json", "").replace("_pred.json", "")
        if len(date_str) == 10 and "-" in date_str:
            nd = date_str.replace("-", "")
        else:
            nd = date_str
        rp = RESULTS_DIR / f"{nd}_results.json"
        if not rp.exists():
            continue
        try:
            results = json.loads(rp.read_text(encoding="utf-8"))
            pred = json.loads(pf.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(results, dict):
            continue
        n_files += 1

        for race in pred.get("races", []):
            race_id = race.get("race_id", "")
            if not race_id or len(race_id) < 6:
                continue
            vc = race_id[4:6]
            if vc not in JRA_CODES:
                continue

            r_result = results.get(race_id)
            if not r_result or not r_result.get("order"):
                continue

            top1 = None
            for o in r_result["order"]:
                fin = o.get("finish", o.get("finish_pos", 99))
                if fin == 1:
                    top1 = o.get("horse_no")
                    break
            if top1 is None:
                continue

            tansho_pay = _get_tansho_payout(r_result.get("payouts", {}), top1)
            picks_by_plan = _race_picks(race)
            if not picks_by_plan:
                continue
            n_races += 1

            for plan, picks in picks_by_plan.items():
                if not picks:
                    continue
                pick_nos = [int(h.get("horse_no", 0)) for h in picks]
                s = stats[plan]
                s[0] += 100 * len(picks)  # bet
                s[2] += 1  # races
                if top1 in pick_nos and tansho_pay > 0:
                    s[1] += tansho_pay
                    s[3] += 1
    return {"stats": stats, "n_files": n_files, "n_races": n_races}


def main():
    cur = collect("", "現 (Lv3 WF backtest)")
    bak = collect("_backup", "_pred_backup.json (3/19 本番運用 backup)")

    print("=" * 100)
    print("【比較】JRA 全期間 ROI: 現 (Lv3 WF) vs _pred_backup (3/19 本番運用)")
    print("=" * 100)
    print(f"{'案':<26} | {'区分':<28} | {'races':>6} {'bet':>10} {'pay':>10} {'ROI':>7}")
    print("-" * 100)
    plan_labels = {
        "plan4_honmei": "案 4: ◎単勝 1 点",
        "plan5_overlap": "案 5: 戦略A合致 (二重一致)",
        "plan5b_overlap_honmei": "派 5b: 二重一致 ∩ ◎",
    }
    for plan, label in plan_labels.items():
        cb, cp, cr, ch = cur["stats"][plan]
        bb, bp, br, bh = bak["stats"][plan]
        cur_roi = cp / cb * 100 if cb else 0
        bak_roi = bp / bb * 100 if bb else 0
        print(f"{label:<26} | {'現 (Lv3 WF)':<28} | {cr:>6,} {cb:>10,} {cp:>10,} {cur_roi:>6.1f}%")
        print(f"{'':<26} | {'_pred_backup (3/19 本番)':<28} | {br:>6,} {bb:>10,} {bp:>10,} {bak_roi:>6.1f}%")
        print(f"{'':<26} | {'差分':<28} | {'':>6} {'':>10} {'':>10} {bak_roi - cur_roi:>+6.1f}pt")
        print()

    print(f"対象ファイル数: 現={cur['n_files']} / backup={bak['n_files']}")
    print(f"集計対象 race: 現={cur['n_races']} / backup={bak['n_races']}")
    print()
    print("=== 分析 ===")
    print("差分が大きい場合 (例 100pt 超) は pred.json の出力源が異なる:")
    print("  - 現 (Lv3 WF): WF backtest = 学習リーク排除 → 真の予測精度")
    print("  - _pred_backup (3/19): 本番運用 = full engine + 学習リークあり可能性")
    print("差分が小さい場合は shobu_score 計算式 (Lv1 vs Lv3) の影響のみ")


if __name__ == "__main__":
    main()
