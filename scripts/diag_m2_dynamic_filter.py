# -*- coding: utf-8 -*-
"""M-2 別試行: 動的フィルター (confidence + composite 順位 + EV 複合)

EV >= 1.0 単独は ROI 67.9% < ◎単勝 75.1% で失敗。
本 diag では confidence × composite 順位 × EV を組合せた動的フィルターを試す:

戦略 D1: ◎単勝 (現運用 ベースライン)
戦略 D2: ◎ かつ confidence in (SS, S) のみ
戦略 D3: ◎ かつ composite >= 50 のみ
戦略 D4: ◎ かつ EV >= 1.2 のみ (◎ 馬の中で EV プラス領域)
戦略 D5: ◎ かつ confidence in (SS, S) かつ EV >= 1.0 のみ
戦略 D6: ◎ かつ confidence in (SS, S, A, B) かつ EV >= 1.2 のみ
戦略 D7: ◎ かつ odds <= 5.0 のみ (低オッズ高確率帯)
戦略 D8: ◎ かつ odds in (5.0, 15.0) のみ (中オッズ穴狙い)
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


STRATEGIES = {
    "D1 ◎単勝 (現運用)": lambda h, conf: h.get("mark") in ("◉", "◎"),
    "D2 ◎ ∩ conf=SS,S": lambda h, conf: h.get("mark") in ("◉", "◎") and conf in ("SS", "S"),
    "D3 ◎ ∩ comp>=50": lambda h, conf: h.get("mark") in ("◉", "◎") and (h.get("composite") or 0) >= 50,
    "D4 ◎ ∩ EV>=1.2": lambda h, conf: h.get("mark") in ("◉", "◎") and (h.get("win_prob") or 0) * (h.get("odds") or 0) >= 1.2,
    "D5 ◎ ∩ SS,S ∩ EV>=1.0": lambda h, conf: h.get("mark") in ("◉", "◎") and conf in ("SS", "S") and (h.get("win_prob") or 0) * (h.get("odds") or 0) >= 1.0,
    "D6 ◎ ∩ SS,S,A,B ∩ EV>=1.2": lambda h, conf: h.get("mark") in ("◉", "◎") and conf in ("SS", "S", "A", "B") and (h.get("win_prob") or 0) * (h.get("odds") or 0) >= 1.2,
    "D7 ◎ ∩ odds<=5.0": lambda h, conf: h.get("mark") in ("◉", "◎") and 0 < (h.get("odds") or 0) <= 5.0,
    "D8 ◎ ∩ 5.0<odds<=15.0": lambda h, conf: h.get("mark") in ("◉", "◎") and 5.0 < (h.get("odds") or 0) <= 15.0,
    "D9 ◎ ∩ comp>=60": lambda h, conf: h.get("mark") in ("◉", "◎") and (h.get("composite") or 0) >= 60,
    "D10 ◎ ∩ comp>=70": lambda h, conf: h.get("mark") in ("◉", "◎") and (h.get("composite") or 0) >= 70,
    "D11 ◎ ∩ comp>=80": lambda h, conf: h.get("mark") in ("◉", "◎") and (h.get("composite") or 0) >= 80,
}


def main():
    stats = {name: {"jra": [0, 0, 0, 0], "nar": [0, 0, 0, 0], "all": [0, 0, 0, 0]} for name in STRATEGIES}

    pred_files = sorted(PRED_DIR.glob("*_pred.json"))
    n = len(pred_files)
    print(f"対象 pred.json: {n}", file=sys.stderr)

    for i, pf in enumerate(pred_files):
        if i % 200 == 0:
            print(f"  進捗 {i}/{n}", file=sys.stderr)
        date_str = pf.name.replace("_pred.json", "")
        rp = RESULTS_DIR / f"{date_str}_results.json"
        if not rp.exists():
            continue
        try:
            results = json.loads(rp.read_text(encoding="utf-8"))
            pred = json.loads(pf.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(results, dict):
            continue

        for race in pred.get("races", []):
            race_id = race.get("race_id", "")
            if not race_id or len(race_id) < 6:
                continue
            vc = race_id[4:6]
            is_jra = vc in JRA_CODES
            cat_key = "jra" if is_jra else "nar"
            conf = race.get("confidence", "") or ""

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

            horses = [h for h in race.get("horses", []) if not h.get("is_scratched")]
            if not horses:
                continue

            for name, predicate in STRATEGIES.items():
                pick = next((h for h in horses if predicate(h, conf)), None)
                if not pick:
                    continue
                for k in (cat_key, "all"):
                    s = stats[name][k]
                    s[0] += 100
                    s[3] += 1
                    if int(pick.get("horse_no", 0)) == top1 and tansho_pay > 0:
                        s[1] += tansho_pay
                        s[2] += 1

    print()
    print("=" * 120)
    print("M-2 動的フィルター戦略 ROI 比較 (WF Lv3 pred.json 全期間)")
    print("=" * 120)
    print(f"{'戦略':<30} | {'区':>4} {'races':>6} {'bet':>10} {'pay':>10} {'hits':>5} {'hit%':>6} {'ROI':>7}")
    print("-" * 120)
    for name in STRATEGIES:
        for cat in ("jra", "nar", "all"):
            b, p, h, r = stats[name][cat]
            if b == 0: continue
            roi = p / b * 100
            hit = h / r * 100 if r else 0
            print(f"{name:<30} | {cat.upper():>4} {r:>6,} {b:>10,} {p:>10,} {h:>5,} {hit:>5.1f}% {roi:>6.1f}%")
        print()

    # 採用判断: ALL で最高 ROI の戦略
    best = max(STRATEGIES, key=lambda n: (stats[n]["all"][1] / max(stats[n]["all"][0], 1) * 100))
    b, p, h, r = stats[best]["all"]
    roi = p / b * 100 if b else 0
    print("=" * 120)
    print(f"最高 ROI: {best} → ALL {roi:.1f}% (races {r:,})")
    if roi >= 100:
        print(f"→ ✅ 採用候補 (黒字)")
    else:
        print(f"→ ❌ {roi:.1f}% < 100% → M-2 さらに本質的改革必要")


if __name__ == "__main__":
    main()
