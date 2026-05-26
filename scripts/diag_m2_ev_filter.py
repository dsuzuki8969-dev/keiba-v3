# -*- coding: utf-8 -*-
"""M-2 ML 根本再設計 候補 (c+δ): EV (期待値) ベース買い目選定

WF Lv3 pred.json の各馬で:
  EV = win_prob × tansho_odds

EV > threshold (期待値プラス領域) の馬だけを単勝買い目とする戦略を検証。

仮説: 既存 ML 予測 (WF ROI 72.7%) は「予測精度」のみを最大化しており、
「ROI 最大化」とは別問題。EV > 1.0 で買い目をフィルタすれば、
期待値プラスの bet のみ採用 → ROI 100% 超に改善する可能性。

複数閾値で感度分析:
  EV >= 1.0 (期待値プラス) / 1.1 / 1.2 / 1.3 / 1.5 / 2.0
  + JRA/NAR 分離 + ALL
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

EV_THRESHOLDS = [1.0, 1.1, 1.2, 1.3, 1.5, 2.0]


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


def main():
    print("=" * 110)
    print("M-2 EV (期待値) ベース買い目選定: WF Lv3 pred.json で全期間検証")
    print("=" * 110)

    # stats[threshold][category] = (bet, pay, hits, races)
    stats = {th: {"jra": [0, 0, 0, 0], "nar": [0, 0, 0, 0], "all": [0, 0, 0, 0]} for th in EV_THRESHOLDS}
    # 比較対象: ◎単勝 (現運用) + EV 閾値なし (全馬対象は無意味なのでスキップ)
    stats_honmei = {"jra": [0, 0, 0, 0], "nar": [0, 0, 0, 0], "all": [0, 0, 0, 0]}

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

            # ◎単勝 (案 4 現運用)
            honmei = next((h for h in horses if h.get("mark") in ("◉", "◎")), None)
            if honmei:
                for k in (cat_key, "all"):
                    s = stats_honmei[k]
                    s[0] += 100
                    s[3] += 1
                    if int(honmei.get("horse_no", 0)) == top1 and tansho_pay > 0:
                        s[1] += tansho_pay
                        s[2] += 1

            # EV 閾値別買い目
            for th in EV_THRESHOLDS:
                picks = []
                for h in horses:
                    wp = h.get("win_prob") or 0
                    odds = h.get("odds") or 0
                    if wp <= 0 or odds <= 0:
                        continue
                    ev = wp * odds
                    if ev >= th:
                        picks.append(h)
                if not picks:
                    continue
                pick_nos = {int(h.get("horse_no", 0)) for h in picks}
                for k in (cat_key, "all"):
                    s = stats[th][k]
                    s[0] += 100 * len(picks)
                    s[3] += 1  # races (1 race として勘定)
                    if top1 in pick_nos and tansho_pay > 0:
                        s[1] += tansho_pay
                        s[2] += 1

    # 結果出力
    print()
    print("=" * 110)
    print("【比較】◎単勝 (現運用) vs EV ベース戦略")
    print("=" * 110)
    print(f"{'戦略':<22} | {'区':>4} {'races':>6} {'bet':>10} {'pay':>10} {'hits':>5} {'hit%':>6} {'ROI':>7}")
    print("-" * 110)

    # ◎単勝
    for cat in ("jra", "nar", "all"):
        b, p, h, r = stats_honmei[cat]
        if b == 0: continue
        roi = p / b * 100
        hit = h / r * 100 if r else 0
        print(f"{'案 4 ◎単勝 (現運用)':<22} | {cat.upper():>4} {r:>6,} {b:>10,} {p:>10,} {h:>5,} {hit:>5.1f}% {roi:>6.1f}%")
    print()

    # EV 閾値別
    for th in EV_THRESHOLDS:
        for cat in ("jra", "nar", "all"):
            b, p, h, r = stats[th][cat]
            if b == 0: continue
            roi = p / b * 100
            hit = h / r * 100 if r else 0
            print(f"{f'EV >= {th}':<22} | {cat.upper():>4} {r:>6,} {b:>10,} {p:>10,} {h:>5,} {hit:>5.1f}% {roi:>6.1f}%")
        print()

    # 採用判断
    print("=" * 110)
    print("【M-2 採用判断】")
    print("=" * 110)
    best_th = None
    best_roi = 0
    for th in EV_THRESHOLDS:
        b, p, h, r = stats[th]["all"]
        if b == 0: continue
        roi = p / b * 100
        if roi > best_roi:
            best_roi = roi
            best_th = th
    if best_th is not None:
        b, p, h, r = stats[best_th]["all"]
        roi = p / b * 100
        print(f"最高 ROI 閾値: EV >= {best_th} で ALL ROI {roi:.1f}% / races {r:,} / bet {b:,}")
        if roi >= 100:
            print(f"→ ✅ 採用候補: EV >= {best_th} で運用妥当 (ROI {roi:.1f}% 黒字)")
        elif roi >= 90:
            print(f"→ ⚠ 閾値・特徴量再調整余地あり ({roi:.1f}% 赤字だが改善見込み)")
        else:
            print(f"→ ❌ EV ベースのみでは ROI {roi:.1f}% < 100%、他の改善 (特徴量/モデル) 必要")


if __name__ == "__main__":
    main()
