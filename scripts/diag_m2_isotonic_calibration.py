# -*- coding: utf-8 -*-
"""M-2 即試行 #3: WF win_prob を Isotonic Calibration で補正 → EV 戦略再評価

仮説: WF backtest の win_prob = prob * 0.40 は単純近似で真の単勝勝率と乖離している。
これを 2024 race で Isotonic regression 学習し、2025-2026 race に適用して
calibrated_win_prob を作り、calibrated EV = cal_wp × odds で買い目選定する。

期待: calibration が機能すれば EV 戦略の ROI が改善する可能性。
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

try:
    from sklearn.isotonic import IsotonicRegression
    HAS_SKLEARN = True
except ImportError:
    HAS_SKLEARN = False

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


def collect_samples(start_yyyymm: str, end_yyyymm: str):
    """(win_prob, is_top1) ペアを race-horse 単位で収集"""
    samples = []  # (win_prob, odds, is_top1, payout, vc)
    pred_files = sorted(PRED_DIR.glob("*_pred.json"))
    for pf in pred_files:
        date_str = pf.name.replace("_pred.json", "")
        if not (start_yyyymm <= date_str[:6] <= end_yyyymm):
            continue
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

            for h in race.get("horses", []):
                if h.get("is_scratched"):
                    continue
                wp = h.get("win_prob")
                od = h.get("odds")
                if wp is None or od is None or wp <= 0 or od <= 0:
                    continue
                hno = int(h.get("horse_no", 0))
                is_top1 = 1 if hno == top1 else 0
                pay = tansho_pay if is_top1 == 1 else 0
                samples.append((float(wp), float(od), is_top1, pay, vc))
    return samples


def main():
    if not HAS_SKLEARN:
        print("ERROR: sklearn が見つかりません。pip install scikit-learn", file=sys.stderr)
        sys.exit(1)

    print("=" * 110)
    print("M-2 Isotonic Calibration による win_prob 補正 + EV 戦略再評価")
    print("=" * 110)

    print("\n[1] 2024 train データ収集中...", file=sys.stderr)
    train = collect_samples("202401", "202412")
    print(f"  train サンプル: {len(train):,}", file=sys.stderr)

    print("\n[2] 2025-2026 test データ収集中...", file=sys.stderr)
    test = collect_samples("202501", "202612")
    print(f"  test サンプル: {len(test):,}", file=sys.stderr)

    if not train or not test:
        print("ERROR: データ不足")
        return

    # Isotonic regression 学習
    X_train = [s[0] for s in train]
    y_train = [s[2] for s in train]
    iso = IsotonicRegression(out_of_bounds="clip")
    iso.fit(X_train, y_train)
    print(f"\n[3] Isotonic 学習完了 (train hit rate {sum(y_train)/len(y_train)*100:.1f}%)")

    # calibration mapping を表示
    print("\n[4] win_prob → calibrated_win_prob mapping サンプル:")
    print(f"  {'raw wp':>8} | {'cal wp':>8}")
    for raw_wp in [0.05, 0.10, 0.15, 0.20, 0.25, 0.30, 0.40, 0.50, 0.60, 0.70, 0.80, 0.90]:
        cal = iso.predict([raw_wp])[0]
        print(f"  {raw_wp:>8.4f} | {cal:>8.4f}")

    # test データに calibration を適用、EV 戦略で集計
    print("\n[5] test データで EV 戦略集計中...", file=sys.stderr)
    # stats[strategy][category] = [bet, pay, hits, races]
    strategies = ["raw EV>=1.0", "cal EV>=1.0", "cal EV>=1.1", "cal EV>=1.2", "cal EV>=1.5"]
    # race 単位で集計 (各 race で picks をまとめる)
    from collections import defaultdict
    race_picks_raw = defaultdict(list)  # rid -> [(hno, odds, is_top1, pay)]
    race_picks_cal = defaultdict(lambda: defaultdict(list))  # threshold -> rid -> picks

    # test を race ごとに分割
    # まず race 単位で再収集 (collect_samples では race 単位の情報が薄かった)
    test_files = sorted(PRED_DIR.glob("*_pred.json"))
    race_stats = defaultdict(lambda: defaultdict(lambda: [0, 0, 0, 0]))  # strategy -> cat -> stats

    for pf in test_files:
        date_str = pf.name.replace("_pred.json", "")
        if not ("202501" <= date_str[:6] <= "202612"):
            continue
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
            cat = "jra" if vc in JRA_CODES else "nar"
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
            # win_prob, odds, calibrated
            picks_by_strat = {s: [] for s in strategies}
            for h in horses:
                wp = h.get("win_prob") or 0
                od = h.get("odds") or 0
                if wp <= 0 or od <= 0:
                    continue
                cal_wp = float(iso.predict([wp])[0])
                ev_raw = wp * od
                ev_cal = cal_wp * od
                hno = int(h.get("horse_no", 0))
                if ev_raw >= 1.0: picks_by_strat["raw EV>=1.0"].append(hno)
                if ev_cal >= 1.0: picks_by_strat["cal EV>=1.0"].append(hno)
                if ev_cal >= 1.1: picks_by_strat["cal EV>=1.1"].append(hno)
                if ev_cal >= 1.2: picks_by_strat["cal EV>=1.2"].append(hno)
                if ev_cal >= 1.5: picks_by_strat["cal EV>=1.5"].append(hno)

            for s, picks in picks_by_strat.items():
                if not picks:
                    continue
                for k in (cat, "all"):
                    arr = race_stats[s][k]
                    arr[0] += 100 * len(picks)
                    arr[3] += 1
                    if top1 in picks and tansho_pay > 0:
                        arr[1] += tansho_pay
                        arr[2] += 1

    print()
    print("=" * 110)
    print("【結果】2025-2026 test 期間で calibration 適用後 EV 戦略 ROI")
    print("=" * 110)
    print(f"{'戦略':<22} | {'区':>4} {'races':>6} {'bet':>11} {'pay':>11} {'hits':>5} {'hit%':>6} {'ROI':>7}")
    print("-" * 110)
    for s in strategies:
        for cat in ("jra", "nar", "all"):
            b, p, h, r = race_stats[s][cat]
            if b == 0: continue
            roi = p / b * 100
            hit = h / r * 100 if r else 0
            print(f"{s:<22} | {cat.upper():>4} {r:>6,} {b:>11,} {p:>11,} {h:>5,} {hit:>5.1f}% {roi:>6.1f}%")
        print()

    # 最高 ROI
    best = max(strategies, key=lambda s: race_stats[s]["all"][1] / max(race_stats[s]["all"][0], 1) * 100)
    b, p, h, r = race_stats[best]["all"]
    roi = p / b * 100 if b else 0
    print("=" * 110)
    print(f"最高 ROI: {best} → ALL {roi:.1f}% (races {r:,})")
    if roi >= 100:
        print(f"→ ✅ 採用候補 (黒字)")
    else:
        print(f"→ ❌ {roi:.1f}% < 100% / 本質的に ML 学習目的の見直し必要")


if __name__ == "__main__":
    main()
