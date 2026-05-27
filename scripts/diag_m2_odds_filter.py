"""試行 #7: ◎単勝 オッズフィルター戦略 (post-hoc 集計)

v4+v2+v1 状態 (mark 統合前 / 最高 ROI 74.7%) の pred.json をベースに、
◎ 馬のオッズが特定範囲のみ買った場合の ROI を計算する。

odds range 候補:
  - all          : 全範囲 (基準 = 74.7%)
  - 1.5-5.0      : 本命 (人気馬)
  - 2.0-7.0      : 中波 (本命〜中穴)
  - 3.0-10.0    : 中穴 (波乱含む)
  - 5.0-15.0    : 中〜大穴
  - 1.5-3.0      : 本命のみ
  - 3.0-7.0      : 中波のみ
"""

import io
import json
import os
import sys
import tarfile
from collections import defaultdict
from typing import Dict, Optional, List, Tuple

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

PRED_DIR = os.path.join(PROJECT_ROOT, "data", "predictions")
RESULTS_DIR = os.path.join(PROJECT_ROOT, "data", "results")
ARCHIVE_DIR = os.path.join(PROJECT_ROOT, "data", "_archive")

# v4+v2+v1 状態 (mark 統合前 = 最高 ROI 状態)
SOURCE_TAR = os.path.join(ARCHIVE_DIR, "predictions_pre_markint_20260527.tar.gz")

# オッズ範囲試行候補
ODDS_RANGES = [
    ("all",       0.0, 9999.0),
    ("<1.3",      0.0, 1.3),
    ("<1.5",      0.0, 1.5),
    ("<1.8",      0.0, 1.8),
    ("<2.0",      0.0, 2.0),
    ("<2.5",      0.0, 2.5),
    ("<3.0",      0.0, 3.0),
    ("1.3-2.0",   1.3, 2.0),
    ("1.5-2.5",   1.5, 2.5),
    ("1.5-3.0",   1.5, 3.0),
    ("2.0-5.0",   2.0, 5.0),
    ("3.0-10.0",  3.0, 10.0),
    ("7.0-20.0",  7.0, 20.0),
    (">10.0",     10.0, 9999.0),
]

WF_PERIODS = {
    "wf_2024": ("20240101", "20241231"),
    "wf_2025": ("20250101", "20251231"),
    "wf_2026": ("20260101", "20261231"),
}


def _is_jra(race_id: str) -> bool:
    try:
        return 1 <= int(race_id[4:6]) <= 10
    except (ValueError, IndexError):
        return False


def load_preds_from_tar(tar_path: str) -> Dict[str, dict]:
    out = {}
    with tarfile.open(tar_path, "r:gz") as tar:
        for member in tar.getmembers():
            name = os.path.basename(member.name)
            if not name.endswith("_pred.json"):
                continue
            date_key = name[:8]
            if not date_key.isdigit() or len(date_key) != 8:
                continue
            f = tar.extractfile(member)
            if f is None:
                continue
            try:
                out[date_key] = json.load(io.TextIOWrapper(f, encoding="utf-8"))
            except Exception:
                pass
    return out


def load_results(date_key: str) -> Optional[dict]:
    fpath = os.path.join(RESULTS_DIR, f"{date_key}_results.json")
    if not os.path.exists(fpath):
        return None
    with open(fpath, "r", encoding="utf-8") as f:
        return json.load(f)


def get_tansho_payout(result_race: dict, winning_horse_no: int) -> Optional[int]:
    payouts = result_race.get("payouts", {})
    tansho = payouts.get("単勝")
    if not tansho or not isinstance(tansho, dict):
        return None
    if str(tansho.get("combo", "")) == str(winning_horse_no):
        p = tansho.get("payout")
        return int(p) if isinstance(p, (int, float)) else None
    return None


def find_honmei_horse(race: dict) -> Optional[dict]:
    for h in race.get("horses", []):
        if h.get("mark") in ("◎", "◉"):
            return h
    return None


def calc_odds_filter_roi(preds: Dict[str, dict]) -> Dict[str, Dict[str, Dict[str, int]]]:
    """各 odds range × 期間 × 組織 で集計"""
    out = {label: defaultdict(lambda: defaultdict(lambda: {
        "races": 0, "hits": 0, "bet": 0, "payout": 0,
    })) for label, _, _ in ODDS_RANGES}

    for date_key, pred_data in preds.items():
        # 期間判定
        period = None
        for pname, (s, e) in WF_PERIODS.items():
            if s <= date_key <= e:
                period = pname
                break
        if period is None:
            continue

        results_data = load_results(date_key)
        if results_data is None:
            continue

        for race in pred_data.get("races", []):
            race_id = race.get("race_id", "")
            if race_id not in results_data:
                continue
            result = results_data[race_id]
            order = result.get("order", [])
            if not order:
                continue
            first_horse = None
            for r in order:
                if r.get("finish") == 1:
                    first_horse = r.get("horse_no")
                    break
            if first_horse is None:
                continue

            org = "JRA" if _is_jra(race_id) else "NAR"

            honmei = find_honmei_horse(race)
            if honmei is None:
                continue
            honmei_no = honmei.get("horse_no")
            honmei_odds = honmei.get("odds") or 0.0
            if honmei_odds <= 0:
                continue

            # 各 odds range でフィルター適用
            for label, lo, hi in ODDS_RANGES:
                if not (lo <= honmei_odds <= hi):
                    continue
                out[label][period][org]["races"] += 1
                out[label][period][org]["bet"] += 100
                if honmei_no == first_horse:
                    out[label][period][org]["hits"] += 1
                    payout = get_tansho_payout(result, first_horse)
                    if payout:
                        out[label][period][org]["payout"] += payout

    return out


def main():
    print("=" * 110)
    print("試行 #7: ◎単勝 オッズフィルター 戦略 (v4+v2+v1 状態 / 最高 ROI 74.7% 基準)")
    print("=" * 110)

    print(f"\nsource: {os.path.basename(SOURCE_TAR)}")

    print("\n[1/2] pred.json ロード中...")
    preds = load_preds_from_tar(SOURCE_TAR)
    print(f"  {len(preds)} 日分ロード")

    print("\n[2/2] オッズ範囲別 ROI 集計...")
    agg = calc_odds_filter_roi(preds)

    # 全期間 TOTAL でランキング
    print("\n" + "=" * 110)
    print("全期間 TOTAL オッズフィルター別 ROI ランキング")
    print("=" * 110)
    print(f"{'オッズ範囲':<14} {'対象 race':>10} {'hit%':>7} {'ROI':>8} {'累計bet':>10} {'累計payout':>10}")
    print("-" * 110)

    rankings = []
    for label, _, _ in ODDS_RANGES:
        races = 0
        hits = 0
        bet = 0
        payout = 0
        for period, period_d in agg[label].items():
            for org, stats in period_d.items():
                races += stats["races"]
                hits += stats["hits"]
                bet += stats["bet"]
                payout += stats["payout"]

        hit_pct = hits / races * 100 if races else 0
        roi = payout / bet * 100 if bet else 0
        rankings.append((label, races, hit_pct, roi, bet, payout))

    # ROI 降順
    rankings.sort(key=lambda x: -x[3])
    for label, races, hit_pct, roi, bet, payout in rankings:
        marker = "***" if roi >= 100 else (" +" if roi > 74.7 else "  ")
        print(f"{label:<14} {races:>10,} {hit_pct:>6.1f}% {roi:>7.1f}% {bet:>10,} {payout:>10,} {marker}")

    # 期間別 / 組織別の詳細 (top 3 のみ)
    print("\n" + "=" * 110)
    print("ROI 上位 5 オッズ範囲 -- 期間別 / 組織別 詳細")
    print("=" * 110)
    for label, _, _, _, _, _ in rankings[:5]:
        print(f"\n=== {label} ===")
        print(f"{'期間':<10} {'組織':<6} {'races':>8} {'hit%':>7} {'ROI':>8}")
        print("-" * 60)
        for period in ["wf_2024", "wf_2025", "wf_2026"]:
            for org in ["JRA", "NAR"]:
                stats = agg[label][period][org]
                if stats["races"] == 0:
                    continue
                h = stats["hits"] / stats["races"] * 100 if stats["races"] else 0
                r = stats["payout"] / stats["bet"] * 100 if stats["bet"] else 0
                print(f"{period:<10} {org:<6} {stats['races']:>8,} {h:>6.1f}% {r:>7.1f}%")


if __name__ == "__main__":
    main()
