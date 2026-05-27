"""M-2 方針 2 (Isotonic) + 方針 4 (popularity_blend) の 3 段階 ROI 比較

3 段階比較:
  1. baseline       : data/_archive/predictions_pre_m2v4_20260527.tar.gz (方針 1-4 全て無し)
  2. v4_only        : data/_archive/predictions_pre_m2v2_20260527.tar.gz (方針 4 のみ適用)
  3. v4_plus_v2     : data/predictions/*_pred.json (方針 4 + 方針 2 = WF backtest 後)

期待結果:
  - baseline → v4    : +1〜3pt (前回 commit 2296eee で確認済)
  - v4 → v4+v2       : ?? (本検証で判明)
  - baseline → v4+v2 : 累積効果
"""

import io
import json
import os
import sys
import tarfile
from collections import defaultdict
from typing import Dict, Optional

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

PRED_DIR = os.path.join(PROJECT_ROOT, "data", "predictions")
RESULTS_DIR = os.path.join(PROJECT_ROOT, "data", "results")
ARCHIVE_DIR = os.path.join(PROJECT_ROOT, "data", "_archive")
BASELINE_TAR = os.path.join(ARCHIVE_DIR, "predictions_pre_m2v4_20260527.tar.gz")
V4_ONLY_TAR = os.path.join(ARCHIVE_DIR, "predictions_pre_m2v2_20260527.tar.gz")

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
    """tar.gz から pred.json を stream で読み込む"""
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
            except Exception as e:
                print(f"  WARNING: tar 内 {name} の読込失敗: {e}")
    return out


def load_current_pred(date_key: str) -> Optional[dict]:
    fpath = os.path.join(PRED_DIR, f"{date_key}_pred.json")
    if not os.path.exists(fpath):
        return None
    with open(fpath, "r", encoding="utf-8") as f:
        return json.load(f)


def load_results(date_key: str) -> Optional[dict]:
    fpath = os.path.join(RESULTS_DIR, f"{date_key}_results.json")
    if not os.path.exists(fpath):
        return None
    with open(fpath, "r", encoding="utf-8") as f:
        return json.load(f)


def get_tansho_payout(result_race: dict, winning_horse_no: int) -> Optional[int]:
    """payouts['単勝'] = {'combo': '14', 'payout': 420} 形式"""
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


def calc_roi_for_pred(pred_data: dict, results_data: dict) -> Dict[str, Dict[str, int]]:
    out = defaultdict(lambda: {
        "tansho_races": 0, "tansho_hits": 0,
        "tansho_bet": 0, "tansho_payout": 0,
        "top1_races": 0, "top1_hits": 0,
    })

    for race in pred_data.get("races", []):
        race_id = race.get("race_id", "")
        if not race_id or race_id not in results_data:
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
        if honmei is not None:
            out[org]["tansho_races"] += 1
            out[org]["tansho_bet"] += 100
            if honmei.get("horse_no") == first_horse:
                out[org]["tansho_hits"] += 1
                payout = get_tansho_payout(result, first_horse)
                if payout:
                    out[org]["tansho_payout"] += payout

        active = [h for h in race.get("horses", [])
                  if not h.get("is_scratched") and not h.get("scrape_failed")]
        if active:
            top1 = max(active, key=lambda h: h.get("win_prob", 0) or 0)
            out[org]["top1_races"] += 1
            if top1.get("horse_no") == first_horse:
                out[org]["top1_hits"] += 1

    return dict(out)


def aggregate_period(pred_loader, period: str) -> Dict[str, Dict[str, int]]:
    start, end = WF_PERIODS[period]
    aggregated = defaultdict(lambda: {
        "tansho_races": 0, "tansho_hits": 0,
        "tansho_bet": 0, "tansho_payout": 0,
        "top1_races": 0, "top1_hits": 0,
    })

    pred_files = []
    for fname in sorted(os.listdir(PRED_DIR)):
        if not fname.endswith("_pred.json") or "_backup" in fname or "_prev" in fname:
            continue
        dkey = fname[:8]
        if not dkey.isdigit():
            continue
        if start <= dkey <= end:
            pred_files.append(dkey)

    for dkey in pred_files:
        pred_data = pred_loader(dkey)
        if pred_data is None:
            continue
        results_data = load_results(dkey)
        if results_data is None:
            continue
        day_stats = calc_roi_for_pred(pred_data, results_data)
        for org, stats in day_stats.items():
            for k, v in stats.items():
                aggregated[org][k] += v

    return dict(aggregated)


def _roi(d: dict) -> float:
    return (d.get("tansho_payout", 0) / d.get("tansho_bet", 1) * 100) if d.get("tansho_bet") else 0


def _hit(d: dict) -> float:
    return (d.get("tansho_hits", 0) / d.get("tansho_races", 1) * 100) if d.get("tansho_races") else 0


def _top1(d: dict) -> float:
    return (d.get("top1_hits", 0) / d.get("top1_races", 1) * 100) if d.get("top1_races") else 0


def _sum_orgs(agg: dict) -> dict:
    """JRA + NAR の合計を計算"""
    keys = ["tansho_races", "tansho_hits", "tansho_bet", "tansho_payout",
            "top1_races", "top1_hits"]
    return {k: sum(agg.get(o, {}).get(k, 0) for o in ["JRA", "NAR"]) for k in keys}


def print_3way_comparison(period: str, b: dict, v4: dict, v4v2: dict):
    """3 段階比較表 (baseline / v4 only / v4+v2)"""
    print(f"\n{'='*95}")
    print(f"=== {period} 3 段階 ROI 比較 ===")
    print(f"{'='*95}")
    print(f"{'組織':<8} {'指標':<18} {'baseline':>10} {'v4_only':>10} {'v4+v2':>10} {'v4-base':>9} {'v2-v4':>9}")
    print("-" * 95)

    for org_key in ["JRA", "NAR", "TOTAL"]:
        if org_key == "TOTAL":
            bd = _sum_orgs(b)
            vd = _sum_orgs(v4)
            vvd = _sum_orgs(v4v2)
        else:
            bd = b.get(org_key, {})
            vd = v4.get(org_key, {})
            vvd = v4v2.get(org_key, {})

        if not bd.get("tansho_races") and not vd.get("tansho_races") and not vvd.get("tansho_races"):
            continue

        b_roi, v_roi, vv_roi = _roi(bd), _roi(vd), _roi(vvd)
        b_hit, v_hit, vv_hit = _hit(bd), _hit(vd), _hit(vvd)
        b_top1, v_top1, vv_top1 = _top1(bd), _top1(vd), _top1(vvd)

        print(f"{org_key:<8} {'◎単勝 ROI':<18} {b_roi:>9.1f}% {v_roi:>9.1f}% {vv_roi:>9.1f}% {v_roi-b_roi:>+8.1f}pt {vv_roi-v_roi:>+8.1f}pt")
        print(f"{org_key:<8} {'◎単勝 hit%':<18} {b_hit:>9.1f}% {v_hit:>9.1f}% {vv_hit:>9.1f}% {v_hit-b_hit:>+8.1f}pt {vv_hit-v_hit:>+8.1f}pt")
        print(f"{org_key:<8} {'TOP1→1着 hit%':<18} {b_top1:>9.1f}% {v_top1:>9.1f}% {vv_top1:>9.1f}% {v_top1-b_top1:>+8.1f}pt {vv_top1-v_top1:>+8.1f}pt")
        print(f"{org_key:<8} {'対象 race 数':<18} {bd.get('tansho_races', 0):>10,} {vd.get('tansho_races', 0):>10,} {vvd.get('tansho_races', 0):>10,}")
        print()


def main():
    print("=" * 95)
    print("M-2 方針 2 (Isotonic) + 方針 4 (popularity_blend) 3 段階 ROI 比較")
    print("=" * 95)
    print(f"\nbaseline tar : {os.path.basename(BASELINE_TAR)}")
    print(f"v4_only tar  : {os.path.basename(V4_ONLY_TAR)}")
    print(f"v4+v2 current: {PRED_DIR}")

    print("\n[1/3] baseline pred.json を tar.gz から読込中...")
    baseline_preds = load_preds_from_tar(BASELINE_TAR)
    print(f"  baseline: {len(baseline_preds)} 日分")

    print("\n[2/3] v4_only pred.json を tar.gz から読込中...")
    v4_only_preds = load_preds_from_tar(V4_ONLY_TAR)
    print(f"  v4_only: {len(v4_only_preds)} 日分")

    print("\n[3/3] 期間別 3 段階集計...")

    all_b = defaultdict(lambda: {
        "tansho_races": 0, "tansho_hits": 0,
        "tansho_bet": 0, "tansho_payout": 0,
        "top1_races": 0, "top1_hits": 0,
    })
    all_v4 = defaultdict(lambda: {
        "tansho_races": 0, "tansho_hits": 0,
        "tansho_bet": 0, "tansho_payout": 0,
        "top1_races": 0, "top1_hits": 0,
    })
    all_v4v2 = defaultdict(lambda: {
        "tansho_races": 0, "tansho_hits": 0,
        "tansho_bet": 0, "tansho_payout": 0,
        "top1_races": 0, "top1_hits": 0,
    })

    for period in ["wf_2024", "wf_2025", "wf_2026"]:
        print(f"\n  集計中: {period}")
        b_agg = aggregate_period(lambda d: baseline_preds.get(d), period)
        v4_agg = aggregate_period(lambda d: v4_only_preds.get(d), period)
        v4v2_agg = aggregate_period(load_current_pred, period)

        print_3way_comparison(period, b_agg, v4_agg, v4v2_agg)

        for org, stats in b_agg.items():
            for k, v in stats.items():
                all_b[org][k] += v
        for org, stats in v4_agg.items():
            for k, v in stats.items():
                all_v4[org][k] += v
        for org, stats in v4v2_agg.items():
            for k, v in stats.items():
                all_v4v2[org][k] += v

    print_3way_comparison("全期間 (wf_2024+2025+2026)", dict(all_b), dict(all_v4), dict(all_v4v2))

    print("\n" + "=" * 95)
    print("M-2 方針 4 + 方針 2 累積効果サマリ")
    print("=" * 95)
    print()
    total_b = _sum_orgs(all_b)
    total_v4 = _sum_orgs(all_v4)
    total_v4v2 = _sum_orgs(all_v4v2)
    print(f"全期間 ◎単勝 ROI:")
    print(f"  baseline           : {_roi(total_b):>6.1f}%")
    print(f"  + 方針 4           : {_roi(total_v4):>6.1f}% ({_roi(total_v4)-_roi(total_b):+.1f}pt)")
    print(f"  + 方針 4 + 方針 2  : {_roi(total_v4v2):>6.1f}% ({_roi(total_v4v2)-_roi(total_b):+.1f}pt 累積 / {_roi(total_v4v2)-_roi(total_v4):+.1f}pt 方針 2 単独)")
    print()
    print(f"目標 ROI 100% 超まで: あと {100 - _roi(total_v4v2):.1f}pt (方針 1 head_win 二段化 + 方針 3 特徴量再選定で達成目指す)")


if __name__ == "__main__":
    main()
