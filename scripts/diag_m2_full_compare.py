"""M-2 全方針 (1, 2, 3, 4) 4 段階 ROI 比較

4 段階比較:
  1. baseline       : predictions_pre_m2v4_20260527.tar.gz (方針 1-4 全て無し)
  2. v4_only        : predictions_pre_m2v2_20260527.tar.gz (方針 4 のみ)
  3. v4_plus_v2     : predictions_pre_m2v1_20260527.tar.gz (方針 4+2)
  4. v4_v2_v1       : data/predictions/*_pred.json (方針 4+2+1 = head_win 適用後)

期待:
  - baseline → v4    : +1.0pt (commit 2296eee 確認済)
  - v4 → v4+v2       : -0.1pt (commit 7da51c1 確認済 / ノイズ)
  - v4+v2 → v4+v2+v1 : 期待 +10〜20pt (head_win 二段化 / 本検証で判明)
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

STAGES = {
    "baseline": os.path.join(ARCHIVE_DIR, "predictions_pre_m2v4_20260527.tar.gz"),
    "v4":       os.path.join(ARCHIVE_DIR, "predictions_pre_m2v2_20260527.tar.gz"),
    "v4+v2":    os.path.join(ARCHIVE_DIR, "predictions_pre_m2v1_20260527.tar.gz"),
    "v4+v2+v1": None,  # data/predictions/*_pred.json (現状)
}

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


def aggregate_stage(pred_loader, period: str) -> Dict[str, Dict[str, int]]:
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
    keys = ["tansho_races", "tansho_hits", "tansho_bet", "tansho_payout",
            "top1_races", "top1_hits"]
    return {k: sum(agg.get(o, {}).get(k, 0) for o in ["JRA", "NAR"]) for k in keys}


def print_4stage(period: str, stages: dict):
    """4 段階比較表 (baseline / v4 / v4+v2 / v4+v2+v1)"""
    print(f"\n{'='*110}")
    print(f"=== {period} 4 段階 ROI 比較 ===")
    print(f"{'='*110}")
    print(f"{'組織':<8} {'指標':<16} {'baseline':>10} {'v4':>10} {'v4+v2':>10} {'v4+v2+v1':>10} {'v1 単独':>9} {'累積':>9}")
    print("-" * 110)

    for org_key in ["JRA", "NAR", "TOTAL"]:
        if org_key == "TOTAL":
            sd = {k: _sum_orgs(stages[k]) for k in ["baseline", "v4", "v4+v2", "v4+v2+v1"]}
        else:
            sd = {k: stages[k].get(org_key, {}) for k in ["baseline", "v4", "v4+v2", "v4+v2+v1"]}

        if not any(sd[k].get("tansho_races") for k in sd):
            continue

        roi = {k: _roi(sd[k]) for k in sd}
        hit = {k: _hit(sd[k]) for k in sd}
        top1 = {k: _top1(sd[k]) for k in sd}

        print(f"{org_key:<8} {'◎単勝 ROI':<16} {roi['baseline']:>9.1f}% {roi['v4']:>9.1f}% {roi['v4+v2']:>9.1f}% {roi['v4+v2+v1']:>9.1f}% {roi['v4+v2+v1']-roi['v4+v2']:>+8.1f}pt {roi['v4+v2+v1']-roi['baseline']:>+8.1f}pt")
        print(f"{org_key:<8} {'◎単勝 hit%':<16} {hit['baseline']:>9.1f}% {hit['v4']:>9.1f}% {hit['v4+v2']:>9.1f}% {hit['v4+v2+v1']:>9.1f}% {hit['v4+v2+v1']-hit['v4+v2']:>+8.1f}pt {hit['v4+v2+v1']-hit['baseline']:>+8.1f}pt")
        print(f"{org_key:<8} {'TOP1→1着 hit%':<16} {top1['baseline']:>9.1f}% {top1['v4']:>9.1f}% {top1['v4+v2']:>9.1f}% {top1['v4+v2+v1']:>9.1f}% {top1['v4+v2+v1']-top1['v4+v2']:>+8.1f}pt {top1['v4+v2+v1']-top1['baseline']:>+8.1f}pt")
        print(f"{org_key:<8} {'対象 race 数':<16} {sd['baseline'].get('tansho_races', 0):>10,} {sd['v4'].get('tansho_races', 0):>10,} {sd['v4+v2'].get('tansho_races', 0):>10,} {sd['v4+v2+v1'].get('tansho_races', 0):>10,}")
        print()


def main():
    print("=" * 110)
    print("M-2 全方針 (1+2+3+4) 4 段階 ROI 比較")
    print("=" * 110)

    print("\n[1/4] tar.gz から各段階 pred.json をロード中...")
    stage_preds = {}
    for stage_name, tar_path in STAGES.items():
        if tar_path is None:
            stage_preds[stage_name] = None  # current
            continue
        if not os.path.exists(tar_path):
            print(f"  WARNING: {tar_path} が見つかりません")
            stage_preds[stage_name] = {}
            continue
        stage_preds[stage_name] = load_preds_from_tar(tar_path)
        print(f"  {stage_name:<10}: {len(stage_preds[stage_name])} 日分")

    def make_loader(stage_name):
        if stage_name == "v4+v2+v1":
            return load_current_pred
        return lambda d: stage_preds[stage_name].get(d) if stage_preds[stage_name] else None

    print("\n[2/4] 期間別 4 段階集計...")

    all_agg = {k: defaultdict(lambda: {
        "tansho_races": 0, "tansho_hits": 0,
        "tansho_bet": 0, "tansho_payout": 0,
        "top1_races": 0, "top1_hits": 0,
    }) for k in ["baseline", "v4", "v4+v2", "v4+v2+v1"]}

    for period in ["wf_2024", "wf_2025", "wf_2026"]:
        print(f"\n  集計中: {period}")
        stage_agg = {}
        for stage_name in ["baseline", "v4", "v4+v2", "v4+v2+v1"]:
            loader = make_loader(stage_name)
            stage_agg[stage_name] = aggregate_stage(loader, period)
            for org, stats in stage_agg[stage_name].items():
                for k, v in stats.items():
                    all_agg[stage_name][org][k] += v

        print_4stage(period, stage_agg)

    print_4stage("全期間 (wf_2024+2025+2026)", {k: dict(v) for k, v in all_agg.items()})

    print("\n" + "=" * 110)
    print("M-2 全方針 累積効果サマリ (全期間 TOTAL)")
    print("=" * 110)
    totals = {k: _sum_orgs(dict(all_agg[k])) for k in all_agg}
    print()
    print(f"全期間 ◎単勝 ROI:")
    print(f"  baseline           : {_roi(totals['baseline']):>6.1f}%")
    print(f"  + 方針 4 (pop_blend): {_roi(totals['v4']):>6.1f}% ({_roi(totals['v4'])-_roi(totals['baseline']):+.1f}pt)")
    print(f"  + 方針 4 + 方針 2  : {_roi(totals['v4+v2']):>6.1f}% ({_roi(totals['v4+v2'])-_roi(totals['baseline']):+.1f}pt 累積 / {_roi(totals['v4+v2'])-_roi(totals['v4']):+.1f}pt v2 単独)")
    print(f"  + 方針 4+2+1 (head_win): {_roi(totals['v4+v2+v1']):>6.1f}% ({_roi(totals['v4+v2+v1'])-_roi(totals['baseline']):+.1f}pt 累積 / {_roi(totals['v4+v2+v1'])-_roi(totals['v4+v2']):+.1f}pt v1 単独)")
    print()
    print(f"目標 ROI 100% 超まで: あと {100 - _roi(totals['v4+v2+v1']):.1f}pt (方針 3 特徴量再選定で達成目指す)")
    print()
    print(f"全期間 ◎単勝 hit%:")
    print(f"  baseline           : {_hit(totals['baseline']):>6.1f}%")
    print(f"  + 方針 4+2+1       : {_hit(totals['v4+v2+v1']):>6.1f}% ({_hit(totals['v4+v2+v1'])-_hit(totals['baseline']):+.1f}pt 累積)")


if __name__ == "__main__":
    main()
