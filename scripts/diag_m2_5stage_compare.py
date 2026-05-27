"""M-2 全方針 + mark 統合 5 段階 ROI 比較

5 段階比較:
  1. baseline       : predictions_pre_m2v4_20260527.tar.gz (方針 1-4 全て無し)
  2. v4_only        : predictions_pre_m2v2_20260527.tar.gz (方針 4 のみ)
  3. v4_plus_v2     : predictions_pre_m2v1_20260527.tar.gz (方針 4+2)
  4. v4+v2+v1       : predictions_pre_markint_20260527.tar.gz (方針 4+2+1 head_win 単体)
  5. v4+v2+v1+mark  : data/predictions/*_pred.json (mark 統合適用後 = head_win → composite adj)
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

STAGES = [
    ("baseline",  os.path.join(ARCHIVE_DIR, "predictions_pre_m2v4_20260527.tar.gz")),
    ("v4",        os.path.join(ARCHIVE_DIR, "predictions_pre_m2v2_20260527.tar.gz")),
    ("v4+v2",     os.path.join(ARCHIVE_DIR, "predictions_pre_m2v1_20260527.tar.gz")),
    ("v4+v2+v1",  os.path.join(ARCHIVE_DIR, "predictions_pre_markint_20260527.tar.gz")),
    ("+mark",     None),  # current
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


def print_5stage(period: str, stages: dict):
    print(f"\n{'='*128}")
    print(f"=== {period} 5 段階 ROI 比較 ===")
    print(f"{'='*128}")
    stage_names = [s[0] for s in STAGES]
    h = f"{'組織':<6} {'指標':<14}"
    for sn in stage_names:
        h += f" {sn:>11}"
    h += f" {'mark 単独':>10} {'累積':>9}"
    print(h)
    print("-" * 128)

    for org_key in ["JRA", "NAR", "TOTAL"]:
        if org_key == "TOTAL":
            sd = {k: _sum_orgs(stages[k]) for k in stage_names}
        else:
            sd = {k: stages[k].get(org_key, {}) for k in stage_names}

        if not any(sd[k].get("tansho_races") for k in sd):
            continue

        roi = {k: _roi(sd[k]) for k in sd}
        top1 = {k: _top1(sd[k]) for k in sd}

        # ROI 行
        roi_line = f"{org_key:<6} {'◎単勝 ROI':<14}"
        for sn in stage_names:
            roi_line += f" {roi[sn]:>10.1f}%"
        roi_line += f" {roi['+mark']-roi['v4+v2+v1']:>+9.1f}pt {roi['+mark']-roi['baseline']:>+8.1f}pt"
        print(roi_line)

        # TOP1 hit% 行
        top1_line = f"{org_key:<6} {'TOP1→1着 hit%':<14}"
        for sn in stage_names:
            top1_line += f" {top1[sn]:>10.1f}%"
        top1_line += f" {top1['+mark']-top1['v4+v2+v1']:>+9.1f}pt {top1['+mark']-top1['baseline']:>+8.1f}pt"
        print(top1_line)
        print()


def main():
    print("=" * 128)
    print("M-2 全方針 + mark 統合 5 段階 ROI 比較")
    print("=" * 128)

    print("\n[1/5] tar.gz から各段階 pred.json をロード中...")
    stage_preds = {}
    for stage_name, tar_path in STAGES:
        if tar_path is None:
            stage_preds[stage_name] = None
            continue
        if not os.path.exists(tar_path):
            print(f"  WARNING: {tar_path} が見つかりません")
            stage_preds[stage_name] = {}
            continue
        stage_preds[stage_name] = load_preds_from_tar(tar_path)
        print(f"  {stage_name:<12}: {len(stage_preds[stage_name])} 日分")

    def make_loader(stage_name):
        if stage_name == "+mark":
            return load_current_pred
        return lambda d: stage_preds[stage_name].get(d) if stage_preds[stage_name] else None

    print("\n[2/5] 期間別 5 段階集計...")

    stage_names = [s[0] for s in STAGES]
    all_agg = {k: defaultdict(lambda: {
        "tansho_races": 0, "tansho_hits": 0,
        "tansho_bet": 0, "tansho_payout": 0,
        "top1_races": 0, "top1_hits": 0,
    }) for k in stage_names}

    for period in ["wf_2024", "wf_2025", "wf_2026"]:
        print(f"\n  集計中: {period}")
        stage_agg = {}
        for sn in stage_names:
            loader = make_loader(sn)
            stage_agg[sn] = aggregate_stage(loader, period)
            for org, stats in stage_agg[sn].items():
                for k, v in stats.items():
                    all_agg[sn][org][k] += v

        print_5stage(period, stage_agg)

    print_5stage("全期間 (wf_2024+2025+2026)", {k: dict(v) for k, v in all_agg.items()})

    print("\n" + "=" * 128)
    print("M-2 全方針 + mark 統合 累積効果サマリ (全期間 TOTAL)")
    print("=" * 128)
    totals = {k: _sum_orgs(dict(all_agg[k])) for k in all_agg}
    print()
    print(f"全期間 ◎単勝 ROI:")
    prev_roi = None
    for sn in stage_names:
        r = _roi(totals[sn])
        diff = f"({r - prev_roi:+.1f}pt)" if prev_roi is not None else ""
        cum = f"({r - _roi(totals['baseline']):+.1f}pt 累積)" if sn != "baseline" else ""
        print(f"  {sn:<14}: {r:>6.1f}% {diff} {cum}")
        prev_roi = r
    print()
    print(f"目標 ROI 100% 超まで: あと {100 - _roi(totals['+mark']):.1f}pt (方針 3 特徴量再選定で達成目指す)")


if __name__ == "__main__":
    main()
