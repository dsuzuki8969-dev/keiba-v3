"""M-2 全方針 複勝戦略 ROI 比較

◎単勝の ROI を改善できなかったため、買い目を ◎複勝に切替えて検証。
単勝 hit% 〜32% に対し複勝 hit% 〜50-60% 期待 → ROI 改善余地大。

5 段階比較:
  1. baseline       : predictions_pre_m2v4_20260527.tar.gz
  2. v4_only        : predictions_pre_m2v2_20260527.tar.gz
  3. v4_plus_v2     : predictions_pre_m2v1_20260527.tar.gz
  4. v4+v2+v1       : predictions_pre_markint_20260527.tar.gz
  5. current        : data/predictions/*_pred.json (試行 #4 head_win TOP1)
"""

import io
import json
import os
import sys
import tarfile
from collections import defaultdict
from typing import Dict, Optional, List

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

PRED_DIR = os.path.join(PROJECT_ROOT, "data", "predictions")
RESULTS_DIR = os.path.join(PROJECT_ROOT, "data", "results")
ARCHIVE_DIR = os.path.join(PROJECT_ROOT, "data", "_archive")

STAGES = [
    ("baseline", os.path.join(ARCHIVE_DIR, "predictions_pre_m2v4_20260527.tar.gz")),
    ("v4",       os.path.join(ARCHIVE_DIR, "predictions_pre_m2v2_20260527.tar.gz")),
    ("v4+v2",    os.path.join(ARCHIVE_DIR, "predictions_pre_m2v1_20260527.tar.gz")),
    ("v4+v2+v1", os.path.join(ARCHIVE_DIR, "predictions_pre_markint_20260527.tar.gz")),
    ("current",  None),
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


def get_fukusho_payout(result_race: dict, target_horse_no: int) -> Optional[int]:
    """複勝配当を取得 (target_horse_no が 3 着内なら配当を返す)

    payouts['複勝'] = [{"combo": "14", "payout": 230}, {"combo": "7", "payout": 270}, ...]
    """
    payouts = result_race.get("payouts", {})
    fukusho = payouts.get("複勝")
    if not fukusho:
        return None
    if isinstance(fukusho, list):
        for entry in fukusho:
            if isinstance(entry, dict) and str(entry.get("combo", "")) == str(target_horse_no):
                p = entry.get("payout")
                if isinstance(p, (int, float)):
                    return int(p)
    return None


def find_honmei_horse(race: dict) -> Optional[dict]:
    for h in race.get("horses", []):
        if h.get("mark") in ("◎", "◉"):
            return h
    return None


def get_top3_finishers(result_race: dict) -> List[int]:
    """1-3 着の horse_no リストを返す"""
    out = []
    for r in result_race.get("order", []):
        fin = r.get("finish")
        if fin in (1, 2, 3):
            out.append((fin, r.get("horse_no")))
    out.sort()
    return [hn for _, hn in out]


def calc_fukusho_roi_for_pred(pred_data: dict, results_data: dict) -> Dict[str, Dict[str, int]]:
    """◎複勝の ROI を集計"""
    out = defaultdict(lambda: {
        "fukusho_races": 0, "fukusho_hits": 0,
        "fukusho_bet": 0, "fukusho_payout": 0,
    })

    for race in pred_data.get("races", []):
        race_id = race.get("race_id", "")
        if not race_id or race_id not in results_data:
            continue

        result = results_data[race_id]
        top3 = get_top3_finishers(result)
        if not top3:
            continue

        org = "JRA" if _is_jra(race_id) else "NAR"

        honmei = find_honmei_horse(race)
        if honmei is not None:
            honmei_no = honmei.get("horse_no")
            out[org]["fukusho_races"] += 1
            out[org]["fukusho_bet"] += 100
            if honmei_no in top3:
                out[org]["fukusho_hits"] += 1
                payout = get_fukusho_payout(result, honmei_no)
                if payout:
                    out[org]["fukusho_payout"] += payout

    return dict(out)


def aggregate_stage(pred_loader, period: str) -> Dict[str, Dict[str, int]]:
    start, end = WF_PERIODS[period]
    aggregated = defaultdict(lambda: {
        "fukusho_races": 0, "fukusho_hits": 0,
        "fukusho_bet": 0, "fukusho_payout": 0,
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
        day_stats = calc_fukusho_roi_for_pred(pred_data, results_data)
        for org, stats in day_stats.items():
            for k, v in stats.items():
                aggregated[org][k] += v

    return dict(aggregated)


def _roi(d: dict) -> float:
    return (d.get("fukusho_payout", 0) / d.get("fukusho_bet", 1) * 100) if d.get("fukusho_bet") else 0


def _hit(d: dict) -> float:
    return (d.get("fukusho_hits", 0) / d.get("fukusho_races", 1) * 100) if d.get("fukusho_races") else 0


def _sum_orgs(agg: dict) -> dict:
    keys = ["fukusho_races", "fukusho_hits", "fukusho_bet", "fukusho_payout"]
    return {k: sum(agg.get(o, {}).get(k, 0) for o in ["JRA", "NAR"]) for k in keys}


def print_5stage(period: str, stages: dict):
    print(f"\n{'='*110}")
    print(f"=== {period} ◎複勝 ROI 比較 (5 段階) ===")
    print(f"{'='*110}")
    stage_names = [s[0] for s in STAGES]
    h = f"{'組織':<6} {'指標':<14}"
    for sn in stage_names:
        h += f" {sn:>10}"
    print(h)
    print("-" * 110)

    for org_key in ["JRA", "NAR", "TOTAL"]:
        if org_key == "TOTAL":
            sd = {k: _sum_orgs(stages[k]) for k in stage_names}
        else:
            sd = {k: stages[k].get(org_key, {}) for k in stage_names}
        if not any(sd[k].get("fukusho_races") for k in sd):
            continue
        roi_line = f"{org_key:<6} {'◎複勝 ROI':<14}"
        hit_line = f"{org_key:<6} {'◎複勝 hit%':<14}"
        for sn in stage_names:
            roi_line += f" {_roi(sd[sn]):>9.1f}%"
            hit_line += f" {_hit(sd[sn]):>9.1f}%"
        print(roi_line)
        print(hit_line)


def main():
    print("=" * 110)
    print("M-2 全方針 ◎複勝 ROI 比較 (試行 #6 / 単勝→複勝への買い目変更)")
    print("=" * 110)

    print("\n[1/3] tar.gz から各段階 pred.json をロード中...")
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

    def make_loader(stage_name):
        if stage_name == "current":
            return load_current_pred
        return lambda d: stage_preds[stage_name].get(d) if stage_preds[stage_name] else None

    print("\n[2/3] 期間別 5 段階集計...")

    stage_names = [s[0] for s in STAGES]
    all_agg = {k: defaultdict(lambda: {
        "fukusho_races": 0, "fukusho_hits": 0,
        "fukusho_bet": 0, "fukusho_payout": 0,
    }) for k in stage_names}

    for period in ["wf_2024", "wf_2025", "wf_2026"]:
        print(f"\n  集計中: {period}")
        stage_agg = {}
        for sn in stage_names:
            stage_agg[sn] = aggregate_stage(make_loader(sn), period)
            for org, stats in stage_agg[sn].items():
                for k, v in stats.items():
                    all_agg[sn][org][k] += v
        print_5stage(period, stage_agg)

    print_5stage("全期間 (wf_2024+2025+2026)", {k: dict(v) for k, v in all_agg.items()})

    print("\n" + "=" * 110)
    print("◎複勝戦略 累積効果サマリ (全期間 TOTAL)")
    print("=" * 110)
    totals = {k: _sum_orgs(dict(all_agg[k])) for k in all_agg}
    print()
    print(f"全期間 ◎複勝 ROI:")
    for sn in stage_names:
        r = _roi(totals[sn])
        h = _hit(totals[sn])
        print(f"  {sn:<14}: ROI {r:>6.1f}%  hit% {h:>6.1f}%")
    print()
    print(f"参考: ◎単勝 全期間 ROI = 74.7% / hit% = 32.2% (v4+v2+v1 状態)")
    print(f"目標 ROI 100% 超まで: あと {100 - _roi(totals[stage_names[-1]]):.1f}pt")


if __name__ == "__main__":
    main()
