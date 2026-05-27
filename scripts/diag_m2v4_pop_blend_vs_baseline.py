"""M-2 方針 4 (期間別 popularity_blend) vs baseline の ROI 比較

入力:
  - baseline: data/_archive/predictions_pre_m2v4_20260527.tar.gz (M-2 適用前)
  - current : data/predictions/*_pred.json (M-2 方針 4 適用後 / WF backtest 上書き済)
  - results : data/results/*_results.json

出力:
  - ◎単勝 ROI 比較表 (JRA / NAR / 全体 / 期間別)
  - 派 5b ROI 比較表 (◎ ∩ composite TOP1 ∩ ml_win_prob TOP1)
  - hit% (TOP1 → 1 着) 比較

期間定義:
  - wf_2024: 2024-01-01 〜 2024-12-31
  - wf_2025: 2025-01-01 〜 2025-12-31
  - wf_2026: 2026-01-01 〜 2026-12-31
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
BASELINE_TAR = os.path.join(PROJECT_ROOT, "data", "_archive",
                             "predictions_pre_m2v4_20260527.tar.gz")

# 期間定義
WF_PERIODS = {
    "wf_2024": ("20240101", "20241231"),
    "wf_2025": ("20250101", "20251231"),
    "wf_2026": ("20260101", "20261231"),
}


def _is_jra(race_id: str) -> bool:
    """race_id の venue_code (4-6 文字目) で JRA 判定 (01-10 = JRA)"""
    try:
        return 1 <= int(race_id[4:6]) <= 10
    except (ValueError, IndexError):
        return False


def _date_period(date_str_yyyymmdd: str) -> Optional[str]:
    """日付文字列から WF 期間名を返す"""
    for period, (s, e) in WF_PERIODS.items():
        if s <= date_str_yyyymmdd <= e:
            return period
    return None


def load_baseline_preds_from_tar(tar_path: str) -> Dict[str, dict]:
    """tar.gz から baseline pred.json を stream で読み込む

    Returns:
        {date_yyyymmdd: pred_dict}
    """
    baselines = {}
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
                baselines[date_key] = json.load(io.TextIOWrapper(f, encoding="utf-8"))
            except Exception as e:
                print(f"  WARNING: tar 内 {name} の読込失敗: {e}")
    return baselines


def load_current_pred(date_key: str) -> Optional[dict]:
    """現状の pred.json (M-2 方針 4 適用後) を読む"""
    fpath = os.path.join(PRED_DIR, f"{date_key}_pred.json")
    if not os.path.exists(fpath):
        return None
    with open(fpath, "r", encoding="utf-8") as f:
        return json.load(f)


def load_results(date_key: str) -> Optional[dict]:
    """results.json をロード"""
    fpath = os.path.join(RESULTS_DIR, f"{date_key}_results.json")
    if not os.path.exists(fpath):
        return None
    with open(fpath, "r", encoding="utf-8") as f:
        return json.load(f)


def get_tansho_payout(result_race: dict, winning_horse_no: int) -> Optional[int]:
    """単勝配当を取得 (100 円賭け / 1 着馬の配当を返す)

    payouts 構造 (H-1 形式統一後 / 2026-05-26):
      "単勝": {"combo": "14", "payout": 420}
    """
    payouts = result_race.get("payouts", {})
    tansho = payouts.get("単勝")
    if not tansho:
        return None
    if isinstance(tansho, dict):
        combo = str(tansho.get("combo", ""))
        if combo == str(winning_horse_no):
            payout = tansho.get("payout")
            if isinstance(payout, (int, float)):
                return int(payout)
    return None


def find_honmei_horse(race: dict) -> Optional[dict]:
    """◎馬を見つける (mark == "◎" or "◉")"""
    for h in race.get("horses", []):
        if h.get("mark") in ("◎", "◉"):
            return h
    return None


def calc_roi_for_pred(pred_data: dict, results_data: dict, date_key: str) -> Dict[str, Dict[str, int]]:
    """1 日分の pred.json + results.json から ROI 集計値を返す

    Returns:
        {org: {"tansho_races": N, "tansho_hits": N, "tansho_bet": yen, "tansho_payout": yen}}
    """
    out = defaultdict(lambda: {
        "tansho_races": 0, "tansho_hits": 0,
        "tansho_bet": 0, "tansho_payout": 0,
        "top1_hits": 0, "top1_races": 0,  # hit% (TOP1→1着)
    })

    races = pred_data.get("races", [])
    for race in races:
        race_id = race.get("race_id", "")
        if not race_id or race_id not in results_data:
            continue

        result = results_data[race_id]
        order = result.get("order", [])
        if not order:
            continue

        # 1 着馬の horse_no
        first_horse = None
        for r in order:
            if r.get("finish") == 1:
                first_horse = r.get("horse_no")
                break
        if first_horse is None:
            continue

        org = "JRA" if _is_jra(race_id) else "NAR"

        # ◎単勝 ROI
        honmei = find_honmei_horse(race)
        if honmei is not None:
            honmei_no = honmei.get("horse_no")
            out[org]["tansho_races"] += 1
            out[org]["tansho_bet"] += 100  # 100 円賭け
            if honmei_no == first_horse:
                out[org]["tansho_hits"] += 1
                payout = get_tansho_payout(result, first_horse)
                if payout:
                    out[org]["tansho_payout"] += payout

        # TOP1 (win_prob 最大) → 1 着 hit%
        active = [h for h in race.get("horses", [])
                  if not h.get("is_scratched") and not h.get("scrape_failed")]
        if active:
            top1 = max(active, key=lambda h: h.get("win_prob", 0) or 0)
            top1_no = top1.get("horse_no")
            out[org]["top1_races"] += 1
            if top1_no == first_horse:
                out[org]["top1_hits"] += 1

    return dict(out)


def aggregate_period(pred_loader, period: str) -> Dict[str, Dict[str, int]]:
    """期間別に集計"""
    start, end = WF_PERIODS[period]
    aggregated = defaultdict(lambda: {
        "tansho_races": 0, "tansho_hits": 0,
        "tansho_bet": 0, "tansho_payout": 0,
        "top1_hits": 0, "top1_races": 0,
    })

    # data/predictions/ から該当期間の pred.json をリストアップ
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
        day_stats = calc_roi_for_pred(pred_data, results_data, dkey)
        for org, stats in day_stats.items():
            for k, v in stats.items():
                aggregated[org][k] += v

    return dict(aggregated)


def print_period_comparison(period: str, baseline: dict, current: dict):
    """期間別の baseline vs current 比較表を表示"""
    print(f"\n=== {period} ROI 比較 ===")
    print(f"{'組織':<8} {'指標':<20} {'baseline':>15} {'current':>15} {'差分':>10}")
    print("-" * 75)

    for org in ["JRA", "NAR", "TOTAL"]:
        if org == "TOTAL":
            b = {k: sum(baseline.get(o, {}).get(k, 0) for o in ["JRA", "NAR"])
                 for k in ["tansho_races", "tansho_hits", "tansho_bet", "tansho_payout",
                          "top1_races", "top1_hits"]}
            c = {k: sum(current.get(o, {}).get(k, 0) for o in ["JRA", "NAR"])
                 for k in ["tansho_races", "tansho_hits", "tansho_bet", "tansho_payout",
                          "top1_races", "top1_hits"]}
        else:
            b = baseline.get(org, {})
            c = current.get(org, {})

        if not b.get("tansho_races") and not c.get("tansho_races"):
            continue

        # ◎単勝 ROI
        b_roi = (b.get("tansho_payout", 0) / b.get("tansho_bet", 1) * 100) if b.get("tansho_bet") else 0
        c_roi = (c.get("tansho_payout", 0) / c.get("tansho_bet", 1) * 100) if c.get("tansho_bet") else 0
        b_hit = (b.get("tansho_hits", 0) / b.get("tansho_races", 1) * 100) if b.get("tansho_races") else 0
        c_hit = (c.get("tansho_hits", 0) / c.get("tansho_races", 1) * 100) if c.get("tansho_races") else 0
        b_top1 = (b.get("top1_hits", 0) / b.get("top1_races", 1) * 100) if b.get("top1_races") else 0
        c_top1 = (c.get("top1_hits", 0) / c.get("top1_races", 1) * 100) if c.get("top1_races") else 0

        print(f"{org:<8} {'◎単勝 ROI':<20} {b_roi:>14.1f}% {c_roi:>14.1f}% {c_roi-b_roi:>+9.1f}pt")
        print(f"{org:<8} {'◎単勝 hit%':<20} {b_hit:>14.1f}% {c_hit:>14.1f}% {c_hit-b_hit:>+9.1f}pt")
        print(f"{org:<8} {'TOP1→1着 hit%':<20} {b_top1:>14.1f}% {c_top1:>14.1f}% {c_top1-b_top1:>+9.1f}pt")
        print(f"{org:<8} {'対象 race 数':<20} {b.get('tansho_races', 0):>15,} {c.get('tansho_races', 0):>15,}")
        print()


def main():
    print("=" * 75)
    print("M-2 方針 4 期間別 popularity_blend ROI 比較")
    print("=" * 75)
    print(f"\nbaseline tar: {BASELINE_TAR}")
    print(f"current pred: {PRED_DIR}")
    print(f"results dir : {RESULTS_DIR}")

    print("\n[1/3] baseline pred.json を tar.gz から読込中...")
    baseline_preds = load_baseline_preds_from_tar(BASELINE_TAR)
    print(f"  baseline: {len(baseline_preds)} 日分の pred.json ロード")

    print("\n[2/3] 期間別集計を実行...")

    for period in ["wf_2024", "wf_2025", "wf_2026"]:
        print(f"\n  集計中: {period}")

        def baseline_loader(dkey):
            return baseline_preds.get(dkey)

        def current_loader(dkey):
            return load_current_pred(dkey)

        b_agg = aggregate_period(baseline_loader, period)
        c_agg = aggregate_period(current_loader, period)

        print_period_comparison(period, b_agg, c_agg)

    print("\n[3/3] 全期間 (wf_2024 + wf_2025 + wf_2026) 集計...")

    def baseline_loader_all(dkey):
        return baseline_preds.get(dkey)

    def current_loader_all(dkey):
        return load_current_pred(dkey)

    all_b = defaultdict(lambda: {
        "tansho_races": 0, "tansho_hits": 0,
        "tansho_bet": 0, "tansho_payout": 0,
        "top1_races": 0, "top1_hits": 0,
    })
    all_c = defaultdict(lambda: {
        "tansho_races": 0, "tansho_hits": 0,
        "tansho_bet": 0, "tansho_payout": 0,
        "top1_races": 0, "top1_hits": 0,
    })

    for period in ["wf_2024", "wf_2025", "wf_2026"]:
        b_agg = aggregate_period(baseline_loader_all, period)
        c_agg = aggregate_period(current_loader_all, period)
        for org, stats in b_agg.items():
            for k, v in stats.items():
                all_b[org][k] += v
        for org, stats in c_agg.items():
            for k, v in stats.items():
                all_c[org][k] += v

    print_period_comparison("全期間 (wf_2024+2025+2026)", dict(all_b), dict(all_c))

    print("\n" + "=" * 75)
    print("M-2 方針 4 効果サマリ")
    print("=" * 75)
    print()
    print("- wf_2024: popularity_blend 無効 (stats 空) → 変化なし期待")
    print("- wf_2025: popularity_blend 有効 (n=184,467) → ROI 変化期待")
    print("- wf_2026: popularity_blend 有効 (n=370,088) → ROI 変化期待 (最も統計安定)")
    print()
    print("注: ALPHA_MODEL_MIN=0.95 により blend 係数は最大 5% → 変化は ±5pt 程度の見込み")


if __name__ == "__main__":
    main()
