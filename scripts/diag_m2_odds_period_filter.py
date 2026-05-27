"""試行 #2: ◎単勝 オッズ × 期間 × 組織 マトリクス集計 (post-hoc)

v4+v2+v1 状態 (mark 統合前 / 最高 ROI 74.7% 基準) の pred.json から、
オッズフィルター + 期間 + 組織の 96 通り組み合わせで ROI を集計する。

集計マトリクス:
  オッズ範囲: <1.1 / <1.2 / <1.3 / <1.5 / <1.7 / <2.0 / <2.5 / <3.0
  期間      : wf_2024 / wf_2025 / wf_2026 / 全期間
  組織      : JRA / NAR / TOTAL
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

RESULTS_DIR = os.path.join(PROJECT_ROOT, "data", "results")
ARCHIVE_DIR = os.path.join(PROJECT_ROOT, "data", "_archive")

# v4+v2+v1 状態 (mark 統合前 = 最高 ROI 状態)
SOURCE_TAR = os.path.join(ARCHIVE_DIR, "predictions_pre_markint_20260527.tar.gz")

# オッズ範囲 (上限のみ、下限は 0)
ODDS_RANGES = [
    ("<1.1", 0.0, 1.1),
    ("<1.2", 0.0, 1.2),
    ("<1.3", 0.0, 1.3),
    ("<1.5", 0.0, 1.5),
    ("<1.7", 0.0, 1.7),
    ("<2.0", 0.0, 2.0),
    ("<2.5", 0.0, 2.5),
    ("<3.0", 0.0, 3.0),
]

WF_PERIODS = {
    "wf_2024": ("20240101", "20241231"),
    "wf_2025": ("20250101", "20251231"),
    "wf_2026": ("20260101", "20261231"),
}

ORGS = ["JRA", "NAR"]
PERIODS = ["wf_2024", "wf_2025", "wf_2026", "全期間"]


def _is_jra(race_id: str) -> bool:
    try:
        return 1 <= int(race_id[4:6]) <= 10
    except (ValueError, IndexError):
        return False


def load_preds_from_tar(tar_path: str) -> Dict[str, dict]:
    """tar.gz から pred.json を全て読み込む"""
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
    """results.json を読み込む"""
    fpath = os.path.join(RESULTS_DIR, f"{date_key}_results.json")
    if not os.path.exists(fpath):
        return None
    with open(fpath, "r", encoding="utf-8") as f:
        return json.load(f)


def get_tansho_payout(result_race: dict, winning_horse_no) -> Optional[int]:
    """単勝払戻金を取得"""
    payouts = result_race.get("payouts", {})
    tansho = payouts.get("単勝")
    if not tansho or not isinstance(tansho, dict):
        return None
    if str(tansho.get("combo", "")) == str(winning_horse_no):
        p = tansho.get("payout")
        return int(p) if isinstance(p, (int, float)) else None
    return None


def find_honmei_horse(race: dict) -> Optional[dict]:
    """◎ 馬を返す"""
    for h in race.get("horses", []):
        if h.get("mark") in ("◎", "◉"):
            return h
    return None


def empty_stats() -> dict:
    return {"races": 0, "hits": 0, "bet": 0, "payout": 0}


def main():
    print("=" * 80)
    print("試行 #2: ◎単勝 オッズ × 期間 × 組織 マトリクス")
    print("=" * 80)
    print(f"\nsource: {os.path.basename(SOURCE_TAR)} (基準 ROI 74.7%)")

    # --- [1/2] pred.json ロード ---
    print("\n[1/2] pred.json ロード中 (tar.gz から)...")
    if not os.path.exists(SOURCE_TAR):
        print(f"  ERROR: {SOURCE_TAR} が見つかりません")
        sys.exit(1)
    preds = load_preds_from_tar(SOURCE_TAR)
    print(f"  {len(preds)} 日分ロード完了")

    # --- [2/2] 集計 ---
    print("\n[2/2] 96 通り組み合わせ集計中...")

    # agg[odds_label][period][org] = {races, hits, bet, payout}
    agg: Dict[str, Dict[str, Dict[str, dict]]] = {
        label: {
            period: {"JRA": empty_stats(), "NAR": empty_stats()}
            for period in WF_PERIODS
        }
        for label, _, _ in ODDS_RANGES
    }

    processed = 0
    for date_key, pred_data in sorted(preds.items()):
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

        processed += 1

        for race in pred_data.get("races", []):
            race_id = race.get("race_id", "")
            if race_id not in results_data:
                continue
            result = results_data[race_id]
            order = result.get("order", [])
            if not order:
                continue

            # 1 着馬番を取得
            first_horse = None
            for r in order:
                if r.get("finish") == 1:
                    first_horse = r.get("horse_no")
                    break
            if first_horse is None:
                continue

            org = "JRA" if _is_jra(race_id) else "NAR"

            # ◎ 馬取得
            honmei = find_honmei_horse(race)
            if honmei is None:
                continue
            honmei_no = honmei.get("horse_no")
            honmei_odds = honmei.get("odds") or 0.0
            if honmei_odds <= 0:
                continue

            # 各オッズ範囲でフィルター適用
            for label, lo, hi in ODDS_RANGES:
                if not (lo <= honmei_odds < hi):
                    continue
                st = agg[label][period][org]
                st["races"] += 1
                st["bet"] += 100
                if honmei_no == first_horse:
                    st["hits"] += 1
                    payout = get_tansho_payout(result, first_horse)
                    if payout:
                        st["payout"] += payout

    print(f"  集計完了: {processed} 日分")

    # --- 出力 ---
    print("\n" + "=" * 80)
    print("ROI 100% 超 race 群リスト (race 数 >= 30 のみ)")
    print("-" * 80)
    print(f"{'オッズ範囲':<10} {'期間':<10} {'組織':<6} {'races':>6} {'hit%':>7} {'ROI':>8} {'bet':>8} {'payout':>9}")
    print("-" * 80)

    over100_list = []
    for label, _, _ in ODDS_RANGES:
        for period in WF_PERIODS:
            for org in ORGS:
                st = agg[label][period][org]
                if st["races"] < 30:
                    continue
                roi = st["payout"] / st["bet"] * 100 if st["bet"] else 0
                if roi >= 100:
                    hit_pct = st["hits"] / st["races"] * 100 if st["races"] else 0
                    over100_list.append((label, period, org, st["races"], hit_pct, roi, st["bet"], st["payout"]))

    if over100_list:
        over100_list.sort(key=lambda x: -x[5])
        for label, period, org, races, hit_pct, roi, bet, payout in over100_list:
            print(f"{label:<10} {period:<10} {org:<6} {races:>6,} {hit_pct:>6.1f}% {roi:>7.1f}% {bet:>8,} {payout:>9,} 🎉")
    else:
        print("  (race 数 >= 30 で ROI 100% 超なし)")

    # --- 全マトリクス ---
    print("\n" + "=" * 80)
    print("全マトリクス (オッズ × 期間 × 組織)")
    print("=" * 80)

    # ヘッダー
    print(f"{'オッズ':<10} {'期間':<10}  {'JRA':>22}  {'NAR':>22}  {'TOTAL':>22}")
    print(f"{'':10} {'':10}  {'races/hit%/ROI':>22}  {'races/hit%/ROI':>22}  {'races/hit%/ROI':>22}")
    print("-" * 90)

    for label, _, _ in ODDS_RANGES:
        for period in list(WF_PERIODS.keys()) + ["全期間"]:
            row = []
            total = empty_stats()
            for org in ORGS:
                if period == "全期間":
                    # 全 WF 期間合算
                    st = empty_stats()
                    for p in WF_PERIODS:
                        for k in st:
                            st[k] += agg[label][p][org][k]
                else:
                    st = agg[label][period][org]
                total_add = st  # 後で TOTAL 計算に使う
                if st["races"] == 0:
                    row.append("  -  /   -  /   -  ")
                else:
                    h = st["hits"] / st["races"] * 100
                    r = st["payout"] / st["bet"] * 100 if st["bet"] else 0
                    row.append(f"{st['races']:>4,}/{h:>5.1f}%/{r:>6.1f}%")
                # TOTAL 合算
                if period == "全期間":
                    for p in WF_PERIODS:
                        for k in total:
                            total[k] += agg[label][p][org][k]
                else:
                    for k in total:
                        total[k] += st[k]

            # TOTAL 列
            if total["races"] == 0:
                total_str = "  -  /   -  /   -  "
            else:
                h = total["hits"] / total["races"] * 100
                r = total["payout"] / total["bet"] * 100 if total["bet"] else 0
                total_str = f"{total['races']:>4,}/{h:>5.1f}%/{r:>6.1f}%"

            print(f"{label:<10} {period:<10}  {row[0]:>22}  {row[1]:>22}  {total_str:>22}")

        print("-" * 90)

    # --- 集計サマリ ---
    print("\n" + "=" * 80)
    print("集計サマリ")
    print("=" * 80)

    # 全期間 TOTAL 最高 ROI (races >= 30)
    best_total = None
    for label, _, _ in ODDS_RANGES:
        total = empty_stats()
        for period in WF_PERIODS:
            for org in ORGS:
                for k in total:
                    total[k] += agg[label][period][org][k]
        if total["races"] < 30:
            continue
        roi = total["payout"] / total["bet"] * 100 if total["bet"] else 0
        if best_total is None or roi > best_total[1]:
            best_total = (label, roi, total["races"])

    if best_total:
        print(f"\n全期間 TOTAL 最高 ROI (races >= 30):")
        print(f"  オッズ範囲 {best_total[0]} / 全期間 / TOTAL = ROI {best_total[1]:.1f}% (races {best_total[2]:,})")

    # 期間別最良 (races >= 30)
    print("\n期間別最良 ROI (races >= 30):")
    for period in list(WF_PERIODS.keys()):
        best_period = None
        for label, _, _ in ODDS_RANGES:
            for org in ORGS + ["TOTAL"]:
                if org == "TOTAL":
                    st = empty_stats()
                    for o in ORGS:
                        for k in st:
                            st[k] += agg[label][period][o][k]
                else:
                    st = agg[label][period][org]
                if st["races"] < 30:
                    continue
                roi = st["payout"] / st["bet"] * 100 if st["bet"] else 0
                if best_period is None or roi > best_period[1]:
                    best_period = (label, roi, org, st["races"])
        if best_period:
            print(f"  {period}: オッズ {best_period[0]} / {best_period[2]} = ROI {best_period[1]:.1f}% (races {best_period[3]:,})")
        else:
            print(f"  {period}: データなし (races >= 30 なし)")

    # 100% 超サマリ
    print(f"\nROI 100% 超達成数 (races >= 30): {len(over100_list)} 件")
    if over100_list:
        print("採用候補:")
        for label, period, org, races, hit_pct, roi, bet, payout in over100_list[:5]:
            print(f"  {label} / {period} / {org}: ROI {roi:.1f}% (races {races:,})")


if __name__ == "__main__":
    main()
