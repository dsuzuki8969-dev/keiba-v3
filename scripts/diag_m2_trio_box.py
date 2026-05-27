"""試行 #1: 複数頭買い戦略 ◎○▲ 三連複 (post-hoc 集計)

v4+v2+v1 状態 (mark 統合前 / 最高 ROI 74.7%) の pred.json をベースに、
◎○▲ 複数頭買い 三連複 5 戦略の ROI を計算する。

戦略:
  A1: ◎○▲ 3頭BOX 三連複 (1通り / 100円)
  A2: ◎○▲△ 4頭BOX 三連複 (4通り / 400円)
  A3: ◎○▲△★ 5頭BOX 三連複 (10通り / 1,000円)
  B1: ◎-○▲ 軸流し 三連複 (◎+他2頭 1通り / 100円)
  B2: ◎軸 - ○▲△ 軸1頭流し 三連複 (3通り / 300円)
"""

import io
import itertools
import json
import os
import sys
import tarfile
from collections import defaultdict
from typing import Dict, List, Optional, Set

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

RESULTS_DIR = os.path.join(PROJECT_ROOT, "data", "results")
ARCHIVE_DIR = os.path.join(PROJECT_ROOT, "data", "_archive")

# v4+v2+v1 状態 (mark 統合前 = 最高 ROI 状態)
SOURCE_TAR = os.path.join(ARCHIVE_DIR, "predictions_pre_markint_20260527.tar.gz")

WF_PERIODS = {
    "wf_2024": ("20240101", "20241231"),
    "wf_2025": ("20250101", "20251231"),
    "wf_2026": ("20260101", "20261231"),
}

# 戦略定義: (戦略ID, 説明, 必要印リスト, 軸印リスト, 流し印リスト, 1通り額)
# BOX 戦略は軸=None / 流し=None で独自処理
STRATEGIES = [
    ("A1", "◎○▲ 3頭BOX",      ["◎", "○", "▲"],           None, None,  100),
    ("A2", "◎○▲△ 4頭BOX",     ["◎", "○", "▲", "△"],       None, None,  100),
    ("A3", "◎○▲△★ 5頭BOX",    ["◎", "○", "▲", "△", "★"],  None, None,  100),
    ("B1", "◎軸-○▲ 流し",      ["◎", "○", "▲"],           ["◎"], ["○", "▲"],  100),
    ("B2", "◎軸-○▲△ 流し",     ["◎", "○", "▲", "△"],      ["◎"], ["○", "▲", "△"],  100),
]


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


def is_trio_hit(payout_combo_str: str, our_horses: Set[int]) -> bool:
    """三連複的中判定: payouts['三連複']['combo'] と購入組合せを照合"""
    hit_set = set(int(x) for x in payout_combo_str.split("-"))
    return hit_set == our_horses


def get_trio_payout(result_race: dict, our_horses: Set[int]) -> Optional[int]:
    """三連複払戻金取得。的中しなければ None"""
    payouts = result_race.get("payouts", {})
    trio = payouts.get("三連複")
    if trio is None:
        return None
    # 単一 dict の場合
    if isinstance(trio, dict):
        combo = trio.get("combo", "")
        if combo and is_trio_hit(combo, our_horses):
            p = trio.get("payout")
            return int(p) if isinstance(p, (int, float)) else None
    # list の場合 (念のため対応)
    elif isinstance(trio, list):
        for item in trio:
            if isinstance(item, dict):
                combo = item.get("combo", "")
                if combo and is_trio_hit(combo, our_horses):
                    p = item.get("payout")
                    return int(p) if isinstance(p, (int, float)) else None
    return None


def get_mark_horse_no(race: dict, mark: str) -> Optional[int]:
    """指定印の horse_no を返す。見つからなければ None"""
    for h in race.get("horses", []):
        if h.get("mark") == mark:
            return h.get("horse_no")
    return None


def get_all_mark_horse_nos(race: dict, marks: List[str]) -> Optional[Dict[str, int]]:
    """複数印の horse_no を辞書で返す。1つでも欠けたら None"""
    result = {}
    for mark in marks:
        no = get_mark_horse_no(race, mark)
        if no is None:
            return None
        result[mark] = no
    return result


def calc_strategy_roi(
    preds: Dict[str, dict],
) -> Dict[str, Dict[str, Dict[str, dict]]]:
    """全戦略 × 期間 × 組織 で集計"""

    # out[strategy_id][period][org] = {races, hits, bet, payout}
    out = {}
    for sid, _, _, _, _, _ in STRATEGIES:
        out[sid] = defaultdict(lambda: defaultdict(lambda: {
            "races": 0, "hits": 0, "bet": 0, "payout": 0,
        }))

    # 基準: ◎単勝 (全範囲)
    out["BASE"] = defaultdict(lambda: defaultdict(lambda: {
        "races": 0, "hits": 0, "bet": 0, "payout": 0,
    }))

    processed_dates = 0
    total_dates = len(preds)

    for i, (date_key, pred_data) in enumerate(sorted(preds.items())):
        # プログレス表示 (100日おき)
        if i % 100 == 0 or i == total_dates - 1:
            pct = (i + 1) / total_dates * 100
            bar_len = 30
            filled = int(bar_len * (i + 1) / total_dates)
            bar = "#" * filled + "." * (bar_len - filled)
            print(f"\r  [{bar}] {pct:.1f}% ({i+1}/{total_dates})", end="", flush=True)

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

            # 着順取得
            finish_map = {}
            for r in order:
                fn = r.get("finish")
                hn = r.get("horse_no")
                if fn is not None and hn is not None:
                    finish_map[hn] = fn

            org = "JRA" if _is_jra(race_id) else "NAR"

            # === 基準: ◎単勝 ===
            honmei_no = get_mark_horse_no(race, "◎")
            if honmei_no is not None:
                out["BASE"][period][org]["races"] += 1
                out["BASE"][period][org]["bet"] += 100
                if finish_map.get(honmei_no) == 1:
                    out["BASE"][period][org]["hits"] += 1
                    # 単勝払戻 (combo==honmei_no を厳格確認 — 取消馬繰上り等の不整合排除)
                    payouts = result.get("payouts", {})
                    tansho = payouts.get("単勝")
                    if isinstance(tansho, dict):
                        if str(tansho.get("combo", "")) == str(honmei_no):
                            p = tansho.get("payout")
                            if p:
                                out["BASE"][period][org]["payout"] += int(p)

            # === 各戦略 ===
            for sid, desc, req_marks, jiku_marks, nagashi_marks, unit_cost in STRATEGIES:
                # 必要印を全部取得
                mark_nos = get_all_mark_horse_nos(race, req_marks)
                if mark_nos is None:
                    # 印が揃っていないレースはスキップ
                    continue

                # 購入組合せを生成
                if jiku_marks is None:
                    # BOX 戦略: C(n, 3) の全組合せ
                    all_nos = list(mark_nos.values())
                    combos = list(itertools.combinations(all_nos, 3))
                else:
                    # 軸流し戦略: 軸の horse_no + 流し2頭の C(len,2) 組合せ
                    jiku_nos = [mark_nos[m] for m in jiku_marks]
                    nagashi_nos = [mark_nos[m] for m in nagashi_marks]
                    # 軸1頭流し: 軸 + 流し相手から2頭選ぶ
                    combos = []
                    for pair in itertools.combinations(nagashi_nos, 2):
                        combo_set = set(jiku_nos) | set(pair)
                        if len(combo_set) == 3:
                            combos.append(tuple(sorted(combo_set)))
                    # 重複除去
                    combos = list(set(combos))

                if not combos:
                    continue

                bet_amount = len(combos) * unit_cost
                out[sid][period][org]["races"] += 1
                out[sid][period][org]["bet"] += bet_amount

                # 的中チェック
                hit = False
                hit_payout = 0
                for combo in combos:
                    trio_set = set(combo)
                    p = get_trio_payout(result, trio_set)
                    if p is not None:
                        hit = True
                        hit_payout = p  # 三連複は1通りしか当たらないので最初のヒットで確定
                        break

                if hit:
                    out[sid][period][org]["hits"] += 1
                    out[sid][period][org]["payout"] += hit_payout

        processed_dates += 1

    print()  # 改行
    return out


def main():
    print("=" * 80)
    print("試行 #1: 複数頭買い戦略 (post-hoc) - ◎○▲ 三連複 5 戦略 ROI 比較")
    print("=" * 80)
    print(f"\nsource: {os.path.basename(SOURCE_TAR)} (v4+v2+v1 / 基準 ROI 74.7%)")

    print("\n[1/2] pred.json ロード中 (tar.gz)...")
    preds = load_preds_from_tar(SOURCE_TAR)
    print(f"  {len(preds)} 日分ロード完了")

    print("\n[2/2] 戦略別 ROI 集計中...")
    agg = calc_strategy_roi(preds)

    # ===== 全期間 TOTAL ランキング =====
    print("\n" + "=" * 80)
    print("全期間 TOTAL 戦略別 ROI ランキング")
    print("=" * 80)
    hdr = f"{'戦略':<22} {'races':>8} {'hits':>7} {'hit%':>7} {'ROI':>8} {'累計bet':>10} {'累計payout':>12}"
    print(hdr)
    print("-" * 80)

    strategy_ids = [sid for sid, _, _, _, _, _ in STRATEGIES] + ["BASE"]
    strategy_descs = {sid: desc for sid, desc, _, _, _, _ in STRATEGIES}
    strategy_descs["BASE"] = "◎単勝(基準)"

    rankings = []
    for sid in strategy_ids:
        races = hits = bet = payout = 0
        for period_d in agg[sid].values():
            for stats in period_d.values():
                races += stats["races"]
                hits += stats["hits"]
                bet += stats["bet"]
                payout += stats["payout"]
        hit_pct = hits / races * 100 if races else 0
        roi = payout / bet * 100 if bet else 0
        rankings.append((sid, strategy_descs[sid], races, hits, hit_pct, roi, bet, payout))

    # ROI 降順
    rankings.sort(key=lambda x: -x[5])
    for sid, desc, races, hits, hit_pct, roi, bet, payout in rankings:
        marker = " ***" if roi >= 100 else (" +" if roi > 74.7 else "   ")
        label = f"{sid} {desc}"
        print(f"{label:<22} {races:>8,} {hits:>7,} {hit_pct:>6.1f}% {roi:>7.1f}%{marker} {bet:>10,} {payout:>12,}")

    # ===== 期間別 / 組織別 詳細 (上位 3 戦略) =====
    print("\n" + "=" * 80)
    print("期間別 / 組織別 詳細 (ROI 上位 3 戦略)")
    print("=" * 80)
    for sid, desc, _, _, _, roi_total, _, _ in rankings[:3]:
        label = f"{sid} {desc} (全期間 ROI={roi_total:.1f}%)"
        print(f"\n=== {label} ===")
        print(f"{'期間':<10} {'組織':<6} {'races':>8} {'hits':>7} {'hit%':>7} {'ROI':>8}")
        print("-" * 60)
        period_totals = []
        for period in ["wf_2024", "wf_2025", "wf_2026"]:
            for org in ["JRA", "NAR"]:
                stats = agg[sid][period][org]
                if stats["races"] == 0:
                    continue
                h = stats["hits"] / stats["races"] * 100
                r = stats["payout"] / stats["bet"] * 100 if stats["bet"] else 0
                print(f"{period:<10} {org:<6} {stats['races']:>8,} {stats['hits']:>7,} {h:>6.1f}% {r:>7.1f}%")

    # ===== 期間別 最良戦略まとめ =====
    print("\n" + "=" * 80)
    print("期間別 最良戦略 (ROI 最大)")
    print("=" * 80)
    print(f"{'期間':<10} {'組織':<6} {'最良戦略':<22} {'ROI':>8} {'hit%':>7}")
    print("-" * 60)
    for period in ["wf_2024", "wf_2025", "wf_2026"]:
        for org in ["JRA", "NAR"]:
            best = None
            best_roi = -1
            best_hit = 0
            for sid, desc, _, _, _, _, _, _ in rankings:
                stats = agg[sid][period][org]
                if stats["races"] == 0:
                    continue
                r = stats["payout"] / stats["bet"] * 100 if stats["bet"] else 0
                if r > best_roi:
                    best_roi = r
                    best_hit = stats["hits"] / stats["races"] * 100
                    best = f"{sid} {desc}"
            if best:
                print(f"{period:<10} {org:<6} {best:<22} {best_roi:>7.1f}% {best_hit:>6.1f}%")

    print("\n完了")


if __name__ == "__main__":
    main()
