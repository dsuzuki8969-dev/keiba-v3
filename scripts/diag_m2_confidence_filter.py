"""試行 #4: 高信頼 race + ◎ + odds フィルター (post-hoc 集計)

v4+v2+v1 状態 (mark 統合前 / 最高 ROI 74.7%) の pred.json をベースに、
race.confidence / ◎ composite 突出度 / ◎ vs 2 番手差 で絞り込んだ場合の
◎単勝 ROI を計算する。

集計フィルター:
  X1: race.confidence 上位限定 ('S+' / 'S+,S' / 'S+,S,A')
  X2: ◎ horse composite 突出  (>= 80 / >= 90 / >= 95)
  X3: ◎ vs 2 番手差           (>= 5 / >= 10 / >= 15)

オッズ範囲: all / <2.0 / <1.5 / <1.3
"""

import io
import json
import os
import sys
import tarfile
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

RESULTS_DIR = os.path.join(PROJECT_ROOT, "data", "results")
ARCHIVE_DIR = os.path.join(PROJECT_ROOT, "data", "_archive")

# v4+v2+v1 状態 (mark 統合前 = 最高 ROI 状態)
SOURCE_TAR = os.path.join(ARCHIVE_DIR, "predictions_pre_markint_20260527.tar.gz")

# オッズ範囲 (label, lo, hi)
ODDS_RANGES = [
    ("all",   0.0,  9999.0),
    ("<2.0",  0.0,  2.0),
    ("<1.5",  0.0,  1.5),
    ("<1.3",  0.0,  1.3),
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
    """tar.gz から *_pred.json を全ロード"""
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


def get_tansho_payout(result_race: dict, winning_horse_no) -> Optional[int]:
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


def get_second_composite(race: dict, honmei_no) -> Optional[float]:
    """◎ 以外の馬で composite が最大のものを返す"""
    best = None
    for h in race.get("horses", []):
        if h.get("horse_no") == honmei_no:
            continue
        c = h.get("composite")
        if c is None:
            continue
        try:
            c = float(c)
        except (TypeError, ValueError):
            continue
        if best is None or c > best:
            best = c
    return best


# ─── Step 1: confidence 値分布調査 ──────────────────────────────────────────

def scan_confidence_dist(preds: Dict[str, dict]) -> dict:
    """全 pred.json から race.confidence 値の出現頻度を収集"""
    dist: Dict[str, int] = defaultdict(int)
    for pred_data in preds.values():
        for race in pred_data.get("races", []):
            conf = race.get("confidence", "")
            dist[str(conf)] += 1
    return dict(sorted(dist.items(), key=lambda x: -x[1]))


# ─── Step 2-3: フィルター × オッズ 集計 ──────────────────────────────────

def _empty_stats():
    return {"races": 0, "hits": 0, "bet": 0, "payout": 0}


def aggregate(
    preds: Dict[str, dict],
    filter_fn,
    filter_label: str,
) -> Dict[str, Dict[str, Dict[str, dict]]]:
    """
    filter_fn(race, honmei) -> bool
    Returns: {odds_label: {period: {org: stats}}}
    """
    out = {
        label: {
            period: {"JRA": _empty_stats(), "NAR": _empty_stats()}
            for period in WF_PERIODS
        }
        for label, _, _ in ODDS_RANGES
    }

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
            # 1 着馬番
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
            try:
                honmei_odds = float(honmei_odds)
            except (TypeError, ValueError):
                continue
            if honmei_odds <= 0:
                continue

            # フィルター適用
            if not filter_fn(race, honmei):
                continue

            # 各 odds range でカウント
            for label, lo, hi in ODDS_RANGES:
                if not (lo <= honmei_odds <= hi):
                    continue
                s = out[label][period][org]
                s["races"] += 1
                s["bet"] += 100
                if honmei_no == first_horse:
                    s["hits"] += 1
                    payout = get_tansho_payout(result, first_horse)
                    if payout:
                        s["payout"] += payout

    return out


def totals(agg: dict, odds_label: str) -> dict:
    """全期間 + 全組織の集計"""
    races = hits = bet = payout = 0
    for period_d in agg[odds_label].values():
        for s in period_d.values():
            races  += s["races"]
            hits   += s["hits"]
            bet    += s["bet"]
            payout += s["payout"]
    return {"races": races, "hits": hits, "bet": bet, "payout": payout}


def roi_str(t: dict) -> str:
    hit_pct = t["hits"] / t["races"] * 100 if t["races"] else 0.0
    roi = t["payout"] / t["bet"] * 100 if t["bet"] else 0.0
    return f"races={t['races']:>5}  hit%={hit_pct:>5.1f}%  ROI={roi:>6.1f}%  bet={t['bet']:>8,}  payout={t['payout']:>9,}"


def print_filter_block(
    label: str,
    agg: dict,
    rankings_collector: list,
    min_races: int = 30,
):
    print(f"\n  {label}")
    print(f"  {'オッズ':<8} {'races':>6} {'hit%':>6} {'ROI':>7} {'bet':>9} {'payout':>10}")
    print("  " + "-" * 60)
    for ol, _, _ in ODDS_RANGES:
        t = totals(agg, ol)
        if t["races"] < min_races:
            continue
        hit_pct = t["hits"] / t["races"] * 100 if t["races"] else 0.0
        roi = t["payout"] / t["bet"] * 100 if t["bet"] else 0.0
        marker = " ***" if roi >= 100 else (" +" if roi > 74.7 else "")
        print(f"  {ol:<8} {t['races']:>6,} {hit_pct:>5.1f}% {roi:>6.1f}% {t['bet']:>9,} {t['payout']:>10,}{marker}")
        rankings_collector.append({
            "filter": label,
            "odds": ol,
            "races": t["races"],
            "hit_pct": hit_pct,
            "roi": roi,
            "bet": t["bet"],
            "payout": t["payout"],
        })


# ─── メイン ──────────────────────────────────────────────────────────────────

def main():
    print("=" * 80)
    print("試行 #4: 高信頼 race + ◎ + odds フィルター (post-hoc 集計)")
    print("=" * 80)
    print(f"source: {os.path.basename(SOURCE_TAR)} (基準 ROI 74.7%)")

    # ── ロード
    print("\n[1/3] pred.json ロード中...")
    preds = load_preds_from_tar(SOURCE_TAR)
    print(f"  {len(preds)} 日分ロード完了")

    # ── Step 1: confidence 分布調査
    print("\n[2/3] confidence 値分布調査...")
    dist = scan_confidence_dist(preds)
    total_races = sum(dist.values())

    print("\n" + "=" * 80)
    print("### Step 1: confidence 値分布")
    print("=" * 80)
    print(f"{'confidence':<12} {'races':>8} {'%':>7}")
    print("-" * 35)
    conf_order = []
    for val, cnt in dist.items():
        pct = cnt / total_races * 100
        print(f"{val!r:<12} {cnt:>8,} ({pct:>5.1f}%)")
        conf_order.append(val)
    print(f"{'(total)':<12} {total_races:>8,} (100.0%)")

    # confidence の型判定 (文字列か数値か)
    sample_vals = list(dist.keys())[:5]
    is_numeric = all(
        (v.replace(".", "").replace("-", "").isdigit() or v == "")
        for v in sample_vals
        if v
    )
    print(f"\n  → 型推定: {'数値' if is_numeric else '文字列カテゴリ'}")
    print(f"  → サンプル値: {sample_vals}")

    # ── Step 2-3: フィルター集計
    print("\n[3/3] フィルター × オッズ ROI 集計...")

    rankings: List[dict] = []

    print("\n" + "=" * 80)
    print("### Step 2-3: フィルター × オッズ ROI 表 (races >= 30 のみ)")
    print("=" * 80)

    # ─── X1: race.confidence 上位限定 ──────────────────────────────────────
    print("\n[X1] race.confidence 上位限定")

    # 文字列カテゴリの場合 (S+/S/A/...) と数値の場合で分岐
    # 典型的な文字列信頼度 ('S+', 'S', 'A', 'B', ...)
    CONF_RANK = {"S+": 0, "S": 1, "A": 2, "B": 3, "C": 4, "D": 5, "E": 6}

    # 実際に存在する confidence 値から X1 閾値候補を生成
    existing_cats = [k for k in dist if k in CONF_RANK]
    existing_cats.sort(key=lambda x: CONF_RANK[x])

    if existing_cats:
        # 文字列カテゴリ → 上位 N 段階
        x1_specs = []
        for i in range(1, min(4, len(existing_cats) + 1)):
            cats = existing_cats[:i]
            x1_specs.append(
                (f"conf={','.join(cats)}", lambda race, h, cats=cats: str(race.get("confidence", "")) in cats)
            )
    else:
        # 数値 confidence → top 5% / top 10% / top 20%
        # まず全 confidence 値を取得して閾値決定
        all_conf_vals = []
        for pred_data in preds.values():
            for race in pred_data.get("races", []):
                c = race.get("confidence")
                if c is not None:
                    try:
                        all_conf_vals.append(float(c))
                    except (TypeError, ValueError):
                        pass
        all_conf_vals.sort()
        n = len(all_conf_vals)
        thr5  = all_conf_vals[int(n * 0.95)] if n else 0
        thr10 = all_conf_vals[int(n * 0.90)] if n else 0
        thr20 = all_conf_vals[int(n * 0.80)] if n else 0
        x1_specs = [
            (f"conf>={thr5:.2f} (top5%)",  lambda race, h, t=thr5:  (float(race.get("confidence") or 0)) >= t),
            (f"conf>={thr10:.2f} (top10%)", lambda race, h, t=thr10: (float(race.get("confidence") or 0)) >= t),
            (f"conf>={thr20:.2f} (top20%)", lambda race, h, t=thr20: (float(race.get("confidence") or 0)) >= t),
        ]

    for flabel, ffn in x1_specs:
        agg = aggregate(preds, ffn, flabel)
        print_filter_block(flabel, agg, rankings)

    # ─── X2: ◎ composite 突出度 ─────────────────────────────────────────────
    print("\n[X2] ◎ horse composite 突出度")
    x2_specs = [
        ("composite >= 80", lambda race, h: (float(h.get("composite") or 0)) >= 80),
        ("composite >= 90", lambda race, h: (float(h.get("composite") or 0)) >= 90),
        ("composite >= 95", lambda race, h: (float(h.get("composite") or 0)) >= 95),
    ]
    for flabel, ffn in x2_specs:
        agg = aggregate(preds, ffn, flabel)
        print_filter_block(flabel, agg, rankings)

    # ─── X3: ◎ vs 2 番手差 ──────────────────────────────────────────────────
    print("\n[X3] ◎ composite vs 2 番手差")

    def make_diff_filter(threshold: float):
        def _fn(race, honmei):
            hc = honmei.get("composite")
            if hc is None:
                return False
            try:
                hc = float(hc)
            except (TypeError, ValueError):
                return False
            sec = get_second_composite(race, honmei.get("horse_no"))
            if sec is None:
                return False
            return (hc - sec) >= threshold
        return _fn

    x3_specs = [
        ("diff >= 5",  make_diff_filter(5.0)),
        ("diff >= 10", make_diff_filter(10.0)),
        ("diff >= 15", make_diff_filter(15.0)),
    ]
    for flabel, ffn in x3_specs:
        agg = aggregate(preds, ffn, flabel)
        print_filter_block(flabel, agg, rankings)

    # ─── 全期間 TOTAL 最高 ROI ランキング ─────────────────────────────────
    print("\n" + "=" * 80)
    print("### 全期間 TOTAL 最高 ROI ランキング Top 10")
    print("=" * 80)
    print(f"  {'順位':<3} {'フィルター':<30} {'オッズ':<8} {'races':>6} {'hit%':>6} {'ROI':>7}")
    print("  " + "-" * 75)
    top = sorted(rankings, key=lambda x: -x["roi"])[:10]
    for i, r in enumerate(top, 1):
        marker = " ***" if r["roi"] >= 100 else (" +" if r["roi"] > 74.7 else "")
        print(f"  {i:<3} {r['filter']:<30} {r['odds']:<8} {r['races']:>6,} "
              f"{r['hit_pct']:>5.1f}% {r['roi']:>6.1f}%{marker}")

    # ─── 結論 ───────────────────────────────────────────────────────────────
    print("\n" + "=" * 80)
    print("### 結論")
    print("=" * 80)
    over100 = [r for r in rankings if r["roi"] >= 100 and r["races"] >= 30]
    if over100:
        print(f"\nROI 100% 超達成: あり ({len(over100)} 件)")
        print("\n採用候補フィルター:")
        for r in sorted(over100, key=lambda x: -x["roi"])[:5]:
            print(f"  {r['filter']} × オッズ {r['odds']} → ROI {r['roi']:.1f}%  (races={r['races']})")
    else:
        print("\nROI 100% 超達成: なし (races >= 30 条件下)")
        print("\n最高 ROI 候補 (Top 3):")
        for r in sorted(rankings, key=lambda x: -x["roi"])[:3]:
            print(f"  {r['filter']} × オッズ {r['odds']} → ROI {r['roi']:.1f}%  (races={r['races']})")


if __name__ == "__main__":
    main()
