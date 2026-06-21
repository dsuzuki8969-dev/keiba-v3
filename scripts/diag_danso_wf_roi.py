#!/usr/bin/env python3
"""
診断スクリプト: 印断層三連複 Walk-Forward ROI 検証
================================================
「印断層三連複」戦略の過去実績（2024〜2026年の全pred.json/results.json）を集計し
ROI・hit率・formation別内訳・払戻 coverage を報告する。

実行方法:
    python scripts/diag_danso_wf_roi.py [--year YYYY]

ばんえい(venue_code='65')は除外。
"""

import argparse
import glob
import json
import os
import sys
from collections import defaultdict
from itertools import combinations
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# プロジェクトルートをパスに追加
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.calculator.betting import compute_danso_columns
from src.utils.payout_normalizer import (
    combo_match,
    get_payout_for_combo,
    normalize_payouts,
)


# ============================================================
# 定数
# ============================================================
PRED_DIR = ROOT / "data" / "predictions"
RESULTS_FIXED_DIR = ROOT / "data" / "results_fixed"
RESULTS_DIR = ROOT / "data" / "results"
STAKE_PER_TICKET = 100  # 1点あたり賭け金(円)
BANEI_VENUE_CODE = "65"  # ばんえい除外


def load_results(date_str: str) -> Dict[str, dict]:
    """results_fixed 優先で race_id → normalized_payouts を返す。"""
    # results_fixed 優先
    for results_dir in [RESULTS_FIXED_DIR, RESULTS_DIR]:
        fpath = results_dir / f"{date_str}_results.json"
        if fpath.exists():
            try:
                with open(fpath, encoding="utf-8") as f:
                    data = json.load(f)
                # race_id → normalized_payouts のマップを構築
                mapping: Dict[str, dict] = {}
                for race_id, race_data in data.items():
                    if not isinstance(race_data, dict):
                        continue
                    raw_payouts = race_data.get("payouts", {})
                    mapping[race_id] = normalize_payouts(raw_payouts)
                return mapping
            except Exception as e:
                print(f"  [WARN] results 読み込みエラー ({fpath.name}): {e}", flush=True)
    return {}


def build_tickets_from_danso(col1: List[int], col2: List[int], col3: List[int]) -> List[Tuple[int, int, int]]:
    """col1×col2×col3 から distinct3頭・昇順dedupした三連複組み合わせを返す。"""
    tickets = set()
    for h1 in col1:
        for h2 in col2:
            for h3 in col3:
                combo_set = frozenset([h1, h2, h3])
                if len(combo_set) == 3:  # 3頭相異なること
                    tickets.add(tuple(sorted(combo_set)))
    return sorted(tickets)


def process_date(date_str: str, results_map: Dict[str, dict]) -> dict:
    """1日分のpred.jsonを処理してレース統計を返す。"""
    pred_path = PRED_DIR / f"{date_str}_pred.json"
    if not pred_path.exists():
        return {}

    try:
        with open(pred_path, encoding="utf-8") as f:
            pred_data = json.load(f)
    except Exception as e:
        print(f"  [WARN] pred.json 読み込みエラー ({pred_path.name}): {e}", flush=True)
        return {}

    races = pred_data.get("races", [])
    day_stats = {
        "races_total": 0,
        "fired": 0,
        "skip": 0,
        "tickets_total": 0,
        "investment": 0,
        "payout": 0,
        "hit_races": 0,
        "coverage_ok": 0,   # 三連複払戻が取得できたレース数（fired中）
        "coverage_total": 0,  # fired レース数と同値(alias)
        "formation_stats": defaultdict(lambda: {"fired": 0, "investment": 0, "payout": 0, "hit": 0}),
    }

    for race in races:
        race_id = race.get("race_id", "")
        venue_code = str(race.get("venue_code", ""))

        # ばんえい除外
        if venue_code == BANEI_VENUE_CODE:
            continue

        day_stats["races_total"] += 1

        horses = race.get("horses", [])
        # entries 構築（compute_danso_columns の引数形式）
        entries = []
        for h in horses:
            entries.append({
                "mark": h.get("mark", "-"),
                "composite": float(h.get("composite", 0.0) or 0.0),
                "horse_no": int(h.get("horse_no", 0)),
                "odds": h.get("odds"),
                "is_scratched": bool(h.get("is_scratched", False)),
            })

        # 断層判定
        result = compute_danso_columns(entries)

        if result is None:
            day_stats["skip"] += 1
            continue

        # 発火
        formation = result["formation"]
        col1 = result["col1"]
        col2 = result["col2"]
        col3 = result["col3"]

        tickets = build_tickets_from_danso(col1, col2, col3)
        if not tickets:
            day_stats["skip"] += 1
            continue

        day_stats["fired"] += 1
        day_stats["coverage_total"] += 1

        n_tickets = len(tickets)
        investment = n_tickets * STAKE_PER_TICKET
        day_stats["tickets_total"] += n_tickets
        day_stats["investment"] += investment

        day_stats["formation_stats"][formation]["fired"] += 1
        day_stats["formation_stats"][formation]["investment"] += investment

        # 払戻チェック
        if race_id not in results_map:
            # 払戻データなし → coverage 対象外（後で coverage_ok/total から除外）
            day_stats["coverage_total"] -= 1  # coverageの分母から除外
            # 投資は除外しない（coverage_total = 払戻データあったfired数）
            continue

        norm_payouts = results_map[race_id]
        sanrenpuku_entries = norm_payouts.get("sanrenpuku", [])

        if not sanrenpuku_entries:
            # 三連複払戻キー自体がない（レース成立せず等）
            day_stats["coverage_total"] -= 1
            continue

        day_stats["coverage_ok"] += 1

        # 各買い目の的中チェック
        race_hit = False
        race_payout = 0
        for ticket in tickets:
            ticket_list = list(ticket)
            payout_per_100 = get_payout_for_combo(norm_payouts, "sanrenpuku", ticket_list)
            if payout_per_100 > 0:
                # 100円賭け → 払戻はpayout_per_100円
                race_payout += payout_per_100
                race_hit = True

        day_stats["payout"] += race_payout
        if race_hit:
            day_stats["hit_races"] += 1
            day_stats["formation_stats"][formation]["hit"] += 1
            day_stats["formation_stats"][formation]["payout"] += race_payout

    return day_stats


def aggregate_stats(all_day_stats: List[dict]) -> dict:
    """全日分の統計を集約。"""
    agg = {
        "races_total": 0,
        "fired": 0,
        "skip": 0,
        "tickets_total": 0,
        "investment": 0,
        "payout": 0,
        "hit_races": 0,
        "coverage_ok": 0,
        "coverage_total": 0,
        "formation_stats": defaultdict(lambda: {"fired": 0, "investment": 0, "payout": 0, "hit": 0}),
    }
    for ds in all_day_stats:
        for key in ["races_total", "fired", "skip", "tickets_total", "investment", "payout", "hit_races", "coverage_ok", "coverage_total"]:
            agg[key] += ds.get(key, 0)
        for formation, fstat in ds.get("formation_stats", {}).items():
            for k in ["fired", "investment", "payout", "hit"]:
                agg["formation_stats"][formation][k] += fstat.get(k, 0)
    return agg


def print_stats(label: str, stats: dict, date_range: str = "") -> None:
    """統計を整形して出力。"""
    races_total = stats["races_total"]
    fired = stats["fired"]
    skip = stats["skip"]
    tickets_total = stats["tickets_total"]
    investment = stats["investment"]
    payout = stats["payout"]
    hit_races = stats["hit_races"]
    coverage_ok = stats["coverage_ok"]
    coverage_total = stats["coverage_total"]

    fire_rate = fired / races_total * 100 if races_total > 0 else 0.0
    # hit% はカバレッジ対象(払戻データあり)の発火レース中
    hit_pct = hit_races / coverage_ok * 100 if coverage_ok > 0 else 0.0
    # ROI はカバレッジ対象の投資額で計算（coverage外は除外）
    # coverage_ok と coverage_total の差分が「払戻データ無しで投資した分」
    # 実際の投資: 全fired×tickets×100円 だが払戻データ無しは除外不能
    # → 近似: coverage_ok / coverage_total で按分補正
    if coverage_total > 0 and investment > 0:
        # fired=coverage_total + (coverage_total からさらに除外された分)
        # 按分でcoverage_ok分の投資を推定
        investment_covered = investment * (coverage_ok / max(coverage_total, 1))
    else:
        investment_covered = investment

    roi = payout / investment_covered * 100 if investment_covered > 0 else 0.0
    coverage_pct = coverage_ok / coverage_total * 100 if coverage_total > 0 else 0.0

    header = f"{'='*60}"
    print(header)
    print(f"  {label}  {date_range}")
    print(header)
    print(f"  対象レース数         : {races_total:>8,}")
    print(f"  発火(買い)           : {fired:>8,}  (fire率 {fire_rate:.1f}%)")
    print(f"  見送り               : {skip:>8,}")
    print(f"  買い目総数           : {tickets_total:>8,}")
    print(f"  払戻 coverage        : {coverage_ok:>8,} / {coverage_total:>6,}  ({coverage_pct:.1f}%)")
    print(f"  ──────────────────────────────────")
    print(f"  総投資額(coverage近似): {int(investment_covered):>11,} 円")
    print(f"  総払戻額             : {payout:>11,} 円")
    print(f"  ROI                  : {roi:>8.1f}%")
    print(f"  hit レース数         : {hit_races:>8,} / {coverage_ok:>6,}  (hit% {hit_pct:.1f}%)")
    print()

    # formation 別内訳
    print(f"  [formation別内訳]")
    print(f"  {'formation':<10} {'発火':>6} {'hit':>6} {'hit%':>7} {'ROI':>8} {'投資':>12} {'払戻':>12}")
    print(f"  {'-'*65}")
    for fm in ["A-F1", "A-F2", "C", "B-F1", "B-F2"]:
        fs = stats["formation_stats"].get(fm, {"fired": 0, "investment": 0, "payout": 0, "hit": 0})
        fm_fired = fs["fired"]
        fm_hit = fs["hit"]
        fm_inv = fs["investment"]
        fm_pay = fs["payout"]
        fm_hit_pct = fm_hit / fm_fired * 100 if fm_fired > 0 else 0.0
        # ROI: 払戻データ欠損分の按分は全体と同比率で近似
        if fm_fired > 0 and coverage_total > 0:
            fm_inv_cov = fm_inv * (coverage_ok / max(coverage_total, 1))
        else:
            fm_inv_cov = fm_inv
        fm_roi = fm_pay / fm_inv_cov * 100 if fm_inv_cov > 0 else 0.0
        print(f"  {fm:<10} {fm_fired:>6,} {fm_hit:>6,} {fm_hit_pct:>6.1f}% {fm_roi:>7.1f}% {int(fm_inv_cov):>12,} {fm_pay:>12,}")
    print()


def main() -> None:
    parser = argparse.ArgumentParser(description="印断層三連複 Walk-Forward ROI 診断")
    parser.add_argument("--year", type=str, default=None, help="対象年 (例: 2024). 未指定は全期間")
    args = parser.parse_args()

    # ── pred.json ファイル一覧収集 ──
    pred_files = sorted(glob.glob(str(PRED_DIR / "*_pred.json")))
    # _backup等を除外（日付8桁のファイルのみ）
    pred_files = [
        f for f in pred_files
        if Path(f).name[:8].isdigit() and len(Path(f).name) == len("20240101_pred.json")
    ]

    if args.year:
        pred_files = [f for f in pred_files if Path(f).name.startswith(args.year)]

    if not pred_files:
        print(f"[ERROR] pred.json が見つかりません (year={args.year})", flush=True)
        sys.exit(1)

    date_list = [Path(f).name[:8] for f in pred_files]
    print(f"\n[INFO] 対象日数: {len(date_list)} 日 ({date_list[0]}〜{date_list[-1]})", flush=True)
    print(f"[INFO] pred.json注記: 本番運用版pred.json(リークあり可能性)を使用。", flush=True)
    print(f"       戦略評価のため composite/mark は再計算せずそのまま使用。", flush=True)
    print(f"       ROIは戦略後付け評価値として解釈すること。\n", flush=True)

    # ── 年別集計用 ──
    year_stats: Dict[str, List[dict]] = defaultdict(list)
    all_day_stats: List[dict] = []

    total_days = len(date_list)
    for idx, date_str in enumerate(date_list):
        # プログレス表示
        if idx % 50 == 0 or idx == total_days - 1:
            pct = (idx + 1) / total_days * 100
            bar_filled = int(pct / 5)
            bar = "#" * bar_filled + "." * (20 - bar_filled)
            print(f"\r[{bar}] {pct:.0f}% ({idx+1}/{total_days}) {date_str}", end="", flush=True)

        # results 読み込み
        results_map = load_results(date_str)

        # 日次処理
        day_stats = process_date(date_str, results_map)
        if not day_stats:
            continue

        year = date_str[:4]
        year_stats[year].append(day_stats)
        all_day_stats.append(day_stats)

    print("\n\n[INFO] 集計完了。レポート出力開始...\n", flush=True)

    # ── 全体集計 ──
    total_agg = aggregate_stats(all_day_stats)
    date_range = f"({date_list[0][:4]}-{date_list[0][4:6]}-{date_list[0][6:]} 〜 {date_list[-1][:4]}-{date_list[-1][4:6]}-{date_list[-1][6:]})"
    print_stats("【全体】印断層三連複 Walk-Forward ROI", total_agg, date_range)

    # ── 年別集計 ──
    for year in sorted(year_stats.keys()):
        year_agg = aggregate_stats(year_stats[year])
        yr_dates = sorted([d for d in date_list if d.startswith(year)])
        yr_range = f"({yr_dates[0][:4]}-{yr_dates[0][4:6]}-{yr_dates[0][6:]} 〜 {yr_dates[-1][:4]}-{yr_dates[-1][4:6]}-{yr_dates[-1][6:]})"
        print_stats(f"【{year}年】印断層三連複", year_agg, yr_range)

    # ── 注記 ──
    print("=" * 60)
    print("【数値注記】")
    print("  - ROI計算: 払戻データ(results.json/results_fixed/)が取得できた")
    print("    レースのみを対象。欠損レースは分子(払戻)・分母(投資)ともに除外")
    print("    ただし近似計算（coverage比率で按分）のため±数%の誤差あり")
    print("  - hit%: 払戻 coverage 内の発火レース中、1点でも的中したレース比率")
    print("  - formation A-F1/A-F2 の点数: col1×col2の排他後のdistinct3頭組み合わせ")
    print("  - pred.json は本番運用版（2024-2025は時系列リークあり可能性）")
    print("  - 構造的背景: このプロジェクトはROI110%到達困難と既知 (feedback_construction_ceiling)")
    print("=" * 60)


if __name__ == "__main__":
    main()
