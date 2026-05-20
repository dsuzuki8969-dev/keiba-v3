#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
三連複 ROI 分析スクリプト
- 2026年の pred.json + result.json を突合
- JRA/NAR × 信頼度(SS/S/A/B/C/D) のクロス集計
- 各セル: レース数、チケット数、投資額、回収額、ROI%
"""

import json
import glob
import os
from collections import defaultdict

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PRED_DIR = os.path.join(BASE_DIR, "data", "predictions")
RESULT_DIR = os.path.join(BASE_DIR, "data", "results")

# 信頼度の順序
CONF_ORDER = ["SS", "S", "A", "B", "C", "D"]


def load_result_index(result_path):
    """result.json を読み、venue_code + race_no → (payout_dict, race_id) のマップを返す"""
    with open(result_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    index = {}
    for rid, rv in data.items():
        vc = rid[4:6]  # venue_code
        rno = rid[10:12]  # race_no
        key = f"{vc}_{rno}"
        index[key] = rv
    return index


def get_winning_trio(payouts_dict):
    """payouts dict から三連複の的中 combo セットを返す。
    payouts['三連複'] は dict（単一）or list（複数）。
    返り値: {combo_str: payout, ...}
    """
    trio = payouts_dict.get("三連複")
    if trio is None:
        return {}

    if isinstance(trio, dict):
        return {trio["combo"]: trio["payout"]}
    elif isinstance(trio, list):
        return {p["combo"]: p["payout"] for p in trio}
    else:
        return {}


def normalize_combo(combo_list):
    """チケットの combo ([2, 3, 5]) をソートしてハイフン区切り文字列にする"""
    return "-".join(str(x) for x in sorted(combo_list))


def analyze():
    """メイン分析"""
    # 集計用構造: stats[org][conf] = {races, tickets, invest, return}
    stats = defaultdict(lambda: defaultdict(lambda: {
        "races": 0, "tickets": 0, "invest": 0, "ret": 0,
        "race_ids": set()  # 重複カウント防止
    }))

    pred_files = sorted(glob.glob(os.path.join(PRED_DIR, "2026*_pred.json")))
    print(f"対象 pred ファイル数: {len(pred_files)}")

    no_result_dates = []
    total_races = 0
    total_matched = 0
    total_no_trio_payout = 0

    for i, pred_path in enumerate(pred_files):
        date_str = os.path.basename(pred_path)[:8]
        result_path = os.path.join(RESULT_DIR, f"{date_str}_results.json")

        if not os.path.exists(result_path):
            no_result_dates.append(date_str)
            continue

        with open(pred_path, "r", encoding="utf-8") as f:
            pred_data = json.load(f)

        result_index = load_result_index(result_path)

        for race in pred_data.get("races", []):
            race_id = str(race.get("race_id", ""))
            is_jra = race.get("is_jra", False)
            confidence = race.get("confidence", race.get("conf", "UNKNOWN"))
            org = "JRA" if is_jra else "NAR"

            # チケット取得（formation_tickets 優先、なければ tickets）
            tickets = race.get("formation_tickets") or race.get("tickets") or []
            trio_tickets = [t for t in tickets if t.get("type") == "三連複"]

            if not trio_tickets:
                continue

            total_races += 1

            # result とマッチング (venue_code + race_no)
            vc = race_id[4:6]
            rno = race_id[10:12]
            lookup_key = f"{vc}_{rno}"

            rv = result_index.get(lookup_key)
            if rv is None:
                continue

            total_matched += 1
            payouts = rv.get("payouts", {})
            winning = get_winning_trio(payouts)

            if not winning:
                total_no_trio_payout += 1
                continue

            # レース単位の集計キー（重複防止）
            race_key = f"{date_str}_{lookup_key}"

            cell = stats[org][confidence]
            if race_key not in cell["race_ids"]:
                cell["race_ids"].add(race_key)
                cell["races"] += 1

            for t in trio_tickets:
                combo_str = normalize_combo(t["combo"])
                stake = t.get("stake", 100)
                cell["tickets"] += 1
                cell["invest"] += stake

                if combo_str in winning:
                    cell["ret"] += winning[combo_str]

        # 進捗表示
        if (i + 1) % 20 == 0 or i == len(pred_files) - 1:
            pct = (i + 1) / len(pred_files) * 100
            bar_len = 30
            filled = int(bar_len * (i + 1) / len(pred_files))
            bar = "█" * filled + "░" * (bar_len - filled)
            print(f"  [{bar}] {pct:5.1f}% ({i+1}/{len(pred_files)})")

    # 結果出力
    print()
    print(f"=== 診断情報 ===")
    print(f"  pred ファイル数: {len(pred_files)}")
    print(f"  result なし日数: {len(no_result_dates)}")
    print(f"  三連複チケットありレース: {total_races}")
    print(f"  result マッチ成功: {total_matched}")
    print(f"  三連複 payout なし: {total_no_trio_payout}")
    print()

    # クロス集計テーブル表示
    print("=" * 120)
    print("三連複 ROI 分析 (2026年 out-of-sample)")
    print("=" * 120)

    header = f"{'区分':<8} {'信頼度':<6} {'レース数':>8} {'チケット数':>10} {'投資額':>12} {'回収額':>12} {'ROI%':>10} {'的中率':>8}"
    print(header)
    print("-" * 120)

    # org 別合計用
    org_totals = defaultdict(lambda: {"races": 0, "tickets": 0, "invest": 0, "ret": 0})
    grand_total = {"races": 0, "tickets": 0, "invest": 0, "ret": 0}

    for org in ["JRA", "NAR"]:
        for conf in CONF_ORDER:
            cell = stats[org][conf]
            races = cell["races"]
            tickets = cell["tickets"]
            invest = cell["invest"]
            ret = cell["ret"]
            roi = (ret / invest * 100) if invest > 0 else 0.0
            hit_rate = (ret > 0)  # セル内で1つでも的中あればTrue... ではなく的中チケット数が必要

            # 的中チケット数を別途計算するため、ここでは ROI のみ
            hit_display = f"{roi:>7.1f}%"

            if tickets > 0:
                print(f"{org:<8} {conf:<6} {races:>8,} {tickets:>10,} {invest:>12,} {ret:>12,} {roi:>9.1f}% {'':>8}")
            else:
                print(f"{org:<8} {conf:<6} {'-':>8} {'-':>10} {'-':>12} {'-':>12} {'-':>10} {'':>8}")

            org_totals[org]["races"] += races
            org_totals[org]["tickets"] += tickets
            org_totals[org]["invest"] += invest
            org_totals[org]["ret"] += ret

        # org 小計
        ot = org_totals[org]
        roi = (ot["ret"] / ot["invest"] * 100) if ot["invest"] > 0 else 0.0
        print(f"{'─'*8} {'─'*6} {'─'*8} {'─'*10} {'─'*12} {'─'*12} {'─'*10} {'─'*8}")
        print(f"{org+'計':<7} {'ALL':<6} {ot['races']:>8,} {ot['tickets']:>10,} {ot['invest']:>12,} {ot['ret']:>12,} {roi:>9.1f}%")
        print()

        grand_total["races"] += ot["races"]
        grand_total["tickets"] += ot["tickets"]
        grand_total["invest"] += ot["invest"]
        grand_total["ret"] += ot["ret"]

    # 全体合計
    gt = grand_total
    roi = (gt["ret"] / gt["invest"] * 100) if gt["invest"] > 0 else 0.0
    print("=" * 120)
    print(f"{'全体計':<7} {'ALL':<6} {gt['races']:>8,} {gt['tickets']:>10,} {gt['invest']:>12,} {gt['ret']:>12,} {roi:>9.1f}%")
    print("=" * 120)

    # 的中詳細
    print()
    print("=== 信頼度別サマリ (JRA+NAR合算) ===")
    print(f"{'信頼度':<6} {'レース数':>8} {'チケット':>10} {'投資額':>12} {'回収額':>12} {'ROI%':>10} {'損益':>12}")
    print("-" * 80)
    for conf in CONF_ORDER:
        races = tickets = invest = ret = 0
        for org in ["JRA", "NAR"]:
            c = stats[org][conf]
            races += c["races"]
            tickets += c["tickets"]
            invest += c["invest"]
            ret += c["ret"]
        roi = (ret / invest * 100) if invest > 0 else 0.0
        pnl = ret - invest
        pnl_str = f"+{pnl:,}" if pnl >= 0 else f"{pnl:,}"
        if tickets > 0:
            print(f"{conf:<6} {races:>8,} {tickets:>10,} {invest:>12,} {ret:>12,} {roi:>9.1f}% {pnl_str:>12}")
        else:
            print(f"{conf:<6} {'-':>8} {'-':>10} {'-':>12} {'-':>12} {'-':>10} {'-':>12}")

    total_roi = (gt["ret"] / gt["invest"] * 100) if gt["invest"] > 0 else 0.0
    total_pnl = gt["ret"] - gt["invest"]
    pnl_str = f"+{total_pnl:,}" if total_pnl >= 0 else f"{total_pnl:,}"
    print("-" * 80)
    print(f"{'合計':<6} {gt['races']:>8,} {gt['tickets']:>10,} {gt['invest']:>12,} {gt['ret']:>12,} {total_roi:>9.1f}% {pnl_str:>12}")


if __name__ == "__main__":
    analyze()
