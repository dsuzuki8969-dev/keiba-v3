#!/usr/bin/env python
"""
D-AI Keiba データ整合性チェックスクリプト

チェック項目:
  1. predictions ↔ race_results リンク状況
  2. payouts_json が空のレコード
  3. order_json が空のレコード（結果未取得）
  4. 着順データ異常（top2/top3のサイズ不正、重複着順）
  5. tickets_json が空の予測
  6. 馬番/着順の範囲外チェック
  7. 月別・年別サマリー

Usage:
  python scripts/check_data_integrity.py
  python scripts/check_data_integrity.py --start 2025-01-01 --end 2026-03-01
  python scripts/check_data_integrity.py --fix-cancelled   # cancelledフラグ修正
"""

import argparse
import json
import os
import re
import sqlite3
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.stdout.reconfigure(encoding="utf-8")

DB_PATH = "data/keiba.db"


def check_integrity(start: str, end: str, verbose: bool = False):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    print(f"\n{'='*65}")
    print(f"  D-AI Keiba データ整合性チェック")
    print(f"  期間: {start} 〜 {end}")
    print(f"{'='*65}\n")

    # ─── 1. predictions 総数 ────────────────────────────────
    pred_rows = conn.execute(
        "SELECT race_id, date, tickets_json, horses_json, field_count, confidence "
        "FROM predictions WHERE date >= ? AND date <= ? ORDER BY date, race_id",
        (start, end),
    ).fetchall()
    total_preds = len(pred_rows)
    print(f"[1] 予測レコード総数: {total_preds:,}")

    # ─── 2. race_results 総数 ─────────────────────────────────
    result_rows = conn.execute(
        "SELECT r.race_id, r.order_json, r.payouts_json, r.cancelled, p.date "
        "FROM race_results r "
        "LEFT JOIN predictions p ON r.race_id = p.race_id "
        "WHERE p.date >= ? AND p.date <= ?",
        (start, end),
    ).fetchall()
    total_results = len(result_rows)
    print(f"[2] 結果レコード総数: {total_results:,}")

    # ─── 3. リンク状況 ────────────────────────────────────────
    pred_ids   = {r["race_id"] for r in pred_rows}
    result_ids = {r["race_id"] for r in result_rows}
    preds_without_result = pred_ids - result_ids
    results_without_pred = result_ids - pred_ids

    print(f"\n[3] リンク整合性")
    print(f"    予測のみ（結果なし）: {len(preds_without_result):,}件")
    print(f"    結果のみ（予測なし）: {len(results_without_pred):,}件")
    if verbose and preds_without_result:
        for rid in sorted(preds_without_result)[:10]:
            print(f"      予測のみ: {rid}")

    # ─── 4. tickets_json 状況 ─────────────────────────────────
    no_tickets    = 0
    empty_tickets = 0
    backfill_cnt  = 0
    live_cnt      = 0
    for r in pred_rows:
        tj = r["tickets_json"]
        if not tj:
            no_tickets += 1
        else:
            tl = json.loads(tj)
            if not tl:
                empty_tickets += 1
            elif any(t.get("signal") == "簡易" for t in tl):
                backfill_cnt += 1
            else:
                live_cnt += 1

    print(f"\n[4] tickets_json 状況")
    print(f"    tickets なし:          {no_tickets:,}件")
    print(f"    tickets 空リスト:      {empty_tickets:,}件")
    print(f"    バックフィル (簡易):   {backfill_cnt:,}件")
    print(f"    ライブ予測:            {live_cnt:,}件")

    # ─── 5. order_json / payouts_json 状況 ─────────────────────
    no_order    = 0
    no_payouts  = 0
    empty_pay   = 0
    cancelled_cnt = 0
    for r in result_rows:
        if not r["order_json"]:
            no_order += 1
        if r["cancelled"]:
            cancelled_cnt += 1
        pj = r["payouts_json"]
        if not pj:
            no_payouts += 1
        else:
            pd = json.loads(pj)
            if not pd:
                empty_pay += 1

    print(f"\n[5] race_results 状況")
    print(f"    order_json なし:       {no_order:,}件")
    print(f"    payouts_json なし:     {no_payouts:,}件")
    print(f"    payouts_json 空:       {empty_pay:,}件")
    print(f"    cancelled=1:           {cancelled_cnt:,}件")

    # ─── 6. 着順データ異常チェック ─────────────────────────────
    from collections import Counter as _Counter
    anomaly_no_top2    = 0
    anomaly_no_top3    = 0
    anomaly_bad_no     = 0
    dead_heat_cnt      = 0   # 同着（正常）
    anomaly_races      = []

    for r in result_rows:
        if not r["order_json"]:
            continue
        order = json.loads(r["order_json"])
        if not order:
            continue

        finishes  = [h["finish"] for h in order if h.get("finish") is not None]
        horse_nos = [h["horse_no"] for h in order]
        fc = _Counter(finishes)

        # 同着判定: 着順の重複があるが2着スキップなどの正規パターン
        #   例: finish=[1,1,3,4,...] → 2頭同着1着 → 正常
        has_dup = len(finishes) != len(set(finishes))
        if has_dup:
            # 同着か異常かを判定: 重複がある位置の次の着順がスキップされていれば同着
            is_valid_tie = True
            for pos, cnt_pos in fc.items():
                if cnt_pos > 1:
                    # pos着が cnt_pos 頭同着 → pos+cnt_pos-1着が存在しないはず
                    expected_skip = pos + cnt_pos - 1
                    if expected_skip in fc:
                        is_valid_tie = False
                        break
            if is_valid_tie:
                dead_heat_cnt += 1
                # 同着レースのtop2/top3はスキップ（正常なのでtop3が異なるサイズもOK）
                continue

        top2 = {h["horse_no"] for h in order if h.get("finish", 99) <= 2}
        top3 = {h["horse_no"] for h in order if h.get("finish", 99) <= 3}

        if len(top2) < 2:
            anomaly_no_top2 += 1
            anomaly_races.append((f"top2サイズ={len(top2)}", r["race_id"]))
        if len(top3) < 3:
            anomaly_no_top3 += 1
            anomaly_races.append((f"top3サイズ={len(top3)}", r["race_id"]))

        # 馬番範囲チェック (1-28)
        if any(n < 1 or n > 28 for n in horse_nos if n is not None):
            anomaly_bad_no += 1
            anomaly_races.append(("馬番範囲外", r["race_id"]))

    print(f"\n[6] 着順データ異常")
    print(f"    同着 (dead heat, 正常): {dead_heat_cnt:,}件")
    print(f"    top2サイズ不正:         {anomaly_no_top2:,}件")
    print(f"    top3サイズ不正:         {anomaly_no_top3:,}件")
    print(f"    馬番範囲外:             {anomaly_bad_no:,}件")
    if verbose and anomaly_races:
        for tag, rid in anomaly_races[:20]:
            print(f"      [{tag}] {rid}")

    # ─── 7. 月別サマリー ──────────────────────────────────────
    print(f"\n[7] 月別サマリー")
    by_month = defaultdict(lambda: {
        "preds": 0, "results": 0, "no_payout": 0, "cancelled": 0
    })

    for r in pred_rows:
        m = r["date"][:7]
        by_month[m]["preds"] += 1

    result_by_id = {r["race_id"]: r for r in result_rows}
    for r in pred_rows:
        rid = r["race_id"]
        m = r["date"][:7]
        rr = result_by_id.get(rid)
        if rr:
            by_month[m]["results"] += 1
            if rr["cancelled"]:
                by_month[m]["cancelled"] += 1
            pj = rr["payouts_json"]
            if not pj or not json.loads(pj):
                by_month[m]["no_payout"] += 1

    print(f"  {'月':<8} {'予測':>6} {'結果':>6} {'払戻なし':>8} {'中止':>5}")
    print(f"  {'-'*38}")
    for m in sorted(by_month.keys()):
        d = by_month[m]
        flag = " !" if d["no_payout"] > 10 else ""
        print(f"  {m:<8} {d['preds']:>6} {d['results']:>6} {d['no_payout']:>8} {d['cancelled']:>5}{flag}")

    # ─── 8. 総合サマリー ──────────────────────────────────────
    total_anomaly = (anomaly_no_top2 + anomaly_no_top3 + anomaly_bad_no)
    total_payout_issues = no_payouts + empty_pay

    print(f"\n{'='*65}")
    print(f"  チェック完了")
    print(f"  着順異常:       {total_anomaly:,}件")
    print(f"  払戻データ不備: {total_payout_issues:,}件")
    print(f"  結果リンク漏れ: {len(preds_without_result):,}件")
    print(f"  ライブ予測:     {live_cnt:,}件  (バックフィル: {backfill_cnt:,}件)")

    if total_anomaly == 0 and total_payout_issues == 0 and len(preds_without_result) == 0:
        print(f"\n  [OK] 重大な問題は見つかりませんでした。")
    else:
        print(f"\n  [!] 上記の問題を確認してください。")
        if total_payout_issues > 0:
            print(f"      払戻不備 → python scripts/backfill_payouts_from_html.py --force で修復可能")
    print(f"{'='*65}\n")

    conn.close()


def main():
    parser = argparse.ArgumentParser(description="データ整合性チェック")
    parser.add_argument("--start",   default="2025-01-01")
    parser.add_argument("--end",     default="2026-03-01")
    parser.add_argument("--verbose", action="store_true", help="異常レコードを詳細表示")
    args = parser.parse_args()
    check_integrity(args.start, args.end, args.verbose)


if __name__ == "__main__":
    main()
