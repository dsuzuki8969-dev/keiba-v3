"""真の成績レポート v2: accuracy モード単独 + 券種別内訳

モード間重複を排除し、accuracy モードのみの実成績を表示する。
"""
from __future__ import annotations

import io
import json
import sys
from pathlib import Path


def safe_int(v):
    try:
        return int(v)
    except (ValueError, TypeError):
        return 0


def payout_for_single(bucket, combo_str: str) -> int:
    if bucket is None:
        return 0
    if isinstance(bucket, dict):
        if str(bucket.get("combo", "")) == combo_str:
            return safe_int(bucket.get("payout", 0))
        return 0
    if isinstance(bucket, list):
        for it in bucket:
            if isinstance(it, dict) and str(it.get("combo", "")) == combo_str:
                return safe_int(it.get("payout", 0))
    return 0


def normalize_combo(ticket: dict) -> list[str]:
    tt = ticket.get("type", "")
    combo = ticket.get("combo", "")
    sortable_types = {"馬連", "三連複", "ワイド"}
    if isinstance(combo, list):
        nos = [str(x) for x in combo]
    elif isinstance(combo, str):
        nos = combo.replace("=", "-").split("-")
    else:
        return []
    if tt in sortable_types:
        nos = sorted(nos, key=lambda x: int(x) if x.isdigit() else 99)
    return ["-".join(nos)]


def ticket_hit_payout(payouts: dict, ticket: dict) -> int:
    tt = ticket.get("type", "")
    bucket = payouts.get(tt)
    if bucket is None:
        return 0
    for c in normalize_combo(ticket):
        p = payout_for_single(bucket, c)
        if p > 0:
            return p
    return 0


def main(date: str) -> int:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

    pred_fp = Path(f"data/predictions/{date}_pred.json")
    res_fp = Path(f"data/results/{date}_results.json")
    if not pred_fp.exists():
        print(f"pred.json なし: {pred_fp}")
        return 1
    if not res_fp.exists():
        print(f"results.json なし: {res_fp}")
        return 1

    with pred_fp.open("r", encoding="utf-8") as f:
        pred = json.load(f)
    with res_fp.open("r", encoding="utf-8") as f:
        actual = json.load(f)

    print(f"=== {date} 真の精度レポート（accuracy モード単独） ===")
    print()

    # 信頼度 × モード別
    summary = {}
    ticket_summary = {}
    matched = 0

    # モード重複除外のため、(race_id, type, combo_str) キーで一意化した集計も取る
    global_seen = {"accuracy": set(), "balanced": set(), "recovery": set()}
    global_stats = {m: {"stake": 0, "ret": 0, "hit": 0, "pts": 0} for m in ("accuracy", "balanced", "recovery")}
    global_union = set()
    global_union_stats = {"stake": 0, "ret": 0, "hit": 0, "pts": 0}

    for r in pred.get("races", []):
        rid = r.get("race_id", "")
        res = actual.get(rid)
        if not res:
            continue
        pay = res.get("payouts", {})
        conf = r.get("confidence", "?")
        tbm = r.get("tickets_by_mode", {}) or {}
        matched += 1

        # 各モードの統計（重複なし）
        for mode in ("accuracy", "balanced", "recovery"):
            ts = tbm.get(mode, []) or []
            for t in ts:
                stk = safe_int(t.get("stake", 0))
                if stk <= 0:
                    continue
                key = (rid, t.get("type", ""), normalize_combo(t)[0] if normalize_combo(t) else "")
                if key in global_seen[mode]:
                    continue
                global_seen[mode].add(key)
                p = ticket_hit_payout(pay, t)
                s = global_stats[mode]
                s["pts"] += 1
                s["stake"] += stk
                if p > 0:
                    s["hit"] += 1
                    s["ret"] += p * stk // 100

                # 信頼度×モード
                key2 = (conf, mode)
                s2 = summary.setdefault(
                    key2, {"stake": 0, "ret": 0, "hit": 0, "pts": 0}
                )
                s2["stake"] += stk
                s2["pts"] += 1
                if p > 0:
                    s2["hit"] += 1
                    s2["ret"] += p * stk // 100

                # 信頼度×券種（accuracy モードのみカウント、真の券種分布）
                if mode == "accuracy":
                    tt = t.get("type", "?")
                    key3 = (conf, tt)
                    s3 = ticket_summary.setdefault(
                        key3, {"stake": 0, "ret": 0, "hit": 0, "pts": 0}
                    )
                    s3["pts"] += 1
                    s3["stake"] += stk
                    if p > 0:
                        s3["hit"] += 1
                        s3["ret"] += p * stk // 100

                # 全モード Union（本当に「買った」1回分）
                ukey = (rid, t.get("type", ""), normalize_combo(t)[0] if normalize_combo(t) else "")
                if ukey not in global_union:
                    global_union.add(ukey)
                    global_union_stats["pts"] += 1
                    global_union_stats["stake"] += stk
                    if p > 0:
                        global_union_stats["hit"] += 1
                        global_union_stats["ret"] += p * stk // 100

    print("=" * 80)
    print(f"モード別の真の成績（重複除外、{matched}レース）")
    print("=" * 80)
    print(f'{"モード":<10}{"点数":<6}{"的中":<6}{"投資":<10}{"払戻":<10}{"ROI":<10}{"的中率":<10}')
    for mode in ("accuracy", "balanced", "recovery"):
        s = global_stats[mode]
        roi = f'{s["ret"] / s["stake"] * 100:.1f}%' if s["stake"] > 0 else "-"
        hr = f'{s["hit"] / s["pts"] * 100:.1f}%' if s["pts"] > 0 else "-"
        print(f'{mode:<10}{s["pts"]:<6}{s["hit"]:<6}{s["stake"]:<10}{s["ret"]:<10}{roi:<10}{hr:<10}')

    print()
    print("=" * 80)
    print("全モード Union（実際に買う単一モード想定時の成績）")
    print("=" * 80)
    s = global_union_stats
    roi = f'{s["ret"] / s["stake"] * 100:.1f}%' if s["stake"] > 0 else "-"
    hr = f'{s["hit"] / s["pts"] * 100:.1f}%' if s["pts"] > 0 else "-"
    print(f"  点数: {s['pts']} / 的中: {s['hit']}")
    print(f"  投資: {s['stake']}円 / 払戻: {s['ret']}円 / ROI: {roi} / 的中率: {hr}")

    print()
    print("=" * 80)
    print("信頼度 × モード（重複除外）")
    print("=" * 80)
    print(f'{"信頼度":<6}{"モード":<10}{"点数":<6}{"的中":<6}{"投資":<8}{"払戻":<10}{"ROI":<8}')
    for conf in ("SS", "S", "A", "B", "C"):
        for mode in ("accuracy", "balanced", "recovery"):
            s = summary.get((conf, mode))
            if not s:
                continue
            roi = f'{s["ret"] / s["stake"] * 100:.1f}%' if s["stake"] > 0 else "-"
            print(f'{conf:<6}{mode:<10}{s["pts"]:<6}{s["hit"]:<6}{s["stake"]:<8}{s["ret"]:<10}{roi:<8}')

    print()
    print("=" * 80)
    print("信頼度 × 券種（accuracy モードのみ = 真の券種分布）")
    print("=" * 80)
    print(f'{"信頼度":<6}{"券種":<10}{"点数":<6}{"的中":<6}{"投資":<8}{"払戻":<10}{"ROI":<8}')
    for conf in ("SS", "S", "A", "B", "C"):
        for tt in ("単勝", "複勝", "馬連", "ワイド", "三連複", "三連単"):
            s = ticket_summary.get((conf, tt))
            if not s:
                continue
            roi = f'{s["ret"] / s["stake"] * 100:.1f}%' if s["stake"] > 0 else "-"
            print(f'{conf:<6}{tt:<10}{s["pts"]:<6}{s["hit"]:<6}{s["stake"]:<8}{s["ret"]:<10}{roi:<8}')

    return 0


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("使い方: python scripts/race_accuracy_report_v2.py YYYYMMDD")
        sys.exit(1)
    sys.exit(main(sys.argv[1]))
