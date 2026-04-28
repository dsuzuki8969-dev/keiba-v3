"""
指定日の信頼度別・券種別・レース別の的中率・回収率レポート

使い方: python scripts/race_accuracy_report.py YYYYMMDD
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
    """payouts[type] が dict or list of dicts のどちらでも対応"""
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
    """ticket.combo を "1-2-7" 形式の文字列リストに変換（順序違いも考慮）"""
    tt = ticket.get("type", "")
    combo = ticket.get("combo", "")
    # 三連複・馬連・ワイドは順序不問
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
    """払戻金額（100円ベース）を返す"""
    tt = ticket.get("type", "")
    bucket = payouts.get(tt)
    if bucket is None:
        return 0
    for c in normalize_combo(ticket):
        p = payout_for_single(bucket, c)
        if p > 0:
            return p
    return 0


def tansho_payout(payouts: dict, hno: int) -> int:
    b = payouts.get("単勝")
    return payout_for_single(b, str(hno))


def fukusho_payout(payouts: dict, hno: int) -> int:
    b = payouts.get("複勝")
    return payout_for_single(b, str(hno))


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

    # 各レースの結果
    print(f"=== {date} 信頼度別 × 券種別 × モード別 的中・回収（全{len(pred['races'])}レース）===")
    print()
    print(
        f'{"レース":<11}{"信頼度":<6}{"◉◎着":<6}{"単勝":<6}{"複勝":<6}'
        f'{"acc":<16}{"bal":<16}{"rec":<16}'
    )
    print("-" * 100)

    summary = {}  # (conf, mode) -> stats
    ticket_summary = {}  # (conf, ticket_type) -> stats
    matched_races = 0

    for r in pred.get("races", []):
        rid = r.get("race_id", "")
        res = actual.get(rid)
        if not res:
            continue
        fm = {rr["horse_no"]: rr["finish"] for rr in res.get("order", [])}
        if not fm:
            continue
        pay = res.get("payouts", {})
        conf = r.get("confidence", "?")
        venue = r.get("venue", "?")
        rno = r.get("race_no", "?")

        hm_no = None
        for h in r.get("horses", []):
            if h.get("mark") in ("◉", "◎"):
                hm_no = h["horse_no"]
                break
        hm_fin = fm.get(hm_no, 99) if hm_no else 99
        tan = tansho_payout(pay, hm_no) if hm_no and hm_fin == 1 else 0
        fuk = fukusho_payout(pay, hm_no) if hm_no and hm_fin <= 3 else 0

        mode_cells = []
        tbm = r.get("tickets_by_mode", {}) or {}
        for mode in ("accuracy", "balanced", "recovery"):
            ts = tbm.get(mode, [])
            pts = len(ts)
            stake = 0
            ret = 0
            hit = 0
            for t in ts:
                stk = safe_int(t.get("stake", 0))
                if stk <= 0:
                    continue
                stake += stk
                p = ticket_hit_payout(pay, t)
                if p > 0:
                    hit += 1
                    ret += p * stk // 100

                tt = t.get("type", "?")
                ts_key = (conf, tt)
                tts = ticket_summary.setdefault(
                    ts_key, {"stake": 0, "ret": 0, "hit": 0, "pts": 0}
                )
                tts["stake"] += stk
                tts["pts"] += 1
                if p > 0:
                    tts["hit"] += 1
                    tts["ret"] += p * stk // 100

            key = (conf, mode)
            s = summary.setdefault(
                key, {"stake": 0, "ret": 0, "hit": 0, "pts": 0, "races": 0, "hit_races": 0}
            )
            s["stake"] += stake
            s["ret"] += ret
            s["hit"] += hit
            s["pts"] += pts
            if pts > 0:
                s["races"] += 1
            if hit > 0:
                s["hit_races"] += 1

            cell = f"{pts}点{hit}的中({ret}円)"
            mode_cells.append(cell)

        matched_races += 1
        print(
            f"{venue}{rno}R".ljust(11)
            + f"{conf:<6}"
            + f"{(hm_fin if hm_fin < 99 else '-'):<6}"
            + f"{tan:<6}"
            + f"{fuk:<6}"
            + f"{mode_cells[0]:<16}{mode_cells[1]:<16}{mode_cells[2]:<16}"
        )

    print()
    print("=" * 90)
    print(f"信頼度別 × モード別 集計（{matched_races}レース）")
    print("=" * 90)
    print(
        f'{"信頼度":<6}{"モード":<10}{"R":<4}{"的中R":<6}{"点数":<6}{"的中":<6}'
        f'{"投資":<8}{"払戻":<10}{"ROI":<8}{"的中率":<8}'
    )
    for conf in ("SS", "S", "A", "B", "C"):
        for mode in ("accuracy", "balanced", "recovery"):
            s = summary.get((conf, mode))
            if not s:
                continue
            roi = f'{s["ret"] / s["stake"] * 100:.1f}%' if s["stake"] > 0 else "-"
            hit_rate = (
                f'{s["hit_races"] / s["races"] * 100:.1f}%' if s["races"] > 0 else "-"
            )
            print(
                f'{conf:<6}{mode:<10}{s["races"]:<4}{s["hit_races"]:<6}'
                f'{s["pts"]:<6}{s["hit"]:<6}{s["stake"]:<8}{s["ret"]:<10}'
                f'{roi:<8}{hit_rate:<8}'
            )

    print()
    print("=" * 80)
    print("信頼度 × 券種 集計")
    print("=" * 80)
    print(
        f'{"信頼度":<6}{"券種":<10}{"点数":<6}{"的中":<6}'
        f'{"投資":<8}{"払戻":<10}{"ROI":<8}'
    )
    for conf in ("SS", "S", "A", "B", "C"):
        for tt in ("馬連", "三連複", "ワイド", "単勝", "複勝", "三連単"):
            s = ticket_summary.get((conf, tt))
            if not s:
                continue
            roi = f'{s["ret"] / s["stake"] * 100:.1f}%' if s["stake"] > 0 else "-"
            print(
                f'{conf:<6}{tt:<10}{s["pts"]:<6}{s["hit"]:<6}'
                f'{s["stake"]:<8}{s["ret"]:<10}{roi:<8}'
            )

    # 全体 total
    tot_stake = sum(s["stake"] for s in summary.values())
    tot_ret = sum(s["ret"] for s in summary.values())
    tot_pts = sum(s["pts"] for s in summary.values())
    tot_hit = sum(s["hit"] for s in summary.values())
    print()
    print("=" * 40)
    print("全体合計 (全モード合算)")
    print("=" * 40)
    print(f"  投資: {tot_stake}円 / 払戻: {tot_ret}円")
    if tot_stake:
        print(f"  ROI: {tot_ret/tot_stake*100:.1f}%")
    print(f"  点数: {tot_pts} / 的中: {tot_hit}")
    if tot_pts:
        print(f"  点数ベース的中率: {tot_hit/tot_pts*100:.1f}%")

    return 0


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("使い方: python scripts/race_accuracy_report.py YYYYMMDD")
        sys.exit(1)
    sys.exit(main(sys.argv[1]))
