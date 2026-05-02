"""5 つの三連単フォーメーションパターン (A-E) のバックテスト集計。

マスター指示 2026-05-02:
A: ◉/◎ → 〇/▲/☆       → 〇/▲/△/★/☆
B: ◉/◎ → 〇/▲/△       → 〇/▲/△/★/☆
C: ◉/◎ → 〇/▲         → 〇/▲/△/★/☆
D: ◉/◎/〇 → ◉/◎/〇/▲   → 〇/▲/△/★/☆
E: ◉/◎ → 〇           → ▲/△/★/☆
F: ◉/◎/〇 → ◉/◎/〇/▲/☆ → 〇/▲/△/★/☆
G: ◉/◎/〇 → ◉/◎/〇/▲/△ → 〇/▲/△/★/☆

全期間 (data/predictions/*_pred.json + data/results/*_results.json) で
三連単フォーメーション買い目をシミュレーション、的中率と回収率を比較。

各点 100円固定、三連単 payouts (修復済み combo) で照合。
"""
from __future__ import annotations

import io
import json
import re
import sys
import time
from itertools import permutations
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

PRED_DIR = Path("data/predictions")
RES_DIR = Path("data/results")

STAKE = 100

# 印グループ定義
HONMEI_MARKS = {"◉", "◎"}                      # 1着候補 (パターン A-C, E)
HONMEI_TAIKOU_MARKS = {"◉", "◎", "○", "〇"}     # 1着候補 (D)
A_2ND = {"○", "〇", "▲", "☆"}                 # A の 2 着
B_2ND = {"○", "〇", "▲", "△"}                 # B の 2 着
C_2ND = {"○", "〇", "▲"}                       # C の 2 着
D_2ND = {"◉", "◎", "○", "〇", "▲"}            # D の 2 着
E_2ND = {"○", "〇"}                            # E の 2 着
F_2ND = {"◉", "◎", "○", "〇", "▲", "☆"}      # F の 2 着
G_2ND = {"◉", "◎", "○", "〇", "▲", "△"}      # G の 2 着
ABC_3RD = {"○", "〇", "▲", "△", "★", "☆"}    # A/B/C/D/F/G の 3 着
E_3RD   = {"▲", "△", "★", "☆"}               # E の 3 着


def _filter_active(horses: list) -> list:
    return [h for h in horses
            if not h.get("is_tokusen_kiken") and not h.get("is_scratched")]


def _horses_by_marks(horses: list, marks: set) -> list[int]:
    """指定の印の馬番リストを返す (重複除去・順番は composite 降順)。"""
    cands = [h for h in horses if (h.get("mark") or "").strip() in marks]
    cands.sort(key=lambda h: -(h.get("composite") or 0))
    seen, out = set(), []
    for h in cands:
        no = h.get("horse_no")
        if no and no not in seen:
            seen.add(no)
            out.append(no)
    return out


def build_tickets(horses: list, m1: set, m2: set, m3: set) -> list[tuple[int, int, int]]:
    """フォーメーション買い目: (1着, 2着, 3着) の順序組合せを全列挙 (重複除く)。"""
    h1 = _horses_by_marks(horses, m1)
    h2 = _horses_by_marks(horses, m2)
    h3 = _horses_by_marks(horses, m3)
    seen, tickets = set(), []
    for a in h1:
        for b in h2:
            if b == a: continue
            for c in h3:
                if c == a or c == b: continue
                key = (a, b, c)
                if key in seen: continue
                seen.add(key)
                tickets.append(key)
    return tickets


def lookup_sanrentan(payouts: dict, combo: tuple[int, int, int]) -> int:
    """三連単 payouts から指定 combo の払戻を取得。"""
    bucket = payouts.get("三連単") or payouts.get("sanrentan")
    if bucket is None:
        return 0
    target = "-".join(str(x) for x in combo)
    if isinstance(bucket, dict):
        if str(bucket.get("combo", "")) == target:
            return int(bucket.get("payout", 0) or 0)
    elif isinstance(bucket, list):
        for it in bucket:
            if isinstance(it, dict) and str(it.get("combo", "")) == target:
                return int(it.get("payout", 0) or 0)
    return 0


PATTERNS = {
    "A": (HONMEI_MARKS, A_2ND, ABC_3RD),
    "B": (HONMEI_MARKS, B_2ND, ABC_3RD),
    "C": (HONMEI_MARKS, C_2ND, ABC_3RD),
    "D": (HONMEI_TAIKOU_MARKS, D_2ND, ABC_3RD),
    "E": (HONMEI_MARKS, E_2ND, E_3RD),
    "F": (HONMEI_TAIKOU_MARKS, F_2ND, ABC_3RD),
    "G": (HONMEI_TAIKOU_MARKS, G_2ND, ABC_3RD),
}


def main():
    pred_files = sorted(PRED_DIR.glob("*_pred.json"))
    pred_files = [f for f in pred_files if "_prev" not in f.name]

    from collections import Counter
    stats = {k: {"races_played": 0, "races_hit": 0,
                 "tickets_total": 0, "tickets_hit": 0,
                 "stake": 0, "payback": 0,
                 "max_payout": 0, "max_date": "", "max_race": "",
                 "tickets_dist": Counter()}  # 1レース毎の点数分布
             for k in PATTERNS}

    started = time.time()
    n_pred = 0

    for fi, fp in enumerate(pred_files):
        date_str = fp.name.split("_")[0]
        if len(date_str) != 8 or not date_str.isdigit():
            continue
        res_fp = RES_DIR / f"{date_str}_results.json"
        if not res_fp.exists():
            continue
        try:
            pred = json.loads(fp.read_text(encoding="utf-8"))
            results = json.loads(res_fp.read_text(encoding="utf-8"))
        except Exception:
            continue
        n_pred += 1

        for r in pred.get("races", []):
            rid = str(r.get("race_id", ""))
            horses = _filter_active(r.get("horses", []))
            if not horses:
                continue
            rdata = results.get(rid)
            if not rdata:
                continue
            payouts = rdata.get("payouts", {})
            if not payouts.get("三連単") and not payouts.get("sanrentan"):
                continue

            for pat_name, (m1, m2, m3) in PATTERNS.items():
                tickets = build_tickets(horses, m1, m2, m3)
                if not tickets:
                    continue
                # 集計
                stake = len(tickets) * STAKE
                payback = 0
                hit_tickets = 0
                for combo in tickets:
                    pb = lookup_sanrentan(payouts, combo)
                    if pb > 0:
                        payback += pb
                        hit_tickets += 1
                race_hit = payback > 0

                s = stats[pat_name]
                s["races_played"] += 1
                if race_hit:
                    s["races_hit"] += 1
                s["tickets_total"] += len(tickets)
                s["tickets_hit"] += hit_tickets
                s["tickets_dist"][len(tickets)] += 1
                s["stake"] += stake
                s["payback"] += payback
                if payback > s["max_payout"]:
                    s["max_payout"] = payback
                    s["max_date"] = date_str
                    s["max_race"] = rid

        if (fi + 1) % 100 == 0 or (fi + 1) == len(pred_files):
            el = time.time() - started
            print(f"  {fi+1}/{len(pred_files)} ({date_str}) elapsed={el:.1f}s", flush=True)

    print()
    print(f"集計対象 pred 日数: {n_pred}")
    print()
    print(f"{'パターン':<5} {'R購入':>7} {'R的中':>7} {'的中率':>7} {'1R点数 (最頻)':>16} {'点数計':>9} {'購入':>12} {'払戻':>12} {'収支':>13} {'ROI':>7}")
    print("─" * 124)
    for k in ["A", "B", "C", "D", "E", "F", "G"]:
        s = stats[k]
        rp = s["races_played"]; rh = s["races_hit"]
        roi = s["payback"] / s["stake"] * 100 if s["stake"] else 0
        rhr = rh / rp * 100 if rp else 0
        bal = s["payback"] - s["stake"]
        # 最頻点数 (整数) と全体に占める割合
        dist = s["tickets_dist"]
        if dist:
            mode_pts, mode_cnt = dist.most_common(1)[0]
            mode_str = f"{mode_pts}点 ({mode_cnt/rp*100:.0f}%)"
        else:
            mode_str = "-"
        print(f"{k:<5} {rp:>7,} {rh:>7,} {rhr:>6.1f}% {mode_str:>16} {s['tickets_total']:>9,} "
              f"{s['stake']:>11,}円 {s['payback']:>11,}円 "
              f"{bal:>+12,}円 {roi:>6.1f}%")
    # 点数分布の詳細 (上位 3)
    print()
    print("点数分布 (上位3):")
    for k in ["A", "B", "C", "D", "E", "F", "G"]:
        s = stats[k]
        top3 = s["tickets_dist"].most_common(3)
        rp = s["races_played"] or 1
        dist_str = ", ".join(f"{p}点 {c/rp*100:.0f}%" for p,c in top3)
        print(f"  {k}: {dist_str}")
    print()
    print(f"最高払戻:")
    for k in ["A", "B", "C", "D", "E", "F", "G"]:
        s = stats[k]
        if s["max_payout"]:
            print(f"  {k}: {s['max_payout']:,}円 ({s['max_date']} race={s['max_race']})")

    elapsed = time.time() - started
    print(f"\nTotal elapsed: {elapsed:.1f}s")


if __name__ == "__main__":
    main()
