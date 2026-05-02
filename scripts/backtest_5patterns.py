"""7 つの三連複フォーメーションパターン (A-G) のバックテスト集計。

マスター指示 2026-05-02 (確定):
A: ◉/◎ → 〇/▲/☆       → 〇/▲/△/★/☆     (9点)
B: ◉/◎ → 〇/▲/△       → 〇/▲/△/★/☆     (9点)
C: ◉/◎ → 〇/▲         → 〇/▲/△/★/☆     (7点)
D: ◉/◎/〇 → ◉/◎/〇/▲   → 〇/▲/△/★/☆    (10点)
E: ◉/◎ → 〇           → ▲/△/★/☆         (4点)
F: ◉/◎/〇 → ◉/◎/〇/▲/☆ → 〇/▲/△/★/☆    (14点)
G: ◉/◎/〇 → ◉/◎/〇/▲/△ → 〇/▲/△/★/☆    (14点)

三連複 (unordered) で計算。 各点 100円固定、 三連複 payouts で照合。
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


# 印優先度 (上位ほど小さい)
MARK_PRIORITY = {"◉": 0, "◎": 1, "○": 2, "〇": 2, "▲": 3, "△": 4, "★": 5, "☆": 6}


def _horse_mark(h: dict) -> str:
    return (h.get("mark") or "").strip()


def _horses_by_marks(horses: list, marks: set) -> list[dict]:
    """指定印の出走馬リスト (重複除去・印優先度昇順 + composite 降順)。"""
    cands = [h for h in horses if _horse_mark(h) in marks]
    cands.sort(key=lambda h: (MARK_PRIORITY.get(_horse_mark(h), 99), -(h.get("composite") or 0)))
    seen, out = set(), []
    for h in cands:
        no = h.get("horse_no")
        if no and no not in seen:
            seen.add(no)
            out.append(h)
    return out


def build_tickets(horses: list, m1: set, m2: set, m3: set) -> list[tuple[int, int, int]]:
    """三連複フォーメーション買い目: unordered set (馬番昇順 tuple) を返す。

    マスター指示 2026-05-02 (確定):
    - 三連複 (unordered) として計算
    - 1 軸 (m1) + 2 着候補 (m2) + 3 着候補 (m3) の組合せを全列挙
    - {a, b, c} を sorted tuple で重複除外
    """
    h1 = _horses_by_marks(horses, m1)
    h2 = _horses_by_marks(horses, m2)
    h3 = _horses_by_marks(horses, m3)
    seen, tickets = set(), []
    for ha in h1:
        a_no = ha.get("horse_no")
        for hb in h2:
            b_no = hb.get("horse_no")
            if b_no == a_no:
                continue
            for hc in h3:
                c_no = hc.get("horse_no")
                if c_no == a_no or c_no == b_no:
                    continue
                key = tuple(sorted([a_no, b_no, c_no]))  # unordered set
                if key in seen:
                    continue
                seen.add(key)
                tickets.append(key)
    return tickets


def lookup_sanrenpuku(payouts: dict, combo: tuple[int, int, int]) -> int:
    """三連複 payouts から指定 combo の払戻を取得 (combo は sorted tuple)。"""
    bucket = payouts.get("三連複") or payouts.get("sanrenpuku")
    if bucket is None:
        return 0
    # 三連複 combo は通常昇順、 入力 combo も sorted tuple
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

    from collections import Counter, defaultdict

    def _new_stat():
        return {"races_played": 0, "races_hit": 0,
                "tickets_total": 0, "tickets_hit": 0,
                "stake": 0, "payback": 0,
                "max_payout": 0, "max_date": "", "max_race": "",
                "tickets_dist": Counter()}

    # 階層: pattern → segment (jra/nar/all) → confidence (SS/S/A/B/C/D/all) → stat
    def _new_segments():
        return {seg: {conf: _new_stat() for conf in ["SS","S","A","B","C","D","all"]}
                for seg in ["jra","nar","all"]}
    stats = {k: _new_segments() for k in PATTERNS}

    JRA_VENUE_CODES = {"01","02","03","04","05","06","07","08","09","10"}
    def _seg_of(rid: str) -> str:
        if len(rid) >= 6 and rid[4:6] in JRA_VENUE_CODES:
            return "jra"
        return "nar"

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
            if not payouts.get("三連複") and not payouts.get("sanrenpuku"):
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
                    pb = lookup_sanrenpuku(payouts, combo)
                    if pb > 0:
                        payback += pb
                        hit_tickets += 1
                race_hit = payback > 0

                seg = _seg_of(rid)
                conf = (r.get("overall_confidence") or "").replace("⁺","+").strip()
                if conf not in ("SS","S","A","B","C","D"): conf = "D"
                # 集計対象: (jra|nar, conf), (jra|nar, all), (all, conf), (all, all)
                for sg in (seg, "all"):
                    for cf in (conf, "all"):
                        s = stats[pat_name][sg][cf]
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

    # セグメント別表示 (JRA / NAR / 全体) × 自信度別 × パターン別
    SEG_LABEL = {"jra": "JRA (中央)", "nar": "NAR (地方)", "all": "全体"}
    CONF_ORDER = ["SS", "S", "A", "B", "C", "D", "all"]

    def _print_segment(seg: str):
        print(f"\n{'='*30} {SEG_LABEL[seg]} {'='*30}")
        print(f"{'P':<3} {'自信':>4} {'1R点':>5} {'R購入':>7} {'R的中':>7} {'的中率':>7} {'購入':>12} {'払戻':>12} {'収支':>13} {'ROI':>7}")
        print("─" * 100)
        for k in ["A", "B", "C", "D", "E", "F", "G"]:
            for conf in CONF_ORDER:
                s = stats[k][seg][conf]
                rp = s["races_played"]
                if rp == 0:
                    continue
                rh = s["races_hit"]
                roi = s["payback"]/s["stake"]*100 if s["stake"] else 0
                rhr = rh/rp*100
                bal = s["payback"] - s["stake"]
                dist = s["tickets_dist"]
                mode_pts = dist.most_common(1)[0][0] if dist else 0
                conf_label = conf if conf != "all" else "計"
                print(f"{k:<3} {conf_label:>4} {mode_pts:>4}点 {rp:>7,} {rh:>7,} {rhr:>6.1f}% "
                      f"{s['stake']:>11,}円 {s['payback']:>11,}円 "
                      f"{bal:>+12,}円 {roi:>6.1f}%")
            print("─" * 100)

    _print_segment("jra")
    _print_segment("nar")
    _print_segment("all")

    elapsed = time.time() - started
    print(f"\nTotal elapsed: {elapsed:.1f}s")


if __name__ == "__main__":
    main()
