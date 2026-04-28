"""三連単フォーメーション ◉/◎⇔○/▲/☆⇒○/▲/△/★/☆/－ を 30点×100円で検証

仕様:
  1着候補: ◉/◎ (1頭)
  2着候補: ○/▲/(☆)
  3着候補: ○/▲/△/★/(☆)/(同断層内無印 1-2頭)
  ⇔ により 1-2着双方向
  各点 100円固定 → 30点 = 3,000円/レース
  期待値フィルタ・skip なし（純粋に全レース実行）
"""
from __future__ import annotations
import io
import sys
import json
import os
from pathlib import Path
from itertools import combinations
from collections import defaultdict
from datetime import date, timedelta

os.environ["PYTHONIOENCODING"] = "utf-8"
sys.path.insert(0, ".")

from src.calculator.betting import (
    ALLOWED_COL1_MARKS,
    calc_sanrenpuku_prob,
    estimate_sanrenpuku_odds,
    _PARTNER_MARK_PRIO,
)


_RANK2_MARKS = {"○", "〇", "▲"}        # 2着候補（☆は条件付き）
_RANK3_MARKS = {"○", "〇", "▲", "△", "★"}  # 3着候補（☆は条件付き、無印は同断層）
GAP_THRESHOLD = 2.5


def find_unmarked_same_gradient(horses, max_n=2):
    """印付き最下位 composite から同断層内の無印馬を返す（最大 max_n 頭）"""
    safe = [h for h in horses if not h.get("is_tokusen_kiken")]
    if not safe:
        return []
    sorted_h = sorted(safe, key=lambda h: -(h.get("composite") or 0))
    marked_set = ALLOWED_COL1_MARKS | _PARTNER_MARK_PRIO.keys()
    last_marked_idx = -1
    for i, h in enumerate(sorted_h):
        if h.get("mark", "") in marked_set:
            last_marked_idx = i
    if last_marked_idx < 0 or last_marked_idx + 1 >= len(sorted_h):
        return []
    found, prev = [], sorted_h[last_marked_idx]
    for h in sorted_h[last_marked_idx + 1:]:
        gap = (prev.get("composite") or 0) - (h.get("composite") or 0)
        if gap >= GAP_THRESHOLD:
            break
        if h.get("mark", "") not in marked_set:
            found.append(h)
            if len(found) >= max_n:
                break
        prev = h
    return found


def build_sanrentan_formation(horses):
    """三連単 30点フォーメーションを生成。

    Returns
    -------
    list[dict]
        [{"type":"三連単", "combo":[1着, 2着, 3着]}, ...]
    """
    # 本命（◎ or ◉） 軸
    honmei = next(
        (h for h in horses
         if h.get("mark", "") in ALLOWED_COL1_MARKS
         and not h.get("is_tokusen_kiken")),
        None,
    )
    if honmei is None:
        # フォールバック: composite 最上位
        cands = sorted(
            [h for h in horses if not h.get("is_tokusen_kiken")],
            key=lambda h: -(h.get("composite") or 0),
        )
        if not cands:
            return []
        honmei = cands[0]

    no_a = honmei.get("horse_no")
    has_oana = any(h.get("mark") == "☆" for h in horses)

    # 2着候補
    rank2_marks = set(_RANK2_MARKS)
    if has_oana:
        rank2_marks.add("☆")
    rank2_horses = [
        h for h in horses
        if h.get("horse_no") != no_a
        and h.get("mark", "") in rank2_marks
        and not h.get("is_tokusen_kiken")
    ]
    rank2_horses.sort(
        key=lambda h: (_PARTNER_MARK_PRIO.get(h.get("mark", ""), 9),
                       -(h.get("composite") or 0))
    )

    # 3着候補
    rank3_marks = set(_RANK3_MARKS)
    if has_oana:
        rank3_marks.add("☆")
    rank3_marked = [
        h for h in horses
        if h.get("horse_no") != no_a
        and h.get("mark", "") in rank3_marks
        and not h.get("is_tokusen_kiken")
    ]
    rank3_marked.sort(
        key=lambda h: (_PARTNER_MARK_PRIO.get(h.get("mark", ""), 9),
                       -(h.get("composite") or 0))
    )
    rank3_unmarked = find_unmarked_same_gradient(horses, max_n=2)
    rank3_horses = rank3_marked + rank3_unmarked

    if not rank2_horses or not rank3_horses:
        return []

    # 三連単フォーメーション ⇔ 双方向
    # パターンA: ◎(1着) → R2(2着) → R3(3着)
    # パターンB: R2(1着) → ◎(2着) → R3(3着)
    tickets = []
    seen = set()
    for h2 in rank2_horses:
        no_b = h2.get("horse_no")
        for h3 in rank3_horses:
            no_c = h3.get("horse_no")
            if no_c == no_a or no_c == no_b:
                continue
            # パターンA: ◎-R2-R3
            keyA = ("A", no_a, no_b, no_c)
            if keyA not in seen:
                seen.add(keyA)
                tickets.append({
                    "type": "三連単",
                    "combo": [no_a, no_b, no_c],
                    "stake": 100,
                })
    for h2 in rank2_horses:
        no_b = h2.get("horse_no")
        for h3 in rank3_horses:
            no_c = h3.get("horse_no")
            if no_c == no_a or no_c == no_b:
                continue
            # パターンB: R2-◎-R3
            keyB = ("B", no_b, no_a, no_c)
            if keyB not in seen:
                seen.add(keyB)
                tickets.append({
                    "type": "三連単",
                    "combo": [no_b, no_a, no_c],
                    "stake": 100,
                })
    return tickets


def get_payout(payouts, ticket):
    bucket = payouts.get(ticket["type"])
    if bucket is None:
        return 0
    nos = "-".join(str(x) for x in ticket["combo"])  # 三連単は順序保持
    if isinstance(bucket, dict):
        return int(bucket.get("payout", 0) or 0) if str(bucket.get("combo", "")) == nos else 0
    if isinstance(bucket, list):
        for it in bucket:
            if isinstance(it, dict) and str(it.get("combo", "")) == nos:
                return int(it.get("payout", 0) or 0)
    return 0


def process_day(date_str, skip_confs=None):
    skip_confs = skip_confs or set()
    pred_fp = Path(f"data/predictions/{date_str}_pred.json")
    res_fp = Path(f"data/results/{date_str}_results.json")
    if not pred_fp.exists() or not res_fp.exists():
        return None
    with pred_fp.open("r", encoding="utf-8") as f:
        pred = json.load(f)
    with res_fp.open("r", encoding="utf-8") as f:
        results = json.load(f)

    stats = {
        "races_played": 0, "races_skipped": 0, "races_hit": 0,
        "points": 0, "hit": 0, "stake": 0, "payback": 0,
        "tickets_per_race": [],
    }
    by_conf = defaultdict(lambda: {
        "races_played": 0, "races_hit": 0,
        "points": 0, "hit": 0, "stake": 0, "payback": 0,
    })

    n_races = 0
    for r in pred.get("races", []):
        n_races += 1
        race_id = str(r.get("race_id", ""))
        conf = r.get("confidence", "C")
        horses = [h for h in r.get("horses", []) if not h.get("is_scratched")]
        if not horses:
            continue
        rdata = results.get(race_id)
        if rdata is None:
            continue
        payouts = rdata.get("payouts", {})
        if not payouts:
            continue
        if "三連単" not in payouts:
            continue
        if conf in skip_confs:
            stats["races_skipped"] += 1
            continue

        tickets = build_sanrentan_formation(horses)
        if not tickets:
            stats["races_skipped"] += 1
            continue
        stats["races_played"] += 1
        by_conf[conf]["races_played"] += 1
        stats["tickets_per_race"].append(len(tickets))
        race_hit = False
        for t in tickets:
            stake = t["stake"]
            pp = get_payout(payouts, t)
            payback = pp * (stake // 100)
            hit = 1 if payback > 0 else 0
            stats["points"] += 1
            stats["hit"] += hit
            stats["stake"] += stake
            stats["payback"] += payback
            by_conf[conf]["points"] += 1
            by_conf[conf]["hit"] += hit
            by_conf[conf]["stake"] += stake
            by_conf[conf]["payback"] += payback
            if hit:
                race_hit = True
        if race_hit:
            stats["races_hit"] += 1
            by_conf[conf]["races_hit"] += 1
    return stats, dict(by_conf), n_races


def main():
    if len(sys.argv) < 3:
        print("usage: simulate_sanrentan_formation.py 20260301 20260331 [skip_confs]")
        print("  skip_confs example: 'C,D' or 'SS,C,D'")
        return 1
    s, e = sys.argv[1], sys.argv[2]
    skip_confs = set(sys.argv[3].split(",")) if len(sys.argv) > 3 else set()
    start = date(int(s[:4]), int(s[4:6]), int(s[6:8]))
    end = date(int(e[:4]), int(e[4:6]), int(e[6:8]))

    grand = {
        "races_played": 0, "races_skipped": 0, "races_hit": 0,
        "points": 0, "hit": 0, "stake": 0, "payback": 0,
        "tickets_per_race": [],
    }
    grand_conf = defaultdict(lambda: {
        "races_played": 0, "races_hit": 0,
        "points": 0, "hit": 0, "stake": 0, "payback": 0,
    })
    total_races = 0
    n_days = 0

    d = start
    while d <= end:
        ds = d.strftime("%Y%m%d")
        result = process_day(ds, skip_confs=skip_confs)
        if result:
            n_days += 1
            stats, by_conf, nr = result
            total_races += nr
            for k, v in stats.items():
                if k == "tickets_per_race":
                    grand[k].extend(v)
                else:
                    grand[k] += v
            for c, v in by_conf.items():
                for k, val in v.items():
                    grand_conf[c][k] += val
            print(f"[{ds}] {nr}R / 買 {stats['races_played']} / 当 {stats['races_hit']} / 投 {stats['stake']:,} / 払 {stats['payback']:,}")
        d += timedelta(days=1)

    print()
    print("=" * 95)
    print(f"期間: {s}～{e}  処理日 {n_days}日 / 総レース {total_races}R")
    print(f"戦略: 三連単フォーメーション ◉/◎⇔○/▲/☆⇒○/▲/△/★/☆/－（30点目安×100円）")
    print("=" * 95)

    rate = grand["races_hit"] / grand["races_played"] * 100 if grand["races_played"] else 0
    p_rate = grand["hit"] / grand["points"] * 100 if grand["points"] else 0
    roi = grand["payback"] / grand["stake"] * 100 if grand["stake"] else 0
    net = grand["payback"] - grand["stake"]
    avg_tp = sum(grand["tickets_per_race"]) / len(grand["tickets_per_race"]) if grand["tickets_per_race"] else 0
    median_tp = sorted(grand["tickets_per_race"])[len(grand["tickets_per_race"])//2] if grand["tickets_per_race"] else 0

    print()
    print("--- 全体サマリー ---")
    print(f"  購入レース数:         {grand['races_played']}R / 総 {total_races}R ({grand['races_played']/max(total_races,1)*100:.1f}%)")
    print(f"  Skip レース数:        {grand['races_skipped']}R")
    print(f"  的中レース数:         {grand['races_hit']}R")
    print(f"  レース的中率:         {rate:.1f}%")
    print(f"  券単位的中率:         {p_rate:.1f}% ({grand['hit']}/{grand['points']})")
    print(f"  1レース平均点数:      {avg_tp:.1f} 点 (median {median_tp})")
    print(f"  1レース平均投資:      {(grand['stake']/max(grand['races_played'],1)):,.0f}円")
    print(f"  投資合計:             {grand['stake']:,}円")
    print(f"  払戻合計:             {grand['payback']:,}円")
    print(f"  ROI:                  {roi:.1f}%")
    print(f"  純利益:               {net:+,}円")
    print(f"  1日平均利益:          {(net/max(n_days,1)):+,.0f}円")
    print(f"  マスター基準:         R的中率 {rate:.1f}% (≥25.0% {'✓' if rate>=25 else '✗'}) / ROI {roi:.1f}% (≥150.0% {'✓' if roi>=150 else '✗'})")

    print()
    print("--- 信頼度別 ---")
    print(f"{'conf':<6}{'買R':>6}{'当R':>5}{'R率':>7}{'点数':>7}{'当':>5}{'券率':>7}{'投資':>11}{'払戻':>11}{'ROI':>8}")
    for c in ("SS", "S", "A", "B", "C", "D"):
        v = grand_conf.get(c)
        if not v or v["races_played"] == 0:
            continue
        r_rate = v["races_hit"] / v["races_played"] * 100
        p_r = v["hit"] / v["points"] * 100 if v["points"] else 0
        roi_c = v["payback"] / v["stake"] * 100 if v["stake"] else 0
        print(f"{c:<6}{v['races_played']:>6}{v['races_hit']:>5}{r_rate:>6.1f}%{v['points']:>7}{v['hit']:>5}{p_r:>6.1f}%{v['stake']:>11,}{v['payback']:>11,}{roi_c:>7.1f}%")

    return 0


if __name__ == "__main__":
    sys.exit(main())
