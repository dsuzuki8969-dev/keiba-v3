"""券種戦略シミュレーション: R的中率 25%/ROI 150% を狙う配券パターン探索

W0: 現状（馬連+三連複） — ベースライン
W1: 馬連+ワイド（◎→○▲★(☆) 各3-4点 / 三連複なし）
W2: ワイドのみ（◎→○▲★☆ 最大5点）
W3: 馬連+ワイド+三連複（各2-3点に絞り合計6-8点）
W4: 馬連3点 + 三連複3点（点数を厳しく絞る）
W5: 三連複1点 + 馬連2点 + ワイド2点（ベスト絞り込み）
W6: ◎-○ ワイド単独（最も的中率高い）
W7: 馬連3点 + ワイド2点（三連複なし、点数絞り）
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
    calc_hit_probability,
    calc_expected_value,
    calc_sanrenpuku_prob,
    estimate_umaren_odds,
    estimate_wide_odds,
    estimate_sanrenpuku_odds,
    FIXED_GAP_THRESHOLD,
    FIXED_MAX_UNMARKED_COL3,
    _PARTNER_MARK_PRIO,
    _allocate_fixed_budget,
    _race_expected_ratio,
)


_PARTNERS_BASE = {"○", "〇", "▲", "★"}
FIXED_BUDGET = 2000
FIXED_TARGET = 2.0


def find_unmarked_same_gradient(horses, max_n=2, gap_threshold=2.5):
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
        if gap >= gap_threshold:
            break
        if h.get("mark", "") not in marked_set:
            found.append(h)
            if len(found) >= max_n:
                break
        prev = h
    return found


def get_honmei_partners(horses, include_oana=True):
    """本命と相手リストを返す"""
    honmei = next(
        (h for h in horses
         if h.get("mark", "") in ALLOWED_COL1_MARKS
         and not h.get("is_tokusen_kiken")),
        None,
    )
    if honmei is None:
        cands = sorted(
            [h for h in horses if not h.get("is_tokusen_kiken")],
            key=lambda h: -(h.get("composite") or 0),
        )
        if not cands:
            return None, []
        honmei = cands[0]
    has_oana = include_oana and any(h.get("mark") == "☆" for h in horses)
    partner_marks = set(_PARTNERS_BASE)
    if has_oana:
        partner_marks.add("☆")
    partners = [
        h for h in horses
        if h.get("horse_no") != honmei.get("horse_no")
        and h.get("mark", "") in partner_marks
        and not h.get("is_tokusen_kiken")
    ]
    partners.sort(
        key=lambda h: (_PARTNER_MARK_PRIO.get(h.get("mark", ""), 9),
                       -(h.get("composite") or 0))
    )
    return honmei, partners


def gen_umaren(honmei, partners, n, is_jra, max_count, min_ev=100):
    if not honmei:
        return []
    no_a = honmei.get("horse_no")
    eff_a = honmei.get("odds") or honmei.get("predicted_tansho_odds") or 10.0
    p2_a = honmei.get("place2_prob") or 0.0
    out = []
    for hb in partners[:max_count + 2]:  # filter で削れるので余裕持たせる
        eff_b = hb.get("odds") or hb.get("predicted_tansho_odds") or 10.0
        p2_b = hb.get("place2_prob") or 0.0
        odds = estimate_umaren_odds(eff_a, eff_b, n, is_jra)
        prob = calc_hit_probability(p2_a, p2_b, "馬連", n)
        ev = calc_expected_value(prob, odds)
        if ev < min_ev:
            continue
        lo, hi = sorted([no_a, hb.get("horse_no")])
        out.append({
            "type": "馬連", "combo": [lo, hi],
            "odds": round(odds, 1), "prob": prob, "ev": round(ev, 1), "stake": 0,
        })
    out.sort(key=lambda x: -x["ev"])
    return out[:max_count]


def gen_wide(honmei, partners, n, is_jra, max_count, min_ev=100):
    if not honmei:
        return []
    no_a = honmei.get("horse_no")
    eff_a = honmei.get("odds") or honmei.get("predicted_tansho_odds") or 10.0
    p3_a = honmei.get("place3_prob") or 0.0
    out = []
    for hb in partners[:max_count + 2]:
        eff_b = hb.get("odds") or hb.get("predicted_tansho_odds") or 10.0
        p3_b = hb.get("place3_prob") or 0.0
        odds = estimate_wide_odds(eff_a, eff_b, n, is_jra)
        prob = calc_hit_probability(p3_a, p3_b, "ワイド", n)
        ev = calc_expected_value(prob, odds)
        if ev < min_ev:
            continue
        lo, hi = sorted([no_a, hb.get("horse_no")])
        out.append({
            "type": "ワイド", "combo": [lo, hi],
            "odds": round(odds, 1), "prob": prob, "ev": round(ev, 1), "stake": 0,
        })
    out.sort(key=lambda x: -x["ev"])
    return out[:max_count]


def gen_sanrenpuku(horses, honmei, partners, n, is_jra, max_count, min_ev=100):
    if not honmei or len(partners) < 2:
        return []
    no_a = honmei.get("horse_no")
    # col2 = ○/▲/(☆), col3 = ○/▲/☆/★ + 同断層無印
    col2_marks = {"○", "〇", "▲"}
    col3_marks = {"○", "〇", "▲", "★"}
    has_oana = any(h.get("mark") == "☆" for h in horses)
    if has_oana:
        col2_marks.add("☆"); col3_marks.add("☆")
    col2 = [h for h in partners if h.get("mark", "") in col2_marks]
    col3_marked = [h for h in partners if h.get("mark", "") in col3_marks]
    col3 = col3_marked + find_unmarked_same_gradient(horses)

    if not col2 or not col3:
        return []

    wp_map = {h.get("horse_no"): (h.get("win_prob") or 0.0) for h in horses}
    odds_map = {
        h.get("horse_no"): max(
            (h.get("odds") or h.get("predicted_tansho_odds") or 10.0), 1.1,
        ) for h in horses
    }
    all_odds = list(odds_map.values())
    s_norm = sum(
        calc_sanrenpuku_prob(wp_map[a], wp_map[b], wp_map[c], n)
        for a, b, c in combinations(list(wp_map.keys()), 3)
    )
    if s_norm <= 0:
        s_norm = 1.0

    out, seen = [], set()
    for hb in col2:
        for hc in col3:
            nos = {no_a, hb.get("horse_no"), hc.get("horse_no")}
            if len(nos) < 3:
                continue
            key = tuple(sorted(nos))
            if key in seen:
                continue
            seen.add(key)
            oa, ob, oc = odds_map[key[0]], odds_map[key[1]], odds_map[key[2]]
            odds = estimate_sanrenpuku_odds(oa, ob, oc, n, is_jra, _all_odds=all_odds)
            raw_prob = calc_sanrenpuku_prob(
                wp_map[key[0]], wp_map[key[1]], wp_map[key[2]], n,
            )
            prob = raw_prob / s_norm
            ev = calc_expected_value(prob, odds)
            if ev < min_ev:
                continue
            out.append({
                "type": "三連複", "combo": list(key),
                "odds": round(odds, 1), "prob": prob, "ev": round(ev, 1), "stake": 0,
            })
    out.sort(key=lambda x: -x["ev"])
    return out[:max_count]


# ============================================================
# 戦略パターン定義
# ============================================================

def strategy_W0(horses, n, is_jra):
    """現状: 馬連 (max 4) + 三連複 (max 全件)"""
    honmei, partners = get_honmei_partners(horses)
    if not honmei: return []
    return (gen_umaren(honmei, partners, n, is_jra, 4)
            + gen_sanrenpuku(horses, honmei, partners, n, is_jra, 999))


def strategy_W1(horses, n, is_jra):
    """馬連3点 + ワイド3点（三連複なし）"""
    honmei, partners = get_honmei_partners(horses)
    if not honmei: return []
    return (gen_umaren(honmei, partners, n, is_jra, 3)
            + gen_wide(honmei, partners, n, is_jra, 3))


def strategy_W2(horses, n, is_jra):
    """ワイド5点（◎→相手5頭）"""
    honmei, partners = get_honmei_partners(horses)
    if not honmei: return []
    return gen_wide(honmei, partners, n, is_jra, 5)


def strategy_W3(horses, n, is_jra):
    """馬連2点 + ワイド2点 + 三連複3点（合計7点絞り）"""
    honmei, partners = get_honmei_partners(horses)
    if not honmei: return []
    return (gen_umaren(honmei, partners, n, is_jra, 2)
            + gen_wide(honmei, partners, n, is_jra, 2)
            + gen_sanrenpuku(horses, honmei, partners, n, is_jra, 3))


def strategy_W4(horses, n, is_jra):
    """馬連3点 + 三連複3点（合計6点絞り、ワイドなし）"""
    honmei, partners = get_honmei_partners(horses)
    if not honmei: return []
    return (gen_umaren(honmei, partners, n, is_jra, 3)
            + gen_sanrenpuku(horses, honmei, partners, n, is_jra, 3))


def strategy_W5(horses, n, is_jra):
    """三連複1点 + 馬連2点 + ワイド2点"""
    honmei, partners = get_honmei_partners(horses)
    if not honmei: return []
    return (gen_sanrenpuku(horses, honmei, partners, n, is_jra, 1)
            + gen_umaren(honmei, partners, n, is_jra, 2)
            + gen_wide(honmei, partners, n, is_jra, 2))


def strategy_W6(horses, n, is_jra):
    """ワイド ◎-○ 1点のみ（究極絞り）"""
    honmei, partners = get_honmei_partners(horses)
    if not honmei: return []
    return gen_wide(honmei, partners, n, is_jra, 1)


def strategy_W7(horses, n, is_jra):
    """馬連2点 + ワイド3点（三連複なし）"""
    honmei, partners = get_honmei_partners(horses)
    if not honmei: return []
    return (gen_umaren(honmei, partners, n, is_jra, 2)
            + gen_wide(honmei, partners, n, is_jra, 3))


def strategy_W8(horses, n, is_jra):
    """ワイド3点（◎→○▲★ のみ）"""
    honmei, partners = get_honmei_partners(horses)
    if not honmei: return []
    # ☆を含めない方針なら partners をフィルタ
    p_no_oana = [p for p in partners if p.get("mark") != "☆"]
    return gen_wide(honmei, p_no_oana, n, is_jra, 3)


def strategy_W9(horses, n, is_jra):
    """馬連3点 + ワイド3点 + 三連複2点（バランス重視）"""
    honmei, partners = get_honmei_partners(horses)
    if not honmei: return []
    return (gen_umaren(honmei, partners, n, is_jra, 3)
            + gen_wide(honmei, partners, n, is_jra, 3)
            + gen_sanrenpuku(horses, honmei, partners, n, is_jra, 2))


STRATEGIES = {
    "W0_現状(馬連+三複)":      strategy_W0,
    "W1_馬連3+ワイド3":         strategy_W1,
    "W2_ワイド5":              strategy_W2,
    "W3_馬連2+ワイド2+三複3": strategy_W3,
    "W4_馬連3+三複3":          strategy_W4,
    "W5_三複1+馬連2+ワイド2": strategy_W5,
    "W6_ワイド1点絞り":        strategy_W6,
    "W7_馬連2+ワイド3":         strategy_W7,
    "W8_ワイド3(☆除外)":       strategy_W8,
    "W9_馬連3+ワイド3+三複2": strategy_W9,
}


def select_with_target(candidates, target=FIXED_TARGET, budget=FIXED_BUDGET):
    if not candidates:
        return [], 0.0, True
    sorted_c = sorted(candidates, key=lambda t: -(t.get("ev", 0) or 0))
    for n_t in range(len(sorted_c), 0, -1):
        trial = [dict(t) for t in sorted_c[:n_t]]
        _allocate_fixed_budget(trial, budget)
        active = [t for t in trial if (t.get("stake", 0) or 0) > 0]
        if not active:
            continue
        ratio = _race_expected_ratio(active)
        if ratio >= target:
            return active, round(ratio, 3), False
    top = sorted_c[0]
    if (top.get("ev", 0) or 0) >= target * 100:
        chosen = [dict(top)]
        chosen[0]["stake"] = min(budget, 1000)
        return chosen, round((top.get("ev", 0) or 0) / 100.0, 3), False
    return [], 0.0, True


def normalize_combo_str(t):
    nos = [str(x) for x in t["combo"]]
    if t["type"] in ("馬連", "ワイド", "三連複"):
        nos = sorted(nos, key=lambda x: int(x) if x.isdigit() else 99)
    return "-".join(nos)


def get_payout(payouts, ticket):
    bucket = payouts.get(ticket["type"])
    if bucket is None:
        return 0
    cs = normalize_combo_str(ticket)
    if isinstance(bucket, dict):
        return int(bucket.get("payout", 0) or 0) if str(bucket.get("combo", "")) == cs else 0
    if isinstance(bucket, list):
        for it in bucket:
            if isinstance(it, dict) and str(it.get("combo", "")) == cs:
                return int(it.get("payout", 0) or 0)
    return 0


def simulate_day(date_str, strategies):
    pred_fp = Path(f"data/predictions/{date_str}_pred.json")
    res_fp = Path(f"data/results/{date_str}_results.json")
    if not pred_fp.exists() or not res_fp.exists():
        return None
    with pred_fp.open("r", encoding="utf-8") as f:
        pred = json.load(f)
    with res_fp.open("r", encoding="utf-8") as f:
        results = json.load(f)

    stats = {sn: {"played": 0, "skipped": 0, "race_hit": 0,
                  "points": 0, "hit": 0, "stake": 0, "payback": 0}
             for sn in strategies}
    n_races = 0
    for r in pred.get("races", []):
        n_races += 1
        race_id = str(r.get("race_id", ""))
        n = r.get("field_count") or len(r.get("horses", []))
        is_jra = r.get("is_jra", True)
        horses = [h for h in r.get("horses", []) if not h.get("is_scratched")]
        if not horses:
            continue
        rdata = results.get(race_id)
        if rdata is None:
            continue
        payouts = rdata.get("payouts", {})
        if not payouts:
            continue

        for sn, fn in strategies.items():
            try:
                cands = fn(horses, n, is_jra)
            except Exception:
                continue
            chosen, ratio, skipped = select_with_target(cands)
            if skipped:
                stats[sn]["skipped"] += 1
                continue
            stats[sn]["played"] += 1
            race_hit = False
            for t in chosen:
                stake = t.get("stake", 0) or 0
                if stake <= 0:
                    continue
                pp = get_payout(payouts, t)
                payback = pp * (stake // 100)
                hit = 1 if payback > 0 else 0
                stats[sn]["points"] += 1
                stats[sn]["hit"] += hit
                stats[sn]["stake"] += stake
                stats[sn]["payback"] += payback
                if hit:
                    race_hit = True
            if race_hit:
                stats[sn]["race_hit"] += 1
    return stats, n_races


def main():
    if len(sys.argv) < 3:
        print("usage: simulate_ticket_strategies.py 20260301 20260331")
        return 1
    s, e = sys.argv[1], sys.argv[2]
    start = date(int(s[:4]), int(s[4:6]), int(s[6:8]))
    end = date(int(e[:4]), int(e[4:6]), int(e[6:8]))

    grand = {sn: {"played": 0, "skipped": 0, "race_hit": 0,
                  "points": 0, "hit": 0, "stake": 0, "payback": 0}
             for sn in STRATEGIES}
    total_races = 0
    n_days = 0
    d = start
    while d <= end:
        ds = d.strftime("%Y%m%d")
        result = simulate_day(ds, STRATEGIES)
        if result:
            n_days += 1
            stats, nr = result
            total_races += nr
            for sn in STRATEGIES:
                for k, v in stats[sn].items():
                    grand[sn][k] += v
        d += timedelta(days=1)

    print()
    print("=" * 115)
    print(f"期間: {s}～{e}  処理日 {n_days}日 / 総レース {total_races}R / 目標: R的中率 25.0% & ROI 150.0%")
    print("=" * 115)
    print()
    h = f"{'戦略':<24}{'買R':>5}{'当R':>5}{'R率':>7}{'点数':>6}{'当':>4}{'券率':>6}{'投資':>10}{'払戻':>11}{'ROI':>7}{'純利益':>11}{'判定':>7}"
    print(h)
    print("-" * len(h))
    rows = []
    for sn, v in grand.items():
        if v["played"] == 0:
            continue
        rate = v["race_hit"] / v["played"] * 100
        p_rate = v["hit"] / v["points"] * 100 if v["points"] else 0
        roi = v["payback"] / v["stake"] * 100 if v["stake"] else 0
        net = v["payback"] - v["stake"]
        ok = "✓" if (rate >= 25.0 and roi >= 150.0) else ("~" if rate >= 25.0 or roi >= 150.0 else "×")
        rows.append((sn, v["played"], v["race_hit"], rate, v["points"], v["hit"],
                     p_rate, v["stake"], v["payback"], roi, net, ok))

    for r in rows:
        print(f"{r[0]:<24}{r[1]:>5}{r[2]:>5}{r[3]:>6.1f}%{r[4]:>6}{r[5]:>4}{r[6]:>5.1f}%{r[7]:>10,}{r[8]:>11,}{r[9]:>6.1f}%{r[10]:>+11,}{r[11]:>7}"
              .encode("utf-8", errors="replace").decode("utf-8"))

    print()
    print("--- マスター基準達成（R的中率 ≥25% & ROI ≥150%） ---")
    qualified = [r for r in rows if r[3] >= 25.0 and r[9] >= 150.0]
    if qualified:
        for r in sorted(qualified, key=lambda x: -x[10])[:5]:
            print(f"  {r[0]:<24} R的中率 {r[3]:.1f}% / ROI {r[9]:.1f}% / 純利益 {r[10]:+,}")
    else:
        print("  なし")
        print()
        print("--- 25% に届く戦略（ROI 問わず） ---")
        for r in sorted(rows, key=lambda x: -x[3])[:5]:
            print(f"  {r[0]:<24} R的中率 {r[3]:.1f}% / ROI {r[9]:.1f}% / 純利益 {r[10]:+,}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
