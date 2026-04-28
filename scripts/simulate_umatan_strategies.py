"""馬連 + 馬単 中心の券種戦略シミュレーション（マスター指示 2026-04-21 第2弾）

三連複を廃止して馬連と馬単に絞った場合の R的中率/ROI を比較。

戦略:
  M0_baseline: 現状 W9（馬連3+ワイド3+三複2 = 比較用）
  M1: 馬連3 + 馬単3（◎1着固定 → 相手3頭）= 6点
  M2: 馬連4 + 馬単4（◎1着固定 → 相手4頭）= 8点
  M3: 馬連3 + 馬単双方向（◎-相手2頭 + 相手-◎2頭）= 7点
  M4: 馬連3 + 馬単3 + ワイド2（参考: ワイド少量混ぜる）
  M5: 馬連4 + 馬単4 + ワイド0 = 8点（馬連馬単のみ最大）
  M6: 馬単のみ 5点（◎1着固定 → 相手5頭）
  M7: 馬連2 + 馬単2 + ワイド2（バランス）
  M8: 馬連3 + 馬単4（馬単重視）
  M9: 馬連4 + 馬単2 + ワイド2（馬連重視）
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
    estimate_umatan_odds,
    estimate_wide_odds,
    estimate_sanrenpuku_odds,
    FIXED_GAP_THRESHOLD,
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
    pmarks = set(_PARTNERS_BASE)
    if has_oana:
        pmarks.add("☆")
    partners = [
        h for h in horses
        if h.get("horse_no") != honmei.get("horse_no")
        and h.get("mark", "") in pmarks
        and not h.get("is_tokusen_kiken")
    ]
    partners.sort(
        key=lambda h: (_PARTNER_MARK_PRIO.get(h.get("mark", ""), 9),
                       -(h.get("composite") or 0))
    )
    return honmei, partners


def gen_umaren(honmei, partners, n, is_jra, max_count, min_ev=100):
    if not honmei: return []
    no_a = honmei.get("horse_no")
    eff_a = honmei.get("odds") or honmei.get("predicted_tansho_odds") or 10.0
    p2_a = honmei.get("place2_prob") or 0.0
    out = []
    for hb in partners[:max_count + 2]:
        eff_b = hb.get("odds") or hb.get("predicted_tansho_odds") or 10.0
        p2_b = hb.get("place2_prob") or 0.0
        odds = estimate_umaren_odds(eff_a, eff_b, n, is_jra)
        prob = calc_hit_probability(p2_a, p2_b, "馬連", n)
        ev = calc_expected_value(prob, odds)
        if ev < min_ev: continue
        lo, hi = sorted([no_a, hb.get("horse_no")])
        out.append({"type": "馬連", "combo": [lo, hi],
                    "odds": round(odds,1), "prob": prob, "ev": round(ev,1), "stake": 0})
    out.sort(key=lambda x: -x["ev"])
    return out[:max_count]


def gen_umatan(honmei, partners, n, is_jra, max_count, bidirectional=False, min_ev=100):
    """馬単。bidirectional=False なら ◎1着固定→相手2着のみ。
    True なら ◎-相手 + 相手-◎ の双方向。
    """
    if not honmei: return []
    no_a = honmei.get("horse_no")
    eff_a = honmei.get("odds") or honmei.get("predicted_tansho_odds") or 10.0
    p2_a = honmei.get("place2_prob") or 0.0
    wp_a = honmei.get("win_prob") or 0.0  # ◎1着確率
    out = []
    for hb in partners[:max_count + 4]:
        eff_b = hb.get("odds") or hb.get("predicted_tansho_odds") or 10.0
        p2_b = hb.get("place2_prob") or 0.0
        wp_b = hb.get("win_prob") or 0.0
        odds_ab = estimate_umatan_odds(eff_a, eff_b, n, is_jra)

        # ◎-相手（◎1着, 相手2着）
        # 確率: P(◎1着) × P(相手 ∈ top2 | ◎1着でない場合) ≈ wp_a × (p2_b * n/(n-1))
        # 単純化: 馬単の calc_hit は馬連と同じ補正を返すが、向き考慮で半分
        prob_ab = wp_a * p2_b * (n / max(n - 1, 1)) * 0.5
        ev_ab = calc_expected_value(prob_ab, odds_ab)
        if ev_ab >= min_ev:
            out.append({"type": "馬単", "combo": [no_a, hb.get("horse_no")],
                        "odds": round(odds_ab, 1), "prob": prob_ab,
                        "ev": round(ev_ab, 1), "stake": 0})

        if bidirectional:
            # 相手-◎（相手1着, ◎2着）
            prob_ba = wp_b * p2_a * (n / max(n - 1, 1)) * 0.5
            ev_ba = calc_expected_value(prob_ba, odds_ab)
            if ev_ba >= min_ev:
                out.append({"type": "馬単", "combo": [hb.get("horse_no"), no_a],
                            "odds": round(odds_ab, 1), "prob": prob_ba,
                            "ev": round(ev_ba, 1), "stake": 0})

    out.sort(key=lambda x: -x["ev"])
    return out[:max_count]


def gen_wide(honmei, partners, n, is_jra, max_count, min_ev=100):
    if not honmei: return []
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
        if ev < min_ev: continue
        lo, hi = sorted([no_a, hb.get("horse_no")])
        out.append({"type": "ワイド", "combo": [lo, hi],
                    "odds": round(odds,1), "prob": prob, "ev": round(ev,1), "stake": 0})
    out.sort(key=lambda x: -x["ev"])
    return out[:max_count]


def gen_sanrenpuku(horses, honmei, partners, n, is_jra, max_count, min_ev=100):
    if not honmei or len(partners) < 2: return []
    no_a = honmei.get("horse_no")
    col2_marks = {"○", "〇", "▲"}
    col3_marks = {"○", "〇", "▲", "★"}
    has_oana = any(h.get("mark") == "☆" for h in horses)
    if has_oana:
        col2_marks.add("☆"); col3_marks.add("☆")
    col2 = [h for h in partners if h.get("mark","") in col2_marks]
    col3_marked = [h for h in partners if h.get("mark","") in col3_marks]
    col3 = col3_marked + find_unmarked_same_gradient(horses)
    if not col2 or not col3: return []
    wp_map = {h.get("horse_no"): (h.get("win_prob") or 0.0) for h in horses}
    odds_map = {h.get("horse_no"): max((h.get("odds") or h.get("predicted_tansho_odds") or 10.0), 1.1) for h in horses}
    all_odds = list(odds_map.values())
    s_norm = sum(calc_sanrenpuku_prob(wp_map[a], wp_map[b], wp_map[c], n)
                 for a, b, c in combinations(list(wp_map.keys()), 3))
    if s_norm <= 0: s_norm = 1.0
    out, seen = [], set()
    for hb in col2:
        for hc in col3:
            nos = {no_a, hb.get("horse_no"), hc.get("horse_no")}
            if len(nos) < 3: continue
            key = tuple(sorted(nos))
            if key in seen: continue
            seen.add(key)
            oa, ob, oc = odds_map[key[0]], odds_map[key[1]], odds_map[key[2]]
            odds = estimate_sanrenpuku_odds(oa, ob, oc, n, is_jra, _all_odds=all_odds)
            raw = calc_sanrenpuku_prob(wp_map[key[0]], wp_map[key[1]], wp_map[key[2]], n)
            prob = raw / s_norm
            ev = calc_expected_value(prob, odds)
            if ev < min_ev: continue
            out.append({"type": "三連複", "combo": list(key),
                        "odds": round(odds,1), "prob": prob, "ev": round(ev,1), "stake": 0})
    out.sort(key=lambda x: -x["ev"])
    return out[:max_count]


# ============================================================
# 戦略
# ============================================================

def s_M0(h, n, j):  # baseline W9
    hm, p = get_honmei_partners(h)
    if not hm: return []
    return (gen_umaren(hm, p, n, j, 3) + gen_wide(hm, p, n, j, 3) + gen_sanrenpuku(h, hm, p, n, j, 2))

def s_M1(h, n, j):  # 馬連3 + 馬単3
    hm, p = get_honmei_partners(h)
    if not hm: return []
    return gen_umaren(hm, p, n, j, 3) + gen_umatan(hm, p, n, j, 3, bidirectional=False)

def s_M2(h, n, j):  # 馬連4 + 馬単4
    hm, p = get_honmei_partners(h)
    if not hm: return []
    return gen_umaren(hm, p, n, j, 4) + gen_umatan(hm, p, n, j, 4, bidirectional=False)

def s_M3(h, n, j):  # 馬連3 + 馬単双方向4
    hm, p = get_honmei_partners(h)
    if not hm: return []
    return gen_umaren(hm, p, n, j, 3) + gen_umatan(hm, p, n, j, 4, bidirectional=True)

def s_M4(h, n, j):  # 馬連3 + 馬単3 + ワイド2
    hm, p = get_honmei_partners(h)
    if not hm: return []
    return (gen_umaren(hm, p, n, j, 3) + gen_umatan(hm, p, n, j, 3, bidirectional=False)
            + gen_wide(hm, p, n, j, 2))

def s_M5(h, n, j):  # 馬連4 + 馬単4
    hm, p = get_honmei_partners(h)
    if not hm: return []
    return gen_umaren(hm, p, n, j, 4) + gen_umatan(hm, p, n, j, 4, bidirectional=False)

def s_M6(h, n, j):  # 馬単のみ5
    hm, p = get_honmei_partners(h)
    if not hm: return []
    return gen_umatan(hm, p, n, j, 5, bidirectional=False)

def s_M7(h, n, j):  # 馬連2 + 馬単2 + ワイド2
    hm, p = get_honmei_partners(h)
    if not hm: return []
    return (gen_umaren(hm, p, n, j, 2) + gen_umatan(hm, p, n, j, 2, bidirectional=False)
            + gen_wide(hm, p, n, j, 2))

def s_M8(h, n, j):  # 馬連3 + 馬単4
    hm, p = get_honmei_partners(h)
    if not hm: return []
    return gen_umaren(hm, p, n, j, 3) + gen_umatan(hm, p, n, j, 4, bidirectional=False)

def s_M9(h, n, j):  # 馬連4 + 馬単2 + ワイド2
    hm, p = get_honmei_partners(h)
    if not hm: return []
    return (gen_umaren(hm, p, n, j, 4) + gen_umatan(hm, p, n, j, 2, bidirectional=False)
            + gen_wide(hm, p, n, j, 2))

def s_M10(h, n, j):  # 馬連2 + 馬単3
    hm, p = get_honmei_partners(h)
    if not hm: return []
    return gen_umaren(hm, p, n, j, 2) + gen_umatan(hm, p, n, j, 3, bidirectional=False)

def s_M11(h, n, j):  # 馬連3 + 馬単2
    hm, p = get_honmei_partners(h)
    if not hm: return []
    return gen_umaren(hm, p, n, j, 3) + gen_umatan(hm, p, n, j, 2, bidirectional=False)


STRATS = {
    "M0_W9_baseline":    s_M0,
    "M1_馬連3+馬単3":     s_M1,
    "M2_馬連4+馬単4":     s_M2,
    "M3_馬連3+馬単双方4": s_M3,
    "M4_馬連3+馬単3+W2": s_M4,
    "M6_馬単のみ5":       s_M6,
    "M7_馬連2+馬単2+W2": s_M7,
    "M8_馬連3+馬単4":     s_M8,
    "M9_馬連4+馬単2+W2": s_M9,
    "M10_馬連2+馬単3":    s_M10,
    "M11_馬連3+馬単2":    s_M11,
}


def select_with_target(candidates, target=FIXED_TARGET, budget=FIXED_BUDGET):
    if not candidates:
        return [], 0.0, True
    sorted_c = sorted(candidates, key=lambda t: -(t.get("ev", 0) or 0))
    for n_t in range(len(sorted_c), 0, -1):
        trial = [dict(t) for t in sorted_c[:n_t]]
        _allocate_fixed_budget(trial, budget)
        active = [t for t in trial if (t.get("stake", 0) or 0) > 0]
        if not active: continue
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
    # 馬単は順序保持
    return "-".join(nos)


def get_payout(payouts, ticket):
    bucket = payouts.get(ticket["type"])
    if bucket is None: return 0
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
    if not pred_fp.exists() or not res_fp.exists(): return None
    with pred_fp.open("r", encoding="utf-8") as f:
        pred = json.load(f)
    with res_fp.open("r", encoding="utf-8") as f:
        results = json.load(f)
    stats = {sn: {"played": 0, "skipped": 0, "race_hit": 0,
                  "points": 0, "hit": 0, "stake": 0, "payback": 0}
             for sn in strategies}
    nr = 0
    for r in pred.get("races", []):
        nr += 1
        race_id = str(r.get("race_id", ""))
        n = r.get("field_count") or len(r.get("horses", []))
        is_jra = r.get("is_jra", True)
        horses = [h for h in r.get("horses", []) if not h.get("is_scratched")]
        if not horses: continue
        rdata = results.get(race_id)
        if rdata is None: continue
        payouts = rdata.get("payouts", {})
        if not payouts: continue
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
                if stake <= 0: continue
                pp = get_payout(payouts, t)
                payback = pp * (stake // 100)
                hit = 1 if payback > 0 else 0
                stats[sn]["points"] += 1
                stats[sn]["hit"] += hit
                stats[sn]["stake"] += stake
                stats[sn]["payback"] += payback
                if hit: race_hit = True
            if race_hit:
                stats[sn]["race_hit"] += 1
    return stats, nr


def main():
    if len(sys.argv) < 3:
        print("usage: simulate_umatan_strategies.py 20260301 20260331")
        return 1
    s, e = sys.argv[1], sys.argv[2]
    start = date(int(s[:4]), int(s[4:6]), int(s[6:8]))
    end = date(int(e[:4]), int(e[4:6]), int(e[6:8]))

    grand = {sn: {"played": 0, "skipped": 0, "race_hit": 0,
                  "points": 0, "hit": 0, "stake": 0, "payback": 0}
             for sn in STRATS}
    total_races = 0
    n_days = 0
    d = start
    while d <= end:
        ds = d.strftime("%Y%m%d")
        result = simulate_day(ds, STRATS)
        if result:
            n_days += 1
            stats, nr = result
            total_races += nr
            for sn in STRATS:
                for k, v in stats[sn].items():
                    grand[sn][k] += v
        d += timedelta(days=1)

    print()
    print("=" * 115)
    print(f"期間: {s}～{e}  処理日 {n_days}日 / 総レース {total_races}R / 目標: R的中率 25.0% & ROI 150.0%")
    print("=" * 115)
    print()
    h = f"{'戦略':<22}{'買R':>5}{'当R':>5}{'R率':>7}{'点数':>6}{'当':>4}{'券率':>6}{'投資':>10}{'払戻':>11}{'ROI':>7}{'純利益':>11}{'判定':>7}"
    print(h)
    print("-" * len(h))
    rows = []
    for sn, v in grand.items():
        if v["played"] == 0: continue
        rate = v["race_hit"] / v["played"] * 100
        p_rate = v["hit"] / v["points"] * 100 if v["points"] else 0
        roi = v["payback"] / v["stake"] * 100 if v["stake"] else 0
        net = v["payback"] - v["stake"]
        ok = "✓" if (rate >= 25.0 and roi >= 150.0) else ("~" if rate >= 25.0 or roi >= 150.0 else "×")
        rows.append((sn, v["played"], v["race_hit"], rate, v["points"], v["hit"],
                     p_rate, v["stake"], v["payback"], roi, net, ok))
    for r in rows:
        print(f"{r[0]:<22}{r[1]:>5}{r[2]:>5}{r[3]:>6.1f}%{r[4]:>6}{r[5]:>4}{r[6]:>5.1f}%{r[7]:>10,}{r[8]:>11,}{r[9]:>6.1f}%{r[10]:>+11,}{r[11]:>7}")

    print()
    print("--- マスター基準達成（R的中率 ≥25% & ROI ≥150%） ---")
    qualified = [r for r in rows if r[3] >= 25.0 and r[9] >= 150.0]
    if qualified:
        for r in sorted(qualified, key=lambda x: -x[10])[:5]:
            print(f"  {r[0]:<22} R的中率 {r[3]:.1f}% / ROI {r[9]:.1f}% / 純利益 {r[10]:+,}")
    else:
        print("  なし。基準を満たす戦略が見つからない")
        print()
        print("--- 25% 達成 TOP3（ROI 不問） ---")
        for r in sorted(rows, key=lambda x: -x[3])[:3]:
            print(f"  {r[0]:<22} R的中率 {r[3]:.1f}% / ROI {r[9]:.1f}% / 純利益 {r[10]:+,}")
        print("--- ROI TOP3（的中率 不問） ---")
        for r in sorted(rows, key=lambda x: -x[9])[:3]:
            print(f"  {r[0]:<22} R的中率 {r[3]:.1f}% / ROI {r[9]:.1f}% / 純利益 {r[10]:+,}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
