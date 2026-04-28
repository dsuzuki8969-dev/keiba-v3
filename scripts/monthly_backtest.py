"""1ヶ月分の pred.json から Phase 3 三連単フォーメーション買い目を再生成し、
results.json と照合して ROI / 的中率を集計する（マスター指示 2026-04-21 第2弾）。

仕様:
  三連単フォーメーション ◉/◎⇔○/▲/(☆)⇒○/▲/△/★/(☆)/(同断層無印1-2頭)
  各点 100円固定 / 信頼度 SS/C/D は skip

--scenario フラグ対応 (2026-04-29):
  baseline         : 現状 rank2={○,〇,▲} / skip={SS,C,D}
  beta1            : ★ を 2着候補に追加 rank2={○,〇,▲,★} / skip={SS,C,D}
  beta1_b_skip     : β1 + B 帯も skip  rank2={○,〇,▲,★} / skip={SS,C,D,B}
  b_skip_only      : 現状 + B 帯 skip  rank2={○,〇,▲}   / skip={SS,C,D,B}
  conservative_b_skip: 同断層無印 max_n=0 + B-skip
  beta2            : 1着候補={◉,◎,▲} / rank2={○,〇,▲} / skip={SS,C,D}
  beta2_b_skip     : beta2 + B 帯 skip
  gamma            : 1⇔2⇔3着 三方向展開 (◉◎ も 3着固定パターン追加)
  gamma_b_skip     : gamma + B 帯 skip
"""
from __future__ import annotations
import io
import sys
import json
import os
import argparse
from pathlib import Path
from itertools import combinations
from collections import defaultdict
from datetime import date, timedelta

os.environ["PYTHONIOENCODING"] = "utf-8"
sys.path.insert(0, ".")

from src.calculator.betting import (
    ALLOWED_COL1_MARKS,
    estimate_sanrenpuku_odds,
    calc_expected_value,
    SANRENTAN_STAKE_PER_TICKET,
    SANRENTAN_MAX_TICKETS,
    SANRENTAN_SKIP_CONFIDENCES,
    SANRENTAN_RANK2_BASE,
    SANRENTAN_RANK3_BASE,
    SANRENTAN_GAP_THRESHOLD,
    SANRENTAN_MAX_UNMARKED_RANK3,
    _PARTNER_MARK_PRIO,
)

# ────────────────────────────────────────────────
# シナリオ定義 (2026-04-29 追加 / 2026-04-29 5シナリオ拡張)
# ────────────────────────────────────────────────
# 各シナリオの共通キー:
#   rank1_marks      : 1着固定候補印セット
#   rank2_marks      : 2着候補印セット
#   rank3_marks      : 3着候補印セット
#   skip_confidences : スキップする信頼度セット
#   unmarked_max_n   : 同断層無印を3着候補に追加する最大頭数 (0=追加しない)
#   expansion        : "AB"=1⇔2双方向のみ / "ABC"=1⇔2⇔3 三方向展開
SCENARIOS: dict[str, dict] = {
    "baseline": {
        "rank1_marks": set(ALLOWED_COL1_MARKS),             # {◉,◎}
        "rank2_marks": set(SANRENTAN_RANK2_BASE),           # {○,〇,▲}
        "rank3_marks": set(SANRENTAN_RANK3_BASE),           # {○,〇,▲,△,★}
        "skip_confidences": set(SANRENTAN_SKIP_CONFIDENCES),# {SS,C,D}
        "unmarked_max_n": SANRENTAN_MAX_UNMARKED_RANK3,     # 2
        "expansion": "AB",
    },
    "beta1": {
        "rank1_marks": set(ALLOWED_COL1_MARKS),
        "rank2_marks": set(SANRENTAN_RANK2_BASE) | {"★"},   # ★ 追加
        "rank3_marks": set(SANRENTAN_RANK3_BASE),
        "skip_confidences": set(SANRENTAN_SKIP_CONFIDENCES),
        "unmarked_max_n": SANRENTAN_MAX_UNMARKED_RANK3,
        "expansion": "AB",
    },
    "beta1_b_skip": {
        "rank1_marks": set(ALLOWED_COL1_MARKS),
        "rank2_marks": set(SANRENTAN_RANK2_BASE) | {"★"},
        "rank3_marks": set(SANRENTAN_RANK3_BASE),
        "skip_confidences": set(SANRENTAN_SKIP_CONFIDENCES) | {"B"},  # B 追加
        "unmarked_max_n": SANRENTAN_MAX_UNMARKED_RANK3,
        "expansion": "AB",
    },
    "b_skip_only": {
        "rank1_marks": set(ALLOWED_COL1_MARKS),
        "rank2_marks": set(SANRENTAN_RANK2_BASE),
        "rank3_marks": set(SANRENTAN_RANK3_BASE),
        "skip_confidences": set(SANRENTAN_SKIP_CONFIDENCES) | {"B"},
        "unmarked_max_n": SANRENTAN_MAX_UNMARKED_RANK3,
        "expansion": "AB",
    },
    # ── 2026-04-29 追加 5シナリオ ──────────────────────────────────
    "conservative_b_skip": {
        # 同断層無印を3着候補に追加しない (max_n=0) + B帯 skip
        "rank1_marks": set(ALLOWED_COL1_MARKS),
        "rank2_marks": set(SANRENTAN_RANK2_BASE),
        "rank3_marks": set(SANRENTAN_RANK3_BASE),
        "skip_confidences": set(SANRENTAN_SKIP_CONFIDENCES) | {"B"},
        "unmarked_max_n": 0,                                # 同断層無印を追加しない
        "expansion": "AB",
    },
    "beta2": {
        # 1着候補に ▲ を追加 (▲ も honmei 扱い)
        "rank1_marks": set(ALLOWED_COL1_MARKS) | {"▲"},     # {◉,◎,▲}
        "rank2_marks": set(SANRENTAN_RANK2_BASE),
        "rank3_marks": set(SANRENTAN_RANK3_BASE),
        "skip_confidences": set(SANRENTAN_SKIP_CONFIDENCES),
        "unmarked_max_n": SANRENTAN_MAX_UNMARKED_RANK3,
        "expansion": "AB",
    },
    "beta2_b_skip": {
        # beta2 + B帯 skip
        "rank1_marks": set(ALLOWED_COL1_MARKS) | {"▲"},
        "rank2_marks": set(SANRENTAN_RANK2_BASE),
        "rank3_marks": set(SANRENTAN_RANK3_BASE),
        "skip_confidences": set(SANRENTAN_SKIP_CONFIDENCES) | {"B"},
        "unmarked_max_n": SANRENTAN_MAX_UNMARKED_RANK3,
        "expansion": "AB",
    },
    "gamma": {
        # 1⇔2⇔3着 三方向展開 (◉◎ を 3着固定パターンも追加)
        "rank1_marks": set(ALLOWED_COL1_MARKS),
        "rank2_marks": set(SANRENTAN_RANK2_BASE),
        "rank3_marks": set(SANRENTAN_RANK3_BASE),
        "skip_confidences": set(SANRENTAN_SKIP_CONFIDENCES),
        "unmarked_max_n": SANRENTAN_MAX_UNMARKED_RANK3,
        "expansion": "ABC",                                  # C/D パターン追加
    },
    "gamma_b_skip": {
        # gamma + B帯 skip
        "rank1_marks": set(ALLOWED_COL1_MARKS),
        "rank2_marks": set(SANRENTAN_RANK2_BASE),
        "rank3_marks": set(SANRENTAN_RANK3_BASE),
        "skip_confidences": set(SANRENTAN_SKIP_CONFIDENCES) | {"B"},
        "unmarked_max_n": SANRENTAN_MAX_UNMARKED_RANK3,
        "expansion": "ABC",
    },
    # ── 2026-04-28 追加: S帯のみ運用 3シナリオ ─────────────────────────────
    "s_only": {
        # S帯のみ運用 (B+A も skip して S 帯レースのみ購入)
        "rank1_marks": ALLOWED_COL1_MARKS,
        "rank2_marks": set(SANRENTAN_RANK2_BASE),           # {○,〇,▲}
        "rank3_marks": set(SANRENTAN_RANK3_BASE),           # {○,〇,▲,△,★}
        "skip_confidences": set(SANRENTAN_SKIP_CONFIDENCES) | {"B", "A"},  # B+A 追加
        "unmarked_max_n": 2,
        "expansion": "AB",
    },
    "s_only_beta1": {
        # S帯のみ + ★を2着候補に追加
        "rank1_marks": ALLOWED_COL1_MARKS,
        "rank2_marks": set(SANRENTAN_RANK2_BASE) | {"★"},   # ★ 追加
        "rank3_marks": set(SANRENTAN_RANK3_BASE),
        "skip_confidences": set(SANRENTAN_SKIP_CONFIDENCES) | {"B", "A"},
        "unmarked_max_n": 2,
        "expansion": "AB",
    },
    "s_only_conservative": {
        # S帯のみ + 同断層無印を3着候補に追加しない (max_n=0)
        "rank1_marks": ALLOWED_COL1_MARKS,
        "rank2_marks": set(SANRENTAN_RANK2_BASE),
        "rank3_marks": set(SANRENTAN_RANK3_BASE),
        "skip_confidences": set(SANRENTAN_SKIP_CONFIDENCES) | {"B", "A"},
        "unmarked_max_n": 0,                                # 同断層無印を追加しない
        "expansion": "AB",
    },
    "s_only_beta1_conservative": {
        # S帯のみ + ★ 2着追加 + max_n=0 (黒字化最有力候補, 2026-04-29 Opus 自走追加)
        "rank1_marks": ALLOWED_COL1_MARKS,
        "rank2_marks": set(SANRENTAN_RANK2_BASE) | {"★"},   # ★ 追加
        "rank3_marks": set(SANRENTAN_RANK3_BASE),
        "skip_confidences": set(SANRENTAN_SKIP_CONFIDENCES) | {"B", "A"},
        "unmarked_max_n": 0,                                # 同断層無印を追加しない
        "expansion": "AB",
    },
}


def find_unmarked_same_gradient(horses, max_n=SANRENTAN_MAX_UNMARKED_RANK3,
                                  gap_threshold=SANRENTAN_GAP_THRESHOLD):
    """同断層の無印馬を最大 max_n 頭返す。max_n=0 の場合は即 [] を返す。"""
    if max_n <= 0:
        return []  # max_n=0 の場合は早期リターン (conservative_b_skip 等で使用)
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


def build_sanrentan_tickets(horses, n, is_jra, scenario: str = "baseline"):
    """三連単フォーメーション ⇔ 双方向で全候補を生成（dict ベース）

    Args:
        horses:   馬データリスト
        n:        出走頭数
        is_jra:   JRA フラグ
        scenario: SCENARIOS キー
                  "baseline"/"beta1"/"beta1_b_skip"/"b_skip_only"/
                  "conservative_b_skip"/"beta2"/"beta2_b_skip"/"gamma"/"gamma_b_skip"/
                  "s_only"/"s_only_beta1"/"s_only_conservative"

    シナリオパラメータ:
        rank1_marks    : 1着固定候補印セット (beta2系で▲追加)
        rank2_marks    : 2着候補印セット
        rank3_marks    : 3着候補印セット
        unmarked_max_n : 同断層無印の3着追加最大頭数 (0=追加なし)
        expansion      : "AB"=1⇔2双方向 / "ABC"=1⇔2⇔3 三方向展開
    """
    if scenario not in SCENARIOS:
        raise ValueError(
            f"unknown scenario: {scenario!r}, expected: {list(SCENARIOS.keys())}"
        )
    sc = SCENARIOS[scenario]

    # ── rank1_marks 対応 (beta2 系で ▲ も 1着候補) ──
    rank1_marks = sc.get("rank1_marks", set(ALLOWED_COL1_MARKS))

    # rank1 候補馬を収集し composite 降順 + 印優先度順でソート
    rank1_horses = [
        h for h in horses
        if h.get("mark", "") in rank1_marks
        and not h.get("is_tokusen_kiken")
    ]
    rank1_horses.sort(
        key=lambda h: (
            _PARTNER_MARK_PRIO.get(h.get("mark", ""), 9),
            -(h.get("composite") or 0),
        )
    )
    if not rank1_horses:
        # フォールバック: composite 最高位の非危険馬
        cands = sorted(
            [h for h in horses if not h.get("is_tokusen_kiken")],
            key=lambda h: -(h.get("composite") or 0),
        )
        if not cands:
            return []
        rank1_horses = [cands[0]]

    has_oana = any(h.get("mark") == "☆" for h in horses)

    rank2_marks = set(sc["rank2_marks"])
    if has_oana:
        rank2_marks.add("☆")

    rank3_marks = set(sc["rank3_marks"])
    if has_oana:
        rank3_marks.add("☆")

    # ── unmarked_max_n 対応 ──
    unmarked_max_n = sc.get("unmarked_max_n", SANRENTAN_MAX_UNMARKED_RANK3)
    expansion = sc.get("expansion", "AB")

    odds_map = {h.get("horse_no"): max(
        (h.get("odds") or h.get("predicted_tansho_odds") or 10.0), 1.1)
        for h in horses}
    all_odds = list(odds_map.values())
    wp_map = {h.get("horse_no"): (h.get("win_prob") or 0.0) for h in horses}
    p2_map = {h.get("horse_no"): (h.get("place2_prob") or 0.0) for h in horses}
    p3_map = {h.get("horse_no"): (h.get("place3_prob") or 0.0) for h in horses}

    tickets, seen = [], set()

    def _push(no_1, no_2, no_3, pat):
        if no_3 in (no_1, no_2) or no_1 == no_2:
            return
        key = (no_1, no_2, no_3)
        if key in seen:
            return
        seen.add(key)
        oa = odds_map.get(no_1, 10.0)
        ob = odds_map.get(no_2, 10.0)
        oc = odds_map.get(no_3, 10.0)
        sanren_odds = estimate_sanrenpuku_odds(oa, ob, oc, n, is_jra, _all_odds=all_odds)
        odds = sanren_odds * 6.5  # 三連単 ≈ 三連複 × 6.5
        prob = wp_map.get(no_1, 0.0) * p2_map.get(no_2, 0.0) * p3_map.get(no_3, 0.0) * 0.5
        ev = calc_expected_value(prob, odds)
        tickets.append({
            "type": "三連単",
            "combo": [no_1, no_2, no_3],
            "pattern": pat,
            "odds": round(odds, 1), "prob": prob, "ev": round(ev, 1),
            "stake": SANRENTAN_STAKE_PER_TICKET,
        })

    # ── 各 rank1 馬を軸に買い目生成 ──
    for honmei in rank1_horses:
        no_a = honmei.get("horse_no")

        rank2 = [h for h in horses
                 if h.get("horse_no") != no_a
                 and h.get("mark", "") in rank2_marks
                 and not h.get("is_tokusen_kiken")]
        rank2.sort(key=lambda h: (_PARTNER_MARK_PRIO.get(h.get("mark", ""), 9),
                                  -(h.get("composite") or 0)))

        rank3_marked = [h for h in horses
                        if h.get("horse_no") != no_a
                        and h.get("mark", "") in rank3_marks
                        and not h.get("is_tokusen_kiken")]
        rank3_marked.sort(key=lambda h: (_PARTNER_MARK_PRIO.get(h.get("mark", ""), 9),
                                          -(h.get("composite") or 0)))
        rank3_unmarked = find_unmarked_same_gradient(horses, max_n=unmarked_max_n)
        # rank3_unmarked から no_a と重複する馬を除外
        rank3_unmarked = [h for h in rank3_unmarked if h.get("horse_no") != no_a]
        rank3 = rank3_marked + rank3_unmarked

        if not rank2 or not rank3:
            continue

        # A パターン: ◉◎(or▲) → rank2 → rank3
        for hb in rank2:
            for hc in rank3:
                _push(no_a, hb.get("horse_no"), hc.get("horse_no"), "A")

        # B パターン: rank2 → ◉◎(or▲) → rank3
        for hb in rank2:
            for hc in rank3:
                _push(hb.get("horse_no"), no_a, hc.get("horse_no"), "B")

        # C/D パターン: expansion="ABC" のとき ◉◎ を 3着固定で追加 (γ 系)
        if expansion == "ABC":
            # C パターン: rank2 → rank3 → ◉◎(or▲)
            for hb in rank2:
                for hc in rank3:
                    _push(hb.get("horse_no"), hc.get("horse_no"), no_a, "C")
            # D パターン: rank3 → rank2 → ◉◎(or▲)
            for hb in rank2:
                for hc in rank3:
                    _push(hc.get("horse_no"), hb.get("horse_no"), no_a, "D")

    if len(tickets) > SANRENTAN_MAX_TICKETS:
        tickets.sort(key=lambda t: -(t.get("ev", 0) or 0))
        tickets = tickets[:SANRENTAN_MAX_TICKETS]
    return tickets


# 2026-04-28 追加: 日本語 ↔ 英字 キーエイリアス
# results.json のキーは取得経路 (netkeiba/競馬ブック/旧形式) で日本語 or 英字が混在
_TICKET_TYPE_ALIAS = {
    "単勝": "tansho", "複勝": "fukusho",
    "枠連": "wakuren", "馬連": "umaren", "ワイド": "wide",
    "馬単": "umatan", "三連複": "sanrenpuku", "三連単": "sanrentan",
}


def _get_payouts_bucket(payouts, ja_key):
    """日本語キー優先、なければ英字 alias で取得 (両方ない場合 None)"""
    if not isinstance(payouts, dict):
        return None
    if ja_key in payouts:
        return payouts[ja_key]
    en_key = _TICKET_TYPE_ALIAS.get(ja_key)
    if en_key and en_key in payouts:
        return payouts[en_key]
    return None


def get_payout(payouts, ticket):
    bucket = _get_payouts_bucket(payouts, ticket["type"])
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


def process_day(date_str, scenario: str = "baseline"):
    """1日分の pred.json と results.json を照合して統計を返す。

    Args:
        date_str: "YYYYMMDD" 形式
        scenario: SCENARIOS キー (default "baseline")
    """
    if scenario not in SCENARIOS:
        raise ValueError(
            f"unknown scenario: {scenario!r}, expected: {list(SCENARIOS.keys())}"
        )
    skip_confidences = SCENARIOS[scenario]["skip_confidences"]

    pred_fp = Path(f"data/predictions/{date_str}_pred.json")
    res_fp = Path(f"data/results/{date_str}_results.json")
    if not pred_fp.exists() or not res_fp.exists():
        return None
    try:
        with pred_fp.open("r", encoding="utf-8") as f:
            pred = json.load(f)
        with res_fp.open("r", encoding="utf-8") as f:
            results = json.load(f)
    except (json.JSONDecodeError, OSError) as ex:
        print(f"[{date_str}] SKIP - JSON parse error: {ex}", file=sys.stderr)
        return None

    stats = {"races_played": 0, "races_skipped": 0, "races_hit": 0,
             "points": 0, "hit": 0, "stake": 0, "payback": 0,
             "tickets_per_race": []}
    by_conf = defaultdict(lambda: {"races_played": 0, "races_hit": 0,
                                    "points": 0, "hit": 0, "stake": 0, "payback": 0})

    for r in pred.get("races", []):
        race_id = str(r.get("race_id", ""))
        conf = r.get("confidence", "C")
        n = r.get("field_count") or len(r.get("horses", []))
        is_jra = r.get("is_jra", True)
        horses = [h for h in r.get("horses", []) if not h.get("is_scratched")]
        if not horses:
            continue
        rdata = results.get(race_id)
        if rdata is None:
            continue
        payouts = rdata.get("payouts", {})
        if not payouts or "三連単" not in payouts:
            continue
        if conf in skip_confidences:
            stats["races_skipped"] += 1
            continue

        try:
            tickets = build_sanrentan_tickets(horses, n, is_jra, scenario=scenario)
        except Exception:
            continue
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

    return {"total": stats, "by_conf": dict(by_conf), "n_races": len(pred.get("races", []))}


def main():
    parser = argparse.ArgumentParser(
        description="三連単フォーメーション月次バックテスト"
    )
    parser.add_argument("start_date", help="開始日 YYYYMMDD")
    parser.add_argument("end_date", help="終了日 YYYYMMDD")
    parser.add_argument(
        "--scenario",
        choices=list(SCENARIOS.keys()),
        default="baseline",
        help=(
            "バックテストシナリオ (default: baseline) / 選択肢: "
            + ", ".join(SCENARIOS.keys())
        ),
    )
    args = parser.parse_args()
    s, e = args.start_date, args.end_date
    scenario = args.scenario
    start = date(int(s[:4]), int(s[4:6]), int(s[6:8]))
    end = date(int(e[:4]), int(e[4:6]), int(e[6:8]))

    grand_t = {"races_played": 0, "races_skipped": 0, "races_hit": 0,
               "points": 0, "hit": 0, "stake": 0, "payback": 0,
               "tickets_per_race": []}
    grand_c = defaultdict(lambda: {"races_played": 0, "races_hit": 0,
                                    "points": 0, "hit": 0, "stake": 0, "payback": 0})
    total_races = 0
    n_days = 0

    d = start
    while d <= end:
        ds = d.strftime("%Y%m%d")
        result = process_day(ds, scenario=scenario)
        if result:
            n_days += 1
            total_races += result["n_races"]
            for k, v in result["total"].items():
                if k == "tickets_per_race":
                    grand_t[k].extend(v)
                else:
                    grand_t[k] += v
            for c, v in result["by_conf"].items():
                for k, val in v.items():
                    grand_c[c][k] += val
            print(f"[{ds}] {result['n_races']}R / 買 {result['total']['races_played']} / skip {result['total']['races_skipped']} / 当 {result['total']['races_hit']}")
        d += timedelta(days=1)

    print()
    print("=" * 95)
    print(f"期間: {s}～{e}  処理日 {n_days}日 / 総レース {total_races}R")
    sc_info = SCENARIOS[scenario]
    print(
        f"戦略: scenario={scenario}"
        f", rank1={sorted(sc_info.get('rank1_marks', set(ALLOWED_COL1_MARKS)))}"
        f", rank2={sorted(sc_info['rank2_marks'])}"
        f", skip={sorted(sc_info['skip_confidences'])}"
        f", unmarked_max_n={sc_info.get('unmarked_max_n', SANRENTAN_MAX_UNMARKED_RANK3)}"
        f", expansion={sc_info.get('expansion', 'AB')}"
        f" / 100円固定"
    )
    print("=" * 95)

    t = grand_t
    rate = t["races_hit"] / t["races_played"] * 100 if t["races_played"] else 0
    p_rate = t["hit"] / t["points"] * 100 if t["points"] else 0
    roi = t["payback"] / t["stake"] * 100 if t["stake"] else 0
    net = t["payback"] - t["stake"]
    avg_tp = sum(t["tickets_per_race"]) / len(t["tickets_per_race"]) if t["tickets_per_race"] else 0

    print()
    print("--- 全体サマリー ---")
    print(f"  総レース数:         {total_races}R")
    print(f"  購入レース数:       {t['races_played']}R ({t['races_played']/max(total_races,1)*100:.1f}%)")
    print(f"  Skip レース数:      {t['races_skipped']}R")
    print(f"  的中レース数:       {t['races_hit']}R")
    print(f"  レース的中率:       {rate:.1f}%")
    print(f"  券単位的中率:       {p_rate:.1f}% ({t['hit']}/{t['points']})")
    print(f"  1レース平均点数:    {avg_tp:.1f} 点")
    print(f"  1レース平均投資:    {(t['stake']/max(t['races_played'],1)):,.0f}円")
    print(f"  投資合計:           {t['stake']:,}円")
    print(f"  払戻合計:           {t['payback']:,}円")
    print(f"  ROI:                {roi:.1f}%")
    print(f"  純利益:             {net:+,}円")
    print(f"  1日平均利益:        {(net/max(n_days,1)):+,.0f}円")
    print(f"  マスター基準:       R的中率 {rate:.1f}% (≥25.0% {'✓' if rate>=25 else '✗'}) / ROI {roi:.1f}% (≥150.0% {'✓' if roi>=150 else '✗'})")

    print()
    print("--- 信頼度別 ---")
    print(f"{'conf':<6}{'買R':>6}{'当R':>5}{'R率':>7}{'点数':>7}{'当':>5}{'券率':>7}{'投資':>11}{'払戻':>11}{'ROI':>8}")
    for c in ("SS", "S", "A", "B", "C", "D"):
        v = grand_c.get(c)
        if not v or v["races_played"] == 0:
            continue
        rr = v["races_hit"] / v["races_played"] * 100
        pr = v["hit"] / v["points"] * 100 if v["points"] else 0
        rc = v["payback"] / v["stake"] * 100 if v["stake"] else 0
        print(f"{c:<6}{v['races_played']:>6}{v['races_hit']:>5}{rr:>6.1f}%{v['points']:>7}{v['hit']:>5}{pr:>6.1f}%{v['stake']:>11,}{v['payback']:>11,}{rc:>7.1f}%")

    return 0


if __name__ == "__main__":
    sys.exit(main())
