#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
レース選択効果の解明: engine印が発火しprob印が見送る低ROIレース群の特性分析

【背景】P0-γ で判明: engine印の価値の大半は「どの馬を選ぶか(印の質=+1.2pt)」でなく
「どのレースを見送るか(発火ゲートの選択眼)」。共通531R(~88%) に対し
prob独自89R(prob買い・engine見送り)は ROI 40.5% と著しく低い。

【目的】engine独自 / prob独自 / 共通 の3集合について、発火を分ける軸-○差
(DANSO_AXIS_GATE=8.0 ゲート)と レース特性・決着特性を集計し、
engineゲートが何を選別しているか(=なぜ prob独自89Rは荒れるか)を定量化する。

【仮説】prob独自89R = prob composite では ◎-○≧8.0(発火)だが
engine composite では ◎-○<8.0(見送り) のレース。
= 「ML確率は1強と見たが engine実力評価では拮抗」→ 自信過剰で穴決着 → 低ROI。

本番非改変・読み取り専用。実行:
  PYTHONIOENCODING=utf-8 python scripts/analyze_race_selection_effect.py
"""
import os
import sys
import json
import statistics
from typing import Dict, List, Optional, Any

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, BASE_DIR)
sys.path.insert(0, os.path.dirname(__file__))  # scripts/ を import パスに（同ディレクトリ compare を import）

from compare_engine_prob_roi import (  # noqa: E402
    load_preds,
    load_results,
    calc_roi_unified_formation,
    get_order_top3,
    get_trio_payouts,
    normalize_combo,
    _build_entries_from_pred_race,
    ENGINE_PRED_DIR,
    PROB_PRED_DIR,
    TARGET_MONTH,
)

MARKS = ("◉", "◎", "○", "▲", "△", "★", "☆")  # ◉=鉄板本命（軸として◎と同格に扱う）


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 特性抽出
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _mark_map(pred_race: Dict) -> Dict[str, Dict]:
    """mark -> horse dict（同一markは最初の1頭のみ採用）"""
    mm: Dict[str, Dict] = {}
    for h in pred_race.get("horses", []):
        m = (h.get("mark") or "").strip()
        if m in MARKS and m not in mm:
            mm[m] = h
    return mm


def _axis_gap(pred_race: Optional[Dict]) -> Optional[float]:
    """◎composite - ○composite。◎か○が無ければ None。"""
    if not pred_race:
        return None
    mm = _mark_map(pred_race)
    a = mm.get("◉") or mm.get("◎")  # 軸 = ◉(鉄板)優先、なければ◎
    b = mm.get("○")
    if not a or not b:
        return None
    ca, cb = a.get("composite"), b.get("composite")
    if ca is None or cb is None:  # composite=0 を 50.0 に誤置換しない（keiba-reviewer P2[4]）
        return None
    return float(ca) - float(cb)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 発火種別 / 見送り理由の診断（compute_danso_columns ロジック再現）
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _has_force_buy(active: List[Dict]) -> bool:
    """force_buy 発火可否を本番関数 build_force_buy_columns で直接判定。

    自前再現では all_nos<3（3頭組めない）guard が抜けるため（keiba-reviewer P1[1]）、
    本番関数を直呼びして実装ズレ・誤判定を根絶する。
    """
    from src.calculator.betting import build_force_buy_columns
    return build_force_buy_columns(active) is not None


def diagnose_formation(entries: List[Dict]) -> str:
    """compute_danso_columns / build_force_buy_columns のどの分岐で
    発火/見送りになるかを診断して種別文字列を返す。

    返り値:
      'C'/'A'/'B'  = danso_gap 発火（団子/○抜け/○▲拮抗）
      'force_buy'  = danso見送りだが ◉/穴 で強制購入発火
      'skip:6頭未満'/'skip:本命不在'/'skip:○不在'/'skip:軸ゲート'/'skip:谷間'
                   = 完全見送り（force_buy も不成立）
    """
    from config.settings import (
        DANSO_AXIS_GATE,
        DANSO_C_SPAN,
        DANSO_A_MARU_SAN,
        DANSO_B_MARU_SAN,
        DANSO_B_SAN_SANKAKU,
    )
    active = [e for e in entries if not e.get("is_scratched", False)]

    def _fallback(reason: str) -> str:
        # danso見送り時、force_buy(◉/穴)で拾えるかを確認
        return "force_buy" if _has_force_buy(active) else f"skip:{reason}"

    if len(active) < 6:
        return _fallback("6頭未満")

    m2e: Dict[str, Dict] = {}
    for e in active:
        m = e.get("mark", "")
        if m in ("◉", "◎", "○", "▲", "△", "★", "☆") and m not in m2e:
            m2e[m] = e

    if "◉" in m2e:
        axis = "◉"
    elif "◎" in m2e:
        axis = "◎"
    else:
        return _fallback("本命不在")
    if "○" not in m2e:
        return _fallback("○不在")

    def comp(mk: str) -> Optional[float]:
        return float(m2e[mk]["composite"]) if mk in m2e else None

    g1 = comp(axis) - comp("○")
    if g1 < DANSO_AXIS_GATE:
        return _fallback("軸ゲート")

    g2 = (comp("○") - comp("▲")) if "▲" in m2e else None
    present = [mk for mk in ("○", "▲", "△", "★", "☆") if mk in m2e]
    span = (comp(present[0]) - comp(present[-1])) if len(present) >= 2 else None

    if span is not None and span < DANSO_C_SPAN:
        return "C"
    if g2 is not None and g2 >= DANSO_A_MARU_SAN:
        return "A"
    if g2 is not None and g2 < DANSO_B_MARU_SAN:
        cs = comp("△")
        if cs is not None and (comp("▲") - cs) >= DANSO_B_SAN_SANKAKU:
            return "B"
    return _fallback("谷間")


def extract_features(
    rid: str,
    engine_race: Optional[Dict],
    prob_race: Optional[Dict],
    result: Optional[Dict],
) -> Dict[str, Any]:
    """1レースの特性を engine/prob 両pred + result から抽出する。"""
    f: Dict[str, Any] = {"race_id": rid}
    meta = engine_race or prob_race or {}
    f["is_jra"] = bool(meta.get("is_jra", False))
    f["field_count"] = int(meta.get("field_count") or 0)
    f["distance"] = int(meta.get("distance") or 0)
    f["surface"] = meta.get("surface", "")
    f["venue"] = meta.get("venue", "")
    f["confidence"] = (meta.get("confidence") or "").strip()

    # 発火を分ける軸-○差（各pred の印体系で算出）
    f["engine_axis_gap"] = _axis_gap(engine_race)
    f["prob_axis_gap"] = _axis_gap(prob_race)

    # ◎の odds / 人気（engine pred 基準）
    mm_e = _mark_map(engine_race) if engine_race else {}
    maru_e = mm_e.get("◉") or mm_e.get("◎")  # 軸 = ◉優先、なければ◎
    f["maru_no"] = int(maru_e.get("horse_no") or 0) if maru_e else None
    f["maru_odds"] = float(maru_e.get("odds") or 0.0) if maru_e else None
    f["maru_pop"] = int(maru_e.get("popularity") or 0) if maru_e else None

    # 決着特性
    order = (result or {}).get("order", [])
    payouts = (result or {}).get("payouts", {})
    top3 = get_order_top3(order)

    # 勝ち馬の人気（pred の popularity で補完）
    winner_no = None
    if order:
        srt = sorted(order, key=lambda x: x.get("finish", 99))
        if srt:
            winner_no = srt[0].get("horse_no")
    f["winner_pop"] = None
    if winner_no is not None and engine_race:
        for h in engine_race.get("horses", []):
            if int(h.get("horse_no") or -1) == int(winner_no):
                f["winner_pop"] = int(h.get("popularity") or 0)
                break

    # ◎の実着順
    f["maru_finish"] = None
    if maru_e and order:
        mn = int(maru_e.get("horse_no") or -2)
        for e in order:
            if int(e.get("horse_no") or -1) == mn:
                f["maru_finish"] = e.get("finish")
                break

    # 三連複配当（勝ち combo の払戻＝そのレースの荒れ度指標。的中有無に依らず）
    f["trio_payout"] = None
    if len(top3) >= 3:
        win_combo = normalize_combo(top3)
        trio = get_trio_payouts(payouts)
        p = trio.get(win_combo)
        if p:
            f["trio_payout"] = int(p)

    return f


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 集計
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _vals(rows: List[Dict], key: str) -> List[float]:
    return [r[key] for r in rows if r.get(key) is not None]


def _med(rows: List[Dict], key: str) -> Optional[float]:
    v = _vals(rows, key)
    return statistics.median(v) if v else None


def _mean(rows: List[Dict], key: str) -> Optional[float]:
    v = _vals(rows, key)
    return statistics.mean(v) if v else None


def _rate(rows: List[Dict], key: str, pred) -> Optional[float]:
    """key の値に pred(v)->bool を適用した割合(%)。None値は分母から除外。"""
    v = _vals(rows, key)
    if not v:
        return None
    return sum(1 for x in v if pred(x)) / len(v) * 100.0


def _fmt(x: Optional[float], spec: str = ".1f") -> str:
    if x is None:
        return "   -"
    return format(x, spec)


def summarize(rows: List[Dict], label: str) -> Dict[str, Any]:
    n = len(rows)
    s = {
        "label": label,
        "n": n,
        "jra_rate": (sum(1 for r in rows if r.get("is_jra")) / n * 100.0) if n else None,
        "field_med": _med(rows, "field_count"),
        "maru_odds_med": _med(rows, "maru_odds"),
        "maru_pop_med": _med(rows, "maru_pop"),
        "e_gap_med": _med(rows, "engine_axis_gap"),
        "p_gap_med": _med(rows, "prob_axis_gap"),
        # 穴決着率(勝ち馬 人気>=6) / 人気決着率(<=3) / ◎飛び率(◎が4着以下 or 着外)
        "ana_rate": _rate(rows, "winner_pop", lambda p: p >= 6),
        "fav_rate": _rate(rows, "winner_pop", lambda p: 1 <= p <= 3),
        "maru_out_rate": _rate(rows, "maru_finish", lambda x: (x or 99) > 3),
        "trio_payout_med": _med(rows, "trio_payout"),
    }
    return s


def print_summary_table(summaries: List[Dict]) -> None:
    print(f"\n{'='*118}")
    print("=== 集合別 特性サマリ（中央値ベース） ===")
    print(f"{'='*118}")
    hdr = (
        f"  {'集合':<14}{'R数':>5}{'JRA%':>7}{'field':>7}{'◎odds':>8}{'◎人気':>7}"
        f"{'e_gap':>8}{'p_gap':>8}{'穴決着%':>9}{'人気決着%':>11}{'◎飛び%':>9}{'三連複中央':>11}"
    )
    print(hdr)
    print(f"  {'-'*114}")
    for s in summaries:
        print(
            f"  {s['label']:<14}{s['n']:>5}{_fmt(s['jra_rate']):>7}{_fmt(s['field_med']):>7}"
            f"{_fmt(s['maru_odds_med'],'.1f'):>8}{_fmt(s['maru_pop_med'],'.0f'):>7}"
            f"{_fmt(s['e_gap_med']):>8}{_fmt(s['p_gap_med']):>8}"
            f"{_fmt(s['ana_rate']):>9}{_fmt(s['fav_rate']):>11}{_fmt(s['maru_out_rate']):>9}"
            f"{_fmt(s['trio_payout_med'],',.0f'):>11}"
        )
    print(f"\n  ※ e_gap=engine印◎-○ composite差中央値 / p_gap=prob印◎-○差中央値（発火ゲート閾値=8.0）")
    print(f"  ※ 穴決着=勝ち馬人気≥6 / 人気決着=人気≤3 / ◎飛び=engine印◎が4着以下")


def dump_prob_only(rows: List[Dict], topn: int = 20) -> None:
    """prob独自レース（engine見送り）を軸-○差ギャップ順にダンプ。"""
    print(f"\n{'='*100}")
    print(f"=== prob独自レース 個票（engine見送り・engine_gap昇順 上位{topn}件） ===")
    print(f"{'='*100}")
    print(f"  ※ 仮説検証: prob_gap≥8.0(発火) かつ engine_gap<8.0(見送り) のレースで穴決着が多いか")
    print(f"  {'race_id':<16}{'venue':<8}{'e_gap':>7}{'p_gap':>7}{'◎人気':>6}{'◎着':>5}{'勝人気':>7}{'三連複':>9}")
    print(f"  {'-'*78}")
    # engine_axis_gap 昇順（engineが最も拮抗と見た＝最も自信なく見送ったレース）
    srt = sorted(rows, key=lambda r: (r.get("engine_axis_gap") if r.get("engine_axis_gap") is not None else 999))
    for r in srt[:topn]:
        print(
            f"  {r['race_id']:<16}{r.get('venue',''):<8}"
            f"{_fmt(r.get('engine_axis_gap')):>7}{_fmt(r.get('prob_axis_gap')):>7}"
            f"{_fmt(r.get('maru_pop'),'.0f'):>6}{_fmt(r.get('maru_finish'),'.0f'):>5}"
            f"{_fmt(r.get('winner_pop'),'.0f'):>7}{_fmt(r.get('trio_payout'),',.0f'):>9}"
        )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# main
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def main() -> None:
    print(f"=== レース選択効果の解明（{TARGET_MONTH}）===")

    print(f"\n[1] pred / results ロード")
    engine_races = load_preds(ENGINE_PRED_DIR, TARGET_MONTH)
    prob_all = load_preds(PROB_PRED_DIR, TARGET_MONTH)
    results = load_results(TARGET_MONTH)
    engine_ids = set(engine_races.keys())
    prob_races = {rid: p for rid, p in prob_all.items() if rid in engine_ids}
    print(f"  engine={len(engine_races)}R / prob(同一race)={len(prob_races)}R / results={len(results)}R")

    print(f"\n[2] 集合分類（compute_danso_columns 発火判定で engine/prob 各 fired を取得）")
    new_engine, _ = calc_roi_unified_formation(engine_races, results, label="engine")
    new_prob, _ = calc_roi_unified_formation(prob_races, results, label="prob")
    engine_fired = new_engine.get("fired_races", set())
    prob_fired = new_prob.get("fired_races", set())
    common = engine_fired & prob_fired
    engine_only = engine_fired - prob_fired
    prob_only = prob_fired - engine_fired
    print(f"  engine発火={len(engine_fired)}R / prob発火={len(prob_fired)}R")
    actual = (len(common), len(engine_only), len(prob_only))
    expected = (531, 37, 89)  # compare_engine_prob_roi の 2026-01 基準値
    match = "✓一致" if actual == expected else f"✗不一致(期待{expected})"
    print(f"  共通={len(common)}R / engine独自={len(engine_only)}R / prob独自={len(prob_only)}R")
    print(f"  [突合] (共通,engine独自,prob独自)={actual} vs 期待{expected} → {match}")

    print(f"\n[3] 特性抽出")
    def _feat_set(ids):
        return [
            extract_features(rid, engine_races.get(rid), prob_races.get(rid), results.get(rid))
            for rid in ids
        ]
    rows_common = _feat_set(common)
    rows_eo = _feat_set(engine_only)
    rows_po = _feat_set(prob_only)

    summaries = [
        summarize(rows_common, "共通(両発火)"),
        summarize(rows_eo, "engine独自"),
        summarize(rows_po, "prob独自"),
    ]
    print_summary_table(summaries)

    # 仮説の直接検証: prob独自で engine_gap<8.0 かつ prob_gap>=8.0 の比率
    print(f"\n{'='*70}")
    print("=== 仮説の直接検証（prob独自89R）===")
    print(f"{'='*70}")
    eg = _vals(rows_po, "engine_axis_gap")
    pg = _vals(rows_po, "prob_axis_gap")
    if eg:
        below = sum(1 for x in eg if x < 8.0)
        print(f"  engine_gap < 8.0 (engine拮抗と判定): {below}/{len(eg)} = {below/len(eg)*100:.1f}%")
        print(f"  engine_gap 中央値={statistics.median(eg):.2f} / 平均={statistics.mean(eg):.2f}")
    if pg:
        above = sum(1 for x in pg if x >= 8.0)
        print(f"  prob_gap   >= 8.0 (prob一強と判定): {above}/{len(pg)} = {above/len(pg)*100:.1f}%")
        print(f"  prob_gap   中央値={statistics.median(pg):.2f} / 平均={statistics.mean(pg):.2f}")
    print(f"\n  [解釈] engine_gap中央値 << prob_gap中央値 なら：")
    print(f"         probが「1強」と見たレースを engine は「実力拮抗」と見て見送り → 穴決着を回避")

    dump_prob_only(rows_po, topn=20)

    # 参考: engine独自レースも同様にダンプ（engineが拾ったが probが見送ったレース）
    print(f"\n{'='*100}")
    print(f"=== engine独自レース 個票（prob見送り・engine_gap降順 上位15件）===")
    print(f"{'='*100}")
    print(f"  {'race_id':<16}{'venue':<8}{'e_gap':>7}{'p_gap':>7}{'◎人気':>6}{'◎着':>5}{'勝人気':>7}{'三連複':>9}")
    print(f"  {'-'*78}")
    for r in sorted(rows_eo, key=lambda r: -(r.get("engine_axis_gap") or -999))[:15]:
        print(
            f"  {r['race_id']:<16}{r.get('venue',''):<8}"
            f"{_fmt(r.get('engine_axis_gap')):>7}{_fmt(r.get('prob_axis_gap')):>7}"
            f"{_fmt(r.get('maru_pop'),'.0f'):>6}{_fmt(r.get('maru_finish'),'.0f'):>5}"
            f"{_fmt(r.get('winner_pop'),'.0f'):>7}{_fmt(r.get('trio_payout'),',.0f'):>9}"
        )

    # ────────────────────────────────────────
    # 発火種別 / 見送り理由の分類（engineゲートの全貌）
    # ────────────────────────────────────────
    from collections import Counter
    print(f"\n\n{'='*70}")
    print("=== 発火種別 / 見送り理由の分類（engineゲートの全貌）===")
    print(f"{'='*70}")

    def _classify(ids, label_a, label_b):
        ca, cb = Counter(), Counter()
        for rid in ids:
            ra = engine_races.get(rid)
            rb = prob_races.get(rid)
            if ra:
                ca[diagnose_formation(_build_entries_from_pred_race(ra))] += 1
            if rb:
                cb[diagnose_formation(_build_entries_from_pred_race(rb))] += 1

        def _fmt_counter(c):
            return " / ".join(f"{k}={v}" for k, v in sorted(c.items(), key=lambda x: -x[1]))
        print(f"    [{label_a:<22}] {_fmt_counter(ca)}")
        print(f"    [{label_b:<22}] {_fmt_counter(cb)}")

    print(f"\n  ■ prob独自{len(prob_only)}R（probが買い・engineが見送り）")
    _classify(prob_only, "engine側=なぜ見送ったか", "prob側=なぜ発火したか")
    print(f"\n  ■ engine独自{len(engine_only)}R（engineが買い・probが見送り）")
    _classify(engine_only, "engine側=なぜ発火したか", "prob側=なぜ見送ったか")
    print(f"\n  ■ 共通{len(common)}R（両方発火）")
    _classify(common, "engine側=発火種別", "prob側=発火種別")
    print(f"\n  [解釈] prob独自89R の engine側が skip:軸ゲート / skip:谷間 に集中していれば、")
    print(f"         engineゲートは『実力拮抗（軸-○差不足）と判定して荒れレースを見送る』装置")

    print(f"\n=== 完了 ===")


if __name__ == "__main__":
    main()
