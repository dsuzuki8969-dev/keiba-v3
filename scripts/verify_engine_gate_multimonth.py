#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
engineゲートの荒れ回避効果 複数月実証（P2複数月確証の代替）

【背景】P0-γ / 精緻化A で判明: engineゲート(◎-○ composite差 ≧ 8.0)は
「実力拮抗レースを見送り、荒れレースを回避する」装置。
2026-01 では prob独自89R(engine見送り)が三連複2,650円・◎飛び25.8%と荒れていた。

【課題】prob印版(p0a_backup)は2026-01のみ・生成スクリプト削除済で、
複数月の prob 比較(+1.2pt 等)は素性問題で再現困難。

【代替】prob比較なしでも engine 単独で「ゲートの荒れ回避」の安定性は検証できる:
  danso発火レース(=軸-○差≧8.0 で実力差明確と判定して買い) vs
  見送りレース(=実力拮抗で見送り) の三連複配当中央値・穴決着率を
  2026-01/02/03 で比較する。
  見送りレースの方が高配当・高穴決着率なら、engineゲートは荒れを正しく回避している。
  これが複数月で一貫すれば、レース選択効果は構造的で安定していると言える。

【純度（keiba-reviewer P1[2] 反映）】force_buy(◉/穴 強制購入)は DANSO_AXIS_GATE を
  通過しない「ゲート外」の購入なので、荒れ回避の文脈では発火から分離して別表示する。
  純粋なゲート効果は「danso発火 vs 見送り」で測る。

本番非改変・読み取り専用。実行:
  PYTHONIOENCODING=utf-8 python scripts/verify_engine_gate_multimonth.py
"""
import os
import sys
import statistics
from typing import Dict, List, Optional

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, BASE_DIR)
sys.path.insert(0, os.path.dirname(__file__))

from compare_engine_prob_roi import (  # noqa: E402
    load_preds,
    load_results,
    _build_entries_from_pred_race,
    get_order_top3,
    get_trio_payouts,
    normalize_combo,
    ENGINE_PRED_DIR,
)
from analyze_race_selection_effect import diagnose_formation  # noqa: E402

MONTHS = ["202601", "202602", "202603"]


def _winner_pop(race: Dict, order: List[Dict]) -> Optional[int]:
    """勝ち馬の人気を pred の popularity から補完して返す。"""
    if not order:
        return None
    srt = sorted(order, key=lambda x: x.get("finish", 99))
    wn = srt[0].get("horse_no") if srt else None
    if wn is None:
        return None
    for h in race.get("horses", []):
        if int(h.get("horse_no") or -1) == int(wn):
            return int(h.get("popularity") or 0)
    return None


def _trio_payout(order: List[Dict], payouts: Dict) -> Optional[int]:
    """そのレースの三連複配当（勝ち combo の払戻）= 荒れ度指標。"""
    top3 = get_order_top3(order)
    if len(top3) < 3:
        return None
    p = get_trio_payouts(payouts).get(normalize_combo(top3))
    return int(p) if p else None


def analyze_month(month: str):
    """1ヶ月の engine pred を diagnose_formation で分類し配当・人気を集計。

    分類（diagnose_formation を単一の真実源として使用）:
      danso     : A/B/C = 軸-○差ゲート通過の純粋発火
      forcebuy  : ◉/穴 強制購入（ゲート外）
      skip      : 見送り全体（skip:*)
      skip_ab   : 見送りのうち軸ゲート/谷間（実力拮抗判定）のみ
    """
    engine_races = load_preds(ENGINE_PRED_DIR, month)
    results = load_results(month)
    danso = {"payout": [], "pop": []}
    forcebuy = {"payout": [], "pop": []}
    skip = {"payout": [], "pop": []}
    skip_ab = {"payout": [], "pop": []}
    for rid, race in engine_races.items():
        venue = race.get("venue", "")
        if venue in ("帯広", "帯広ばんえい", "65"):  # ばんえい除外
            continue
        res = results.get(rid)
        if not res:
            continue
        order = res.get("order", [])
        if not order:
            continue
        payout = _trio_payout(order, res.get("payouts", {}))
        pop = _winner_pop(race, order)
        diag = diagnose_formation(_build_entries_from_pred_race(race))

        if diag in ("A", "B", "C"):
            bucket = danso
        elif diag == "force_buy":
            bucket = forcebuy
        else:  # skip:*
            bucket = skip
            if "軸ゲート" in diag or "谷間" in diag:
                if payout is not None:
                    skip_ab["payout"].append(payout)
                if pop is not None:
                    skip_ab["pop"].append(pop)
        if payout is not None:
            bucket["payout"].append(payout)
        if pop is not None:
            bucket["pop"].append(pop)
    return danso, forcebuy, skip, skip_ab


def _med(v: List[float]) -> Optional[float]:
    return statistics.median(v) if v else None


def _ana_rate(pops: List[int]) -> Optional[float]:
    return (sum(1 for p in pops if p >= 6) / len(pops) * 100.0) if pops else None


def _fmt(x: Optional[float], spec: str = ",.0f") -> str:
    return format(x, spec) if x is not None else "   -"


def main() -> None:
    print("=== engineゲートの荒れ回避効果 複数月実証（force_buy分離・純度版）===\n")
    print(
        f"  {'月':<8}{'danso発火R':>11}{'配当':>9}{'穴%':>8} |"
        f"{'force_buyR':>11}{'配当':>9}{'穴%':>8} |"
        f"{'見送R':>7}{'配当':>9}{'穴%':>8}"
    )
    print(f"  {'-'*98}")
    summary = []
    for m in MONTHS:
        danso, forcebuy, skip, skip_ab = analyze_month(m)
        dc, dca = _med(danso["payout"]), _ana_rate(danso["pop"])
        sc, sca = _med(skip["payout"]), _ana_rate(skip["pop"])
        summary.append((m, dc, sc, dca, sca, skip_ab))
        print(
            f"  {m:<8}{len(danso['payout']):>11}{_fmt(dc):>9}{_fmt(dca,'.1f'):>8} |"
            f"{len(forcebuy['payout']):>11}{_fmt(_med(forcebuy['payout'])):>9}{_fmt(_ana_rate(forcebuy['pop']),'.1f'):>8} |"
            f"{len(skip['payout']):>7}{_fmt(sc):>9}{_fmt(sca,'.1f'):>8}"
        )

    print(f"\n  {'='*72}")
    print(f"  [判定] 各月で 見送配当 > danso発火配当（純粋ゲート効果）が成立するか")
    print(f"  {'='*72}")
    all_payout_ok = True
    for m, dc, sc, dca, sca, skip_ab in summary:
        payout_ok = (sc is not None and dc is not None and sc > dc)
        ana_ok = (sca is not None and dca is not None and sca > dca)
        all_payout_ok = all_payout_ok and payout_ok
        mark = "✓" if payout_ok else "△"
        dpay = (sc - dc) if (sc is not None and dc is not None) else None
        dana = (sca - dca) if (sca is not None and dca is not None) else None
        ab_c = _med(skip_ab["payout"])
        ab_a = _ana_rate(skip_ab["pop"])
        print(
            f"  {mark} {m}: 配当差(見送-danso)={_fmt(dpay,'+,.0f')}円 / 穴%差={_fmt(dana,'+.1f')}pt"
            f"  ｜実力拮抗見送のみ: 配当={_fmt(ab_c)}円 穴%={_fmt(ab_a,'.1f')}"
        )
    print(f"\n  [総合] 配当: {'✅ 全月で 見送 > danso発火 = 荒れ回避は方向一貫' if all_payout_ok else '⚠️ 一部月で不成立'}")
    print(f"  [留意] 効果『量』は月変動しうる（2026-01 が突出した可能性）。穴%は副次指標で月により強弱。")
    print(f"\n  ※ prob比較(+1.2pt/+6.0pt)は2026-01のみ（p0a_backup生成手順喪失・別途）")
    print(f"  ※ 本検証は engine単独でゲートの荒れ回避の『複数月の方向安定性』のみを確認")
    print(f"  ※ force_buy(◉/穴強制購入)は DANSO_AXIS_GATE 外のため danso発火から分離表示")


if __name__ == "__main__":
    main()
