"""
KPI評価スクリプト — ハイブリッド指数の実績をKPI目標と比較

使い方:
  python scripts/evaluate_kpi.py                          # 全期間・全体
  python scripts/evaluate_kpi.py --year 2026              # 2026年のみ
  python scripts/evaluate_kpi.py --year 2026 --scope jra  # JRAのみ
  python scripts/evaluate_kpi.py --year 2026 --scope nar  # NARのみ
  python scripts/evaluate_kpi.py --year 2026 --compare    # JRA/NAR横並び比較
  python scripts/evaluate_kpi.py --grade-dist             # グレード分布も表示
  python scripts/evaluate_kpi.py --after 2026-03-15       # 指定日以降のみ集計
"""
import argparse
import io
import json
import os
import sys

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True)
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from src.results_tracker import aggregate_detailed
from config.settings import PREDICTIONS_DIR

# ============================================================
# KPI目標定義（scope別）
# ============================================================

# 印別: 勝率 / 連対率 / 複勝率
MARK_KPI = {
    "all": {
        "◉": {"win": 65.0, "place2": 80.0, "placed": 88.0},
        "◎": {"win": 30.0, "place2": 48.0, "placed": 62.0},
        "○": {"win": 18.0, "place2": 35.0, "placed": 50.0},
        "▲": {"win": 13.0, "place2": 28.0, "placed": 42.0},
        "△": {"win":  9.0, "place2": 20.0, "placed": 34.0},
        "★": {"win":  5.0, "place2": 14.0, "placed": 25.0},
    },
    "jra": {
        "◉": {"win": 55.0, "place2": 72.0, "placed": 82.0},
        "◎": {"win": 25.0, "place2": 42.0, "placed": 56.0},
        "○": {"win": 15.0, "place2": 30.0, "placed": 44.0},
        "▲": {"win": 10.0, "place2": 22.0, "placed": 36.0},
        "△": {"win":  7.0, "place2": 16.0, "placed": 28.0},
        "★": {"win":  4.0, "place2": 10.0, "placed": 20.0},
    },
    "nar": {
        "◉": {"win": 75.0, "place2": 88.0, "placed": 94.0},
        "◎": {"win": 38.0, "place2": 56.0, "placed": 68.0},
        "○": {"win": 22.0, "place2": 42.0, "placed": 56.0},
        "▲": {"win": 16.0, "place2": 34.0, "placed": 48.0},
        "△": {"win": 12.0, "place2": 24.0, "placed": 40.0},
        "★": {"win":  7.0, "place2": 18.0, "placed": 30.0},
    },
}

# 自信度別: 的中率 / 回収率
CONF_KPI = {
    "all": {
        "SS": {"hit_rate": 65.0, "roi": 120.0},
        "S":  {"hit_rate": 55.0, "roi": 115.0},
        "A":  {"hit_rate": 40.0, "roi": 110.0},
        "B":  {"hit_rate": 30.0, "roi": 100.0},
        "C":  {"hit_rate": 22.0, "roi":  95.0},
        "D":  {"hit_rate": 15.0, "roi":  85.0},
        "E":  {"hit_rate":  8.0, "roi":  80.0},
    },
    "jra": {
        "SS": {"hit_rate": 55.0, "roi": 140.0},
        "S":  {"hit_rate": 45.0, "roi": 130.0},
        "A":  {"hit_rate": 32.0, "roi": 120.0},
        "B":  {"hit_rate": 22.0, "roi": 105.0},
        "C":  {"hit_rate": 15.0, "roi":  95.0},
        "D":  {"hit_rate": 10.0, "roi":  85.0},
        "E":  {"hit_rate":  5.0, "roi":  80.0},
    },
    "nar": {
        "SS": {"hit_rate": 78.0, "roi": 105.0},
        "S":  {"hit_rate": 68.0, "roi": 100.0},
        "A":  {"hit_rate": 50.0, "roi":  98.0},
        "B":  {"hit_rate": 38.0, "roi":  95.0},
        "C":  {"hit_rate": 28.0, "roi":  90.0},
        "D":  {"hit_rate": 18.0, "roi":  85.0},
        "E":  {"hit_rate": 10.0, "roi":  80.0},
    },
}

# グレード分布目標（scope共通）
GRADE_DIST_TARGET = {
    "SS": 2.5, "S": 7.5, "A": 20.0, "B": 40.0,
    "C": 20.0, "D": 7.5, "E": 2.5,
}

SCOPE_LABEL = {"all": "全体", "jra": "JRA", "nar": "NAR"}


def _gap_str(actual, target):
    """差分を文字列で返す"""
    diff = actual - target
    sign = "+" if diff >= 0 else ""
    return f"{sign}{diff:.1f}"


def _pct(n, d):
    """安全なパーセント計算"""
    return round(n / d * 100, 1) if d > 0 else 0.0


# ============================================================
# 表示関数
# ============================================================

def evaluate_overall(stats, scope="all"):
    """全体サマリー（単勝ベース）"""
    print()
    print("=" * 80)
    print(f"■ 全体サマリー [{SCOPE_LABEL[scope]}]")
    print("=" * 80)
    tr = stats.get("total_races", 0)

    hm_total = stats.get("honmei_total", 0)
    hm_wr = stats.get("honmei_win_rate", 0.0)
    hm_p2r = stats.get("honmei_place2_rate", 0.0)
    hm_pr = stats.get("honmei_rate", 0.0)
    # 単勝シミュレーション（◉◎各100円）
    hm_tansho_stake = stats.get("honmei_tansho_stake", hm_total * 100)
    hm_tansho_ret = stats.get("honmei_tansho_ret", 0)
    hm_tansho_roi = round(hm_tansho_ret / hm_tansho_stake * 100, 1) if hm_tansho_stake > 0 else 0.0
    hm_tansho_profit = hm_tansho_ret - hm_tansho_stake
    # 複勝シミュレーション（◉◎各100円）
    hm_fukusho_stake = stats.get("honmei_fukusho_stake", 0)
    hm_fukusho_ret = stats.get("honmei_fukusho_ret", 0)
    hm_fukusho_roi = round(hm_fukusho_ret / hm_fukusho_stake * 100, 1) if hm_fukusho_stake > 0 else 0.0

    print(f"  総レース数:       {tr:,}")
    print(f"  本命(◉◎)総数:    {hm_total:,}")
    print(f"  本命 勝率:        {hm_wr:.1f}%")
    print(f"  本命 連対率:      {hm_p2r:.1f}%")
    print(f"  本命 複勝率:      {hm_pr:.1f}%")
    print(f"  単勝回収率:       {hm_tansho_roi:.1f}%  (投資 ¥{hm_tansho_stake:,} → 回収 ¥{hm_tansho_ret:,})")
    print(f"  単勝収支:         ¥{hm_tansho_profit:,}")
    if hm_fukusho_stake > 0:
        print(f"  複勝回収率:       {hm_fukusho_roi:.1f}%  (投資 ¥{hm_fukusho_stake:,} → 回収 ¥{hm_fukusho_ret:,})")

    # 穴馬・危険馬サマリー
    ana = stats.get("by_ana", {})
    kiken = stats.get("by_kiken", {})
    if ana.get("total", 0) > 0:
        print(f"  穴馬(☆):  {ana['total']:,}頭  勝率{ana.get('win_rate', 0):.1f}%  単勝ROI {ana.get('tansho_roi', 0):.1f}%")
    if kiken.get("total", 0) > 0:
        print(f"  危険馬(×): {kiken['total']:,}頭  凡走率{kiken.get('fell_rate', 0):.1f}%")


def evaluate_marks(stats, scope="all"):
    """印別成績をKPI目標と比較"""
    by_mark = stats.get("by_mark", {})
    if not by_mark:
        print("  印別データなし")
        return

    mark_kpi = MARK_KPI.get(scope, MARK_KPI["all"])

    print("=" * 90)
    print(f"■ 印別成績（KPI比較）[{SCOPE_LABEL[scope]}]")
    print("=" * 90)
    header = f"{'印':>2}  {'件数':>5}  {'勝率':>6} {'目標':>6} {'Gap':>7}  {'連対率':>6} {'目標':>6} {'Gap':>7}  {'複勝率':>6} {'目標':>6} {'Gap':>7}"
    print(header)
    print("-" * 90)

    mark_order = ["◉", "◎", "○", "▲", "△", "★", "☆", "×"]
    for mk in mark_order:
        ms = by_mark.get(mk)
        if not ms:
            continue
        t = ms.get("total", 0)
        if t == 0:
            continue

        wr = _pct(ms.get("win", 0), t)
        p2r = _pct(ms.get("place2", 0), t)
        pr = _pct(ms.get("placed", 0), t)

        kpi = mark_kpi.get(mk)
        if kpi:
            wg = _gap_str(wr, kpi["win"])
            p2g = _gap_str(p2r, kpi["place2"])
            pg = _gap_str(pr, kpi["placed"])
            print(f"{mk:>2}  {t:>5}  {wr:>5.1f}% {kpi['win']:>5.1f}% {wg:>7}  {p2r:>5.1f}% {kpi['place2']:>5.1f}% {p2g:>7}  {pr:>5.1f}% {kpi['placed']:>5.1f}% {pg:>7}")
        else:
            # ☆×はKPI目標なし
            print(f"{mk:>2}  {t:>5}  {wr:>5.1f}%    —        —   {p2r:>5.1f}%    —        —   {pr:>5.1f}%    —        —")

    # 単勝回収率も表示
    print()
    print("  【単勝回収率シミュレーション（各印100円）】")
    for mk in mark_order:
        ms = by_mark.get(mk)
        if not ms:
            continue
        stake = ms.get("tansho_stake", 0)
        ret = ms.get("tansho_ret", 0)
        roi = _pct(ret, stake) if stake > 0 else 0.0
        print(f"  {mk}: {roi:>6.1f}%  (投資 ¥{stake:,} → 回収 ¥{ret:,})")


def evaluate_confidence(stats, scope="all"):
    """自信度別成績をKPI目標と比較"""
    by_conf = stats.get("by_conf", {})
    if not by_conf:
        print("  自信度別データなし")
        return

    conf_kpi = CONF_KPI.get(scope, CONF_KPI["all"])

    print()
    print("=" * 80)
    print(f"■ 自信度別成績（KPI比較）[{SCOPE_LABEL[scope]}]")
    print("=" * 80)
    header = f"{'自信度':>4}  {'レース':>6}  {'的中率':>6} {'目標':>6} {'Gap':>7}  {'回収率':>6} {'目標':>6} {'Gap':>7}"
    print(header)
    print("-" * 80)

    conf_order = ["SS", "S", "A", "B", "C", "D", "E"]
    for conf in conf_order:
        cs = by_conf.get(conf)
        if not cs:
            continue
        races = cs.get("total", 0)
        if races == 0:
            continue
        hits = cs.get("hits", 0)
        stake = cs.get("stake", 0)
        ret = cs.get("ret", 0)

        hit_rate = _pct(hits, races)
        roi = _pct(ret, stake) if stake > 0 else 0.0

        kpi = conf_kpi.get(conf)
        if kpi:
            hg = _gap_str(hit_rate, kpi["hit_rate"])
            rg = _gap_str(roi, kpi["roi"])
            print(f"  {conf:>2}  {races:>6}  {hit_rate:>5.1f}% {kpi['hit_rate']:>5.1f}% {hg:>7}  {roi:>5.1f}% {kpi['roi']:>5.1f}% {rg:>7}")


def _dev_to_grade(dev: float) -> str:
    """偏差値 → SS/S/A/B/C/D/E の7段階グレード"""
    if dev >= 65.0:
        return "SS"
    if dev >= 61.0:
        return "S"
    if dev >= 56.0:
        return "A"
    if dev >= 49.0:
        return "B"
    if dev >= 44.0:
        return "C"
    if dev >= 39.0:
        return "D"
    return "E"


def evaluate_grade_distribution(year_filter, scope="all"):
    """予想JSONからグレード分布を集計"""
    from src.results_tracker import list_prediction_dates, load_prediction

    dates = list_prediction_dates()
    if year_filter and year_filter != "all":
        dates = [d for d in dates if d.startswith(str(year_filter))]

    # カテゴリ別のグレードカウント
    cats = {
        "jockey": {"SS": 0, "S": 0, "A": 0, "B": 0, "C": 0, "D": 0, "E": 0},
        "trainer": {"SS": 0, "S": 0, "A": 0, "B": 0, "C": 0, "D": 0, "E": 0},
        "bloodline": {"SS": 0, "S": 0, "A": 0, "B": 0, "C": 0, "D": 0, "E": 0},
    }
    totals = {"jockey": 0, "trainer": 0, "bloodline": 0}

    for date in dates:
        pred = load_prediction(date)
        if not pred:
            continue
        for race in pred.get("races", []):
            # scopeフィルタ
            if scope == "jra" and not race.get("is_jra", True):
                continue
            if scope == "nar" and race.get("is_jra", True):
                continue

            for h in race.get("horses", []):
                # 騎手・調教師: グレード文字列を直接参照
                for cat, key in [("jockey", "jockey_grade"), ("trainer", "trainer_grade")]:
                    g = h.get(key)
                    if g and g in cats[cat]:
                        cats[cat][g] += 1
                        totals[cat] += 1
                # 血統: bloodline_dev（偏差値）からグレードに変換
                bl_dev = h.get("bloodline_dev")
                if bl_dev is not None and isinstance(bl_dev, (int, float)):
                    g = _dev_to_grade(bl_dev)
                    cats["bloodline"][g] += 1
                    totals["bloodline"] += 1

    if totals["jockey"] == 0:
        print("  グレードデータなし")
        return

    print()
    print("=" * 80)
    print(f"■ グレード分布（KPI比較）[{SCOPE_LABEL[scope]}]")
    print("=" * 80)
    header = f"{'グレード':>4}  {'騎手':>7}  {'調教師':>7}  {'血統':>7}  {'目標':>6}"
    print(header)
    print("-" * 80)

    for g in ("SS", "S", "A", "B", "C", "D", "E"):
        jp = _pct(cats["jockey"][g], totals["jockey"])
        tp = _pct(cats["trainer"][g], totals["trainer"])
        bp = _pct(cats["bloodline"][g], totals["bloodline"])
        tgt = GRADE_DIST_TARGET[g]
        print(f"    {g:>2}  {jp:>5.1f}%   {tp:>5.1f}%   {bp:>5.1f}%   {tgt:>5.1f}%")

    print()
    print(f"  サンプル数: 騎手={totals['jockey']:,}, 調教師={totals['trainer']:,}, 血統={totals['bloodline']:,}")


# ============================================================
# 横並び比較表示
# ============================================================

def evaluate_compare(detail, year):
    """JRA/NAR/全体を横並びで比較表示"""
    print()
    print("=" * 80)
    print(f"■ JRA / NAR 比較 ({year})")
    print("=" * 80)

    labels = ["ALL", "JRA", "NAR"]
    keys = ["all", "jra", "nar"]
    stats_list = [detail[k]["stats"] for k in keys]

    # 本命成績比較
    print()
    print(f"  {'':>16} {'ALL':>12} {'JRA':>12} {'NAR':>12}")
    print(f"  {'-'*52}")

    rows = [
        ("レース数",    lambda s: f"{s.get('total_races', 0):>10,}"),
        ("本命数",      lambda s: f"{s.get('honmei_total', 0):>10,}"),
        ("勝率",        lambda s: f"{s.get('honmei_win_rate', 0):>9.1f}%"),
        ("連対率",      lambda s: f"{s.get('honmei_place2_rate', 0):>9.1f}%"),
        ("複勝率",      lambda s: f"{s.get('honmei_rate', 0):>9.1f}%"),
        ("単勝ROI",     lambda s: f"{s.get('honmei_tansho_roi', 0):>9.1f}%"),
        ("単勝収支",    lambda s: f"¥{s.get('honmei_tansho_ret', 0) - s.get('honmei_tansho_stake', 0):>+9,}"),
        ("複勝ROI",     lambda s: f"{s.get('honmei_fukusho_roi', 0):>9.1f}%"),
    ]
    for label, fn in rows:
        vals = "".join(f" {fn(s):>12}" for s in stats_list)
        print(f"  {label:>16}{vals}")

    # 印別勝率比較
    print()
    print(f"  {'印別勝率':>16} {'ALL':>12} {'JRA':>12} {'NAR':>12}")
    print(f"  {'-'*52}")
    for mk in ["◉", "◎", "○", "▲", "△", "★", "☆", "×"]:
        vals = []
        for s in stats_list:
            ms = s.get("by_mark", {}).get(mk, {})
            t = ms.get("total", 0)
            wr = _pct(ms.get("win", 0), t) if t > 0 else 0.0
            vals.append(f"{wr:>9.1f}%({t:>4})")
        print(f"  {mk:>16} {''.join(f' {v:>12}' for v in vals)}")

    # 自信度別比較
    print()
    print(f"  {'自信度':>16} {'ALL 的中/ROI':>14} {'JRA 的中/ROI':>14} {'NAR 的中/ROI':>14}")
    print(f"  {'-'*60}")
    for conf in ["SS", "S", "A", "B", "C", "D", "E"]:
        vals = []
        for s in stats_list:
            cs = s.get("by_conf", {}).get(conf, {})
            t = cs.get("total", 0)
            hr = _pct(cs.get("hits", 0), t) if t > 0 else 0.0
            roi = _pct(cs.get("ret", 0), cs.get("stake", 0)) if cs.get("stake", 0) > 0 else 0.0
            vals.append(f"{hr:>5.1f}/{roi:>5.1f}%")
        print(f"  {conf:>16} {''.join(f' {v:>14}' for v in vals)}")


# ============================================================
# 自信度別 win_prob 分布統計
# ============================================================

def evaluate_winprob_distribution(year_filter, scope="all"):
    """自信度別の本命馬win_prob分布・的中/不的中比較・モデルレベル別的中率"""
    from src.results_tracker import list_prediction_dates, load_prediction
    import math

    dates = list_prediction_dates()
    if year_filter and year_filter != "all":
        dates = [d for d in dates if d.startswith(str(year_filter))]

    # 自信度別: 本命馬のwin_prob分布
    conf_wp = {}  # {conf: {"hit": [wp...], "miss": [wp...]}}
    # モデルレベル別: 的中率
    level_stats = {}  # {level: {"races": 0, "hits": 0}}

    results_dir = os.path.join(os.path.dirname(PREDICTIONS_DIR), "results")

    for date in dates:
        pred = load_prediction(date)
        if not pred:
            continue

        # 結果読み込み
        rfpath = os.path.join(results_dir, f"{date.replace('-', '')}_results.json")
        if not os.path.exists(rfpath):
            continue
        try:
            with open(rfpath, "r", encoding="utf-8") as f:
                result_data = json.load(f)
        except Exception:
            continue

        actual_map = {}
        if isinstance(result_data, dict) and "races" in result_data:
            for r in result_data["races"]:
                rid = r.get("race_id", "")
                if rid:
                    actual_map[rid] = {int(o["horse_no"]): o["finish"] for o in r.get("order", [])}
        elif isinstance(result_data, dict):
            for rid, rdata in result_data.items():
                if isinstance(rdata, dict) and "order" in rdata:
                    actual_map[rid] = {r["horse_no"]: r["finish"] for r in rdata["order"]}

        for race in pred.get("races", []):
            race_id = race.get("race_id", "")
            if not race_id:
                continue

            # scopeフィルタ
            is_jra = race.get("is_jra", True)
            if scope == "jra" and not is_jra:
                continue
            if scope == "nar" and is_jra:
                continue

            confidence = race.get("confidence", "B")
            finish_map = actual_map.get(race_id, {})
            if not finish_map:
                continue

            # 本命馬のwin_prob
            for h in race.get("horses", []):
                mk = h.get("mark", "")
                if mk not in ("◉", "◎"):
                    continue
                wp = h.get("win_prob")
                if wp is None:
                    continue
                hno = h.get("horse_no")
                pos = finish_map.get(hno, finish_map.get(str(hno), 99))

                if confidence not in conf_wp:
                    conf_wp[confidence] = {"hit": [], "miss": []}
                if pos == 1:
                    conf_wp[confidence]["hit"].append(wp)
                else:
                    conf_wp[confidence]["miss"].append(wp)

                # モデルレベル別
                ml = h.get("model_level")
                if ml is not None:
                    if ml not in level_stats:
                        level_stats[ml] = {"races": 0, "hits": 0}
                    level_stats[ml]["races"] += 1
                    if pos == 1:
                        level_stats[ml]["hits"] += 1

    if not conf_wp:
        return

    print()
    print("=" * 80)
    print(f"■ 自信度別 本命win_prob分布 [{SCOPE_LABEL[scope]}]")
    print("=" * 80)
    print(f"  {'自信度':>4}  {'的中時avg':>9}  {'不的中avg':>9}  {'差':>7}  {'的中σ':>7}  {'不的中σ':>7}  {'N':>5}")
    print("-" * 80)

    for conf in ["SS", "S", "A", "B", "C", "D", "E"]:
        if conf not in conf_wp:
            continue
        hits = conf_wp[conf]["hit"]
        misses = conf_wp[conf]["miss"]
        n_total = len(hits) + len(misses)
        if n_total == 0:
            continue

        def _stats(vals):
            if not vals:
                return 0.0, 0.0
            avg = sum(vals) / len(vals)
            std = (sum((v - avg) ** 2 for v in vals) / len(vals)) ** 0.5
            return avg, std

        h_avg, h_std = _stats(hits)
        m_avg, m_std = _stats(misses)
        gap = h_avg - m_avg

        print(f"    {conf:>2}  {h_avg:>8.4f}  {m_avg:>8.4f}  {gap:>+6.4f}  {h_std:>6.4f}  {m_std:>6.4f}  {n_total:>5}")

    # モデルレベル別的中率
    if level_stats:
        print()
        print("=" * 60)
        print(f"■ モデルレベル別 本命的中率 [{SCOPE_LABEL[scope]}]")
        print("=" * 60)
        level_names = {4: "Lv4(競馬場)", 3: "Lv3(SMILE)", 2: "Lv2(JRA/NAR)", 1: "Lv1(馬場)", 0: "Lv0(global)"}
        print(f"  {'レベル':>12}  {'的中率':>7}  {'的中':>5}  {'レース':>6}")
        print("-" * 60)
        for level in sorted(level_stats.keys(), reverse=True):
            ls = level_stats[level]
            rate = _pct(ls["hits"], ls["races"])
            lname = level_names.get(level, f"Lv{level}")
            print(f"  {lname:>12}  {rate:>6.1f}%  {ls['hits']:>5}  {ls['races']:>6}")


# ============================================================
# 競馬場別KPI
# ============================================================

def evaluate_by_venue(detail, scope="all"):
    """競馬場別の本命勝率・自信度別的中率・回収率を表示"""
    by_venue = detail[scope].get("by_venue", {})
    if not by_venue:
        print("  競馬場別データなし")
        return

    # JRA/NAR判別
    try:
        from data.masters.venue_master import JRA_VENUE_CODES
        jra_codes_set = JRA_VENUE_CODES
    except Exception:
        jra_codes_set = frozenset()

    jra_venues = {}
    nar_venues = {}
    for vname, vdata in by_venue.items():
        if vdata.get("total_races", 0) < 5:
            continue
        # venue_masterからコード取得してJRA/NAR判別
        try:
            from data.masters.venue_master import get_venue_code
            code = get_venue_code(vname)
            if code and code in jra_codes_set:
                jra_venues[vname] = vdata
            else:
                nar_venues[vname] = vdata
        except Exception:
            nar_venues[vname] = vdata

    # スコープに応じてフィルタ
    if scope == "jra":
        groups = [("JRA", jra_venues)]
    elif scope == "nar":
        groups = [("NAR", nar_venues)]
    else:
        groups = [("JRA", jra_venues), ("NAR", nar_venues)]

    for group_label, venues in groups:
        if not venues:
            continue

        print()
        print("=" * 100)
        print(f"■ 競馬場別成績 【{group_label}】")
        print("=" * 100)
        print(f"  {'場名':>6}  {'ﾚｰｽ':>5}  {'本命勝率':>7}  {'連対率':>6}  {'複勝率':>6}  {'単勝ROI':>7}  {'SS的中':>6}  {'S的中':>6}  {'A的中':>6}  {'B的中':>6}")
        print(f"  {'-' * 90}")

        # 本命勝率でソート
        rows = []
        for vname, vdata in sorted(venues.items()):
            tr = vdata.get("total_races", 0)
            if tr == 0:
                continue
            hm_total = vdata.get("honmei_total", 0)
            hm_wr = vdata.get("honmei_win_rate", 0)
            hm_p2r = vdata.get("honmei_place2_rate", 0)
            hm_pr = vdata.get("honmei_rate", 0)

            # 単勝ROI
            stake = vdata.get("honmei_tansho_stake", 0)
            ret = vdata.get("honmei_tansho_ret", 0)
            roi = _pct(ret, stake) if stake > 0 else 0.0

            # 自信度別的中率
            by_conf = vdata.get("by_conf", {})
            conf_rates = {}
            for c in ["SS", "S", "A", "B"]:
                cs = by_conf.get(c, {})
                ct = cs.get("total", 0)
                ch = cs.get("hits", 0)
                conf_rates[c] = _pct(ch, ct) if ct > 0 else None

            rows.append((vname, tr, hm_wr, hm_p2r, hm_pr, roi, conf_rates))

        rows.sort(key=lambda r: r[2], reverse=True)

        for vname, tr, wr, p2r, pr, roi, cr in rows:
            ss_str = f"{cr['SS']:5.1f}%" if cr['SS'] is not None else "    —"
            s_str = f"{cr['S']:5.1f}%" if cr['S'] is not None else "    —"
            a_str = f"{cr['A']:5.1f}%" if cr['A'] is not None else "    —"
            b_str = f"{cr['B']:5.1f}%" if cr['B'] is not None else "    —"
            print(f"  {vname:>6}  {tr:>5}  {wr:>6.1f}%  {p2r:>5.1f}%  {pr:>5.1f}%  {roi:>6.1f}%  {ss_str}  {s_str}  {a_str}  {b_str}")


# ============================================================
# メイン
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="KPI評価スクリプト")
    parser.add_argument("--year", type=str, default="all", help="対象年（例: 2026）")
    parser.add_argument("--scope", choices=["all", "jra", "nar"], default="all",
                        help="集計スコープ: JRA / NAR / 全体")
    parser.add_argument("--compare", action="store_true", help="JRA/NAR/全体を横並び比較表示")
    parser.add_argument("--grade-dist", action="store_true", help="グレード分布も表示")
    parser.add_argument("--by-venue", action="store_true", help="競馬場別KPIを表示")
    parser.add_argument("--after", type=str, default="", help="この日付以降のみ集計 (YYYY-MM-DD)")
    args = parser.parse_args()

    year = args.year
    scope = args.scope
    after_label = f", {args.after}以降" if args.after else ""
    print(f"\n  D-AI競馬 KPI評価  (対象: {year}{after_label}, スコープ: {SCOPE_LABEL[scope]})")
    print(f"  {'='*40}\n")

    # キャッシュクリア（新予想JSONを読み直すため）
    from src.results_tracker import invalidate_aggregate_cache
    invalidate_aggregate_cache()

    detail = aggregate_detailed(year_filter=year, after_filter=args.after)
    if not detail:
        print("  データなし。予想JSONと結果JSONが揃っているか確認してください。")
        return

    stats = detail[scope]["stats"]
    if stats.get("total_races", 0) == 0:
        print(f"  {SCOPE_LABEL[scope]}のデータなし。")
        return

    evaluate_overall(stats, scope)
    evaluate_marks(stats, scope)
    evaluate_confidence(stats, scope)

    if args.grade_dist:
        evaluate_grade_distribution(year, scope)

    if args.compare:
        evaluate_compare(detail, year)

    if args.by_venue:
        evaluate_by_venue(detail, scope)

    # 自信度別 win_prob 分布統計（常に表示）
    evaluate_winprob_distribution(year, scope)

    print()


if __name__ == "__main__":
    main()
