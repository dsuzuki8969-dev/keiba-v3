"""
KPI達成度測定スクリプト

測定対象:
  1. 印別成績（◉◎○▲△★）
  2. 自信度別成績（SS/S/A/B/C/D/E）
  3. ◉の閾値シミュレーション（最適閾値探索）
  4. グレード分布

使い方:
  python scripts/measure_kpi.py
  python scripts/measure_kpi.py --after 2026-01-01
"""
import argparse
import io
import json
import os
import sys
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True)
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from rich.console import Console
from rich.progress import Progress, BarColumn, TimeRemainingColumn, TextColumn, MofNCompleteColumn
from rich.table import Table

console = Console()

from config.settings import PREDICTIONS_DIR

PROJECT_ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
RESULTS_DIR = os.path.join(PROJECT_ROOT, "data", "results")


# ============================================================
# データ読み込み
# ============================================================
def get_available_dates(after_filter: str = "") -> List[str]:
    dates = []
    for fn in os.listdir(PREDICTIONS_DIR):
        if not fn.endswith("_pred.json") or "_backup" in fn:
            continue
        date_str = fn.replace("_pred.json", "")
        if len(date_str) != 8:
            continue
        date_hyphen = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        if after_filter and date_hyphen < after_filter:
            continue
        result_path = os.path.join(RESULTS_DIR, f"{date_str}_results.json")
        if os.path.exists(result_path):
            dates.append(date_str)
    dates.sort()
    return dates


def _extract_payout(payouts: dict, bet_type: str, horse_no: int) -> int:
    data = payouts.get(bet_type)
    if not data:
        return 0
    hno_str = str(horse_no)

    def _match_entry(entry: dict) -> int:
        combo = entry.get("combo")
        if combo is not None and str(combo) == hno_str:
            return int(entry.get("payout", 0) or 0)
        hno = entry.get("horse_no") or entry.get("umaban")
        if hno is not None and int(hno) == horse_no:
            return int(entry.get("payout", 0) or entry.get("払戻", 0) or 0)
        return 0

    if isinstance(data, dict):
        return _match_entry(data)
    elif isinstance(data, list):
        for entry in data:
            if isinstance(entry, dict):
                v = _match_entry(entry)
                if v > 0:
                    return v
            elif isinstance(entry, (list, tuple)) and len(entry) >= 2:
                if int(entry[0]) == horse_no:
                    return int(entry[1])
    return 0


# ============================================================
# KPI集計
# ============================================================
class MarkKPI:
    """印別KPI"""
    def __init__(self):
        self.data = defaultdict(lambda: {
            "total": 0, "win": 0, "place2": 0, "place3": 0,
            "tansho_stake": 0, "tansho_ret": 0,
            "fukusho_stake": 0, "fukusho_ret": 0,
        })

    def add(self, mark: str, finish: int, tansho: int, fukusho: int):
        d = self.data[mark]
        d["total"] += 1
        if finish == 1:
            d["win"] += 1
        if finish <= 2:
            d["place2"] += 1
        if finish <= 3:
            d["place3"] += 1
        d["tansho_stake"] += 100
        if finish == 1:
            d["tansho_ret"] += tansho
        d["fukusho_stake"] += 100
        if finish <= 3:
            d["fukusho_ret"] += fukusho


class ConfidenceKPI:
    """自信度別KPI"""
    def __init__(self):
        self.data = defaultdict(lambda: {
            "total": 0, "honmei_win": 0,
            "tansho_stake": 0, "tansho_ret": 0,
        })

    def add(self, confidence: str, honmei_finish: int, tansho: int):
        d = self.data[confidence]
        d["total"] += 1
        if honmei_finish == 1:
            d["honmei_win"] += 1
        d["tansho_stake"] += 100
        if honmei_finish == 1:
            d["tansho_ret"] += tansho


class GradeKPI:
    """グレード分布"""
    def __init__(self):
        self.counts = defaultdict(int)
        self.total = 0

    def add(self, grade: str):
        self.counts[grade] += 1
        self.total += 1


class TekipanSimulator:
    """◉閾値シミュレーション"""
    def __init__(self):
        self.candidates = []  # (gap, wp, p3p, finish, tansho, fukusho)

    def add(self, gap: float, wp: float, p3p: float, finish: int, tansho: int, fukusho: int):
        self.candidates.append((gap, wp, p3p, finish, tansho, fukusho))

    def simulate(self, gap_th: float, wp_th: float, p3p_th: float) -> dict:
        total = 0
        win = 0
        place2 = 0
        place3 = 0
        tansho_stake = 0
        tansho_ret = 0
        fukusho_stake = 0
        fukusho_ret = 0
        for gap, wp, p3p, finish, tansho, fukusho in self.candidates:
            if gap >= gap_th and wp >= wp_th and p3p >= p3p_th:
                total += 1
                if finish == 1:
                    win += 1
                if finish <= 2:
                    place2 += 1
                if finish <= 3:
                    place3 += 1
                tansho_stake += 100
                if finish == 1:
                    tansho_ret += tansho
                fukusho_stake += 100
                if finish <= 3:
                    fukusho_ret += fukusho

        def _pct(n, d):
            return round(n / d * 100, 1) if d > 0 else 0.0

        return {
            "total": total,
            "win_rate": _pct(win, total),
            "place2_rate": _pct(place2, total),
            "place3_rate": _pct(place3, total),
            "tansho_roi": _pct(tansho_ret, tansho_stake),
            "fukusho_roi": _pct(fukusho_ret, fukusho_stake),
            "appearance_rate": _pct(total, len(self.candidates)),
        }


# ============================================================
# メイン
# ============================================================
def main():
    parser = argparse.ArgumentParser(description="KPI達成度測定")
    parser.add_argument("--after", default="", help="この日付以降のデータのみ使用")
    args = parser.parse_args()

    dates = get_available_dates(after_filter=args.after)
    console.print(f"[bold cyan]KPI達成度測定[/bold cyan]  対象日数: {len(dates)}")

    mark_kpi = MarkKPI()
    conf_kpi = ConfidenceKPI()
    grade_kpi = GradeKPI()
    tekipan_sim = TekipanSimulator()
    total_races = 0

    with Progress(
        TextColumn("[bold blue]{task.description}"),
        BarColumn(), MofNCompleteColumn(), TimeRemainingColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("データ処理中", total=len(dates))
        for date_str in dates:
            pred_path = os.path.join(PREDICTIONS_DIR, f"{date_str}_pred.json")
            result_path = os.path.join(RESULTS_DIR, f"{date_str}_results.json")
            try:
                with open(pred_path, "r", encoding="utf-8") as f:
                    pred = json.load(f)
                with open(result_path, "r", encoding="utf-8") as f:
                    results = json.load(f)
            except Exception:
                progress.advance(task)
                continue

            for race in pred.get("races", []):
                race_id = race.get("race_id", "")
                result = results.get(race_id)
                if not result or not result.get("order"):
                    continue

                finish_map = {
                    r["horse_no"]: r["finish"]
                    for r in result["order"]
                    if isinstance(r, dict) and "horse_no" in r and "finish" in r
                }
                if not finish_map:
                    continue

                horses = race.get("horses", [])
                if len(horses) < 3:
                    continue

                total_races += 1
                payouts = result.get("payouts", {})
                confidence = race.get("confidence", "B")

                # composite順ソート
                sorted_h = sorted(horses, key=lambda h: -(h.get("composite", 0) or 0))
                honmei = sorted_h[0] if sorted_h else None

                # 自信度別KPI（◎の着順で判定）
                if honmei:
                    hno = honmei["horse_no"]
                    finish = finish_map.get(hno, 99)
                    tansho = _extract_payout(payouts, "単勝", hno) or _extract_payout(payouts, "tansho", hno)
                    conf_kpi.add(confidence, finish, tansho)

                # 印別KPI
                for h in horses:
                    mark = h.get("mark", "")
                    if not mark:
                        continue
                    hno = h["horse_no"]
                    finish = finish_map.get(hno, 99)
                    tansho = _extract_payout(payouts, "単勝", hno) or _extract_payout(payouts, "tansho", hno)
                    fukusho = _extract_payout(payouts, "複勝", hno) or _extract_payout(payouts, "fukusho", hno)
                    mark_kpi.add(mark, finish, tansho, fukusho)

                    # グレード分布（主要グレード全項目）
                    for grade_key in ["jockey_grade", "trainer_grade", "sire_grade",
                                      "mgs_grade", "owner_grade", "last3f_grade"]:
                        g = h.get(grade_key)
                        if g and g != "—":
                            grade_kpi.add(g)
                    # detail_grades内のサブグレード
                    for detail_key in ["course_detail_grades", "jockey_detail_grades",
                                       "trainer_detail_grades", "bloodline_detail_grades"]:
                        detail = h.get(detail_key)
                        if isinstance(detail, dict):
                            for sg in detail.values():
                                if sg and sg != "—":
                                    grade_kpi.add(sg)

                # ◉シミュレーション用データ（composite 1位の情報）
                if honmei and len(sorted_h) >= 2:
                    gap = (honmei.get("composite", 0) or 0) - (sorted_h[1].get("composite", 0) or 0)
                    wp = honmei.get("win_prob", 0) or 0
                    p3p = honmei.get("place3_prob", 0) or 0
                    hno = honmei["horse_no"]
                    finish = finish_map.get(hno, 99)
                    tansho = _extract_payout(payouts, "単勝", hno) or _extract_payout(payouts, "tansho", hno)
                    fukusho = _extract_payout(payouts, "複勝", hno) or _extract_payout(payouts, "fukusho", hno)
                    tekipan_sim.add(gap, wp, p3p, finish, tansho, fukusho)

            progress.advance(task)

    # ============================================================
    # 結果出力
    # ============================================================
    def _pct(n, d):
        return round(n / d * 100, 1) if d > 0 else 0.0

    console.print(f"\n[bold]全{total_races}レース[/bold]")

    # ---- 印別成績 ----
    console.print("\n[bold cyan]■ 印別成績[/bold cyan]")
    # 目標値
    targets = {
        "◉": (65.0, 75.0, 90.0),
        "◎": (35.0, 45.0, 60.0),
        "○": (20.0, 30.0, 45.0),
        "▲": (15.0, 25.0, 40.0),
        "△": (10.0, 20.0, 35.0),
        "★": (5.0, 15.0, 25.0),
    }
    mark_order = ["◉", "◎", "○", "▲", "△", "★", "☆", "×"]
    t = Table(title="印別成績 vs 目標")
    t.add_column("印", style="bold")
    t.add_column("件数", justify="right")
    t.add_column("勝率", justify="right")
    t.add_column("目標", justify="right")
    t.add_column("差", justify="right")
    t.add_column("連対率", justify="right")
    t.add_column("複勝率", justify="right")
    t.add_column("単回収", justify="right")
    t.add_column("複回収", justify="right")

    for mark in mark_order:
        d = mark_kpi.data.get(mark)
        if not d or d["total"] == 0:
            continue
        wr = _pct(d["win"], d["total"])
        p2r = _pct(d["place2"], d["total"])
        p3r = _pct(d["place3"], d["total"])
        tr = _pct(d["tansho_ret"], d["tansho_stake"])
        fr = _pct(d["fukusho_ret"], d["fukusho_stake"])
        tgt = targets.get(mark)
        tgt_str = f"{tgt[0]:.0f}%" if tgt else "-"
        diff = f"{wr - tgt[0]:+.1f}" if tgt else "-"
        diff_style = "green" if tgt and wr >= tgt[0] else "red"
        t.add_row(
            mark, str(d["total"]),
            f"{wr:.1f}%", tgt_str,
            f"[{diff_style}]{diff}[/{diff_style}]",
            f"{p2r:.1f}%", f"{p3r:.1f}%",
            f"{tr:.1f}%", f"{fr:.1f}%",
        )
    console.print(t)

    # ---- 自信度別成績 ----
    console.print("\n[bold cyan]■ 自信度別成績（◎的中率 = ◎が1着の率）[/bold cyan]")
    conf_targets = {
        "SS": (60.0, 150.0), "S": (50.0, 120.0), "A": (40.0, 100.0),
        "B": (25.0, 90.0), "C": (25.0, 80.0), "D": (20.0, 70.0), "E": (10.0, 60.0),
    }
    conf_order = ["SS", "S", "A", "B", "C", "D", "E"]
    t2 = Table(title="自信度別成績 vs 目標")
    t2.add_column("自信度", style="bold")
    t2.add_column("レース数", justify="right")
    t2.add_column("◎的中率", justify="right")
    t2.add_column("目標", justify="right")
    t2.add_column("差", justify="right")
    t2.add_column("単回収率", justify="right")
    t2.add_column("回収目標", justify="right")
    t2.add_column("差", justify="right")

    for conf in conf_order:
        d = conf_kpi.data.get(conf)
        if not d or d["total"] == 0:
            continue
        hit = _pct(d["honmei_win"], d["total"])
        roi = _pct(d["tansho_ret"], d["tansho_stake"])
        tgt = conf_targets.get(conf, (0, 0))
        diff_h = hit - tgt[0]
        diff_r = roi - tgt[1]
        h_style = "green" if diff_h >= 0 else "red"
        r_style = "green" if diff_r >= 0 else "red"
        t2.add_row(
            conf, str(d["total"]),
            f"{hit:.1f}%", f"{tgt[0]:.0f}%",
            f"[{h_style}]{diff_h:+.1f}[/{h_style}]",
            f"{roi:.1f}%", f"{tgt[1]:.0f}%",
            f"[{r_style}]{diff_r:+.1f}[/{r_style}]",
        )
    console.print(t2)

    # ---- グレード分布 ----
    console.print("\n[bold cyan]■ グレード分布[/bold cyan]")
    grade_targets = {"SS": 2.5, "S": 7.5, "A": 20.0, "B": 40.0, "C": 20.0, "D": 7.5, "E": 2.5}
    grade_order = ["SS", "S", "A", "B", "C", "D", "E"]
    t3 = Table(title="グレード分布 vs 目標（正規分布）")
    t3.add_column("グレード", style="bold")
    t3.add_column("件数", justify="right")
    t3.add_column("実績%", justify="right")
    t3.add_column("目標%", justify="right")
    t3.add_column("差", justify="right")

    for g in grade_order:
        cnt = grade_kpi.counts.get(g, 0)
        pct = _pct(cnt, grade_kpi.total)
        tgt = grade_targets.get(g, 0)
        diff = pct - tgt
        d_style = "green" if abs(diff) <= 5.0 else "red"
        t3.add_row(g, str(cnt), f"{pct:.1f}%", f"{tgt:.1f}%", f"[{d_style}]{diff:+.1f}[/{d_style}]")
    console.print(t3)

    # ---- ◉閾値シミュレーション ----
    console.print("\n[bold cyan]■ ◉閾値シミュレーション[/bold cyan]")
    t4 = Table(title="◉閾値探索（目標: 勝率65%, 連対75%, 複勝90%）")
    t4.add_column("gap", justify="right")
    t4.add_column("wp", justify="right")
    t4.add_column("p3p", justify="right")
    t4.add_column("件数", justify="right")
    t4.add_column("出現率", justify="right")
    t4.add_column("勝率", justify="right")
    t4.add_column("連対率", justify="right")
    t4.add_column("複勝率", justify="right")
    t4.add_column("単回収", justify="right")
    t4.add_column("複回収", justify="right")

    # 主要な閾値組み合わせをテスト
    for gap_th in [1.5, 3.0, 5.0, 7.0, 10.0]:
        for wp_th in [0.20, 0.25, 0.30, 0.35]:
            for p3p_th in [0.0, 0.50, 0.65, 0.75]:
                r = tekipan_sim.simulate(gap_th, wp_th, p3p_th)
                if r["total"] < 30:
                    continue
                # 勝率50%以上 or 有望な組み合わせのみ表示
                if r["win_rate"] < 30.0 and gap_th < 5.0:
                    continue
                t4.add_row(
                    f"{gap_th:.1f}", f"{wp_th:.0%}", f"{p3p_th:.0%}",
                    str(r["total"]), f"{r['appearance_rate']:.1f}%",
                    f"{r['win_rate']:.1f}%", f"{r['place2_rate']:.1f}%",
                    f"{r['place3_rate']:.1f}%",
                    f"{r['tansho_roi']:.1f}%", f"{r['fukusho_roi']:.1f}%",
                )
    console.print(t4)


if __name__ == "__main__":
    main()
