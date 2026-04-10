"""
ファクター別グレード成績集計スクリプト

6大ファクター（能力・展開・適性・騎手・調教師・血統）＋総合指数の
SS/S/A/B/C/D/E グレード別成績を集計する。

使い方:
  python scripts/evaluate_factor_grades.py                          # 全期間・全体
  python scripts/evaluate_factor_grades.py --year 2026              # 2026年のみ
  python scripts/evaluate_factor_grades.py --year 2026 --scope jra  # JRAのみ
  python scripts/evaluate_factor_grades.py --after 2026-03-01       # 指定日以降
  python scripts/evaluate_factor_grades.py --factor ability         # 特定ファクターのみ
"""
import argparse
import io
import json
import os
import sys

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True)
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from rich.console import Console
from rich.table import Table
from rich.text import Text
from rich.panel import Panel

import statistics

from config.settings import PREDICTIONS_DIR, RESULTS_DIR
from src.calculator.grades import dev_to_grade

console = Console(width=120)

# ============================================================
# 定数
# ============================================================

JRA_CODES = {"01", "02", "03", "04", "05", "06", "07", "08", "09", "10"}

GRADE_ORDER = ["SS", "S", "A", "B", "C", "D", "E"]

# グレード別の色
GRADE_COLORS = {
    "SS": "bold bright_red",
    "S": "bold red",
    "A": "bold yellow",
    "B": "white",
    "C": "dim cyan",
    "D": "dim blue",
    "E": "dim white",
}

# ファクター定義: (JSONキー, 日本語名, 色)
FACTORS = [
    ("ability_total", "能力", "bright_green"),
    ("pace_total", "展開", "bright_cyan"),
    ("course_total", "適性", "bright_yellow"),
    ("jockey_dev", "騎手", "bright_magenta"),
    ("trainer_dev", "調教師", "bright_blue"),
    ("bloodline_dev", "血統", "bright_red"),
    ("composite", "総合指数", "bold bright_white"),
]


# ============================================================
# 集計ロジック
# ============================================================

_TARGET_MEAN = 52.5
_TARGET_SIGMA = 7.0


def _normalize_race_devs(horses: list, factor_keys: list) -> dict:
    """レース内の偏差値をN(52.5, 6.4)に正規化する。
    Returns: {factor_key: {horse_no: normalized_dev}}
    """
    result = {}
    for key in factor_keys:
        vals = [(h.get("horse_no"), h.get(key)) for h in horses if h.get(key) is not None]
        if len(vals) < 2:
            # 正規化不可 → 生の値をそのまま使用
            result[key] = {hno: v for hno, v in vals}
            continue
        raw_vals = [v for _, v in vals]
        mu = statistics.mean(raw_vals)
        sigma = statistics.pstdev(raw_vals) or 1.0
        normalized = {}
        for hno, v in vals:
            n = _TARGET_MEAN + (v - mu) / sigma * _TARGET_SIGMA
            normalized[hno] = max(20.0, min(100.0, n))
        result[key] = normalized
    return result


def _new_grade_bucket():
    return {"total": 0, "win": 0, "place2": 0, "placed": 0, "tansho_ret": 0}


def collect(year_filter: str, after_filter: str, scope: str) -> dict:
    """全日付を走査してファクター×グレード別の成績を集計する。"""
    # ファクター別 → グレード別の集計コンテナ
    stats = {}
    for key, _, _ in FACTORS:
        stats[key] = {g: _new_grade_bucket() for g in GRADE_ORDER}

    pred_dir = PREDICTIONS_DIR
    res_dir = RESULTS_DIR

    # 利用可能な日付を取得
    pred_files = sorted(f for f in os.listdir(pred_dir) if f.endswith("_pred.json"))

    for pf in pred_files:
        date_raw = pf[:8]  # YYYYMMDD
        date_dash = f"{date_raw[:4]}-{date_raw[4:6]}-{date_raw[6:8]}"

        # フィルタ
        if year_filter and year_filter != "all":
            if not date_raw.startswith(year_filter):
                continue
        if after_filter:
            if date_dash < after_filter:
                continue

        # 結果ファイル存在チェック
        rf = os.path.join(res_dir, f"{date_raw}_results.json")
        if not os.path.exists(rf):
            continue

        # 読み込み
        with open(os.path.join(pred_dir, pf), "r", encoding="utf-8") as f:
            pred = json.load(f)
        with open(rf, "r", encoding="utf-8") as f:
            results = json.load(f)

        for race in pred.get("races", []):
            race_id = race.get("race_id", "")
            if not race_id:
                continue

            # scope フィルタ
            vc = race_id[4:6] if len(race_id) >= 6 else ""
            is_jra = vc in JRA_CODES
            if scope == "jra" and not is_jra:
                continue
            if scope == "nar" and is_jra:
                continue

            # 結果取得
            result = results.get(race_id)
            if not result:
                continue

            order = result.get("order", [])
            finish_map = {r["horse_no"]: r["finish"] for r in order}
            odds_map = {r["horse_no"]: r.get("odds", 0) for r in order}

            if not finish_map:
                continue

            # レース単位で偏差値をN(52.5, 6.4)に正規化
            factor_keys = [k for k, _, _ in FACTORS]
            norm_map = _normalize_race_devs(race.get("horses", []), factor_keys)

            # 各馬の集計
            for h in race.get("horses", []):
                hno = h.get("horse_no")
                if hno is None:
                    continue
                pos = finish_map.get(hno)
                if pos is None:
                    continue
                # 取消・除外（着順が0や99以上）はスキップ
                if not isinstance(pos, int) or pos <= 0 or pos >= 99:
                    continue

                odds = odds_map.get(hno, 0) or 0

                # 各ファクターの正規化済み偏差値 → グレード → 集計
                for key, _, _ in FACTORS:
                    val = norm_map.get(key, {}).get(hno)
                    if val is None:
                        continue
                    grade = dev_to_grade(val)
                    if grade == "—":
                        continue
                    bucket = stats[key].get(grade)
                    if bucket is None:
                        continue

                    bucket["total"] += 1
                    if pos == 1:
                        bucket["win"] += 1
                        bucket["tansho_ret"] += int(odds * 100) if odds else 0
                    if pos <= 2:
                        bucket["place2"] += 1
                    if pos <= 3:
                        bucket["placed"] += 1

    return stats


# ============================================================
# 表示
# ============================================================

def _color_rate(val: float, thresholds: tuple = (30, 20, 10)) -> Text:
    """率の値を閾値に応じて色付け"""
    s = f"{val:5.1f}%"
    if val >= thresholds[0]:
        return Text(s, style="bold bright_green")
    elif val >= thresholds[1]:
        return Text(s, style="bright_yellow")
    elif val >= thresholds[2]:
        return Text(s, style="white")
    else:
        return Text(s, style="dim white")


def _color_roi(val: float) -> Text:
    """回収率を色付け"""
    s = f"{val:.0f}%"
    if val >= 100:
        return Text(s, style="bold bright_green")
    elif val >= 80:
        return Text(s, style="bright_yellow")
    elif val >= 50:
        return Text(s, style="white")
    else:
        return Text(s, style="dim white")


def print_factor_table(factor_key: str, factor_name: str, factor_color: str, grade_stats: dict):
    """1ファクターのグレード別成績をRichテーブルで出力する。"""
    total_all = sum(grade_stats[g]["total"] for g in GRADE_ORDER)

    table = Table(
        title=f"[{factor_color}]{factor_name}[/] グレード別成績",
        title_style=f"bold {factor_color}",
        border_style="bright_black",
        show_lines=True,
        padding=(0, 1),
        expand=False,
    )

    table.add_column("グレード", justify="center", style="bold", no_wrap=True, min_width=6)
    table.add_column("件数", justify="right", no_wrap=True, min_width=7)
    table.add_column("構成比", justify="right", no_wrap=True, min_width=6)
    table.add_column("成績 (1着-2着-3着-着外)", justify="center", no_wrap=True, min_width=26)
    table.add_column("勝率", justify="right", no_wrap=True, min_width=7)
    table.add_column("連対率", justify="right", no_wrap=True, min_width=7)
    table.add_column("複勝率", justify="right", no_wrap=True, min_width=7)
    table.add_column("単回収率", justify="right", no_wrap=True, min_width=7)

    for g in GRADE_ORDER:
        b = grade_stats[g]
        n = b["total"]
        g_style = GRADE_COLORS.get(g, "white")

        if n == 0:
            table.add_row(
                Text(g, style=g_style),
                "0",
                "-",
                "-",
                "-",
                "-",
                "-",
                "-",
            )
            continue

        w, p2, p3 = b["win"], b["place2"], b["placed"]
        out = n - p3
        # 成績文字列
        record = Text()
        record.append(f"{w}", style="bold bright_red")
        record.append("-")
        record.append(f"{p2 - w}", style="bold bright_yellow")
        record.append("-")
        record.append(f"{p3 - p2}", style="bold bright_green")
        record.append("-")
        record.append(f"{out}", style="dim")

        win_r = w / n * 100
        p2_r = p2 / n * 100
        p3_r = p3 / n * 100
        roi = b["tansho_ret"] / (n * 100) * 100 if n else 0
        pct = n / total_all * 100 if total_all else 0

        table.add_row(
            Text(g, style=g_style),
            f"{n:,}",
            f"{pct:.1f}%",
            record,
            _color_rate(win_r, (25, 15, 8)),
            _color_rate(p2_r, (40, 25, 15)),
            _color_rate(p3_r, (50, 35, 20)),
            _color_roi(roi),
        )

    # 合計行
    total_w = sum(grade_stats[g]["win"] for g in GRADE_ORDER)
    total_p2 = sum(grade_stats[g]["place2"] for g in GRADE_ORDER)
    total_p3 = sum(grade_stats[g]["placed"] for g in GRADE_ORDER)
    total_ret = sum(grade_stats[g]["tansho_ret"] for g in GRADE_ORDER)
    total_out = total_all - total_p3
    avg_win = total_w / total_all * 100 if total_all else 0
    avg_p2 = total_p2 / total_all * 100 if total_all else 0
    avg_p3 = total_p3 / total_all * 100 if total_all else 0
    avg_roi = total_ret / (total_all * 100) * 100 if total_all else 0

    total_record = Text()
    total_record.append(f"{total_w}", style="bold")
    total_record.append("-")
    total_record.append(f"{total_p2 - total_w}", style="bold")
    total_record.append("-")
    total_record.append(f"{total_p3 - total_p2}", style="bold")
    total_record.append("-")
    total_record.append(f"{total_out}", style="bold")

    table.add_row(
        Text("合計", style="bold"),
        Text(f"{total_all:,}", style="bold"),
        "100%",
        total_record,
        Text(f"{avg_win:5.1f}%", style="bold"),
        Text(f"{avg_p2:5.1f}%", style="bold"),
        Text(f"{avg_p3:5.1f}%", style="bold"),
        Text(f"{avg_roi:.0f}%", style="bold"),
    )

    console.print()
    console.print(table)


def main():
    parser = argparse.ArgumentParser(description="ファクター別グレード成績集計")
    parser.add_argument("--year", default="all", help="集計年 (例: 2026)")
    parser.add_argument("--scope", choices=["all", "jra", "nar"], default="all", help="JRA/NAR/全体")
    parser.add_argument("--after", default="", help="この日付以降のみ集計 (YYYY-MM-DD)")
    parser.add_argument("--factor", default="all",
                        choices=["all", "ability", "pace", "course", "jockey", "trainer", "bloodline", "composite"],
                        help="特定ファクターのみ表示")
    args = parser.parse_args()

    # ファクターフィルタ用マッピング
    factor_key_map = {
        "ability": "ability_total",
        "pace": "pace_total",
        "course": "course_total",
        "jockey": "jockey_dev",
        "trainer": "trainer_dev",
        "bloodline": "bloodline_dev",
        "composite": "composite",
    }

    scope_label = {"all": "全体", "jra": "JRA", "nar": "NAR"}[args.scope]
    year_label = args.year if args.year != "all" else "全期間"
    after_label = f" ({args.after}以降)" if args.after else ""

    console.print()
    console.print(Panel(
        f"[bold]ファクター別グレード成績集計[/bold]\n"
        f"[dim]対象: {scope_label} / {year_label}{after_label}[/dim]",
        border_style="bright_blue",
        padding=(0, 2),
    ))

    stats = collect(args.year, args.after, args.scope)

    for key, name, color in FACTORS:
        if args.factor != "all" and factor_key_map.get(args.factor) != key:
            continue
        print_factor_table(key, name, color, stats[key])

    console.print()


if __name__ == "__main__":
    main()
