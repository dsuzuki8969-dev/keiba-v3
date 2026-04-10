"""
印別成績集計スクリプト

予測JSONのmarkとrace_logの着順を突き合わせて、
全印および特定印の成績を集計する。
"""

import argparse
import json
import glob
import sqlite3
import sys
import os
import io
from collections import defaultdict
from datetime import datetime

# Windows環境でのUTF-8出力対応
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

# プロジェクトルートをパスに追加
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.log import get_logger
from rich.console import Console
from rich.table import Table
from rich import box

logger = get_logger(__name__)
console = Console(file=sys.stdout, force_terminal=True, width=130)

# 全印の定義（表示順）
ALL_MARKS = ["◉", "◎", "○", "▲", "△", "★", "☆", "×", "－", ""]

# 印の説明
MARK_LABELS = {
    "◉": "◉ 本命（自信）",
    "◎": "◎ 本命",
    "○": "○ 対抗",
    "▲": "▲ 単穴",
    "△": "△ 連下",
    "★": "★ 注目",
    "☆": "☆ 厳選穴馬",
    "×": "× 危険",
    "－": "－ 無印",
    "": "（空）",
}


def load_predictions(pred_dir: str, start_date: str | None, end_date: str | None) -> list[dict]:
    """予測JSONファイルを読み込む"""
    files = sorted(glob.glob(os.path.join(pred_dir, "*_pred.json")))
    logger.info(f"予測ファイル数: {len(files)}")

    all_entries = []  # [{race_id, horse_no, horse_name, mark, is_jra, venue, odds, popularity, date}, ...]

    loaded = 0
    skipped = 0
    for fpath in files:
        fname = os.path.basename(fpath)
        # ファイル名から日付抽出 (YYYYMMDD_pred.json)
        date_str = fname.split("_")[0]
        if len(date_str) != 8:
            continue

        # 日付フィルタ
        file_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        if start_date and file_date < start_date:
            continue
        if end_date and file_date > end_date:
            continue

        try:
            with open(fpath, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, Exception):
            skipped += 1
            continue

        races = data.get("races", [])
        if not isinstance(races, list):
            skipped += 1
            continue

        for race in races:
            race_id = race.get("race_id", "")
            is_jra = race.get("is_jra", False)
            venue = race.get("venue", "")

            for horse in race.get("horses", []):
                mark = horse.get("mark", "")
                entry = {
                    "race_id": str(race_id),
                    "horse_no": int(horse.get("horse_no", 0)),
                    "horse_name": horse.get("horse_name", ""),
                    "mark": mark,
                    "is_jra": is_jra,
                    "venue": venue,
                    "pred_odds": horse.get("odds"),
                    "popularity": horse.get("popularity"),
                    "date": file_date,
                    "tokusen_score": horse.get("tokusen_score", 0),
                    "ana_score": horse.get("ana_score", 0),
                }
                all_entries.append(entry)
        loaded += 1

    logger.info(f"読み込み: {loaded}ファイル, スキップ: {skipped}ファイル, エントリ: {len(all_entries)}件")
    return all_entries


def load_results(db_path: str, start_date: str | None, end_date: str | None) -> dict:
    """race_logから着順データを取得"""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    query = "SELECT race_id, horse_no, horse_name, finish_pos, popularity, win_odds, is_jra FROM race_log WHERE 1=1"
    params = []
    if start_date:
        query += " AND race_date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND race_date <= ?"
        params.append(end_date)

    rows = conn.execute(query, params).fetchall()
    conn.close()

    # (race_id, horse_no) → {finish_pos, popularity, win_odds}
    results = {}
    for row in rows:
        key = (str(row["race_id"]), int(row["horse_no"]))
        results[key] = {
            "finish_pos": row["finish_pos"],
            "popularity": row["popularity"],
            "win_odds": row["win_odds"],
            "is_jra": row["is_jra"],
        }

    logger.info(f"race_log結果: {len(results)}件")
    return results


def merge_data(entries: list[dict], results: dict) -> list[dict]:
    """予測と結果を突き合わせ"""
    merged = []
    matched = 0
    unmatched = 0

    for entry in entries:
        key = (entry["race_id"], entry["horse_no"])
        if key in results:
            result = results[key]
            entry["finish_pos"] = result["finish_pos"]
            entry["actual_popularity"] = result["popularity"]
            entry["actual_odds"] = result["win_odds"]
            entry["result_is_jra"] = result["is_jra"]
            merged.append(entry)
            matched += 1
        else:
            unmatched += 1

    logger.info(f"突き合わせ: マッチ {matched}件, 未マッチ {unmatched}件")
    return merged


def calc_stats(data: list[dict]) -> dict:
    """成績統計を計算"""
    if not data:
        return {
            "count": 0, "win": 0, "top2": 0, "top3": 0,
            "win_rate": 0, "top2_rate": 0, "top3_rate": 0,
            "avg_pop": 0, "avg_odds": 0,
            "win_return": 0, "place_return": 0,
            "total_bet_win": 0, "total_return_win": 0,
        }

    count = len(data)
    win = sum(1 for d in data if d["finish_pos"] == 1)
    top2 = sum(1 for d in data if d["finish_pos"] <= 2)
    top3 = sum(1 for d in data if d["finish_pos"] <= 3)

    pops = [d["actual_popularity"] for d in data if d["actual_popularity"] and d["actual_popularity"] > 0]
    avg_pop = sum(pops) / len(pops) if pops else 0

    odds_list = [d["actual_odds"] for d in data if d["actual_odds"] and d["actual_odds"] > 0]
    avg_odds = sum(odds_list) / len(odds_list) if odds_list else 0

    # 単勝回収率: 勝った馬のオッズ合計 / 総賭け金(=count)
    win_returns = sum(d["actual_odds"] for d in data if d["finish_pos"] == 1 and d["actual_odds"] and d["actual_odds"] > 0)
    # 複勝回収率は正確なデータがないので概算（3着内でオッズの1/4程度）は省略
    # → 単勝回収率のみ算出

    return {
        "count": count,
        "win": win,
        "top2": top2,
        "top3": top3,
        "win_rate": win / count * 100 if count else 0,
        "top2_rate": top2 / count * 100 if count else 0,
        "top3_rate": top3 / count * 100 if count else 0,
        "avg_pop": avg_pop,
        "avg_odds": avg_odds,
        "total_bet_win": count,
        "total_return_win": win_returns,
        "win_return_rate": win_returns / count * 100 if count else 0,
    }


def show_summary_table(data: list[dict], title: str = "全印別成績サマリー"):
    """全印の成績をテーブル表示"""
    table = Table(title=title, box=box.SIMPLE_HEAVY, expand=False)
    table.add_column("印", justify="center", no_wrap=True)
    table.add_column("頭数", justify="right")
    table.add_column("勝率", justify="right")
    table.add_column("連対率", justify="right")
    table.add_column("3着内率", justify="right")
    table.add_column("平均人気", justify="right")
    table.add_column("平均ｵｯｽﾞ", justify="right")
    table.add_column("単回収率", justify="right")
    table.add_column("勝", justify="right")
    table.add_column("2着内", justify="right")
    table.add_column("3着内", justify="right")

    for mark in ALL_MARKS:
        subset = [d for d in data if d["mark"] == mark]
        if not subset:
            continue
        stats = calc_stats(subset)
        label = MARK_LABELS.get(mark, mark)

        # 値フォーマット
        win_str = f"{stats['win_rate']:.1f}%"
        top3_str = f"{stats['top3_rate']:.1f}%"
        roi_str = f"{stats['win_return_rate']:.1f}%"

        # 色分け（Richマークアップ）
        if stats["win_rate"] >= 20:
            win_str = f"[bold green]{win_str}[/bold green]"
        elif stats["win_rate"] >= 10:
            win_str = f"[green]{win_str}[/green]"

        if stats["top3_rate"] >= 50:
            top3_str = f"[bold green]{top3_str}[/bold green]"
        elif stats["top3_rate"] >= 30:
            top3_str = f"[green]{top3_str}[/green]"

        if stats["win_return_rate"] >= 100:
            roi_str = f"[bold green]{roi_str}[/bold green]"
        elif stats["win_return_rate"] >= 80:
            roi_str = f"[yellow]{roi_str}[/yellow]"
        else:
            roi_str = f"[red]{roi_str}[/red]"

        table.add_row(
            label,
            str(stats["count"]),
            win_str,
            f"{stats['top2_rate']:.1f}%",
            top3_str,
            f"{stats['avg_pop']:.1f}",
            f"{stats['avg_odds']:.1f}",
            roi_str,
            str(stats["win"]),
            str(stats["top2"]),
            str(stats["top3"]),
        )

    console.print(table)


def show_detail_analysis(data: list[dict], mark: str):
    """特定印の詳細分析"""
    subset = [d for d in data if d["mark"] == mark]
    if not subset:
        console.print(f"[red]{MARK_LABELS.get(mark, mark)} のデータがありません[/]")
        return

    label = MARK_LABELS.get(mark, mark)
    console.print(f"\n[bold cyan]{'='*60}[/]")
    console.print(f"[bold cyan] {label} 詳細分析 （{len(subset)}頭）[/]")
    console.print(f"[bold cyan]{'='*60}[/]\n")

    # --- 基本成績 ---
    stats = calc_stats(subset)
    basic_table = Table(title="基本成績", box=box.SIMPLE_HEAVY)
    basic_table.add_column("項目", width=16)
    basic_table.add_column("値", justify="right")
    basic_table.add_row("頭数", str(stats["count"]))
    basic_table.add_row("勝率", f"{stats['win_rate']:.1f}%")
    basic_table.add_row("連対率", f"{stats['top2_rate']:.1f}%")
    basic_table.add_row("3着内率", f"{stats['top3_rate']:.1f}%")
    basic_table.add_row("平均人気", f"{stats['avg_pop']:.1f}")
    basic_table.add_row("平均オッズ", f"{stats['avg_odds']:.1f}")
    basic_table.add_row("単勝回収率", f"{stats['win_return_rate']:.1f}%")
    console.print(basic_table)

    # --- JRA/NAR別 ---
    jra = [d for d in subset if d.get("is_jra") or d.get("result_is_jra")]
    nar = [d for d in subset if not (d.get("is_jra") or d.get("result_is_jra"))]

    if jra or nar:
        jn_table = Table(title="JRA/NAR別成績", box=box.SIMPLE_HEAVY)
        jn_table.add_column("区分", width=8)
        jn_table.add_column("頭数", justify="right")
        jn_table.add_column("勝率", justify="right")
        jn_table.add_column("連対率", justify="right")
        jn_table.add_column("3着内率", justify="right")
        jn_table.add_column("平均人気", justify="right")
        jn_table.add_column("単勝回収率", justify="right")

        for label_jn, sub in [("JRA", jra), ("NAR", nar)]:
            if not sub:
                continue
            s = calc_stats(sub)
            jn_table.add_row(
                label_jn, str(s["count"]),
                f"{s['win_rate']:.1f}%", f"{s['top2_rate']:.1f}%",
                f"{s['top3_rate']:.1f}%", f"{s['avg_pop']:.1f}",
                f"{s['win_return_rate']:.1f}%",
            )
        console.print(jn_table)

    # --- 月別推移 ---
    monthly = defaultdict(list)
    for d in subset:
        ym = d["date"][:7]  # YYYY-MM
        monthly[ym].append(d)

    if monthly:
        m_table = Table(title="月別推移", box=box.SIMPLE_HEAVY)
        m_table.add_column("月", width=10)
        m_table.add_column("頭数", justify="right")
        m_table.add_column("勝率", justify="right")
        m_table.add_column("連対率", justify="right")
        m_table.add_column("3着内率", justify="right")
        m_table.add_column("平均人気", justify="right")
        m_table.add_column("単勝回収率", justify="right")

        for ym in sorted(monthly.keys()):
            s = calc_stats(monthly[ym])
            roi_style = "green" if s["win_return_rate"] >= 100 else ("yellow" if s["win_return_rate"] >= 80 else "red")
            m_table.add_row(
                ym, str(s["count"]),
                f"{s['win_rate']:.1f}%", f"{s['top2_rate']:.1f}%",
                f"{s['top3_rate']:.1f}%", f"{s['avg_pop']:.1f}",
                f"[{roi_style}]{s['win_return_rate']:.1f}%[/]",
            )
        console.print(m_table)

    # --- 人気別分布 ---
    pop_groups = defaultdict(list)
    for d in subset:
        pop = d.get("actual_popularity")
        if not pop or pop <= 0:
            continue
        if pop <= 3:
            group = "1-3番人気"
        elif pop <= 6:
            group = "4-6番人気"
        elif pop <= 9:
            group = "7-9番人気"
        else:
            group = "10番人気以下"
        pop_groups[group].append(d)

    if pop_groups:
        p_table = Table(title="人気帯別成績", box=box.SIMPLE_HEAVY)
        p_table.add_column("人気帯", width=14)
        p_table.add_column("頭数", justify="right")
        p_table.add_column("構成比", justify="right")
        p_table.add_column("勝率", justify="right")
        p_table.add_column("3着内率", justify="right")
        p_table.add_column("平均オッズ", justify="right")
        p_table.add_column("単勝回収率", justify="right")

        total = len(subset)
        for group in ["1-3番人気", "4-6番人気", "7-9番人気", "10番人気以下"]:
            sub = pop_groups.get(group, [])
            if not sub:
                continue
            s = calc_stats(sub)
            roi_style = "green" if s["win_return_rate"] >= 100 else "red"
            p_table.add_row(
                group, str(s["count"]),
                f"{len(sub)/total*100:.1f}%",
                f"{s['win_rate']:.1f}%", f"{s['top3_rate']:.1f}%",
                f"{s['avg_odds']:.1f}",
                f"[{roi_style}]{s['win_return_rate']:.1f}%[/]",
            )
        console.print(p_table)

    # --- 個別人気別（1-10番人気） ---
    pop_individual = defaultdict(list)
    for d in subset:
        pop = d.get("actual_popularity")
        if pop and 1 <= pop <= 15:
            pop_individual[pop].append(d)

    if pop_individual:
        pi_table = Table(title="個別人気別分布・成績", box=box.SIMPLE_HEAVY)
        pi_table.add_column("人気", justify="center", width=6)
        pi_table.add_column("頭数", justify="right")
        pi_table.add_column("構成比", justify="right")
        pi_table.add_column("勝率", justify="right")
        pi_table.add_column("3着内率", justify="right")
        pi_table.add_column("単勝回収率", justify="right")

        total = len(subset)
        for pop in sorted(pop_individual.keys()):
            sub = pop_individual[pop]
            s = calc_stats(sub)
            pi_table.add_row(
                f"{pop}", str(s["count"]),
                f"{len(sub)/total*100:.1f}%",
                f"{s['win_rate']:.1f}%", f"{s['top3_rate']:.1f}%",
                f"{s['win_return_rate']:.1f}%",
            )
        console.print(pi_table)

    # --- 会場別（上位10会場） ---
    venue_data = defaultdict(list)
    for d in subset:
        v = d.get("venue", "不明")
        if v:
            venue_data[v].append(d)

    if venue_data:
        v_table = Table(title="会場別成績（上位10）", box=box.SIMPLE_HEAVY)
        v_table.add_column("会場", width=10)
        v_table.add_column("頭数", justify="right")
        v_table.add_column("勝率", justify="right")
        v_table.add_column("3着内率", justify="right")
        v_table.add_column("単勝回収率", justify="right")

        sorted_venues = sorted(venue_data.items(), key=lambda x: len(x[1]), reverse=True)[:10]
        for venue, sub in sorted_venues:
            s = calc_stats(sub)
            v_table.add_row(
                venue, str(s["count"]),
                f"{s['win_rate']:.1f}%", f"{s['top3_rate']:.1f}%",
                f"{s['win_return_rate']:.1f}%",
            )
        console.print(v_table)


def main():
    parser = argparse.ArgumentParser(description="印別成績集計スクリプト")
    parser.add_argument("--mark", type=str, default=None,
                        help="特定の印だけ詳細集計（例: --mark ☆）")
    parser.add_argument("--period", type=str, nargs=2, metavar=("FROM", "TO"),
                        help="期間指定（例: --period 2026-01 2026-03）")
    parser.add_argument("--jra-only", action="store_true", help="JRAのみ")
    parser.add_argument("--nar-only", action="store_true", help="NARのみ")
    parser.add_argument("--pred-dir", type=str, default=None,
                        help="予測JSONディレクトリ（デフォルト: data/predictions/）")
    parser.add_argument("--db", type=str, default=None,
                        help="DBパス（デフォルト: data/keiba.db）")
    args = parser.parse_args()

    # パス解決
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    pred_dir = args.pred_dir or os.path.join(project_root, "data", "predictions")
    db_path = args.db or os.path.join(project_root, "data", "keiba.db")

    # 期間
    start_date = None
    end_date = None
    if args.period:
        # YYYY-MM 形式を YYYY-MM-01 / YYYY-MM-31 に変換
        start_date = args.period[0] if len(args.period[0]) > 7 else args.period[0] + "-01"
        end_date = args.period[1] if len(args.period[1]) > 7 else args.period[1] + "-31"

    console.print(f"[bold]予測ディレクトリ: {pred_dir}[/]")
    console.print(f"[bold]データベース: {db_path}[/]")
    if start_date:
        console.print(f"[bold]期間: {start_date} ~ {end_date}[/]")
    console.print()

    # データ読み込み
    entries = load_predictions(pred_dir, start_date, end_date)
    results = load_results(db_path, start_date, end_date)
    merged = merge_data(entries, results)

    if not merged:
        console.print("[red]突き合わせ可能なデータがありません[/]")
        return

    # JRA/NARフィルタ
    if args.jra_only:
        merged = [d for d in merged if d.get("is_jra") or d.get("result_is_jra")]
        console.print("[bold yellow]JRAのみにフィルタ[/]\n")
    elif args.nar_only:
        merged = [d for d in merged if not (d.get("is_jra") or d.get("result_is_jra"))]
        console.print("[bold yellow]NARのみにフィルタ[/]\n")

    # 取消馬除外（finish_pos=0の場合）
    merged = [d for d in merged if d.get("finish_pos") and d["finish_pos"] > 0]

    console.print(f"[bold]分析対象: {len(merged)}頭[/]\n")

    # 全印サマリーは必ず表示
    show_summary_table(merged)

    # 特定印の詳細分析
    if args.mark:
        show_detail_analysis(merged, args.mark)
    else:
        # デフォルトで☆の詳細も表示
        star_data = [d for d in merged if d["mark"] == "☆"]
        if star_data:
            show_detail_analysis(merged, "☆")


if __name__ == "__main__":
    main()
