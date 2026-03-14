"""
結果照合スクリプト
使い方:
  python run_results.py YYYY-MM-DD          # 指定日の結果を取得・照合
  python run_results.py YYYY-MM-DD --show   # 照合結果をコンソール表示のみ
  python run_results.py --summary           # 通算成績を表示
"""
import sys, io, os
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import re
from src.results_tracker import (
    load_prediction, fetch_actual_results, compare_and_aggregate,
    list_prediction_dates, aggregate_all,
)


def _fmt_money(v: int) -> str:
    return ('+' if v >= 0 else '') + f"{v:,}円"


def show_summary(year: str = "all"):
    data = aggregate_all(year_filter=year)
    title = "通算成績" if year == "all" else f"{year}年成績"
    print(f"\n{'='*50}")
    print(f"  {title}")
    print(f"{'='*50}")
    if not data["total_races"]:
        print("  データなし（予想JSONが存在しないか、結果照合が未実施）")
        return

    print(f"  予想レース数  : {data['total_races']} R")
    print(f"  買い目的中率  : {data['hit_rate']:.1f}%  ({data['hit_tickets']}/{data['total_tickets']})")
    print(f"  ◎勝率        : {data.get('honmei_win_rate', 0):.1f}%")
    print(f"  ◎複勝率      : {data['honmei_rate']:.1f}%  ({data['honmei_placed']}/{data['honmei_total']})")
    print(f"  回収率        : {data['roi']:.1f}%")
    print(f"  収支          : {_fmt_money(data['profit'])}")

    if data.get("by_ticket_type"):
        print(f"\n  【券種別】")
        for tt, s in data["by_ticket_type"].items():
            if s["total"] == 0:
                continue
            hr  = s["hits"] / s["total"] * 100
            roi = s["ret"] / s["stake"] * 100 if s["stake"] else 0
            print(f"    {tt:6s}: {s['total']}買い目  的中率{hr:.1f}%  回収率{roi:.1f}%")

    if data.get("by_mark"):
        print(f"\n  【印別複勝率】")
        for mk in ["◎", "○", "▲", "△", "☆"]:
            s = data["by_mark"].get(mk)
            if not s or s["total"] == 0:
                continue
            wr = s["win"]    / s["total"] * 100
            pr = s["placed"] / s["total"] * 100
            print(f"    {mk}: {s['total']}頭  勝率{wr:.1f}%  複勝率{pr:.1f}%")

    if data.get("by_confidence"):
        print(f"\n  【自信度別】")
        for conf in ["SS","S","A","B","C","D"]:
            s = data["by_confidence"].get(conf)
            if not s or s["races"] == 0:
                continue
            roi  = s["ret"] / s["stake"] * 100 if s["stake"] else 0
            prof = s["ret"] - s["stake"]
            print(f"    {conf:4s}: {s['races']}R  的中{s['hits']}  "
                  f"回収率{roi:.1f}%  収支{_fmt_money(prof)}")
    print()


def run_fetch_and_compare(date: str):
    pred = load_prediction(date)
    if not pred:
        print(f"[ERROR] {date} の予想JSONが見つかりません")
        print("  → run_analysis_date.py を実行して予想を生成してください")
        sys.exit(1)

    print(f"[1/2] {date} の実際の着順を取得中...")
    from src.scraper.netkeiba import NetkeibaClient
    client = NetkeibaClient()
    fetch_actual_results(date, client)

    print(f"[2/2] 照合・集計中...")
    result = compare_and_aggregate(date)
    if not result:
        print("[ERROR] 照合に失敗しました")
        sys.exit(1)

    print(f"\n{'='*50}")
    print(f"  {date} 結果照合完了")
    print(f"{'='*50}")
    print(f"  予想レース数  : {result['total_races']} R")
    print(f"  買い目的中率  : {result['hit_rate']:.1f}%  ({result['hit_tickets']}/{result['total_tickets']})")
    print(f"  ◎勝率        : {result.get('honmei_win_rate', 0):.1f}%")
    print(f"  ◎複勝率      : {result['honmei_rate']:.1f}%")
    print(f"  回収率        : {result['roi']:.1f}%")
    print(f"  収支          : {_fmt_money(result['profit'])}")
    print()


if __name__ == "__main__":
    args = sys.argv[1:]

    if not args or "--help" in args:
        print(__doc__)
        sys.exit(0)

    if "--summary" in args:
        year = next((a for a in args if re.fullmatch(r"\d{4}", a)), "all")
        show_summary(year)
        sys.exit(0)

    date = next((a for a in args if re.fullmatch(r"\d{4}-\d{2}-\d{2}", a)), None)
    if not date:
        print("[ERROR] 日付を YYYY-MM-DD 形式で指定してください")
        sys.exit(1)

    if "--show" in args:
        result = compare_and_aggregate(date)
        if not result:
            print(f"[ERROR] {date} の照合データがありません（先に結果取得が必要）")
        else:
            print(f"\n{date} 照合結果:")
            for k, v in result.items():
                if k not in ("by_confidence", "by_ticket_type", "by_mark", "by_date"):
                    print(f"  {k}: {v}")
        sys.exit(0)

    run_fetch_and_compare(date)
