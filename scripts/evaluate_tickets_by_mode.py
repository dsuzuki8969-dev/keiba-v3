"""買い目指南 Phase 1-d: 3モード別 KPI 評価スクリプト

pred.json の tickets_by_mode と race_log DB を突き合わせて、
accuracy / balanced / recovery 3モードの的中率・回収率を集計する。

使い方:
  python scripts/evaluate_tickets_by_mode.py --after 2026-04-01
  python scripts/evaluate_tickets_by_mode.py --start 2024-01-01 --end 2026-03-31
  python scripts/evaluate_tickets_by_mode.py --date 2026-04-19   # 1日だけ
  python scripts/evaluate_tickets_by_mode.py --scope jra         # JRA のみ
"""

import argparse
import glob
import itertools
import json
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))


DB_PATH = _PROJECT_ROOT / "data" / "keiba.db"
PRED_DIR = _PROJECT_ROOT / "data" / "predictions"

MODES = ("accuracy", "balanced", "recovery")


def _load_finish_map(dates: list[str]) -> dict[str, dict[int, int]]:
    """race_id → {horse_no: finish_pos} のマップを構築"""
    if not dates:
        return {}
    con = sqlite3.connect(str(DB_PATH))
    cur = con.cursor()
    placeholders = ",".join("?" * len(dates))
    cur.execute(
        f"SELECT race_id, horse_no, finish_pos FROM race_log "
        f"WHERE race_date IN ({placeholders})",
        dates,
    )
    result: dict[str, dict[int, int]] = defaultdict(dict)
    for rid, hno, fp in cur.fetchall():
        if hno is None or fp is None:
            continue
        try:
            result[str(rid)][int(hno)] = int(fp)
        except (ValueError, TypeError):
            continue
    con.close()
    return result


def _pred_date_range(start: str, end: str, after: str, date: str) -> list[str]:
    """対象日付 (YYYYMMDD) を返す"""
    all_files = sorted(PRED_DIR.glob("*_pred.json"))
    dates = []
    for f in all_files:
        stem = f.stem  # "20260419_pred"
        if not stem.endswith("_pred"):
            continue
        ymd = stem.replace("_pred", "")
        if len(ymd) != 8 or not ymd.isdigit():
            continue
        iso = f"{ymd[:4]}-{ymd[4:6]}-{ymd[6:8]}"
        if date and iso != date:
            continue
        if start and iso < start:
            continue
        if end and iso > end:
            continue
        if after and iso < after:
            continue
        dates.append(iso)
    return dates


def _is_hit(ticket: dict, finish_by_hno: dict[int, int]) -> bool:
    """1つの ticket が当たっているか判定

    - 三連複: combo の3頭が 1-3 着
    - 馬連:   combo の2頭が 1-2 着
    - 馬単:   a→b が 1→2 着
    - 単勝:   combo[0] が 1 着
    - 複勝:   combo[0] が 1-3 着
    """
    ttype = ticket.get("type", "")
    combo = ticket.get("combo") or []
    if not combo:
        a, b, c = ticket.get("a"), ticket.get("b"), ticket.get("c")
        combo = [x for x in (a, b, c) if x is not None]
    if not combo:
        return False
    try:
        combo = [int(x) for x in combo]
    except (ValueError, TypeError):
        return False
    finishes = [finish_by_hno.get(h, 0) for h in combo]
    if any(f == 0 for f in finishes):
        return False

    if ttype in ("三連複", "3連複", "trio"):
        return sorted(finishes) == [1, 2, 3]
    if ttype in ("馬連", "umaren"):
        return sorted(finishes) == [1, 2]
    if ttype in ("馬単", "umatan", "exacta"):
        return finishes == [1, 2]
    if ttype == "単勝":
        return finishes[0] == 1
    if ttype == "複勝":
        return finishes[0] in (1, 2, 3)
    # フォールバック: 3頭 → 三連複、2頭 → 馬連
    if len(combo) == 3:
        return sorted(finishes) == [1, 2, 3]
    if len(combo) == 2:
        return sorted(finishes) == [1, 2]
    return False


def _ticket_payout(ticket: dict, is_hit: bool) -> int:
    """1 ticket の払戻 (円). hit なら stake*odds, 外れなら 0"""
    if not is_hit:
        return 0
    if "payback_if_hit" in ticket:
        try:
            return int(ticket["payback_if_hit"] or 0)
        except (ValueError, TypeError):
            pass
    stake = ticket.get("stake", 0) or 0
    odds = ticket.get("odds", 0) or 0
    try:
        return int(float(stake) * float(odds))
    except (ValueError, TypeError):
        return 0


def _mode_kpi(mode_name: str, race_tickets: list[list[dict]],
              finish_maps: list[dict[int, int]]) -> dict:
    """1モードの KPI を算出"""
    n_races_with_tickets = 0
    n_hit_races = 0
    total_stake = 0
    total_payout = 0
    ticket_counts = []
    for tickets, fm in zip(race_tickets, finish_maps):
        if not tickets or not fm:
            continue
        n_races_with_tickets += 1
        ticket_counts.append(len(tickets))
        race_hit = False
        for t in tickets:
            stake = t.get("stake", 0) or 0
            try:
                stake = int(float(stake))
            except (ValueError, TypeError):
                stake = 0
            if stake <= 0:
                continue
            total_stake += stake
            hit = _is_hit(t, fm)
            if hit:
                race_hit = True
                total_payout += _ticket_payout(t, True)
        if race_hit:
            n_hit_races += 1

    hit_rate = (n_hit_races / n_races_with_tickets * 100.0) if n_races_with_tickets else 0.0
    roi = (total_payout / total_stake * 100.0) if total_stake else 0.0
    avg_tickets = (sum(ticket_counts) / len(ticket_counts)) if ticket_counts else 0.0

    return {
        "mode": mode_name,
        "n_races": n_races_with_tickets,
        "n_hit": n_hit_races,
        "hit_rate": hit_rate,
        "total_stake": total_stake,
        "total_payout": total_payout,
        "roi": roi,
        "avg_tickets": avg_tickets,
    }


def main():
    parser = argparse.ArgumentParser(description="買い目 3モード別 KPI 評価")
    parser.add_argument("--start", default="", help="開始日 YYYY-MM-DD")
    parser.add_argument("--end", default="", help="終了日 YYYY-MM-DD")
    parser.add_argument("--after", default="", help="この日付以降 YYYY-MM-DD")
    parser.add_argument("--date", default="", help="単日 YYYY-MM-DD")
    parser.add_argument("--scope", choices=["all", "jra", "nar"], default="all")
    args = parser.parse_args()

    dates = _pred_date_range(args.start, args.end, args.after, args.date)
    if not dates:
        print("対象日付が見つかりません")
        return 1

    print(f"対象期間: {dates[0]} ~ {dates[-1]} ({len(dates)}日)")
    print(f"スコープ: {args.scope}\n")

    finish_db = _load_finish_map(dates)
    print(f"race_log から {len(finish_db)} レース分の着順を読み込み")

    # モード別にレースのチケット列と finish_map をペアで保持
    mode_race_tickets: dict[str, list[list[dict]]] = {m: [] for m in MODES}
    mode_finish_maps: dict[str, list[dict[int, int]]] = {m: [] for m in MODES}
    skip_counter = defaultdict(int)
    total_races = 0

    for iso in dates:
        ymd = iso.replace("-", "")
        pred_path = PRED_DIR / f"{ymd}_pred.json"
        if not pred_path.exists():
            continue
        try:
            with pred_path.open(encoding="utf-8") as f:
                data = json.load(f)
        except json.JSONDecodeError:
            continue
        for race in data.get("races", []):
            rid = str(race.get("race_id", ""))
            if not rid:
                continue
            # スコープフィルタ
            is_jra = race.get("is_jra", True)
            if args.scope == "jra" and not is_jra:
                continue
            if args.scope == "nar" and is_jra:
                continue
            fm = finish_db.get(rid, {})
            if not fm:
                continue
            total_races += 1

            bd = race.get("bet_decision") or {}
            if bd.get("skip"):
                skip_counter[bd.get("reason", "unknown")] += 1

            tbm = race.get("tickets_by_mode") or {}
            for mode in MODES:
                lst = tbm.get(mode, []) or []
                mode_race_tickets[mode].append(lst)
                mode_finish_maps[mode].append(fm)

    print(f"結果照合済みレース: {total_races} レース\n")
    if skip_counter:
        print(f"skip 内訳: {dict(skip_counter)}\n")

    # 各モード集計
    print("=" * 74)
    print(f"  {'モード':<10} {'Rカバー':>6} {'的中R':>5} {'的中率':>7} "
          f"{'平均点数':>7} {'投資':>10} {'払戻':>10} {'回収率':>7}")
    print("=" * 74)
    for mode in MODES:
        k = _mode_kpi(mode, mode_race_tickets[mode], mode_finish_maps[mode])
        print(f"  {k['mode']:<10} {k['n_races']:>6} {k['n_hit']:>5} "
              f"{k['hit_rate']:>6.1f}% {k['avg_tickets']:>6.1f} "
              f"¥{k['total_stake']:>8,} ¥{k['total_payout']:>8,} "
              f"{k['roi']:>6.1f}%")
    print("=" * 74)

    # Phase 1 リリース基準（バランスモード）
    balanced = _mode_kpi("balanced",
                         mode_race_tickets["balanced"],
                         mode_finish_maps["balanced"])
    print("\n[Phase 1 リリース判定] (バランスモード基準)")
    hit_ok = 25.0 <= balanced["hit_rate"] <= 45.0
    roi_ok = 130.0 <= balanced["roi"] <= 220.0
    print(f"  的中率 {balanced['hit_rate']:.1f}%"
          f" ({'OK' if hit_ok else 'NG'}: 目標 25-45%)")
    print(f"  回収率 {balanced['roi']:.1f}%"
          f" ({'OK' if roi_ok else 'NG'}: 目標 130-220%)")

    return 0 if (hit_ok and roi_ok) else 2


if __name__ == "__main__":
    sys.exit(main())
