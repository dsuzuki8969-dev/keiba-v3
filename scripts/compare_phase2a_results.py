# -*- coding: utf-8 -*-
"""Phase 2-A (Platt較正) 前後比較スクリプト

変更前(before_phase2a)と変更後のpred.jsonを比較し、
◎的中率・ROI・印変化率を集計する。
"""
import json
import os
import sqlite3
import statistics
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.stdout.reconfigure(encoding="utf-8")

DB_PATH = Path("data/keiba.db")
PRED_DIR = Path("data/predictions")

NAR_VENUES = {
    "大井", "船橋", "川崎", "浦和", "園田", "姫路", "名古屋", "笠松",
    "金沢", "門別", "盛岡", "水沢", "高知", "佐賀",
}


def get_results(conn, race_id):
    """レース結果を取得 (1着,2着,3着,三連複払戻)"""
    row = conn.execute(
        "SELECT order_json, payouts_json FROM race_results WHERE race_id=?",
        (race_id,),
    ).fetchone()
    if not row:
        return None
    order = json.loads(row[0])
    payouts = json.loads(row[1])
    w = s = t = None
    for o in order:
        f = o.get("finish")
        if f == 1: w = o["horse_no"]
        elif f == 2: s = o["horse_no"]
        elif f == 3: t = o["horse_no"]

    sanren_payout = 0
    # romaji キー("sanrenpuku") と日本語キー("三連複") の両方に対応
    sp = payouts.get("sanrenpuku") or payouts.get("三連複")
    has_payout = sp is not None
    if isinstance(sp, list):
        # romaji形式: [{"combo": "...", "payout": N}]
        for p in sp:
            if isinstance(p, dict):
                sanren_payout = p.get("payout", 0) or 0
            elif isinstance(p, (int, float)):
                sanren_payout = p or 0
    elif isinstance(sp, dict):
        # 日本語形式: {"combo": "...", "payout": N}
        sanren_payout = sp.get("payout", 0) or 0

    return {
        "winner": w, "second": s, "third": t,
        "sanren_payout": sanren_payout, "has_payout": has_payout,
    }


def analyze_pred(pred_data, conn, filter_venues=None):
    """pred.jsonを分析して的中率とROIを計算"""
    stats = {
        "honmei_1st": 0, "honmei_2nd": 0, "honmei_3rd": 0,
        "honmei_total": 0, "races": 0,
        "bet": 0, "payout": 0, "roi_races": 0, "hits": 0,
        "venue_stats": defaultdict(lambda: {"h1": 0, "h3": 0, "cnt": 0}),
    }
    for race in pred_data.get("races", []):
        venue = race.get("venue", "")
        if venue not in NAR_VENUES:
            continue
        if filter_venues and venue not in filter_venues:
            continue

        result = get_results(conn, race.get("race_id"))
        if not result or result["winner"] is None:
            continue

        w, s, t = result["winner"], result["second"], result["third"]
        stats["races"] += 1

        # ◎的中率
        for h in race.get("horses", []):
            if h.get("mark") == "◎":
                stats["honmei_total"] += 1
                hno = h.get("horse_no")
                vs = stats["venue_stats"][venue]
                vs["cnt"] += 1
                if hno == w:
                    stats["honmei_1st"] += 1
                    vs["h1"] += 1
                if hno in (w, s):
                    stats["honmei_2nd"] += 1
                if hno in (w, s, t):
                    stats["honmei_3rd"] += 1
                    vs["h3"] += 1

        # ROI (払戻データありのみ)
        if not result["has_payout"] or not all([w, s, t]):
            continue
        tickets = [tk for tk in race.get("tickets", []) if tk.get("type") == "三連複"]
        if not tickets:
            continue
        stats["roi_races"] += 1
        for tk in tickets:
            stake = tk.get("stake", 100) or 100
            stats["bet"] += stake
            combo = sorted(int(x) for x in (tk.get("combo") or []))
            actual = sorted([w, s, t])
            if combo == actual:
                stats["payout"] += (result["sanren_payout"] or 0) * (stake / 100)
                stats["hits"] += 1

    stats["roi"] = stats["payout"] / stats["bet"] * 100 if stats["bet"] else 0
    return stats


def main():
    conn = sqlite3.connect(str(DB_PATH))

    # before_phase2a ファイルを持ち、かつ pred.json が再分析済み(更新済み)の日付を自動検出
    before_files = sorted(PRED_DIR.glob("*_pred_before_phase2a.json"))
    dates = []
    skipped = []
    for f in before_files:
        d = f.name[:8]
        pred_path = PRED_DIR / f"{d}_pred.json"
        if pred_path.exists() and pred_path.stat().st_mtime > f.stat().st_mtime:
            dates.append(d)
        else:
            skipped.append(d)
    if skipped:
        print(f"  ⚠ 未再分析の日付をスキップ: {skipped}")

    print("=" * 80)
    print("  Phase 2-A (Platt較正) 前後比較")
    print(f"  対象日: {len(dates)}日 ({dates[0]}〜{dates[-1]})")
    print("=" * 80)

    pct = lambda n, d: f"{n}/{d} ({n/d*100:.1f}%)" if d else "N/A"

    # 日別比較
    print(f"\n{'日付':>10} | {'旧◎1着率':>14} | {'新◎1着率':>14} | {'旧◎複勝率':>14} | {'新◎複勝率':>14} | {'旧ROI':>8} | {'新ROI':>8}")
    print("-" * 100)

    old_total = {"h1": 0, "h2": 0, "h3": 0, "cnt": 0, "bet": 0, "payout": 0, "races": 0, "roi_races": 0, "hits": 0}
    new_total = {"h1": 0, "h2": 0, "h3": 0, "cnt": 0, "bet": 0, "payout": 0, "races": 0, "roi_races": 0, "hits": 0}
    old_venue_stats = defaultdict(lambda: {"h1": 0, "h3": 0, "cnt": 0})
    new_venue_stats = defaultdict(lambda: {"h1": 0, "h3": 0, "cnt": 0})

    for date in dates:
        old_path = PRED_DIR / f"{date}_pred_before_phase2a.json"
        new_path = PRED_DIR / f"{date}_pred.json"
        with open(old_path, "r", encoding="utf-8") as f:
            old_data = json.load(f)
        with open(new_path, "r", encoding="utf-8") as f:
            new_data = json.load(f)

        old_s = analyze_pred(old_data, conn)
        new_s = analyze_pred(new_data, conn)

        # 集計マッピング (analyze_pred の出力キー → old_total/new_total のキー)
        _map = {"honmei_1st": "h1", "honmei_2nd": "h2", "honmei_3rd": "h3",
                "honmei_total": "cnt", "bet": "bet", "payout": "payout",
                "races": "races", "roi_races": "roi_races", "hits": "hits"}
        for src, dst in _map.items():
            old_total[dst] += old_s[src]
            new_total[dst] += new_s[src]

        # 会場別
        for v, vs in old_s["venue_stats"].items():
            for k3 in ["h1", "h3", "cnt"]:
                old_venue_stats[v][k3] += vs[k3]
        for v, vs in new_s["venue_stats"].items():
            for k3 in ["h1", "h3", "cnt"]:
                new_venue_stats[v][k3] += vs[k3]

        old_roi = f"{old_s['roi']:>.1f}%" if old_s["bet"] else "N/A"
        new_roi = f"{new_s['roi']:>.1f}%" if new_s["bet"] else "N/A"
        print(f"{date:>10} | {pct(old_s['honmei_1st'], old_s['honmei_total']):>14} | "
              f"{pct(new_s['honmei_1st'], new_s['honmei_total']):>14} | "
              f"{pct(old_s['honmei_3rd'], old_s['honmei_total']):>14} | "
              f"{pct(new_s['honmei_3rd'], new_s['honmei_total']):>14} | "
              f"{old_roi:>8} | {new_roi:>8}")

    print("-" * 100)
    old_roi_t = old_total["payout"] / old_total["bet"] * 100 if old_total["bet"] else 0
    new_roi_t = new_total["payout"] / new_total["bet"] * 100 if new_total["bet"] else 0
    print(f"{'合計':>10} | {pct(old_total['h1'], old_total['cnt']):>14} | "
          f"{pct(new_total['h1'], new_total['cnt']):>14} | "
          f"{pct(old_total['h3'], old_total['cnt']):>14} | "
          f"{pct(new_total['h3'], new_total['cnt']):>14} | "
          f"{old_roi_t:>7.1f}% | {new_roi_t:>7.1f}%")

    # 改善幅
    d_h1 = (new_total["h1"] / new_total["cnt"] - old_total["h1"] / old_total["cnt"]) * 100 if old_total["cnt"] and new_total["cnt"] else 0
    d_h3 = (new_total["h3"] / new_total["cnt"] - old_total["h3"] / old_total["cnt"]) * 100 if old_total["cnt"] and new_total["cnt"] else 0
    d_roi = new_roi_t - old_roi_t
    print(f"\n  ◎1着率 改善: {d_h1:+.1f}pt")
    print(f"  ◎複勝率 改善: {d_h3:+.1f}pt")
    print(f"  三連複 ROI 改善: {d_roi:+.1f}pt")
    print(f"  対象レース数: {old_total['races']}R (◎{old_total['cnt']}頭) / ROI対象: {old_total['roi_races']}R")

    # 会場別
    print(f"\n{'='*80}")
    print(f"  会場別 ◎1着率")
    print(f"{'='*80}")
    print(f"{'会場':>6} | {'旧◎1着率':>14} | {'新◎1着率':>14} | {'Δ':>6} | {'旧◎複勝率':>14} | {'新◎複勝率':>14}")
    print("-" * 80)
    all_venues = sorted(set(list(old_venue_stats.keys()) + list(new_venue_stats.keys())),
                        key=lambda v: old_venue_stats[v]["cnt"], reverse=True)
    for v in all_venues:
        ov = old_venue_stats[v]
        nv = new_venue_stats[v]
        if ov["cnt"] < 3 and nv["cnt"] < 3:
            continue
        d = ((nv["h1"]/nv["cnt"]) - (ov["h1"]/ov["cnt"])) * 100 if ov["cnt"] and nv["cnt"] else 0
        print(f"{v:>6} | {pct(ov['h1'], ov['cnt']):>14} | {pct(nv['h1'], nv['cnt']):>14} | "
              f"{d:>+5.1f} | {pct(ov['h3'], ov['cnt']):>14} | {pct(nv['h3'], nv['cnt']):>14}")

    conn.close()
    print(f"\n{'='*80}")
    print("完了")


if __name__ == "__main__":
    main()
