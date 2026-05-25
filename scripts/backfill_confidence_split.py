"""
過去データ backfill: tansho_confidence / sanrenpuku_confidence を一括計算・更新

対象:
  1. predictions テーブルの全行 → horses_json + tickets_json から新スコアを計算し UPDATE
  2. data/predictions/*_pred.json → 各レースに tansho_confidence / sanrenpuku_confidence を追記

Usage:
    python scripts/backfill_confidence_split.py
"""
import json
import sqlite3
import sys
import glob
from math import comb
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

DB_PATH = Path("data/keiba.db")

# 閾値（betting.py と同じ値）
TANSHO_TH = {"SS": 0.78, "S": 0.55, "A": 0.42, "B": 0.30, "C": 0.20, "D": 0.08}
SANREN_TH = {"SS": 0.75, "S": 0.62, "A": 0.55, "B": 0.50, "C": 0.47, "D": 0.44}


def calc_tansho_score(horses: list[dict]) -> float:
    """単勝自信度スコア v2 (horses_json の dict リスト用)"""
    if len(horses) < 2:
        return 0.5
    by_shobu = sorted(horses, key=lambda h: h.get("shobu_score", 0), reverse=True)
    by_comp = sorted(horses, key=lambda h: h.get("composite", 0), reverse=True)
    comp_top3_nos = {h.get("horse_no") for h in by_comp[:3]}
    shobu_top2_nos = {by_shobu[0].get("horse_no"), by_shobu[1].get("horse_no")}
    wp = by_shobu[0].get("win_prob", 0)
    wp_norm = min(wp / 0.25, 1.0)
    agreement = len(shobu_top2_nos & comp_top3_nos) / 2.0
    by_wp = sorted(horses, key=lambda h: h.get("win_prob", 0), reverse=True)
    wp_2nd = by_wp[1].get("win_prob", 0) if len(by_wp) >= 2 else 0
    wp_gap = wp - wp_2nd
    dominance = min(wp_gap / 0.12, 1.0) if wp_gap > 0 else 0.0
    shobu1 = by_shobu[0].get("shobu_score", 0)
    shobu2 = by_shobu[1].get("shobu_score", 0)
    shobu_gap = shobu1 - shobu2
    gap_norm = min(shobu_gap / 3.0, 1.0) if shobu_gap > 0 else 0.0
    return wp_norm * 0.35 + agreement * 0.30 + dominance * 0.20 + gap_norm * 0.15


def calc_sanren_score(horses: list[dict], tickets: list[dict]) -> float:
    """三連複自信度スコア v3 (horses_json + tickets_json の dict リスト用)"""
    n = len(horses)
    if n < 4:
        return 0.10 if n >= 3 else 0.0
    by_comp = sorted(horses, key=lambda h: h.get("composite", 0), reverse=True)
    by_wp = sorted(horses, key=lambda h: h.get("win_prob", 0), reverse=True)
    top3_nos = {h.get("horse_no") for h in by_comp[:3]}
    comp3 = by_comp[2].get("composite", 0)
    comp4 = by_comp[3].get("composite", 0)
    wall = comp3 - comp4
    wall_norm = min(wall / 10.0, 1.0) if wall > 0 else 0.0
    total_wp = sum(h.get("win_prob", 0) for h in horses)
    top3_wp = sum(h.get("win_prob", 0) for h in horses if h.get("horse_no") in top3_nos)
    share = (top3_wp / total_wp) if total_wp > 0 else 0
    share_norm = min(share / 0.60, 1.0)
    ml_top3_nos = {h.get("horse_no") for h in by_wp[:3]}
    ml_agreement = len(top3_nos & ml_top3_nos) / 3.0
    comp5 = by_comp[4].get("composite", 0) if n >= 5 else comp4
    avg_edge = (wall + (comp3 - comp5)) / 2.0
    edge_norm = min(avg_edge / 6.0, 1.0) if avg_edge > 0 else 0.0
    sanren_tix = [t for t in tickets if t.get("type") == "三連複"]
    n_tickets = len(sanren_tix)
    total_combos = comb(n, 3)
    if total_combos > 0 and n_tickets > 0:
        cov_rate = n_tickets / total_combos
        cov_score = max(0, 1.0 - cov_rate / 0.05)
    else:
        cov_score = 0.0
    return (wall_norm * 0.20 + share_norm * 0.25 + ml_agreement * 0.20
            + edge_norm * 0.20 + cov_score * 0.15)


def score_to_level(score: float, thresholds: dict) -> str:
    for lv in ["SS", "S", "A", "B", "C", "D"]:
        if score >= thresholds[lv]:
            return lv
    return "E"


def backfill_db():
    """predictions テーブルの全行を更新"""
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    rows = conn.execute(
        "SELECT id, horses_json, tickets_json FROM predictions"
    ).fetchall()
    total = len(rows)
    print(f"[DB] 対象: {total} 行")

    updated = 0
    for i, row in enumerate(rows):
        horses = json.loads(row["horses_json"]) if row["horses_json"] else []
        tickets = json.loads(row["tickets_json"]) if row["tickets_json"] else []

        t_score = calc_tansho_score(horses)
        s_score = calc_sanren_score(horses, tickets)
        t_lv = score_to_level(t_score, TANSHO_TH)
        s_lv = score_to_level(s_score, SANREN_TH)

        conn.execute(
            "UPDATE predictions SET tansho_confidence=?, sanrenpuku_confidence=? WHERE id=?",
            (t_lv, s_lv, row["id"]),
        )
        updated += 1

        if (i + 1) % 1000 == 0 or i + 1 == total:
            pct = (i + 1) / total * 100
            bar_len = int(pct / 5)
            bar = "█" * bar_len + "░" * (20 - bar_len)
            print(f"  [{bar}] {pct:5.1f}%  ({i+1}/{total})", flush=True)

    conn.commit()
    conn.close()
    print(f"[DB] 完了: {updated} 行更新")


def backfill_pred_json():
    """data/predictions/*_pred.json にフィールド追記"""
    pred_files = sorted(glob.glob("data/predictions/*_pred.json"))
    # _prev ファイルも含む
    total = len(pred_files)
    print(f"\n[JSON] 対象: {total} ファイル")

    updated_files = 0
    updated_races = 0

    for i, pf in enumerate(pred_files):
        try:
            with open(pf, encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, UnicodeDecodeError):
            continue

        races = data.get("races", [])
        if not races:
            continue

        modified = False
        for race in races:
            horses = race.get("horses", [])
            tickets = race.get("tickets", [])

            t_score = calc_tansho_score(horses)
            s_score = calc_sanren_score(horses, tickets)
            t_lv = score_to_level(t_score, TANSHO_TH)
            s_lv = score_to_level(s_score, SANREN_TH)

            if race.get("tansho_confidence") != t_lv or race.get("sanrenpuku_confidence") != s_lv:
                race["tansho_confidence"] = t_lv
                race["sanrenpuku_confidence"] = s_lv
                modified = True
                updated_races += 1

        if modified:
            with open(pf, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=None, separators=(",", ":"))
            updated_files += 1

        if (i + 1) % 200 == 0 or i + 1 == total:
            pct = (i + 1) / total * 100
            bar_len = int(pct / 5)
            bar = "█" * bar_len + "░" * (20 - bar_len)
            print(f"  [{bar}] {pct:5.1f}%  ({i+1}/{total} files, {updated_races} races)", flush=True)

    print(f"[JSON] 完了: {updated_files} ファイル / {updated_races} レース更新")


if __name__ == "__main__":
    # まず ALTER TABLE を実行（カラムが無い場合に備える）
    conn = sqlite3.connect(str(DB_PATH))
    for ddl in [
        "ALTER TABLE predictions ADD COLUMN tansho_confidence TEXT DEFAULT 'B'",
        "ALTER TABLE predictions ADD COLUMN sanrenpuku_confidence TEXT DEFAULT 'B'",
    ]:
        try:
            conn.execute(ddl)
        except sqlite3.OperationalError:
            pass  # 既存カラムならスキップ
    conn.commit()
    conn.close()

    print("=" * 60)
    print("  自信度分離 backfill (tansho / sanrenpuku confidence)")
    print("=" * 60)
    backfill_db()
    backfill_pred_json()
    print("\n✅ 全完了")
