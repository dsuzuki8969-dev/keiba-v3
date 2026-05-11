"""
pred.json の 5 頭未満レースを race_log から補完するスクリプト。

【背景】
出馬表 scraper の構造的バグで、5/5 等で 3 頭立て pred.json が生成された。
過去日アクセスでは scraper が結果ページを参照し 3 頭しか取れない。
race_log には完全な 12 頭立てデータあり → ここから馬名・騎手等を補完する。

【方針】
- pred.json の各 race で len(horses) < 5 をチェック
- race_log から完全データ取得 (12-14 頭立て)
- 馬名・騎手・馬番・odds・finish_pos のみ補完
- 予想値 (win_prob / mark / composite 等) は付与せず empty (=「予想生成不可」として明示)
- LIVE STATS の異常集計を防ぐため `scrape_failed: true` フラグ付与

【使い方】
    python scripts/repair_pred_from_race_log.py 2026-05-05
    python scripts/repair_pred_from_race_log.py 2026-05-05 --dry-run

【検証】
- pred.json mtime 更新
- 5 頭未満が 0 になることを確認
- 補完馬には scrape_failed=True フラグ
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import DATABASE_PATH


def repair(date: str, dry_run: bool = False) -> int:
    """指定日の pred.json を race_log から補完する。

    Args:
        date: "YYYY-MM-DD"
        dry_run: True なら書き込まず差分だけ表示

    Returns:
        補完したレース数
    """
    date_key = date.replace("-", "")
    pred_path = PROJECT_ROOT / "data" / "predictions" / f"{date_key}_pred.json"
    if not pred_path.exists():
        print(f"[ERR] pred.json なし: {pred_path}")
        return 0

    with open(pred_path, encoding="utf-8") as f:
        pred = json.load(f)

    races = pred.get("races", [])
    target = [r for r in races if isinstance(r, dict) and len(r.get("horses", [])) < 5]
    if not target:
        print(f"[OK] {date}: 5 頭未満なし (修復対象 0 件)")
        return 0

    print(f"[INFO] {date}: 5 頭未満 {len(target)} 件 を race_log から補完")

    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    repaired = 0
    skipped = 0

    for race in target:
        rid = race.get("race_id")
        if not rid:
            skipped += 1
            continue

        # race_log から馬一覧取得
        c.execute(
            """
            SELECT horse_no, horse_id, horse_name, jockey_name, finish_pos,
                   win_odds, tansho_odds
            FROM race_log
            WHERE race_id = ?
            ORDER BY horse_no
            """,
            (rid,),
        )
        log_rows = c.fetchall()
        if len(log_rows) < 5:
            print(f"  [SKIP] {rid}: race_log {len(log_rows)} 頭 (補完不可)")
            skipped += 1
            continue

        # 既存 pred.json horses の horse_no セット (重複防止)
        existing_no = {h.get("horse_no") for h in race.get("horses", [])}

        # race_log の不足馬を補完 (既存 horse_no は維持・追加のみ)
        added = 0
        for row in log_rows:
            if row["horse_no"] in existing_no:
                continue
            stub = {
                "horse_no": row["horse_no"],
                "horse_id": row["horse_id"] or "",
                "horse_name": row["horse_name"] or "",
                "jockey_name": row["jockey_name"] or "",
                "odds": row["tansho_odds"] or row["win_odds"],
                "finish_pos": row["finish_pos"],
                # 予想値は付与しない (= empty)
                "win_prob": 0.0,
                "place2_prob": 0.0,
                "place3_prob": 0.0,
                "composite": 0.0,
                "mark": "",
                # 補完フラグ (LIVE STATS 集計除外用)
                "scrape_failed": True,
                "repair_source": "race_log",
                "repair_at": datetime.now().isoformat(),
            }
            race["horses"].append(stub)
            added += 1

        if added > 0:
            # レースレベルでも scrape_failed フラグ付与 (LIVE STATS 除外用)
            race["scrape_failed"] = True
            race["repair_at"] = datetime.now().isoformat()
            race["repair_added_horses"] = added
            print(f"  [OK]   {rid}: 追加 {added} 頭 (元 {len(existing_no)} → 計 {len(race['horses'])} 頭)")
            repaired += 1

    conn.close()

    if dry_run:
        print(f"\n[DRY-RUN] 補完 {repaired} 件 / skip {skipped} 件 (書き込みなし)")
        return repaired

    if repaired > 0:
        # composite=0 のスタブ馬を偏差値ベースで修復
        from src.results_tracker import heal_zero_composite_races
        heal_zero_composite_races(pred)

        # バックアップ取って書き込み
        backup = pred_path.with_suffix(".json.bak.repair")
        with open(backup, "w", encoding="utf-8") as f:
            json.dump(pred, f, ensure_ascii=False, indent=2)
        # 元ファイル更新時刻を新しく
        pred["repaired_at"] = datetime.now().isoformat()
        with open(pred_path, "w", encoding="utf-8") as f:
            json.dump(pred, f, ensure_ascii=False, indent=2)
        print(f"\n[DONE] {pred_path}: 補完 {repaired} 件 / skip {skipped} 件 / バックアップ {backup.name}")

    return repaired


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("date", help="YYYY-MM-DD")
    ap.add_argument("--dry-run", action="store_true", help="差分のみ表示・書き込まず")
    args = ap.parse_args()

    repair(args.date, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
