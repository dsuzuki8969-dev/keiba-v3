"""
pred.json に欠損した race を NAR official scraper で個別取得して追加するスクリプト。

【背景】
2026-05-06: 5/5 17:00 predict_tomorrow_runner 実行中 WinError 32 等で
門別 R5 が pred.json から欠落。
2026-05-07: 同様に 5/6 17:00 で船橋 R2-R4 が欠落。
race_log にも結果未取得 (= run_analysis_date.py 経由では取れない)。

【検証】
個別 OfficialNARScraper.get_full_entry() 呼び出しでは正常取得可能と確認。
→ 直接呼び出しで pred.json に新規 race として追加する。

【使い方】
    python scripts/repair_missing_races.py [--dry-run]

【補完対象】
    TARGETS リスト で hard-code (date, baba_code, race_no, venue, vc)

【検証】
- pred.json mtime 更新
- 該当 race_id が pred.races に追加される
- 馬データ scrape_failed=True (= 予想生成不可フラグ)
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.scraper.official_nar import OfficialNARScraper

# (date, baba_code, race_no, venue_name, netkeiba_vc)
TARGETS = [
    ("2026-05-06", "36", 5, "門別", "30"),
    ("2026-05-07", "19", 2, "船橋", "43"),
    ("2026-05-07", "19", 3, "船橋", "43"),
    ("2026-05-07", "19", 4, "船橋", "43"),
]


def repair(dry_run: bool = False) -> int:
    s = OfficialNARScraper()
    count = 0
    for date, baba, rno, venue, vc in TARGETS:
        race_date = date.replace("-", "/")
        date_key = date.replace("-", "")
        pred_path = PROJECT_ROOT / "data" / "predictions" / f"{date_key}_pred.json"

        with open(pred_path, encoding="utf-8") as f:
            pred = json.load(f)

        rid = f"2026{vc}{date_key[4:8]}{rno:02d}"

        if any(r.get("race_id") == rid for r in pred["races"] if isinstance(r, dict)):
            print(f"[SKIP] {date} {venue} R{rno} ({rid}): 既存")
            continue

        try:
            r_info, horses = s.get_full_entry(race_date, rno, baba)
        except Exception as e:
            print(f"[ERR]  {date} {venue} R{rno}: {e}")
            continue

        if not horses or len(horses) < 2:
            print(f"[ERR]  {date} {venue} R{rno}: 馬数不足 {len(horses) if horses else 0}")
            continue

        horse_list = [
            {
                "horse_no": getattr(h, "horse_no", 0),
                "horse_id": getattr(h, "horse_id", "") or "",
                "horse_name": getattr(h, "horse_name", "") or getattr(h, "name", "") or "",
                "jockey_name": getattr(h, "jockey_name", "") or getattr(h, "jockey", "") or "",
                "sex": getattr(h, "sex", "") or "",
                "age": getattr(h, "age", 0),
                "odds": getattr(h, "odds", None) or getattr(h, "tansho_odds", None),
                "win_prob": 0.0,
                "place2_prob": 0.0,
                "place3_prob": 0.0,
                "composite": 0.0,
                "mark": "",
                "scrape_failed": True,
                "repair_source": "individual_scrape",
            }
            for h in horses
        ]

        new_race = {
            "race_id": rid,
            "venue": venue,
            "venue_code": vc,
            "race_no": rno,
            "race_name": getattr(r_info, "race_name", "") if r_info else "",
            "distance": getattr(r_info, "distance", 0) if r_info else 0,
            "surface": getattr(r_info, "surface", "") if r_info else "",
            "horses": horse_list,
            "scrape_failed": True,
            "repair_at": datetime.now().isoformat(),
        }

        if dry_run:
            print(f"[DRY]  {date} {venue} R{rno} ({rid}): 追加予定 {len(horse_list)} 頭")
        else:
            pred["races"].append(new_race)
            with open(pred_path, "w", encoding="utf-8") as f:
                json.dump(pred, f, ensure_ascii=False, indent=2)
            print(f"[OK]   {date} {venue} R{rno} ({rid}): 追加 {len(horse_list)} 頭")
        count += 1

    return count


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    n = repair(dry_run=args.dry_run)
    print(f"\n[DONE] 補完 {n} 件")
