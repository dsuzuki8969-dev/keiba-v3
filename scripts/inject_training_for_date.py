"""指定日の全 NAR 対応レースで training_records（調教 + 厩舎コメント）を取得し
pred.json に直接注入する緊急 backfill スクリプト。

マスター指示 2026-04-23: 園田で反映されていない問題の対応。
  venue_code 49(園田新) が _NAR_TRAINING_SUPPORTED に未登録だったバグを修正。
  このスクリプトで今日分を直ちに再取得。

使い方:
  python scripts/inject_training_for_date.py 20260423
"""
from __future__ import annotations
import io, json, sys, time
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
sys.path.insert(0, ".")

from src.scraper.keibabook_training import (
    KeibabookTrainingScraper, KeibabookClient, _NAR_TRAINING_SUPPORTED,
)
from src.models import TrainingRecord


def rec_to_dict(r: TrainingRecord) -> dict:
    """TrainingRecord → dict（pred.json 用）"""
    if r is None:
        return {}
    d = {}
    for attr in ("date", "venue", "course", "splits", "partner", "position",
                 "rider", "track_condition", "lap_count", "intensity_label",
                 "sigma_from_mean", "comment", "stable_comment"):
        if hasattr(r, attr):
            d[attr] = getattr(r, attr)
    return d


def main() -> None:
    if len(sys.argv) < 2:
        print("使い方: python scripts/inject_training_for_date.py YYYYMMDD")
        return
    date_key = sys.argv[1].replace("-", "")
    date_iso = f"{date_key[:4]}-{date_key[4:6]}-{date_key[6:8]}"
    fp = Path(f"data/predictions/{date_key}_pred.json")
    if not fp.exists():
        print(f"pred not found: {fp}")
        return

    pred = json.loads(fp.read_text(encoding="utf-8"))
    client = KeibabookClient()
    scraper = KeibabookTrainingScraper(client)

    stats = {"races_processed": 0, "races_got_data": 0, "horses_updated": 0}
    t0 = time.time()

    for r in pred.get("races", []):
        rid = str(r.get("race_id", ""))
        if not rid or len(rid) < 12:
            continue
        vc = rid[4:6]
        if vc not in _NAR_TRAINING_SUPPORTED:
            continue
        # training_records が既に埋まってる馬が 50% 以上ならスキップ
        horses = [h for h in r.get("horses", []) if not h.get("is_scratched")]
        if not horses:
            continue
        has_tr = sum(1 for h in horses if h.get("training_records"))
        if has_tr / len(horses) > 0.5:
            continue  # 既に取得済
        stats["races_processed"] += 1
        venue = r.get("venue", "?")
        rno = r.get("race_no", 0)
        print(f"  処理中: {venue}{rno}R ({rid})", flush=True)
        try:
            tmap = scraper.fetch(rid, date_iso)
        except Exception as e:
            print(f"    ERR: {e}")
            continue
        if not tmap:
            continue
        stats["races_got_data"] += 1
        # 馬名でマッチング
        by_name = {h.get("horse_name"): h for h in horses}
        for name, records in tmap.items():
            h = by_name.get(name)
            if not h:
                continue
            # dict 化
            tr_list = [rec_to_dict(x) for x in records]
            tr_list = [x for x in tr_list if x]
            if tr_list:
                h["training_records"] = tr_list
                stats["horses_updated"] += 1

    # 書き戻し
    fp.write_text(json.dumps(pred, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    el = time.time() - t0
    print(f"\n完了 {el:.1f}秒")
    print(f"  対象レース: {stats['races_processed']}")
    print(f"  データ取得成功: {stats['races_got_data']}")
    print(f"  馬数更新: {stats['horses_updated']}")


if __name__ == "__main__":
    main()
