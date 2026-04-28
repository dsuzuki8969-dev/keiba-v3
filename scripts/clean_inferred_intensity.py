"""過去 pred.json の勝手に推定された intensity_label を削除

マスター指示 2026-04-23 v6.0.1:
  「ブックに無いのに『極軽め』等を勝手に付けるな」

方針:
  - training_records[*].intensity_label について、確証のあるソース判定が困難なため
    全件を一旦クリアせず、特徴的な「推定漏れ」だけを扱う。
  - 今日の pred.json は再スクレイプで正しい値に入れ替えるのが正道。
  - ただし緊急対応として、以下の条件下のみ intensity_label を空文字化する:
    * NAR レースで intensity_label が "極軽め" か "軽め" で、かつ stable_comment/comment が短くブック原文にラベル語が含まれない

実際には「推定値だったかどうか」を pred.json から完全復元できないため、
v6.0.1 では 今日 (2026-04-23) の NAR 全レースの training_records を
再スクレイプ＋上書き（修正済 keibabook_training.py を使って）するアプローチを取る。

使い方:
  python scripts/clean_inferred_intensity.py 20260423       # 再スクレイプで上書き
  python scripts/clean_inferred_intensity.py 20260423 --dry # 現状表示のみ
"""
from __future__ import annotations
import io, json, sys, time
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
sys.path.insert(0, ".")


def main() -> None:
    args = sys.argv[1:]
    dry = "--dry" in args
    args = [a for a in args if a != "--dry"]
    date_key = args[0].replace("-", "") if args else time.strftime("%Y%m%d")
    date_iso = f"{date_key[:4]}-{date_key[4:6]}-{date_key[6:8]}"

    fp = Path(f"data/predictions/{date_key}_pred.json")
    if not fp.exists():
        print(f"pred not found: {fp}")
        return
    pred = json.loads(fp.read_text(encoding="utf-8"))

    # 現状集計
    intensity_counts: dict = {}
    for r in pred.get("races", []):
        for h in r.get("horses", []):
            for tr in h.get("training_records") or []:
                lbl = tr.get("intensity_label", "")
                intensity_counts[lbl] = intensity_counts.get(lbl, 0) + 1
    print("現状 intensity_label 分布:")
    for k, v in sorted(intensity_counts.items(), key=lambda x: -x[1]):
        print(f"  {repr(k):20s}: {v}")

    if dry:
        return

    # NAR レースのみ再スクレイプで上書き（keibabook_training 修正済）
    from src.scraper.keibabook_training import (
        KeibabookTrainingScraper, KeibabookClient, _NAR_TRAINING_SUPPORTED,
    )
    client = KeibabookClient()
    scraper = KeibabookTrainingScraper(client)

    updated = 0
    t0 = time.time()
    for r in pred.get("races", []):
        rid = str(r.get("race_id", ""))
        if len(rid) < 12:
            continue
        vc = rid[4:6]
        if vc not in _NAR_TRAINING_SUPPORTED:
            continue
        horses = [h for h in r.get("horses", []) if not h.get("is_scratched")]
        if not horses:
            continue
        try:
            tmap = scraper.fetch(rid, date_iso)
        except Exception as e:
            print(f"  ERR {r.get('venue')}{r.get('race_no')}R: {e}")
            continue
        if not tmap:
            continue
        by_name = {h.get("horse_name"): h for h in horses}
        for name, recs in tmap.items():
            h = by_name.get(name)
            if not h or not recs:
                continue
            new_tr_list = []
            for rec in recs:
                d = {}
                for attr in ("date", "venue", "course", "splits", "partner",
                             "position", "rider", "track_condition", "lap_count",
                             "intensity_label", "sigma_from_mean", "comment",
                             "stable_comment"):
                    if hasattr(rec, attr):
                        d[attr] = getattr(rec, attr)
                new_tr_list.append(d)
            if new_tr_list:
                h["training_records"] = new_tr_list
                updated += 1
        print(f"  {r.get('venue')}{r.get('race_no')}R: 再スクレイプ完了", flush=True)

    # 書き戻し
    fp.write_text(json.dumps(pred, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    print(f"\n完了 ({time.time()-t0:.1f}s): {updated} 馬更新")


if __name__ == "__main__":
    main()
