"""
pred.json 内の各馬 `pace_estimated_total_time`, `pace_estimated_front3f`, `pace_estimated_mid_sec`
をレース全体の `predicted_race_time` に比例スケールして整合させる。

背景:
- 既存 pred.json は engine.py 旧ロジックで生成されており、個別馬のタイムがレース全体の
  predicted_race_time と整合していない（皐月賞 G1 で個別 2:02 / 全体 1:59.7 の乖離）
- engine.py を修正済だが、pred.json を再生成するには長時間のフルパイプライン実行が必要
- 本スクリプトは JSON を直接リスケールすることで軽量に整合化する（スクレイピング・ML不要）

動作:
- 各レースで (front + mid + last_3f) の合計を predicted_race_time に揃える
- スケール倍率は ±15% にクリップ（暴走防止）
- バックアップは *.bak_tt_rescale で保存

使い方:
    python scripts/rescale_pace_total_time.py --date 2026-04-19
    python scripts/rescale_pace_total_time.py --pred-path data/predictions/20260419_pred.json
    python scripts/rescale_pace_total_time.py --date 2026-04-19 --dry-run
"""
from __future__ import annotations

import argparse
import json
import shutil
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import List

ROOT = Path(__file__).resolve().parent.parent


def _fmt_hhmmss(sec: float) -> str:
    if sec < 0 or sec != sec:
        sec = 0
    sec = int(sec)
    h, rem = divmod(sec, 3600)
    m, s = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def _progress(label: str, cur: int, total: int, t0: float) -> None:
    elapsed = time.time() - t0
    rate = cur / elapsed if elapsed > 0 else 0
    remain = (total - cur) / rate if rate > 0 else 0
    pct = (cur / total * 100) if total > 0 else 0
    filled = int((cur / total if total else 0) * 20)
    bar = "[" + "■" * filled + "□" * (20 - filled) + "]"
    print(
        f"{label} {bar} {cur}/{total} {pct:5.1f}% "
        f"経過 {_fmt_hhmmss(elapsed)} / 残り {_fmt_hhmmss(remain)}",
        flush=True,
    )


def resolve_pred_path(args: argparse.Namespace) -> Path:
    if args.pred_path:
        return Path(args.pred_path)
    if args.date:
        d = datetime.strptime(args.date, "%Y-%m-%d").strftime("%Y%m%d")
        return ROOT / "data" / "predictions" / f"{d}_pred.json"
    raise SystemExit("--date または --pred-path を指定してください")


def rescale_race(race: dict) -> int:
    """1レース分を書き換え。書き換えた馬数を返す

    設計:
    - レース内全馬の total_time の中央値を算出
    - scale = predicted_race_time / median_total
    - 全馬の front3f/mid_sec/last3f/total に同じ scale を適用（馬間差は相対的に維持）
    - scale は ±15% にクリップ
    """
    target_tt = race.get("predicted_race_time")
    if not target_tt or target_tt <= 0:
        return 0

    horses: List[dict] = race.get("horses") or []
    if not horses:
        return 0

    # 全馬の total_time を収集
    totals = []
    for h in horses:
        tt = h.get("pace_estimated_total_time")
        if tt and tt > 0:
            try:
                totals.append(float(tt))
            except (TypeError, ValueError):
                pass

    if not totals:
        return 0

    # median（異常値に強い）
    totals_sorted = sorted(totals)
    median_total = totals_sorted[len(totals_sorted) // 2]
    if median_total <= 0:
        return 0

    sc = target_tt / median_total
    sc = max(0.85, min(1.15, sc))

    # 全馬に同じ scale 適用
    changed = 0
    for h in horses:
        f3f = h.get("pace_estimated_front3f")
        mid = h.get("pace_estimated_mid_sec")
        l3f = h.get("pace_estimated_last3f")
        tt = h.get("pace_estimated_total_time")
        if f3f is None or mid is None or l3f is None or tt is None:
            continue
        try:
            f3f = float(f3f); mid = float(mid); l3f = float(l3f); tt = float(tt)
        except (TypeError, ValueError):
            continue
        h["pace_estimated_front3f"] = round(f3f * sc, 2)
        h["pace_estimated_mid_sec"] = round(mid * sc, 2)
        h["pace_estimated_last3f"] = round(l3f * sc, 2)
        h["pace_estimated_total_time"] = round(tt * sc, 2)
        changed += 1
    return changed


def main() -> int:
    ap = argparse.ArgumentParser(description="pred.json のタイムをレース全体に整合化")
    ap.add_argument("--date", type=str, default="")
    ap.add_argument("--pred-path", type=str, default="")
    ap.add_argument("--dry-run", action="store_true", help="保存しない")
    args = ap.parse_args()

    pred_path = resolve_pred_path(args)
    if not pred_path.exists():
        print(f"[ERROR] 存在しません: {pred_path}")
        return 1

    print("=" * 72)
    print(f"pred.json タイム整合スクリプト")
    print(f"対象: {pred_path}")
    if args.dry_run:
        print("(DRY-RUN)")
    print("=" * 72)

    data = json.loads(pred_path.read_text(encoding="utf-8"))
    races: List[dict] = data.get("races") or []
    if not races:
        print("[WARN] races 空")
        return 0

    t0 = time.time()
    total_changed = 0
    total_horses = 0
    for i, r in enumerate(races, 1):
        if r.get("is_banei"):
            continue
        ch = rescale_race(r)
        total_changed += ch
        total_horses += len(r.get("horses") or [])
        if i <= 5 or i % 50 == 0 or i == len(races):
            venue = r.get("venue", "")
            rno = r.get("race_no", "")
            print(
                f"  [{i:>3}/{len(races)}] {venue} {rno}R  predicted={r.get('predicted_race_time')}  "
                f"書換={ch}/{len(r.get('horses') or [])}",
                flush=True,
            )
        if i % 10 == 0 or i == len(races):
            _progress("進捗:", i, len(races), t0)

    print()
    print("=" * 72)
    print(f"書き換え: {total_changed} 頭 / 全 {total_horses} 頭")

    if args.dry_run:
        print("(DRY-RUN: 保存なし)")
        return 0

    # バックアップ
    bak = pred_path.with_suffix(pred_path.suffix + ".bak_tt_rescale")
    try:
        shutil.copy2(pred_path, bak)
        print(f"バックアップ: {bak}")
    except Exception as e:
        print(f"[WARN] バックアップ失敗: {e}")

    pred_path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(f"保存: {pred_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
