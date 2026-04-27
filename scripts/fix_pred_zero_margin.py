"""
pred.json の past_3_runs[].margin が「fin > 1 かつ margin = 0」の不正データを null 化する。

背景:
- NAR 岩手系（venue 36 水沢）race_results に着差データ欠損 → race_log.margin_ahead=0 残存
- pred.json 側の past_3_runs[].margin = 0 で「+0.0 (4着)」と誤表示
- 1着以外で margin=0 は理論的に同タイム (極稀) → 着差不明として表示すべき

動作:
- past_3_runs[].finish_pos > 1 かつ past_3_runs[].margin in (0, 0.0, None ?) を null 化
- finish_pos == 1 の margin=0 は正常値なので保持
- 1 つ以上更新があれば pred.json を上書き保存（バックアップ自動作成）

使い方:
    python scripts/fix_pred_zero_margin.py             # 当日 pred.json
    python scripts/fix_pred_zero_margin.py --recent 7  # 直近 7 日
    python scripts/fix_pred_zero_margin.py --dry-run
"""
from __future__ import annotations
import argparse, json, shutil, sys
from datetime import date, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PRED_DIR = PROJECT_ROOT / "data" / "predictions"


def fix_one(path: Path, dry_run: bool) -> dict:
    if not path.exists():
        return {"date": path.stem, "status": "missing"}
    with open(path, encoding="utf-8") as f:
        pred = json.load(f)
    fixed = 0
    total_runs = 0
    for r in pred.get("races", []):
        for h in r.get("horses", []):
            for past in (h.get("past_3_runs") or h.get("past_runs") or []):
                total_runs += 1
                fp = past.get("finish_pos") or 0
                m = past.get("margin")
                # finish_pos>1 かつ margin が 0 (or 0.0) の場合は null 化
                if isinstance(m, (int, float)) and m == 0 and fp > 1:
                    past["margin"] = None
                    fixed += 1
    if not dry_run and fixed > 0:
        ts = date.today().strftime("%Y%m%d")
        bak = path.with_suffix(f".json.bak_zeromargin_{ts}")
        if not bak.exists():
            shutil.copy(path, bak)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(pred, f, ensure_ascii=False, separators=(",", ":"))
    return {
        "date": path.stem.replace("_pred", ""),
        "status": "ok",
        "total_runs": total_runs,
        "fixed": fixed,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("date", nargs="?", default=date.today().strftime("%Y%m%d"))
    parser.add_argument("--recent", type=int, default=0)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    targets = []
    if args.recent > 0:
        today = date.today()
        for i in range(args.recent):
            d = today - timedelta(days=i)
            targets.append(PRED_DIR / f"{d.strftime('%Y%m%d')}_pred.json")
    else:
        targets.append(PRED_DIR / f"{args.date}_pred.json")

    print(f"[INFO] 対象 {len(targets)} ファイル ({'DRY-RUN' if args.dry_run else '本実行'})")
    total_fixed = 0
    for p in targets:
        result = fix_one(p, args.dry_run)
        if result["status"] == "missing":
            print(f"  [SKIP] {result['date']}")
            continue
        print(f"  [{result['date']}] runs={result['total_runs']} fixed={result['fixed']}")
        total_fixed += result["fixed"]
    print(f"\n[完了] 合計 fixed={total_fixed}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
