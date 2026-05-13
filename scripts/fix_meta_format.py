"""pred.json の _meta.format / confidence を修正する軽量スクリプト

WFパッチで再生成された tickets に _meta.format が欠落しており、
ダッシュボードの M' 戦略集計 (hybrid_summary._is_m_prime_pred) が
全レースをスキップしていた問題を修正する。
"""
import json
import os
import sys
from glob import glob

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PREDICTIONS_DIR = os.path.join(PROJECT_ROOT, "data", "predictions")

M_PRIME_FORMAT = "M': 自信度別 三連複 (SS=E/S=C/A=C/B/C/D=D/E=skip)"
PATTERN_MAP = {"SS": "E", "S": "C", "A": "C", "B": "D", "C": "D", "D": "D", "E": "skip"}


def fix_file(fpath: str, dry_run: bool = False) -> dict:
    with open(fpath, "r", encoding="utf-8") as f:
        data = json.load(f)

    fixed = 0
    skipped = 0

    for race in data.get("races", []):
        tbm = race.get("tickets_by_mode", {}) or {}
        meta = tbm.get("_meta", {}) or {}
        fmt = meta.get("format", "") or ""

        # 既に M' format が設定済みならスキップ
        if fmt.startswith("M'"):
            skipped += 1
            continue

        # tickets に三連複があるか確認
        tix = race.get("tickets", []) or []
        has_sanren = any(t.get("type") == "三連複" for t in tix)
        if not has_sanren:
            continue

        # confidence 推定: tickets[0].pattern → "M'-X" → X から逆引き
        confidence = ""
        # 1. 既存の overall_confidence
        confidence = race.get("overall_confidence", "") or race.get("confidence", "") or ""
        # 2. tickets の pattern から推定
        if not confidence:
            for t in tix:
                pat = t.get("pattern", "")
                if pat.startswith("M'-"):
                    code = pat.split("-", 1)[1]
                    rev_map = {"E": "SS", "C": "A", "D": "B"}
                    confidence = rev_map.get(code, "B")
                    break
        if not confidence:
            confidence = "B"

        pat = PATTERN_MAP.get(confidence, "D")

        # _meta を更新
        if not tbm:
            tbm = {}
            race["tickets_by_mode"] = tbm
        if "_meta" not in tbm:
            tbm["_meta"] = {}

        tbm["_meta"]["format"] = M_PRIME_FORMAT
        tbm["_meta"]["confidence"] = confidence
        tbm["_meta"]["pattern"] = pat
        if "skipped" not in tbm["_meta"]:
            tbm["_meta"]["skipped"] = pat == "skip"

        # overall_confidence も設定
        if not race.get("overall_confidence"):
            race["overall_confidence"] = confidence

        fixed += 1

    if fixed > 0 and not dry_run:
        with open(fpath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, separators=(",", ":"))

    return {"fixed": fixed, "skipped": skipped}


def main():
    import argparse
    parser = argparse.ArgumentParser(description="pred.json _meta.format 修正")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    targets = sorted(glob(os.path.join(PREDICTIONS_DIR, "*_pred.json")))
    targets = [f for f in targets if "_prev" not in os.path.basename(f)]
    print(f"対象: {len(targets)} ファイル")

    total_fixed = 0
    total_skipped = 0
    files_modified = 0

    for i, fpath in enumerate(targets):
        result = fix_file(fpath, dry_run=args.dry_run)
        total_fixed += result["fixed"]
        total_skipped += result["skipped"]
        if result["fixed"] > 0:
            files_modified += 1

        if (i + 1) % 100 == 0 or i == len(targets) - 1:
            pct = (i + 1) / len(targets) * 100
            print(f"  [{i+1}/{len(targets)}] {pct:.0f}% -修正={total_fixed:,} レース, "
                  f"スキップ={total_skipped:,}, ファイル変更={files_modified}")

    print(f"\n完了: {total_fixed:,} レース修正, {files_modified} ファイル変更"
          f"{' (dry-run)' if args.dry_run else ''}")


if __name__ == "__main__":
    main()
