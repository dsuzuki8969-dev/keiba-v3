"""
取消馬を含む formation_columns / tickets を pred.json から浄化する

results_tracker.py と同じロジックを既存の pred.json に適用する。
フルパイプライン再実行を回避し、JSON レベルで修正のみ行う。
"""
from __future__ import annotations

import json
import sys
from pathlib import Path


def _ticket_has_scratched(t: dict, scratched: set[int]) -> bool:
    """チケット t が取消馬を含むか判定"""
    for k in ("a", "b", "c"):
        v = t.get(k)
        if isinstance(v, int) and v in scratched:
            return True
        if isinstance(v, list) and any(x in scratched for x in v):
            return True
    combo = t.get("combo") or ""
    if isinstance(combo, str) and combo:
        for token in combo.replace("=", "-").split("-"):
            try:
                if int(token) in scratched:
                    return True
            except (ValueError, TypeError):
                pass
    return False


def cleanup_race(race: dict) -> tuple[int, int]:
    """1 レースから取消馬を含むチケットを除去。
    戻り値: (除去した ticket 数, 浄化対象馬数)
    """
    horses = race.get("horses", [])
    scratched = {h["horse_no"] for h in horses if h.get("is_scratched")}
    if not scratched:
        return 0, 0

    # formation_columns から取消馬を除外
    fc = race.get("formation_columns") or {}
    for col_key in ("col1", "col2", "col3"):
        col = fc.get(col_key)
        if isinstance(col, list):
            fc[col_key] = [h for h in col if h not in scratched]
    col1_empty = not (fc.get("col1") or [])
    if race.get("formation_columns") is not None:
        race["formation_columns"] = fc

    removed = 0
    # tickets_by_mode 浄化
    tbm = race.get("tickets_by_mode")
    if isinstance(tbm, dict):
        for mode_k, ts in list(tbm.items()):
            if isinstance(ts, list):
                new_ts = [] if col1_empty else [
                    t for t in ts if not _ticket_has_scratched(t, scratched)
                ]
                removed += len(ts) - len(new_ts)
                tbm[mode_k] = new_ts

    # formation 浄化
    form = race.get("formation")
    if isinstance(form, dict):
        for k, ts in list(form.items()):
            if isinstance(ts, list):
                new_ts = [] if col1_empty else [
                    t for t in ts if not _ticket_has_scratched(t, scratched)
                ]
                removed += len(ts) - len(new_ts)
                form[k] = new_ts

    # bet_decision を「取消により買い目無効」に更新
    has_any_ticket = any(
        (tbm.get(m) if isinstance(tbm, dict) else None)
        for m in ("accuracy", "balanced", "recovery")
    )
    if col1_empty or not has_any_ticket:
        bd = race.get("bet_decision") or {}
        if isinstance(bd, dict):
            bd["skip"] = True
            bd["skip_reasons"] = list(set((bd.get("skip_reasons") or []) + ["scratched"]))
            bd["message"] = "取消馬により買い目無効"
            race["bet_decision"] = bd

    return removed, len(scratched)


def main() -> int:
    if len(sys.argv) < 2:
        print("使い方: python scripts/cleanup_scratched_tickets.py <pred.json のパス>")
        return 1

    path = Path(sys.argv[1])
    if not path.exists():
        print(f"ファイルが存在しません: {path}")
        return 1

    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    total_removed = 0
    affected_races = 0
    for race in data.get("races", []):
        removed, scratched = cleanup_race(race)
        if scratched:
            affected_races += 1
            if removed:
                print(
                    f"  {race.get('venue', '?')} {race.get('race_no', '?')}R: "
                    f"取消 {scratched} 頭, 除去 {removed} 点"
                )
            total_removed += removed

    # バックアップ
    backup_path = path.with_suffix(".json.scratched_backup")
    with backup_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    # 上書き保存
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"\n[OK] 取消馬を含むレース: {affected_races}, 除去チケット合計: {total_removed}")
    print(f"バックアップ: {backup_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
