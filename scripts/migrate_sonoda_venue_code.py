"""
園田 venue_code 49 → 50 全期間 pred.json 一括マイグレーション

真因（2026-04-28 根本対策）:
  venue_master.py で園田 = "49" (SPAT4 ベース) で生成していたが、
  netkeiba race_id では園田 = "50" で配信されている。
  → 過去 pred.json の園田 race_id に 49 が含まれるため結果取得不可。

対応:
  全 data/predictions/*_pred.json を走査して
  園田 + venue_code=49 の race_id を 202649XXXXRR → 202650XXXXRR に置換。
  バックアップ取得後、アトミック書き込みで更新。

使用方法:
  # dry-run（変更しない、対象確認のみ）
  python scripts/migrate_sonoda_venue_code.py --dry-run

  # 本番実行（バックアップ取得後に全ファイル更新）
  python scripts/migrate_sonoda_venue_code.py --apply
"""

import argparse
import json
import shutil
import sys
from datetime import datetime
from pathlib import Path

PRED_DIR = Path("data/predictions")
BACKUP_SUFFIX = f".bak_sonoda_full_{datetime.now().strftime('%Y%m%d')}"


def migrate_file(fp: Path, dry_run: bool) -> int:
    """指定 pred.json を園田 venue_code 49 → 50 に置換。修正件数を返す。"""
    try:
        with open(fp, encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        print(f"[SKIP] 読み込みエラー {fp.name}: {e}")
        return 0

    fixed_races = 0
    for race in data.get("races", []):
        rid = str(race.get("race_id", ""))
        venue = race.get("venue", "")
        # 園田 + race_id[4:6]==49 を判定 (誤った venue_code のみ修正対象)
        if venue == "園田" and len(rid) == 12 and rid[4:6] == "49":
            new_rid = rid[:4] + "50" + rid[6:]
            if dry_run:
                print(f"  [DRY] {fp.name}: {rid} → {new_rid}")
            else:
                race["race_id"] = new_rid
                # horses[] 内の race_id も同期（もしあれば）
                for h in race.get("horses", []):
                    if "race_id" in h:
                        h["race_id"] = new_rid
            fixed_races += 1

    if fixed_races == 0:
        return 0

    if not dry_run:
        # バックアップ取得（既存があれば別名）
        bak = fp.with_suffix(fp.suffix + BACKUP_SUFFIX)
        n = 1
        while bak.exists():
            n += 1
            bak = fp.with_suffix(fp.suffix + BACKUP_SUFFIX + f"_{n}")
        shutil.copy2(fp, bak)
        print(f"  [BAK] バックアップ: {bak.name}")

        # アトミック書き込み (.tmp → rename)
        tmp = fp.with_suffix(fp.suffix + ".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        tmp.replace(fp)
        print(f"  [OK] {fp.name}: {fixed_races} レース修正")

    return fixed_races


def main():
    parser = argparse.ArgumentParser(description="園田 venue_code 49→50 全期間マイグレーション")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--dry-run", action="store_true", help="変更せず対象を表示のみ")
    group.add_argument("--apply", action="store_true", help="バックアップ後に実際に変更")
    args = parser.parse_args()

    dry_run = args.dry_run

    if dry_run:
        print("=== DRY-RUN モード（変更しません）===")
    else:
        print("=== APPLY モード（バックアップ後に変更）===")

    pred_files = sorted(PRED_DIR.glob("*_pred.json"))
    print(f"対象ファイル数: {len(pred_files)}")

    total_files = 0
    total_races = 0
    for fp in pred_files:
        n = migrate_file(fp, dry_run=dry_run)
        if n > 0:
            total_files += 1
            total_races += n

    print(f"\n{'[DRY-RUN]' if dry_run else '[RESULT]'} 修正対象: {total_files} ファイル / {total_races} レース")
    if dry_run:
        print("本番実行: python scripts/migrate_sonoda_venue_code.py --apply")


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    main()
