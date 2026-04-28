"""
園田 venue_code 49 → 50 修正 (本日 pred.json)

真因:
  pred.json の園田レース race_id は venue_code=49 で生成されているが、
  netkeiba は園田を venue_code=50 で配信している。
  → race_log 結果取得が「結果未確定」(空 tbody) で全 skip
  → race_log 0 件、画面「レース結果はまだありません」

対応:
  本日 pred.json の園田 race_id を 202649XXXXRR → 202650XXXXRR に書き換え。
  バックアップ取得後、race_id 含む全フィールドを更新。
  race_log は空なので次回 fetch で正しい race_id で取得される。

将来対応 (別タスク):
  venue_master.py の園田 venue_code 定義を 49 ではなく 50 に統一する根本対策。
  audit_pred_venue.py の VENUE_NAME_TO_CODE / VENUE_CODE_TO_NAME も同様。
"""

import json
import shutil
import sys
from pathlib import Path

PRED_DIR = Path("data/predictions")


def fix_pred_json(date_str: str) -> int:
    """指定日の pred.json で園田 venue_code 49 → 50 に置換"""
    filename = date_str.replace("-", "") + "_pred.json"
    fp = PRED_DIR / filename
    if not fp.exists():
        print(f"[ERROR] pred.json 不在: {fp}")
        return 0

    # バックアップ取得 (既存があれば数値サフィックス)
    bak = fp.with_suffix(fp.suffix + ".bak_sonoda")
    n = 1
    while bak.exists():
        n += 1
        bak = fp.with_suffix(fp.suffix + f".bak_sonoda_{n}")
    shutil.copy2(fp, bak)
    print(f"[INFO] バックアップ作成: {bak}")

    with open(fp, encoding="utf-8") as f:
        data = json.load(f)

    fixed_races = 0
    for race in data.get("races", []):
        rid = str(race.get("race_id", ""))
        venue = race.get("venue", "")
        # 園田 + race_id[4:6]==49 を判定 (誤った venue_code のみ修正対象)
        if venue == "園田" and len(rid) == 12 and rid[4:6] == "49":
            new_rid = rid[:4] + "50" + rid[6:]
            race["race_id"] = new_rid
            # horses[] 内の race_id も同期 (もしあれば)
            for h in race.get("horses", []):
                if "race_id" in h:
                    h["race_id"] = new_rid
            fixed_races += 1

    if fixed_races == 0:
        print("[INFO] 修正対象なし (既に 50 か、園田レース不在)")
        # 不要バックアップ削除
        bak.unlink()
        return 0

    # アトミック書き込み (.tmp → rename)
    tmp = fp.with_suffix(fp.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    tmp.replace(fp)

    print(f"[OK] 園田 race_id 修正完了: {fixed_races} レース")
    return fixed_races


def main():
    date_str = sys.argv[1] if len(sys.argv) > 1 else "2026-04-28"
    print(f"対象日: {date_str}")
    n = fix_pred_json(date_str)
    print(f"完了: {n} レース修正")


if __name__ == "__main__":
    main()
