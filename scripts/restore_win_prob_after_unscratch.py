"""取消解除後 win_prob=0 のままになっている馬の確率を中間値から復元

経緯:
  取消検出時(odds=None and pop=None) に win_prob=0 にクリアしていたが、
  オッズ復帰時は is_scratched=False にするだけで win_prob は 0 のままだった。
  このバグで一部馬が常に 0.0% として表示される問題があった (例: 2026-04-16 大井6R #11〜#14)。

処理内容:
  data/predictions/YYYYMMDD_pred.json を全走査し、
  オッズ/人気が付いているのに win_prob=0 の馬を中間診断値 (pre_pop_prob 等) から復元する。

使い方:
  python scripts/restore_win_prob_after_unscratch.py            # 全日付処理
  python scripts/restore_win_prob_after_unscratch.py 2026-04-16 # 特定日付のみ
  python scripts/restore_win_prob_after_unscratch.py --dry-run  # 確認のみ
"""
import argparse
import json
import sys
from pathlib import Path

# プロジェクトルートをパスに追加
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.calculator.popularity_blend import restore_win_prob_if_zero

PRED_DIR = Path(__file__).resolve().parent.parent / "data" / "predictions"


def process_file(fpath: Path, dry_run: bool = False) -> dict:
    """1ファイルを処理して統計を返す"""
    stats = {"races": 0, "restored_horses": 0, "normalized_races": 0}

    if not fpath.exists():
        return stats
    try:
        with open(fpath, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        print(f"  {fpath.name}: 読み込みエラー {e}")
        return stats

    modified = False

    for race in data.get("races", []):
        stats["races"] += 1
        horses = race.get("horses", [])
        if not horses:
            continue

        # オッズ情報があるレースのみ対象（pre-day は win_prob 正常なのでスキップ）
        has_any_odds = any(h.get("odds") is not None for h in horses)
        if not has_any_odds:
            continue

        race_restored = 0
        for h in horses:
            # オッズ/人気が両方あるのに win_prob=0 → 復元対象
            if (h.get("win_prob") or 0) > 0:
                continue
            if h.get("odds") is None or h.get("popularity") is None:
                continue
            # is_scratched=True なら取消状態維持（0のままでOK）
            if h.get("is_scratched"):
                continue

            if restore_win_prob_if_zero(h, field_count=len(horses)):
                race_restored += 1
                h["is_scratched"] = False  # 念のため解除状態を確定
                print(f"    {race.get('venue')}{race.get('race_no')}R #{h.get('horse_no')}: "
                      f"pop={h.get('popularity')} odds={h.get('odds')} "
                      f"→ win={h['win_prob']:.4f}")

        if race_restored > 0:
            stats["restored_horses"] += race_restored
            stats["normalized_races"] += 1
            modified = True

            # 再正規化: win=1.0, place2=min(n,2), place3=min(n,3)
            active = [h for h in horses if not h.get("is_scratched")]
            n_active = len(active)
            if n_active >= 2:
                target2 = min(n_active, 2)
                target3 = min(n_active, 3)
                for pk, ts in [("win_prob", 1.0), ("place2_prob", target2), ("place3_prob", target3)]:
                    total = sum(h.get(pk, 0) for h in active)
                    if total > 0:
                        for h in active:
                            h[pk] = round(min(1.0, h.get(pk, 0) / total * ts), 4)

    if modified and not dry_run:
        try:
            with open(fpath, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"  {fpath.name}: 保存エラー {e}")

    return stats


def find_all_dates() -> list:
    """data/predictions/ 下の全日付ファイルを新しい順に返す"""
    files = []
    for f in PRED_DIR.glob("*_pred.json"):
        if "_prev" in f.name or ".bak" in f.name:
            continue
        files.append(f)
    files.sort(reverse=True, key=lambda p: p.name)
    return files


def main():
    parser = argparse.ArgumentParser(description="取消解除馬の win_prob 復元")
    parser.add_argument("date", nargs="?", default=None,
                        help="対象日付 YYYY-MM-DD or YYYYMMDD（省略時は全日付）")
    parser.add_argument("--dry-run", action="store_true",
                        help="書き込みせず統計のみ表示")
    args = parser.parse_args()

    if args.date:
        dk = args.date.replace("-", "")
        files = [PRED_DIR / f"{dk}_pred.json"]
    else:
        files = find_all_dates()

    print(f"=== restore_win_prob 開始 {len(files)}ファイル ===")
    if args.dry_run:
        print("[DRY-RUN モード: 書き込みなし]")

    grand = {"races": 0, "restored_horses": 0, "normalized_races": 0}

    for i, fpath in enumerate(files, 1):
        print(f"\n[{i}/{len(files)}] {fpath.name}")
        s = process_file(fpath, dry_run=args.dry_run)
        print(f"  → races={s['races']} restored={s['restored_horses']}頭 "
              f"races_normalized={s['normalized_races']}")
        for k in grand:
            grand[k] += s[k]

    print(f"\n=== 完了 ===")
    print(f"  総レース数       : {grand['races']}")
    print(f"  復元頭数合計     : {grand['restored_horses']}")
    print(f"  再正規化レース数 : {grand['normalized_races']}")


if __name__ == "__main__":
    main()
