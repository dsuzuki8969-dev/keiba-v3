"""過去 pred.json の印を補正する 1 回限りスクリプト。

マスター指示 2026-05-02:
- × (危険馬) 印は削除 → '-' (無印) に変換
- ☆ (穴馬) 不在レースは ana_score 最大の馬を ☆ に昇格
  (ana_score 0 のレースは composite 最大の無印馬を ☆ に)

処理対象: data/predictions/*_pred.json (全 849 ファイル)
保存後は backtest_5patterns.py / hybrid_summary キャッシュ無効化が必要。
"""
from __future__ import annotations

import io
import json
import sys
import time
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

PRED_DIR = Path("data/predictions")


def fix_race(race: dict) -> tuple[int, int]:
    """1 レース分の印補正。

    Returns: (n_x_removed, n_hoshi_added)
    """
    horses = race.get("horses", [])
    n_x = 0
    # Step 1: × → '-' (無印) に変換
    for h in horses:
        if (h.get("mark") or "").strip() == "×":
            h["mark"] = "－"  # 全角ハイフン (無印標準表記)
            n_x += 1

    # Step 2: ☆ 不在ならアナ馬を昇格
    has_hoshi = any((h.get("mark") or "").strip() == "☆" for h in horses)
    if has_hoshi:
        return n_x, 0

    # 無印 (None or '-' or '－' or '') の出走馬を候補に
    def is_unmarked(h: dict) -> bool:
        m = (h.get("mark") or "").strip()
        return m in ("", "-", "－")

    # 取消馬除外
    cands = [h for h in horses
             if is_unmarked(h)
             and not h.get("is_tokusen_kiken")
             and not h.get("is_scratched")]
    if not cands:
        return n_x, 0

    # ana_score > 0 優先、なければ composite 最大
    cands_with_ana = [h for h in cands if (h.get("ana_score") or 0) > 0]
    if cands_with_ana:
        chosen = max(cands_with_ana, key=lambda h: (h.get("ana_score") or 0))
    else:
        chosen = max(cands, key=lambda h: (h.get("composite") or 0))
    chosen["mark"] = "☆"
    return n_x, 1


def main():
    files = sorted(PRED_DIR.glob("*_pred.json"))
    files = [f for f in files if "_prev" not in f.name]
    print(f"pred ファイル数: {len(files)}")

    started = time.time()
    total_x = 0
    total_hoshi = 0
    files_modified = 0

    for fi, fp in enumerate(files):
        try:
            data = json.loads(fp.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"  読込失敗 {fp.name}: {e}", flush=True)
            continue

        f_x = 0
        f_hoshi = 0
        for r in data.get("races", []):
            x, hoshi = fix_race(r)
            f_x += x
            f_hoshi += hoshi

        if f_x or f_hoshi:
            try:
                fp.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
                files_modified += 1
            except Exception as e:
                print(f"  保存失敗 {fp.name}: {e}", flush=True)
                continue

        total_x += f_x
        total_hoshi += f_hoshi

        if (fi + 1) % 100 == 0 or (fi + 1) == len(files):
            elapsed = time.time() - started
            print(f"  {fi+1}/{len(files)}: total_x={total_x} total_hoshi_added={total_hoshi} "
                  f"files_mod={files_modified} elapsed={elapsed:.1f}s", flush=True)

    print(f"\n完了: 削除した× = {total_x}, 補完した☆ = {total_hoshi}, "
          f"files_modified = {files_modified}, elapsed = {time.time()-started:.1f}s")


if __name__ == "__main__":
    main()
