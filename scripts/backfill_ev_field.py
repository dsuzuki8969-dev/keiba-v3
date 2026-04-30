"""過去 pred.json の ev フィールド欠落を win_prob × odds で補完する。

マスター指示 2026-05-01:
2025 年の三連複成績が異常 (R 数少 / 的中率 13.1%) の真因対策。
pred.json の ev フィールドが 2025 年は完全に None だった
(2024 年: 25% / 2025 年: 0% / 2026 年: 29%)。

_get_ev_with_fallback では fallback で win_prob × odds を計算しているが、
それでも 2024/2026 の「直接 ev 値」(engine が複雑な計算で出した品質) より
2025 年の fallback ev が低めに出るため、 _layer1_sanrenpuku の閾値
p_ev >= 1.0 / 1.3 の発動率が下がっていた。

本スクリプト:
- 全 pred.json を走査
- ev=None の馬のみ、 win_prob × (odds or predicted_tansho_odds) で補完
- ev=直接値 が既にある馬は触らない
- 結果: 2024/2026 (一部直接値) と 2025 (全 fallback) が ev フィールド存在で統一
"""
from __future__ import annotations

import io
import json
import sys
import time
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

PRED_DIR = Path("data/predictions")


def main():
    files = sorted(PRED_DIR.glob("*_pred.json"))
    files = [f for f in files if "_prev" not in f.name]
    print(f"pred ファイル数: {len(files)}")

    started = time.time()
    total_horses = 0
    total_filled = 0
    total_skip = 0
    files_modified = 0

    for fi, fp in enumerate(files):
        try:
            data = json.loads(fp.read_text(encoding="utf-8"))
        except Exception:
            continue

        modified = False
        for r in data.get("races", []):
            for h in r.get("horses", []):
                total_horses += 1
                if h.get("ev") not in (None, 0, 0.0):
                    continue
                wp = h.get("win_prob") or 0
                o = h.get("odds") or h.get("predicted_tansho_odds") or 0
                if wp > 0 and o > 0:
                    h["ev"] = round(float(wp) * float(o), 4)
                    total_filled += 1
                    modified = True
                else:
                    total_skip += 1

        if modified:
            try:
                fp.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
                files_modified += 1
            except Exception as e:
                print(f"  保存失敗: {fp.name}: {e}", flush=True)

        if (fi + 1) % 100 == 0 or (fi + 1) == len(files):
            elapsed = time.time() - started
            print(f"  {fi+1}/{len(files)}: horses={total_horses} "
                  f"filled={total_filled} skip={total_skip} files_mod={files_modified} "
                  f"elapsed={elapsed:.1f}s", flush=True)

    print(f"\n完了: horses={total_horses}, filled={total_filled}, "
          f"skip(計算不可)={total_skip}, files_modified={files_modified}, "
          f"elapsed={time.time()-started:.1f}s")


if __name__ == "__main__":
    main()
