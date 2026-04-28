"""
pred.json のダブルコンマ (,,) を除去して JSON をバリデート修復する。

背景:
- 2026-04-19 05:10 の予想生成で `"mgs_grade": "A",,` のような JSON 構文違反が混入
- フロントがパースエラーで何も表示されない
- バックアップ (bak_tt_rescale) から戻すと 00:54 時点の古いデータに戻るため、
  破損ファイルを直接修復するのが最小インパクト

動作:
- 正規表現 `,\s*,` を `,` に置換（文字列内は対象外と想定、grep で検出済み）
- json.loads でバリデーション成功したら保存
- バックアップは *.bak_doublecomma
"""
from __future__ import annotations

import json
import re
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def main() -> int:
    pred_path = ROOT / "data" / "predictions" / "20260419_pred.json"
    if not pred_path.exists():
        print(f"[ERROR] 存在しません: {pred_path}")
        return 1

    print(f"対象: {pred_path}")
    raw = pred_path.read_text(encoding="utf-8")
    before_size = len(raw)

    # ダブルコンマ (空白混じり含む) → シングルコンマ
    pattern = re.compile(r",\s*,")
    fixed = pattern.sub(",", raw)

    # 念のため 3連コンマ以上もループで吸収
    while ",," in fixed:
        fixed = fixed.replace(",,", ",")

    # ダブルクォート2連で始まる key (`""key":`) → `"key":` (シリアライザーのバグ混入)
    # 行頭または空白の後に "" が来て直後が小文字/英字+":"というパターン
    dq_key = re.compile(r'""([A-Za-z_][A-Za-z0-9_]*":)')
    hits = dq_key.findall(fixed)
    if hits:
        print(f"ダブルクォート key 修復: {len(hits)} 箇所")
    fixed = dq_key.sub(r'"\1', fixed)

    after_size = len(fixed)
    print(f"サイズ: {before_size} → {after_size} ({before_size - after_size} bytes 削除)")

    # JSON バリデーション
    try:
        data = json.loads(fixed)
        print(f"[OK] JSON パース成功: races={len(data.get('races') or [])}")
    except json.JSONDecodeError as e:
        print(f"[ERROR] JSON パース失敗: {e}")
        # 失敗時は失敗位置を出力して中断
        line = fixed.split("\n")[e.lineno - 1] if e.lineno > 0 else ""
        print(f"該当行 ({e.lineno}): {line[:200]}")
        return 2

    # バックアップ
    bak = pred_path.with_suffix(pred_path.suffix + ".bak_doublecomma")
    try:
        shutil.copy2(pred_path, bak)
        print(f"バックアップ: {bak}")
    except Exception as e:
        print(f"[WARN] バックアップ失敗: {e}")

    # 保存（パース成功したデータを再シリアライズ = 完全整形）
    pred_path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(f"保存: {pred_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
