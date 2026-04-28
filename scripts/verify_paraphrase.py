"""
パラフレーズ検収スクリプト。

pred.json を走査し、stable_comment を持つ馬のうち
stable_comment_bullets が注入されている割合・品質を集計する。

観点:
  1. 注入率（何%の馬に bullets が入っているか）
  2. bullets の件数分布（1〜5 件の内訳）
  3. 文字数分布（全角 10〜30 字想定から逸脱していないか）
  4. サロゲート文字混入チェック
  5. 箇条書きが原文とそっくりコピーでないか（簡易）
"""
from __future__ import annotations

import json
import re
import sys
from collections import Counter
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

ROOT = Path(__file__).resolve().parent.parent


def extract_body(raw: str) -> str:
    if not raw:
        return ""
    m = re.match(r"^[◎○▲△★☆×◯]?[^【]{0,30}【[^】]{1,20}】", raw)
    if m:
        return raw[m.end():].strip()
    if "――" in raw:
        return raw.split("――", 1)[1].strip()
    return raw.strip()


def has_surrogate(s: str) -> bool:
    try:
        s.encode("utf-8")
        return False
    except UnicodeEncodeError:
        return True


def main(argv: list[str]) -> int:
    date = argv[1] if len(argv) > 1 else ""
    if not date:
        print("使い方: verify_paraphrase.py YYYY-MM-DD")
        return 1
    date_compact = date.replace("-", "")
    pred_path = ROOT / "data" / "predictions" / f"{date_compact}_pred.json"
    if not pred_path.exists():
        print(f"pred.json が存在しない: {pred_path}")
        return 1

    with open(pred_path, encoding="utf-8") as f:
        pred = json.load(f)

    total = 0
    with_body = 0
    with_bullets = 0
    bullet_count_hist: Counter[int] = Counter()
    len_hist: Counter[str] = Counter()
    surrogate_hits: list[str] = []
    too_short: list[str] = []
    too_long: list[str] = []
    identity_copy: list[str] = []  # 原文とそっくり

    sample_ok: list[tuple[str, list[str]]] = []  # 表示用

    for r in pred.get("races", []):
        for h in r.get("horses", []):
            trs = h.get("training_records") or []
            if not trs:
                continue
            tr = trs[0]
            raw = tr.get("stable_comment") or ""
            body = extract_body(raw)
            if not body:
                continue
            total += 1
            with_body += 1
            bullets = tr.get("stable_comment_bullets")
            if not isinstance(bullets, list) or not bullets:
                continue
            with_bullets += 1
            bullet_count_hist[len(bullets)] += 1
            for b in bullets:
                if not isinstance(b, str):
                    continue
                n = len(b)
                # 文字数ビニング
                if n < 5:
                    len_hist["<5"] += 1
                    too_short.append(b)
                elif n < 10:
                    len_hist["5-9"] += 1
                elif n < 31:
                    len_hist["10-30"] += 1
                elif n < 50:
                    len_hist["31-49"] += 1
                    too_long.append(b)
                else:
                    len_hist[">=50"] += 1
                    too_long.append(b)
                if has_surrogate(b):
                    surrogate_hits.append(b)
                # そっくりコピー検知: 箇条書きがそのまま原文に含まれる and 長い
                if n >= 15 and b in body:
                    identity_copy.append(f"{h.get('horse_name')}: {b}")
            if len(sample_ok) < 8:
                sample_ok.append((h.get("horse_name", ""), bullets))

    print(f"=== 検収結果 ({pred_path.name}) ===")
    print(f"本文あり対象: {with_body} 件")
    print(f"bullets 注入 : {with_bullets} 件 ({100*with_bullets/max(with_body,1):.1f}%)")
    print(f"注入漏れ     : {with_body - with_bullets} 件")
    print()
    print("【箇条書き件数分布】")
    for n in sorted(bullet_count_hist):
        print(f"  {n} 件: {bullet_count_hist[n]}")
    print()
    print("【1要素あたり文字数分布】")
    for k in ["<5", "5-9", "10-30", "31-49", ">=50"]:
        if k in len_hist:
            marker = "  " if k == "10-30" else " ⚠"
            print(f" {marker} {k}: {len_hist[k]}")
    print()
    print(f"【サロゲート混入】: {len(surrogate_hits)} 件")
    for s in surrogate_hits[:3]:
        print(f"    {repr(s)[:80]}")
    print()
    print(f"【短すぎ(<5)】: {len(too_short)} 件  例: {too_short[:3]}")
    print(f"【長すぎ(>=31)】: {len(too_long)} 件  例: {too_long[:3]}")
    print(f"【原文そっくり】: {len(identity_copy)} 件  例: {identity_copy[:3]}")
    print()
    print("【サンプル出力】")
    for name, bullets in sample_ok:
        print(f"  [{name}]")
        for b in bullets:
            print(f"    ・{b}")
        print()

    # 全件注入できたら exit 0
    if with_bullets < with_body:
        print("⚠ 未注入が残っています")
        return 2
    if surrogate_hits:
        print("⚠ サロゲート混入があります")
        return 3
    print("✓ 全件検収 OK")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
