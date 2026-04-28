"""
training_records の comment / stable_comment から
頻出する純粋な短評フレーズを抽出して頻度解析する。

目的:
- 競馬ブック調教コメントの完コピ表示を避けるため、
  頻出短評のパラフレーズ辞書を構築する基礎データを得る。

出力:
- data/analysis/training_comment_freq.csv  短評頻度トップ N
- data/analysis/stable_comment_freq.csv    厩舎の話頻度トップ N
- docs/TRAINING_COMMENT_ANALYSIS.md  サマリー
"""
from __future__ import annotations

import csv
import re
import sqlite3
import sys
import time
from collections import Counter
from pathlib import Path

# UTF-8 出力
sys.stdout.reconfigure(encoding="utf-8")

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "data" / "keiba.db"
OUT_DIR = ROOT / "data" / "analysis"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# 併せ馬情報らしさの判定パターン
# 「（3歳）」「（Ｃ１）」「（オープン）」のようなクラス括弧+強度+秒数
PAIR_INFO_PAT = re.compile(
    r"[（(][^）)]{1,10}[）)]\s*(馬なり|強め|一杯|仕掛|末強め|G前|末仕掛).*(先着|遅れ|同入|同時)",
)
# 固有名詞らしき連続カタカナ（4文字以上）が文頭にある
KATAKANA_PAT = re.compile(r"^[ァ-ヴー]{4,}")
# 秒数が含まれる（併せ馬情報の特徴）
SEC_PAT = re.compile(r"\d+\.\d+秒")


def is_pair_info(c: str) -> bool:
    """併せ馬情報っぽければ True。純粋な短評なら False。"""
    if not c:
        return False
    c = c.strip()
    if PAIR_INFO_PAT.search(c):
        return True
    if KATAKANA_PAT.match(c) and (SEC_PAT.search(c) or "秒" in c):
        return True
    # 「XXX同入」「XXX先着」「XXX同時入線」 馬名単独で始まる
    if KATAKANA_PAT.match(c) and ("同入" in c or "先着" in c or "遅れ" in c):
        return True
    return False


def normalize(c: str) -> str:
    """空白・改行を正規化。文字種は保持。"""
    if not c:
        return ""
    return re.sub(r"\s+", " ", c).strip()


def progress(i: int, total: int, t0: float, prefix: str = ""):
    if total == 0:
        return
    pct = 100.0 * i / total
    dt = time.time() - t0
    eta = dt * (total - i) / max(i, 1) if i > 0 else 0
    bar_len = 30
    filled = int(bar_len * i / total)
    bar = "█" * filled + "░" * (bar_len - filled)
    print(
        f"\r{prefix}[{bar}] {pct:5.1f}% ({i:,}/{total:,}) "
        f"経過{dt:5.1f}s 残{eta:5.1f}s",
        end="",
        flush=True,
    )


def main() -> int:
    t_start = time.time()
    print(f"DB: {DB}")
    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    # =========================
    # 1. comment (短評)
    # =========================
    print("\n=== [1/2] comment (調教短評) 頻度解析 ===")
    cur.execute(
        "SELECT COUNT(*) FROM training_records WHERE comment IS NOT NULL AND comment != ''"
    )
    total = cur.fetchone()[0]
    print(f"非空 comment: {total:,} 行")

    cur.execute(
        "SELECT comment FROM training_records WHERE comment IS NOT NULL AND comment != ''"
    )
    pure_counter: Counter[str] = Counter()
    pair_counter: Counter[str] = Counter()
    t0 = time.time()
    for i, (c,) in enumerate(cur, 1):
        c = normalize(c)
        if not c:
            continue
        if is_pair_info(c):
            pair_counter[c] += 1
        else:
            pure_counter[c] += 1
        if i % 10000 == 0:
            progress(i, total, t0, "  ")
    progress(total, total, t0, "  ")
    print()
    print(f"  純粋短評ユニーク数: {len(pure_counter):,}")
    print(f"  併せ馬情報ユニーク数: {len(pair_counter):,}")

    # CSV 出力（純粋短評トップ500）
    pure_csv = OUT_DIR / "training_comment_freq.csv"
    with open(pure_csv, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["comment", "count"])
        for phrase, cnt in pure_counter.most_common(500):
            w.writerow([phrase, cnt])
    print(f"  → {pure_csv} (トップ500)")

    # トップ30を表示
    print("\n  ▼ 純粋短評トップ30:")
    for phrase, cnt in pure_counter.most_common(30):
        print(f"    {cnt:>6,} 回  「{phrase}」")

    # =========================
    # 2. stable_comment (厩舎の話)
    # =========================
    print("\n=== [2/2] stable_comment (厩舎の話) 頻度解析 ===")
    cur.execute(
        "SELECT COUNT(*) FROM training_records "
        "WHERE stable_comment IS NOT NULL AND stable_comment != ''"
    )
    total = cur.fetchone()[0]
    print(f"非空 stable_comment: {total:,} 行")

    # 厩舎の話は長文（〜200文字）なので、重複判定で頻度化
    stable_counter: Counter[str] = Counter()
    if total > 0:
        cur.execute(
            "SELECT stable_comment FROM training_records "
            "WHERE stable_comment IS NOT NULL AND stable_comment != ''"
        )
        t0 = time.time()
        for i, (c,) in enumerate(cur, 1):
            c = normalize(c)
            if c:
                stable_counter[c] += 1
            if i % 5000 == 0:
                progress(i, total, t0, "  ")
        progress(total, total, t0, "  ")
        print()

    stable_csv = OUT_DIR / "stable_comment_freq.csv"
    with open(stable_csv, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["stable_comment", "count"])
        for phrase, cnt in stable_counter.most_common(200):
            w.writerow([phrase, cnt])
    print(f"  → {stable_csv} (トップ200)")

    # サマリー
    print(f"\n総処理時間: {time.time() - t_start:.1f} 秒")
    print("完了")
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
