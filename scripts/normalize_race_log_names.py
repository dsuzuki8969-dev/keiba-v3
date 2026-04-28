"""
race_log テーブルの trainer_name / jockey_name を personnel_db.json のフルネームに統一する。

背景:
- race_log に「尾形」「友道」「栗東野中」のような姓のみ／地域接頭辞付きの短縮名が混在
- personnel_db.json には trainer_id / jockey_id ごとにフルネームが保存済み
- ID で突合してフルネームで UPDATE することで、ダッシュボードの名前マッチング不具合を根治する

動作:
1. personnel_db.json から {trainer_id: フルネーム}, {jockey_id: フルネーム} マップ構築
2. NFKC 正規化（全角→半角、ピリオド・空白の統一）
3. race_log の trainer_id / jockey_id 毎に UPDATE
4. 件数と未解決レコードを報告

使い方:
    python scripts/normalize_race_log_names.py            # 実行
    python scripts/normalize_race_log_names.py --dry-run  # 変更内容だけ見る
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
import unicodedata
from pathlib import Path
from typing import Dict, Tuple

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "keiba.db"
PERSONNEL_PATH = ROOT / "data" / "personnel_db.json"


def _fmt_hhmmss(sec: float) -> str:
    if sec < 0 or sec != sec:
        sec = 0
    sec = int(sec)
    h, rem = divmod(sec, 3600)
    m, s = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def _progress(label: str, cur: int, total: int, t0: float) -> None:
    elapsed = time.time() - t0
    rate = cur / elapsed if elapsed > 0 else 0
    remain = (total - cur) / rate if rate > 0 else 0
    pct = (cur / total * 100) if total > 0 else 0
    filled = int((cur / total if total else 0) * 20)
    bar = "[" + "■" * filled + "□" * (20 - filled) + "]"
    print(
        f"{label} {bar} {cur}/{total} {pct:5.1f}% "
        f"経過 {_fmt_hhmmss(elapsed)} / 残り {_fmt_hhmmss(remain)}",
        flush=True,
    )


def _nfkc(s: str) -> str:
    """全角・全角ピリオド・全角スペース等を半角に寄せる"""
    if not s:
        return ""
    # unicodedata.normalize('NFKC', ...) で全角→半角
    n = unicodedata.normalize("NFKC", s)
    # 全角スペース → 半角スペース（NFKCで半角になるがダブル保険）
    n = n.replace("\u3000", " ").strip()
    return n


def build_name_maps() -> Tuple[Dict[str, str], Dict[str, str]]:
    """personnel_db.json から ID→フルネーム マップを構築"""
    d = json.loads(PERSONNEL_PATH.read_text(encoding="utf-8"))
    tr_map: Dict[str, str] = {}
    for tid, v in (d.get("trainers") or {}).items():
        if not isinstance(v, dict):
            continue
        name = v.get("trainer_name") or v.get("stable_name") or ""
        name = _nfkc(name)
        if name:
            tr_map[tid] = name
    jk_map: Dict[str, str] = {}
    for jid, v in (d.get("jockeys") or {}).items():
        if not isinstance(v, dict):
            continue
        name = v.get("jockey_name") or ""
        name = _nfkc(name)
        if name:
            jk_map[jid] = name
    return tr_map, jk_map


def main() -> int:
    ap = argparse.ArgumentParser(description="race_log の調教師名・騎手名をフルネーム化")
    ap.add_argument("--dry-run", action="store_true", help="実際には書き込まない")
    args = ap.parse_args()

    if not DB_PATH.exists():
        print(f"[ERROR] DB が見つかりません: {DB_PATH}")
        return 1
    if not PERSONNEL_PATH.exists():
        print(f"[ERROR] personnel_db.json が見つかりません: {PERSONNEL_PATH}")
        return 1

    print("=" * 72)
    print("race_log 名前正規化スクリプト")
    print(f"DB: {DB_PATH}")
    print(f"辞書: {PERSONNEL_PATH}")
    if args.dry_run:
        print("(DRY-RUN モード: 実際の更新はしません)")
    print("=" * 72)

    tr_map, jk_map = build_name_maps()
    print(f"辞書構築完了: 調教師 {len(tr_map)} 件 / 騎手 {len(jk_map)} 件")

    con = sqlite3.connect(str(DB_PATH))
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    # ----- 調教師 -----
    print()
    print("[1/2] 調教師名の正規化")
    cur.execute(
        """
        SELECT trainer_id, trainer_name, COUNT(*) AS cnt
        FROM race_log
        WHERE trainer_id IS NOT NULL AND trainer_id != ''
        GROUP BY trainer_id, trainer_name
        """
    )
    rows_tr = cur.fetchall()
    total_tr = len(rows_tr)
    upd_tr = 0
    upd_tr_rows = 0
    skipped_tr = 0
    unresolved_tr = []
    t0 = time.time()
    for i, row in enumerate(rows_tr, 1):
        tid = row["trainer_id"]
        old = row["trainer_name"] or ""
        cnt = row["cnt"]
        full = tr_map.get(tid)
        if not full:
            unresolved_tr.append((tid, old, cnt))
            skipped_tr += 1
            continue
        # 既に一致していればスキップ
        if _nfkc(old) == full:
            skipped_tr += 1
            continue
        if not args.dry_run:
            r = cur.execute(
                "UPDATE race_log SET trainer_name = ? WHERE trainer_id = ?",
                (full, tid),
            )
            upd_tr_rows += r.rowcount
        else:
            upd_tr_rows += cnt
        upd_tr += 1
        if i <= 20 or i % 100 == 0:
            print(f"  {tid}: {old!r:30s} -> {full!r}  (rows={cnt})", flush=True)
        if i % 50 == 0 or i == total_tr:
            _progress("  進捗:", i, total_tr, t0)

    # ----- 騎手 -----
    print()
    print("[2/2] 騎手名の正規化")
    cur.execute(
        """
        SELECT jockey_id, jockey_name, COUNT(*) AS cnt
        FROM race_log
        WHERE jockey_id IS NOT NULL AND jockey_id != ''
        GROUP BY jockey_id, jockey_name
        """
    )
    rows_jk = cur.fetchall()
    total_jk = len(rows_jk)
    upd_jk = 0
    upd_jk_rows = 0
    skipped_jk = 0
    unresolved_jk = []
    t0 = time.time()
    for i, row in enumerate(rows_jk, 1):
        jid = row["jockey_id"]
        old = row["jockey_name"] or ""
        cnt = row["cnt"]
        full = jk_map.get(jid)
        if not full:
            unresolved_jk.append((jid, old, cnt))
            skipped_jk += 1
            continue
        if _nfkc(old) == full:
            skipped_jk += 1
            continue
        if not args.dry_run:
            r = cur.execute(
                "UPDATE race_log SET jockey_name = ? WHERE jockey_id = ?",
                (full, jid),
            )
            upd_jk_rows += r.rowcount
        else:
            upd_jk_rows += cnt
        upd_jk += 1
        if i <= 20 or i % 100 == 0:
            print(f"  {jid}: {old!r:30s} -> {full!r}  (rows={cnt})", flush=True)
        if i % 50 == 0 or i == total_jk:
            _progress("  進捗:", i, total_jk, t0)

    if not args.dry_run:
        con.commit()
    con.close()

    print()
    print("=" * 72)
    print("結果サマリ")
    print(f"  調教師: 対象 {total_tr} ID / 更新 {upd_tr} ID ({upd_tr_rows} rows) / スキップ {skipped_tr} ID")
    print(f"  騎手:   対象 {total_jk} ID / 更新 {upd_jk} ID ({upd_jk_rows} rows) / スキップ {skipped_jk} ID")
    if unresolved_tr:
        print(f"  未解決調教師ID (personnel_dbに無い): {len(unresolved_tr)} 件（先頭5件表示）")
        for tid, name, cnt in unresolved_tr[:5]:
            print(f"    - {tid} {name!r} rows={cnt}")
    if unresolved_jk:
        print(f"  未解決騎手ID (personnel_dbに無い): {len(unresolved_jk)} 件（先頭5件表示）")
        for jid, name, cnt in unresolved_jk[:5]:
            print(f"    - {jid} {name!r} rows={cnt}")
    print("=" * 72)
    if args.dry_run:
        print("(DRY-RUN: 実際の更新はしていません)")
    else:
        print("DB 更新完了")
    return 0


if __name__ == "__main__":
    sys.exit(main())
