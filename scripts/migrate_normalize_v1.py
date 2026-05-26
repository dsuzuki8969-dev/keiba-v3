# -*- coding: utf-8 -*-
"""H-1: format 統一 migration v1

目的: 「DB date が hyphen 有/無 で 51/49% 混在」「payouts キー 日本語/英語 混在」を一括解消。

統一形式 (マスター承認 2026-05-26):
- date: YYYY-MM-DD (ISO 8601)
- payouts キー: 日本語 (単勝/複勝/枠連/馬連/馬単/ワイド/三連複/三連単)

対象:
1. DB race_results.date: YYYYMMDD → YYYY-MM-DD
2. DB match_results.date: YYYYMMDD → YYYY-MM-DD
3. DB predictions.date: YYYYMMDD → YYYY-MM-DD
4. DB race_results.payouts_json 内の英語キー → 日本語キー
5. data/predictions/*_pred.json: 内部参照は触らない (ファイル名 YYYYMMDD は維持)
6. data/results/*_results.json: 各 race の payouts キーを日本語に統一

注意:
- training_records (597K 行) は別仕様 ('12/31' 形式) で対象外
- predictions_old (5K 行) は元から hyphen 形式で対象外
- DB の UPSERT 制約 (date, race_id) で衝突する場合あり → COALESCE で旧 row 削除

Usage:
    python scripts/migrate_normalize_v1.py            # dry-run
    python scripts/migrate_normalize_v1.py --execute  # 本実行
"""
from __future__ import annotations
import argparse
import json
import sqlite3
import sys
import time
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "data" / "keiba.db"
PREDICTIONS_DIR = PROJECT_ROOT / "data" / "predictions"
RESULTS_DIR = PROJECT_ROOT / "data" / "results"

# 英語 → 日本語 payouts キーマッピング
PAYOUT_KEY_EN_TO_JP = {
    "tansho": "単勝",
    "fukusho": "複勝",
    "wakuren": "枠連",
    "umaren": "馬連",
    "umatan": "馬単",
    "wide": "ワイド",
    "sanrenpuku": "三連複",
    "sanrentan": "三連単",
    "3連複": "三連複",
    "3連単": "三連単",
}


def normalize_date(d: str) -> str:
    """YYYYMMDD → YYYY-MM-DD"""
    if not d or len(d) < 8:
        return d
    if "-" in d:
        return d  # 既に hyphen 形式
    if len(d) == 8 and d.isdigit():
        return f"{d[:4]}-{d[4:6]}-{d[6:]}"
    return d  # 想定外形式は touch しない


def normalize_payouts_keys(payouts: dict) -> tuple[dict, bool]:
    """英語キー → 日本語キー変換。両方ある場合は値をマージ。

    Returns: (normalized_payouts, changed_flag)
    """
    if not isinstance(payouts, dict):
        return payouts, False
    changed = False
    new_payouts = {}
    for k, v in payouts.items():
        jp_key = PAYOUT_KEY_EN_TO_JP.get(k, k)
        if jp_key != k:
            changed = True
        if jp_key in new_payouts:
            # 既存値とマージ (list なら extend、dict はそのまま)
            existing = new_payouts[jp_key]
            if isinstance(existing, list) and isinstance(v, list):
                new_payouts[jp_key] = existing + v
            # それ以外は既存優先 (新規 v は捨てる)
        else:
            new_payouts[jp_key] = v
    return new_payouts, changed


def progress_bar(done: int, total: int, label: str = "", width: int = 30) -> None:
    pct = done / total * 100 if total else 0.0
    filled = int(width * done / total) if total else 0
    bar = "█" * filled + "░" * (width - filled)
    print(f"\r[{bar}] {pct:5.1f}% ({done:,}/{total:,}) {label}", end="", flush=True)


def migrate_db_dates(conn, execute: bool) -> dict:
    """DB の date を YYYY-MM-DD 形式に統一。

    UPSERT 衝突対策: (date, race_id) UNIQUE 制約あるテーブルで、
    旧 YYYYMMDD と新 YYYY-MM-DD の row が両方存在する race は
    新 (hyphen) を残して旧 (no-hyphen) を削除する。
    """
    stats = {}
    cur = conn.cursor()

    # 対象テーブル: 列名と PK 構造
    targets = [
        ("race_results", "race_id"),
        ("match_results", "race_id"),
        ("predictions", "race_id"),
    ]

    for tbl, pk_col in targets:
        print(f"\n[{tbl}] migration...")
        # no-hyphen 行を抽出
        no_hyphen_rows = cur.execute(
            f"SELECT date, {pk_col} FROM {tbl} WHERE date LIKE '________' AND date NOT LIKE '%-%'"
        ).fetchall()
        print(f"  no-hyphen 行: {len(no_hyphen_rows):,}")

        # 衝突判定: 同 race_id で hyphen 形式が既にあるか
        conflict = 0
        target_update = 0
        for old_date, rid in no_hyphen_rows:
            new_date = normalize_date(old_date)
            existing = cur.execute(
                f"SELECT 1 FROM {tbl} WHERE date=? AND {pk_col}=?",
                (new_date, rid),
            ).fetchone()
            if existing:
                conflict += 1
            else:
                target_update += 1

        print(f"  UPDATE 可能 (衝突なし): {target_update:,}")
        print(f"  衝突 (hyphen 形式既存): {conflict:,} → 旧 no-hyphen を削除")

        if not execute:
            stats[tbl] = {"update": target_update, "delete": conflict}
            continue

        # 衝突: no-hyphen 側を削除 (新しい hyphen 側を残す)
        if conflict > 0:
            # 削除は executemany で一括
            del_rows = []
            for old_date, rid in no_hyphen_rows:
                new_date = normalize_date(old_date)
                existing = cur.execute(
                    f"SELECT 1 FROM {tbl} WHERE date=? AND {pk_col}=?",
                    (new_date, rid),
                ).fetchone()
                if existing:
                    del_rows.append((old_date, rid))
            cur.executemany(
                f"DELETE FROM {tbl} WHERE date=? AND {pk_col}=?",
                del_rows,
            )
            conn.commit()

        # UPDATE: no-hyphen → hyphen
        if target_update > 0:
            # SQLite で文字列操作: substr で YYYY-MM-DD 形式へ
            cur.execute(f"""
                UPDATE {tbl}
                SET date = substr(date,1,4) || '-' || substr(date,5,2) || '-' || substr(date,7,2)
                WHERE date LIKE '________' AND date NOT LIKE '%-%'
            """)
            conn.commit()

        stats[tbl] = {"update": target_update, "delete": conflict}

        # 検証
        remaining = cur.execute(
            f"SELECT COUNT(*) FROM {tbl} WHERE date LIKE '________' AND date NOT LIKE '%-%'"
        ).fetchone()[0]
        print(f"  完了。残り no-hyphen: {remaining}")

    return stats


def migrate_db_payouts(conn, execute: bool) -> dict:
    """race_results.payouts_json の英語キーを日本語キーに変換"""
    print(f"\n[race_results.payouts_json] 英語 → 日本語キー変換...")
    cur = conn.cursor()

    # 英語キーを含む payouts_json 行を抽出
    rows = cur.execute(
        "SELECT rowid, payouts_json FROM race_results WHERE payouts_json LIKE '%tansho%' OR payouts_json LIKE '%fukusho%' OR payouts_json LIKE '%sanrenpuku%' OR payouts_json LIKE '%sanrentan%' OR payouts_json LIKE '%umaren%' OR payouts_json LIKE '%umatan%' OR payouts_json LIKE '%wakuren%' OR payouts_json LIKE '%wide%'"
    ).fetchall()
    print(f"  英語キーを含む row: {len(rows):,}")

    if not execute:
        return {"target": len(rows), "updated": 0}

    t0 = time.time()
    updated = 0
    err = 0
    updates = []
    for rowid, pj in rows:
        try:
            p = json.loads(pj)
            new_p, changed = normalize_payouts_keys(p)
            if changed:
                updates.append((json.dumps(new_p, ensure_ascii=False), rowid))
        except Exception:
            err += 1

    # バッチ UPDATE
    if updates:
        cur.executemany(
            "UPDATE race_results SET payouts_json=? WHERE rowid=?",
            updates,
        )
        conn.commit()
        updated = len(updates)

    elapsed = time.time() - t0
    print(f"  UPDATE: {updated:,} ({elapsed:.1f}s, error={err})")
    return {"target": len(rows), "updated": updated, "error": err}


def migrate_results_json(execute: bool) -> dict:
    """data/results/*_results.json の payouts キーを日本語に統一"""
    print(f"\n[results.json] payouts キー日本語化...")
    files = sorted(RESULTS_DIR.glob("*_results.json"))
    print(f"  対象ファイル: {len(files):,}")

    if not execute:
        # サンプル 5 件の dry-run
        sample_changed = 0
        for fp in files[:5]:
            try:
                data = json.load(fp.open(encoding="utf-8"))
            except Exception:
                continue
            for rid, r in data.items() if isinstance(data, dict) else []:
                if not isinstance(r, dict):
                    continue
                _, changed = normalize_payouts_keys(r.get("payouts", {}))
                if changed:
                    sample_changed += 1
        print(f"  dry-run sample (5 files): 変換対象 race {sample_changed}")
        return {"sample_changed": sample_changed}

    t0 = time.time()
    n_files_changed = 0
    n_race_changed = 0
    for i, fp in enumerate(files):
        try:
            data = json.load(fp.open(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(data, dict):
            continue
        file_dirty = False
        for rid, r in data.items():
            if not isinstance(r, dict):
                continue
            payouts = r.get("payouts", {})
            new_p, changed = normalize_payouts_keys(payouts)
            if changed:
                r["payouts"] = new_p
                file_dirty = True
                n_race_changed += 1
        if file_dirty:
            with fp.open("w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            n_files_changed += 1

        if (i + 1) % 50 == 0 or (i + 1) == len(files):
            progress_bar(i + 1, len(files), f"files_changed={n_files_changed} races_changed={n_race_changed}")

    print()
    elapsed = time.time() - t0
    print(f"  完了 ({elapsed:.1f}s): {n_files_changed} ファイル / {n_race_changed} race UPDATE")
    return {"files": n_files_changed, "races": n_race_changed}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--execute", action="store_true", help="本実行 (default: dry-run)")
    parser.add_argument("--skip-json", action="store_true", help="JSON ファイル変換をスキップ")
    args = parser.parse_args()

    mode = "EXECUTE" if args.execute else "DRY-RUN"
    print("=" * 60)
    print(f"  H-1 format 統一 migration v1 [{mode}]")
    print("  date: YYYY-MM-DD / payouts キー: 日本語")
    print("=" * 60)

    conn = sqlite3.connect(str(DB_PATH))

    # 事前統計
    print("\n=== 事前統計 ===")
    for tbl in ["race_results", "match_results", "predictions"]:
        rows = conn.execute(f"""
            SELECT
                SUM(CASE WHEN date LIKE '____-__-__' THEN 1 ELSE 0 END) hyphen,
                SUM(CASE WHEN date LIKE '________' AND date NOT LIKE '%-%' THEN 1 ELSE 0 END) no_hyphen,
                COUNT(*) total
            FROM {tbl}
        """).fetchone()
        h, nh, t = rows
        print(f"  {tbl}: total={t:,} hyphen={h:,} no_hyphen={nh:,}")

    # Migration 1: DB date
    db_date_stats = migrate_db_dates(conn, args.execute)

    # Migration 2: DB payouts keys
    db_payouts_stats = migrate_db_payouts(conn, args.execute)

    conn.close()

    # Migration 3: results.json payouts keys
    if not args.skip_json:
        json_stats = migrate_results_json(args.execute)

    # 事後検証
    if args.execute:
        print("\n=== 事後検証 ===")
        conn = sqlite3.connect(str(DB_PATH))
        for tbl in ["race_results", "match_results", "predictions"]:
            no_h = conn.execute(
                f"SELECT COUNT(*) FROM {tbl} WHERE date LIKE '________' AND date NOT LIKE '%-%'"
            ).fetchone()[0]
            print(f"  {tbl} 残 no-hyphen: {no_h:,}")
        en_keys = conn.execute(
            "SELECT COUNT(*) FROM race_results WHERE payouts_json LIKE '%tansho%' OR payouts_json LIKE '%sanrenpuku%'"
        ).fetchone()[0]
        print(f"  race_results 残英語キー: {en_keys:,}")
        conn.close()


if __name__ == "__main__":
    main()
