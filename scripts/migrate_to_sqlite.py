"""
JSON → SQLite 一括移行スクリプト

実行: python scripts/migrate_to_sqlite.py [--dry-run]

処理順序:
  1. スキーマ初期化
  2. personnel_db.json → personnel テーブル
  3. trainer_baseline_db.json → personnel テーブル（trainer型）
  4. course_db_preload.json → course_db テーブル
  5. data/predictions/*.json → predictions テーブル
  6. data/results/*.json → race_results テーブル
"""

import json
import os
import sys

# プロジェクトルートを sys.path に追加
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from config.settings import (
    BLOODLINE_DB_PATH,
    COURSE_DB_PRELOAD_PATH,
    PERSONNEL_DB_PATH,
    PREDICTIONS_DIR,
    RESULTS_DIR,
    TRAINER_BASELINE_DB_PATH,
    DATABASE_PATH,
)
from src.database import (
    init_schema,
    save_personnel_all,
    set_personnel,
    set_course_db,
    save_prediction,
    save_results,
    get_db_stats,
    personnel_count,
    course_db_count,
)


DRY_RUN = "--dry-run" in sys.argv


def log(msg: str) -> None:
    print(msg, flush=True)


def load_json(path: str) -> dict | list | None:
    if not os.path.exists(path):
        log(f"  [SKIP] {path} が見つかりません")
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# ============================================================
# 1. スキーマ初期化
# ============================================================

def step_init():
    log("\n=== Step 1: スキーマ初期化 ===")
    if not DRY_RUN:
        init_schema()
        log(f"  DB: {DATABASE_PATH}")
    log("  OK")


# ============================================================
# 2. personnel_db.json → personnel テーブル
# ============================================================

def step_personnel():
    log("\n=== Step 2: personnel_db.json 移行 ===")
    data = load_json(PERSONNEL_DB_PATH)
    if data is None:
        return

    jockeys = data.get("jockeys", {})
    trainers = data.get("trainers", {})
    log(f"  騎手: {len(jockeys)} 件, 調教師: {len(trainers)} 件")

    if not DRY_RUN:
        save_personnel_all({"jockeys": jockeys, "trainers": trainers})
        log(f"  → personnel テーブル: {personnel_count()} 件")


# ============================================================
# 3. trainer_baseline_db.json → personnel テーブル（上書き・追加）
# ============================================================

def step_trainer_baseline():
    log("\n=== Step 3: trainer_baseline_db.json 移行 ===")
    data = load_json(TRAINER_BASELINE_DB_PATH)
    if data is None:
        return

    # trainer_baseline は {trainer_id: {...}} 形式
    count = 0
    if not DRY_RUN:
        for tid, tdata in data.items():
            if isinstance(tdata, dict):
                set_personnel(tid, "trainer_baseline", tdata)
                count += 1
    else:
        count = len([k for k, v in data.items() if isinstance(v, dict)])
    log(f"  trainer_baseline: {count} 件 → personnel テーブル（trainer_baseline型）")


# ============================================================
# 4. course_db_preload.json → course_db テーブル
# ============================================================

def step_course_db():
    log("\n=== Step 4: course_db_preload.json 移行 ===")
    data = load_json(COURSE_DB_PRELOAD_PATH)
    if data is None:
        return

    course_db = data.get("course_db", {})
    log(f"  エントリ数: {len(course_db)} 件")

    if not DRY_RUN:
        set_course_db(course_db)
        log(f"  → course_db テーブル: {course_db_count()} 件")


# ============================================================
# 5. data/predictions/*.json → predictions テーブル
# ============================================================

def step_predictions():
    log("\n=== Step 5: predictions/*.json 移行 ===")
    if not os.path.exists(PREDICTIONS_DIR):
        log(f"  [SKIP] {PREDICTIONS_DIR} が見つかりません")
        return

    files = sorted(
        [f for f in os.listdir(PREDICTIONS_DIR) if f.endswith("_pred.json")]
    )
    log(f"  ファイル数: {len(files)}")

    ok = 0
    err = 0
    for fname in files:
        fpath = os.path.join(PREDICTIONS_DIR, fname)
        # ファイル名から日付を抽出: YYYYMMDD_pred.json
        raw_date = fname.replace("_pred.json", "")
        if len(raw_date) != 8:
            log(f"  [SKIP] ファイル名が不正: {fname}")
            continue
        date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:8]}"
        try:
            payload = load_json(fpath)
            if payload is None:
                continue
            # payload の date フィールドを正規化
            if "date" not in payload:
                payload["date"] = date
            if not DRY_RUN:
                save_prediction(date, payload)
            ok += 1
            log(f"  [{date}] {len(payload.get('races', []))} レース")
        except Exception as e:
            err += 1
            log(f"  [ERROR] {fname}: {e}")

    log(f"  完了: {ok} ファイル成功, {err} エラー")


# ============================================================
# 6. data/results/*.json → race_results テーブル
# ============================================================

def step_results():
    log("\n=== Step 6: results/*.json 移行 ===")
    if not os.path.exists(RESULTS_DIR):
        log(f"  [SKIP] {RESULTS_DIR} が見つかりません")
        return

    files = sorted(
        [f for f in os.listdir(RESULTS_DIR) if f.endswith("_results.json")]
    )
    log(f"  ファイル数: {len(files)}")

    ok = 0
    err = 0
    for fname in files:
        fpath = os.path.join(RESULTS_DIR, fname)
        raw_date = fname.replace("_results.json", "")
        if len(raw_date) != 8:
            log(f"  [SKIP] ファイル名が不正: {fname}")
            continue
        date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:8]}"
        try:
            data = load_json(fpath)
            if data is None:
                continue
            if not DRY_RUN:
                save_results(date, data)
            ok += 1
            log(f"  [{date}] {len(data)} レース")
        except Exception as e:
            err += 1
            log(f"  [ERROR] {fname}: {e}")

    log(f"  完了: {ok} ファイル成功, {err} エラー")


# ============================================================
# 最終確認
# ============================================================

def step_verify():
    log("\n=== 最終確認 ===")
    if DRY_RUN:
        log("  (dry-run のため DB 確認スキップ)")
        return
    stats = get_db_stats()
    for table, count in stats.items():
        log(f"  {table}: {count} 件")


# ============================================================
# メイン
# ============================================================

if __name__ == "__main__":
    if DRY_RUN:
        log("=== DRY RUN モード（DB への書き込みなし）===")

    step_init()
    step_personnel()
    step_trainer_baseline()
    step_course_db()
    step_predictions()
    step_results()
    step_verify()

    log("\n移行完了!")
