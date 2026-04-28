"""
SQLite データベース ラッパー

テーブル:
  predictions  - 日付別予想データ（races ごとの行）
  race_results - 日付別結果データ（race_id ごとの行）
  match_results- 照合済み集計データ
  personnel    - 騎手・調教師マスタ（JSON blob）
  course_db    - コースDBマスタ（JSON blob）
"""

import json
import os as _os
import sqlite3
import threading
from contextlib import contextmanager
from typing import Dict, List, Optional

from config.settings import DATABASE_PATH
from src.log import get_logger

logger = get_logger(__name__)


# ============================================================
# 接続管理（スレッドセーフ）
# ============================================================

_local = threading.local()


def get_db() -> sqlite3.Connection:
    """スレッドローカルな DB 接続を返す（WAL モード）"""
    if not hasattr(_local, "conn") or _local.conn is None:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA foreign_keys=ON")
        _local.conn = conn
    return _local.conn


@contextmanager
def transaction():
    """トランザクション付きコンテキストマネージャ"""
    conn = get_db()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise


# ============================================================
# スキーマ初期化
# ============================================================

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS predictions (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    date             TEXT NOT NULL,
    race_id          TEXT NOT NULL,
    venue            TEXT NOT NULL DEFAULT '',
    race_no          INTEGER NOT NULL DEFAULT 0,
    race_name        TEXT DEFAULT '',
    surface          TEXT DEFAULT '',
    distance         INTEGER DEFAULT 0,
    grade            TEXT DEFAULT '',
    confidence       TEXT DEFAULT 'B',
    pace_pred        TEXT DEFAULT '',
    field_count      INTEGER DEFAULT 0,
    horses_json      TEXT NOT NULL DEFAULT '[]',
    tickets_json     TEXT NOT NULL DEFAULT '[]',
    formation_json   TEXT NOT NULL DEFAULT '[]',
    value_bets_json  TEXT NOT NULL DEFAULT '[]',
    version          INTEGER DEFAULT 2,
    created_at       TEXT DEFAULT (datetime('now','localtime')),
    UNIQUE(date, race_id)
);
CREATE INDEX IF NOT EXISTS idx_pred_date ON predictions(date);

CREATE TABLE IF NOT EXISTS race_results (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    date         TEXT NOT NULL,
    race_id      TEXT NOT NULL,
    venue        TEXT DEFAULT '',
    race_no      INTEGER DEFAULT 0,
    cancelled    INTEGER DEFAULT 0,
    order_json   TEXT NOT NULL DEFAULT '[]',
    payouts_json TEXT NOT NULL DEFAULT '{}',
    fetched_at   TEXT DEFAULT (datetime('now','localtime')),
    UNIQUE(date, race_id)
);
CREATE INDEX IF NOT EXISTS idx_result_date ON race_results(date);

CREATE TABLE IF NOT EXISTS match_results (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    date           TEXT NOT NULL,
    race_id        TEXT NOT NULL,
    venue          TEXT DEFAULT '',
    race_no        INTEGER DEFAULT 0,
    hit_tickets    INTEGER DEFAULT 0,
    total_tickets  INTEGER DEFAULT 0,
    stake          INTEGER DEFAULT 0,
    ret            INTEGER DEFAULT 0,
    honmei_placed  INTEGER DEFAULT 0,
    honmei_win     INTEGER DEFAULT 0,
    by_mark_json   TEXT DEFAULT '{}',
    by_ticket_json TEXT DEFAULT '{}',
    by_ana_json    TEXT DEFAULT '{}',
    by_kiken_json  TEXT DEFAULT '{}',
    created_at     TEXT DEFAULT (datetime('now','localtime')),
    UNIQUE(date, race_id)
);
CREATE INDEX IF NOT EXISTS idx_match_date ON match_results(date);

CREATE TABLE IF NOT EXISTS personnel (
    person_id   TEXT NOT NULL,
    person_type TEXT NOT NULL,
    data_json   TEXT NOT NULL,
    updated_at  TEXT DEFAULT (datetime('now','localtime')),
    PRIMARY KEY (person_id, person_type)
);

CREATE TABLE IF NOT EXISTS course_db (
    course_key TEXT PRIMARY KEY,
    data_json  TEXT NOT NULL,
    updated_at TEXT DEFAULT (datetime('now','localtime'))
);

CREATE TABLE IF NOT EXISTS race_log (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    race_date    TEXT NOT NULL,
    race_id      TEXT NOT NULL,
    venue_code   TEXT NOT NULL DEFAULT '',
    surface      TEXT DEFAULT '',
    distance     INTEGER DEFAULT 0,
    horse_no     INTEGER NOT NULL,
    finish_pos   INTEGER NOT NULL,
    jockey_id    TEXT DEFAULT '',
    jockey_name  TEXT DEFAULT '',
    trainer_id   TEXT DEFAULT '',
    trainer_name TEXT DEFAULT '',
    field_count  INTEGER DEFAULT 0,
    is_jra       INTEGER DEFAULT 0,
    win_odds     REAL DEFAULT NULL,
    UNIQUE(race_id, horse_no)
);
CREATE INDEX IF NOT EXISTS idx_racelog_date    ON race_log(race_date);
CREATE INDEX IF NOT EXISTS idx_racelog_jockey  ON race_log(jockey_id);
CREATE INDEX IF NOT EXISTS idx_racelog_trainer ON race_log(trainer_id);
CREATE INDEX IF NOT EXISTS idx_racelog_venue   ON race_log(venue_code);
CREATE INDEX IF NOT EXISTS idx_racelog_surface ON race_log(surface);
CREATE INDEX IF NOT EXISTS idx_personnel_type  ON personnel(person_type);
CREATE INDEX IF NOT EXISTS idx_pred_race_id    ON predictions(race_id);
CREATE INDEX IF NOT EXISTS idx_result_race_id  ON race_results(race_id);

CREATE TABLE IF NOT EXISTS horses (
    horse_id         TEXT PRIMARY KEY,              -- 正規 horse_id (10桁数字 or nar_xxx or B_xxx)
    horse_name       TEXT NOT NULL,                 -- 馬名
    sire_name        TEXT,                          -- 父
    dam_name         TEXT,                          -- 母
    bms_name         TEXT,                          -- 母父
    birth_year       INTEGER,                       -- 生年
    sex              TEXT,                          -- 性別
    color            TEXT,                          -- 毛色
    breeder          TEXT,                          -- 生産者
    owner            TEXT,                          -- 馬主
    is_jra           INTEGER DEFAULT 1,             -- JRA 所属フラグ (1=JRA, 0=NAR)
    first_seen_date  TEXT,                          -- race_log 最古出走日
    last_seen_date   TEXT,                          -- race_log 最新出走日
    race_count       INTEGER DEFAULT 0,             -- 通算出走回数
    netkeiba_id      TEXT,                          -- D Phase 2: netkeiba horse_id (10桁数字。old_10digitはhorse_id直値。nar/B_prefixはNULL)
    created_at       TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at       TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_horses_name        ON horses(horse_name);
CREATE INDEX IF NOT EXISTS idx_horses_lastseen    ON horses(last_seen_date DESC);
"""


_SCHEMA_INITIALIZED = False


def init_schema() -> None:
    """テーブルを初期化する（冪等・既存データ保持）"""
    global _SCHEMA_INITIALIZED
    if _SCHEMA_INITIALIZED:
        return
    conn = get_db()
    conn.executescript(_SCHEMA_SQL)
    # 既存DBへの列追加・インデックス追加（ALTER TABLE/CREATE INDEX は IF NOT EXISTS 非対応なので try/except）
    for ddl in [
        # 既存マイグレーション
        "ALTER TABLE race_log ADD COLUMN win_odds REAL DEFAULT NULL",
        "ALTER TABLE race_log ADD COLUMN running_style TEXT DEFAULT NULL",
        "ALTER TABLE race_log ADD COLUMN sire_name TEXT DEFAULT ''",
        "ALTER TABLE race_log ADD COLUMN bms_name TEXT DEFAULT ''",
        "ALTER TABLE race_log ADD COLUMN condition TEXT DEFAULT ''",
        # Phase 1: race_log完全化（30カラム追加）
        # 馬レベル（走行データ）
        "ALTER TABLE race_log ADD COLUMN horse_id TEXT DEFAULT ''",
        "ALTER TABLE race_log ADD COLUMN horse_name TEXT DEFAULT ''",
        "ALTER TABLE race_log ADD COLUMN gate_no INTEGER DEFAULT 0",
        "ALTER TABLE race_log ADD COLUMN sex TEXT DEFAULT ''",
        "ALTER TABLE race_log ADD COLUMN age INTEGER DEFAULT 0",
        "ALTER TABLE race_log ADD COLUMN weight_kg REAL DEFAULT 0",
        "ALTER TABLE race_log ADD COLUMN odds REAL",
        "ALTER TABLE race_log ADD COLUMN tansho_odds REAL",
        "ALTER TABLE race_log ADD COLUMN popularity INTEGER",
        "ALTER TABLE race_log ADD COLUMN horse_weight INTEGER",
        "ALTER TABLE race_log ADD COLUMN weight_change INTEGER",
        "ALTER TABLE race_log ADD COLUMN position_4c INTEGER DEFAULT 0",
        "ALTER TABLE race_log ADD COLUMN positions_corners TEXT DEFAULT ''",
        "ALTER TABLE race_log ADD COLUMN finish_time_sec REAL DEFAULT 0",
        "ALTER TABLE race_log ADD COLUMN last_3f_sec REAL DEFAULT 0",
        "ALTER TABLE race_log ADD COLUMN first_3f_sec REAL",
        "ALTER TABLE race_log ADD COLUMN margin_ahead REAL DEFAULT 0",
        "ALTER TABLE race_log ADD COLUMN margin_behind REAL DEFAULT 0",
        "ALTER TABLE race_log ADD COLUMN status TEXT",
        # レースレベル
        "ALTER TABLE race_log ADD COLUMN course_id TEXT DEFAULT ''",
        "ALTER TABLE race_log ADD COLUMN grade TEXT DEFAULT ''",
        "ALTER TABLE race_log ADD COLUMN race_name TEXT DEFAULT ''",
        "ALTER TABLE race_log ADD COLUMN weather TEXT DEFAULT ''",
        "ALTER TABLE race_log ADD COLUMN direction TEXT DEFAULT ''",
        "ALTER TABLE race_log ADD COLUMN race_first_3f REAL",
        "ALTER TABLE race_log ADD COLUMN race_pace TEXT DEFAULT ''",
        # 計算値・メタ
        "ALTER TABLE race_log ADD COLUMN pace TEXT",
        "ALTER TABLE race_log ADD COLUMN is_generation INTEGER DEFAULT 0",
        "ALTER TABLE race_log ADD COLUMN race_level_dev REAL",
        "ALTER TABLE race_log ADD COLUMN run_dev REAL",  # 走破偏差値（馬指数グラフ用、バックフィル＋日次更新）
        "ALTER TABLE race_log ADD COLUMN source TEXT DEFAULT ''",
        # training_recordsテーブル
        """CREATE TABLE IF NOT EXISTS training_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            race_id TEXT NOT NULL,
            horse_name TEXT NOT NULL,
            horse_id TEXT DEFAULT '',
            date TEXT DEFAULT '',
            course TEXT DEFAULT '',
            splits_json TEXT DEFAULT '{}',
            rider TEXT DEFAULT '',
            track_condition TEXT DEFAULT '',
            lap_count TEXT DEFAULT '',
            intensity_label TEXT DEFAULT '',
            sigma_from_mean REAL DEFAULT 0,
            comment TEXT DEFAULT '',
            stable_comment TEXT DEFAULT '',
            source TEXT DEFAULT 'keibabook',
            UNIQUE(race_id, horse_name, date, course)
        )""",
        # インデックス
        "CREATE INDEX IF NOT EXISTS idx_racelog_sire ON race_log(sire_name)",
        "CREATE INDEX IF NOT EXISTS idx_racelog_bms  ON race_log(bms_name)",
        "CREATE INDEX IF NOT EXISTS idx_racelog_venue   ON race_log(venue_code)",
        "CREATE INDEX IF NOT EXISTS idx_racelog_surface ON race_log(surface)",
        "CREATE INDEX IF NOT EXISTS idx_racelog_horseid ON race_log(horse_id)",
        "CREATE INDEX IF NOT EXISTS idx_racelog_horseid_date ON race_log(horse_id, race_date DESC)",
        "CREATE INDEX IF NOT EXISTS idx_racelog_finish  ON race_log(finish_pos)",
        # 追加インデックス（jockey_name検索の高速化、venue×surface×distance集計）
        "CREATE INDEX IF NOT EXISTS idx_racelog_jockeyname ON race_log(jockey_name)",
        "CREATE INDEX IF NOT EXISTS idx_racelog_trainername ON race_log(trainer_name)",
        "CREATE INDEX IF NOT EXISTS idx_racelog_venue_surf_dist ON race_log(venue_code, surface, distance)",
        "CREATE INDEX IF NOT EXISTS idx_racelog_sirename ON race_log(sire_name)",
        "CREATE INDEX IF NOT EXISTS idx_racelog_bmsname ON race_log(bms_name)",
        "CREATE INDEX IF NOT EXISTS idx_training_raceid ON training_records(race_id)",
        "CREATE INDEX IF NOT EXISTS idx_training_horse  ON training_records(horse_name)",
        "CREATE INDEX IF NOT EXISTS idx_personnel_type  ON personnel(person_type)",
        "CREATE INDEX IF NOT EXISTS idx_pred_race_id    ON predictions(race_id)",
        "CREATE INDEX IF NOT EXISTS idx_result_race_id  ON race_results(race_id)",
        # UNIQUE INDEX 追加（重複行再発防止 2026-04-27）
        # predictions: 同一 race_id × 異なる date の重複を禁止（save_prediction の OR REPLACE を確実に動かす）
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_predictions_race_id_unique ON predictions(race_id)",
        # race_log: INSERT OR IGNORE の dedupe 動作保証（重複行混入防止）
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_racelog_race_horse_unique ON race_log(race_id, horse_no)",
        # LLM パラフレーズキャッシュ（ローカル Qwen2.5-7B by LM Studio）
        """CREATE TABLE IF NOT EXISTS stable_comment_paraphrase_cache (
            input_hash TEXT PRIMARY KEY,
            original TEXT NOT NULL,
            paraphrased TEXT NOT NULL,
            created_at TEXT DEFAULT (datetime('now','localtime'))
        )""",
        "CREATE INDEX IF NOT EXISTS idx_paraphrase_created ON stable_comment_paraphrase_cache(created_at)",
        # Plan-γ Phase 1: 相対偏差値カラム追加（2026-04-27）
        # 同 race_id 内の run_dev を z-score 正規化した値（帯広は順位ベース）
        "ALTER TABLE race_log ADD COLUMN relative_dev REAL",
        "CREATE INDEX IF NOT EXISTS idx_racelog_relative_dev ON race_log(relative_dev)",
        # D Phase 1: horses マスターテーブル（2026-04-28）
        # 既存 DB への確実な適用のため for ddl にも記載（_SCHEMA_SQL と冪等）
        """CREATE TABLE IF NOT EXISTS horses (
            horse_id         TEXT PRIMARY KEY,
            horse_name       TEXT NOT NULL,
            sire_name        TEXT,
            dam_name         TEXT,
            bms_name         TEXT,
            birth_year       INTEGER,
            sex              TEXT,
            color            TEXT,
            breeder          TEXT,
            owner            TEXT,
            is_jra           INTEGER DEFAULT 1,
            first_seen_date  TEXT,
            last_seen_date   TEXT,
            race_count       INTEGER DEFAULT 0,
            created_at       TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at       TEXT DEFAULT CURRENT_TIMESTAMP
        )""",
        "CREATE INDEX IF NOT EXISTS idx_horses_name     ON horses(horse_name)",
        "CREATE INDEX IF NOT EXISTS idx_horses_lastseen ON horses(last_seen_date DESC)",
        # D Phase 2: netkeiba_id カラム追加（2026-04-28）
        # old_10digit（10桁数字）の horse_id は netkeiba_id 直値。nar/B_prefix は NULL のまま（将来スクレイパー連携）
        "ALTER TABLE horses ADD COLUMN netkeiba_id TEXT",
        "CREATE INDEX IF NOT EXISTS idx_horses_netkeiba_id ON horses(netkeiba_id)",
    ]:
        try:
            conn.execute(ddl)
        except sqlite3.OperationalError as _e:
            # 既存カラム/インデックスはスキップ（duplicate column / already exists 等）
            _msg = str(_e).lower()
            if "duplicate column" in _msg or "already exists" in _msg:
                continue
            logger.debug("マイグレーション失敗 (継続): %s → %s", ddl[:60], _e)
    # 冗長インデックス削除（UNIQUE(date, race_id) でカバー済み）
    try:
        conn.execute("DROP INDEX IF EXISTS idx_match_date")
    except Exception:
        pass
    conn.commit()
    # executescript() が PRAGMA をリセットするため再設定
    conn.execute("PRAGMA foreign_keys=ON")
    _SCHEMA_INITIALIZED = True


# ============================================================
# DB バックアップ・クリーンアップ
# ============================================================


def backup_db(backup_dir: str = None, retain_count: int = 7) -> str:
    """
    SQLite オンラインバックアップ（VACUUM中もブロックしない）。
    backup_dir には日付付きファイル名で保存し、古い世代は retain_count を超えたら削除。
    Returns: バックアップファイルパス（失敗時は空文字列）
    """
    import glob as _glob
    from datetime import datetime as _dt

    if backup_dir is None:
        backup_dir = _os.path.join(_os.path.dirname(DATABASE_PATH), "backups")
    _os.makedirs(backup_dir, exist_ok=True)

    ts = _dt.now().strftime("%Y%m%d_%H%M%S")
    bk_path = _os.path.join(backup_dir, f"keiba_{ts}.db")
    try:
        src_conn = sqlite3.connect(DATABASE_PATH)
        dst_conn = sqlite3.connect(bk_path)
        try:
            src_conn.backup(dst_conn)
        finally:
            dst_conn.close()
            src_conn.close()
        logger.info("DB バックアップ完了: %s", bk_path)
    except Exception as e:
        logger.warning("DB バックアップ失敗: %s", e, exc_info=True)
        return ""

    # 古いバックアップを削除（retain_count を超えた分）
    try:
        all_bks = sorted(_glob.glob(_os.path.join(backup_dir, "keiba_*.db")))
        if len(all_bks) > retain_count:
            for old in all_bks[:-retain_count]:
                try:
                    _os.remove(old)
                    logger.debug("古いバックアップを削除: %s", old)
                except OSError:
                    pass
    except Exception:
        pass
    return bk_path


def cleanup_db(drop_old_tables: bool = False) -> dict:
    """
    DB クリーンアップ:
    - predictions_old テーブル削除（drop_old_tables=True 時）
    - horse_id 空文字行のカウント報告
    Returns: {"dropped": [...], "empty_horse_id": int, "vacuum": bool}
    """
    result = {"dropped": [], "empty_horse_id": 0, "vacuum": False}
    conn = get_db()
    try:
        # 空horse_idカウント
        result["empty_horse_id"] = conn.execute(
            "SELECT COUNT(*) FROM race_log WHERE horse_id IS NULL OR horse_id=''"
        ).fetchone()[0]

        if drop_old_tables:
            # 古いテーブルが存在する場合のみ削除
            for tbl in ["predictions_old"]:
                exists = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                    (tbl,)
                ).fetchone()
                if exists:
                    conn.execute(f"DROP TABLE {tbl}")
                    result["dropped"].append(tbl)
                    logger.info("古いテーブルを削除: %s", tbl)
            conn.commit()
    except Exception as e:
        logger.warning("cleanup_db 失敗: %s", e, exc_info=True)
    return result


# ============================================================
# 予想データ CRUD
# ============================================================


def save_prediction(date: str, payload: dict) -> None:
    """
    予想データを DB に保存する。
    payload = {"date": date, "version": 2, "races": [...]}
    races の各要素を 1 行として INSERT OR REPLACE。
    """
    races = payload.get("races", [])
    with transaction() as conn:
        for race in races:
            conn.execute(
                """
                INSERT OR REPLACE INTO predictions
                  (date, race_id, venue, race_no, race_name, surface, distance,
                   grade, confidence, pace_pred, field_count,
                   horses_json, tickets_json, formation_json, value_bets_json, version)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    date,
                    race.get("race_id", ""),
                    race.get("venue", ""),
                    race.get("race_no", 0),
                    race.get("race_name", ""),
                    race.get("surface", ""),
                    race.get("distance", 0),
                    race.get("grade", ""),
                    race.get("confidence", "B"),
                    race.get("pace_predicted", ""),
                    race.get("field_count", 0),
                    json.dumps(race.get("horses", []), ensure_ascii=False),
                    json.dumps(race.get("tickets", []), ensure_ascii=False),
                    json.dumps(race.get("formation_tickets", []), ensure_ascii=False),
                    json.dumps(race.get("value_bets", []), ensure_ascii=False),
                    payload.get("version", 2),
                ),
            )


def load_prediction(date: str) -> Optional[dict]:
    """
    指定日の予想データを DB から読み込み、JSON ファイルと同じ形式で返す。
    Returns: {"date": date, "version": 2, "races": [...]} or None
    """
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM predictions WHERE date=? ORDER BY race_no", (date,)
    ).fetchall()
    if not rows:
        return None

    races = []
    version = 2
    for row in rows:
        version = row["version"]
        race = {
            "race_id": row["race_id"],
            "venue": row["venue"],
            "race_no": row["race_no"],
            "race_name": row["race_name"],
            "surface": row["surface"],
            "distance": row["distance"],
            "grade": row["grade"],
            "confidence": row["confidence"],
            "pace_predicted": row["pace_pred"],
            "field_count": row["field_count"],
            "horses": json.loads(row["horses_json"]),
            "tickets": json.loads(row["tickets_json"]),
            "formation_tickets": json.loads(row["formation_json"]),
            "value_bets": json.loads(row["value_bets_json"]),
        }
        races.append(race)

    return {"date": date, "version": version, "races": races}


def list_prediction_dates() -> List[str]:
    """予想済み日付一覧（新しい順、YYYY-MM-DD 形式）"""
    conn = get_db()
    rows = conn.execute(
        "SELECT DISTINCT date FROM predictions ORDER BY date DESC"
    ).fetchall()
    return [row["date"] for row in rows]


def prediction_exists(date: str) -> bool:
    """指定日の予想データが存在するか"""
    conn = get_db()
    row = conn.execute(
        "SELECT 1 FROM predictions WHERE date=? LIMIT 1", (date,)
    ).fetchone()
    return row is not None


def get_predictions_by_date_range(from_date: str, to_date: str) -> List[dict]:
    """
    日付範囲内の全予想レース一覧（軽量）を返す。
    各要素: {date, race_id, venue, race_no, confidence, field_count}
    """
    conn = get_db()
    rows = conn.execute(
        """
        SELECT date, race_id, venue, race_no, race_name, surface, distance,
               grade, confidence, field_count
        FROM predictions
        WHERE date BETWEEN ? AND ?
        ORDER BY date DESC, race_no
        """,
        (from_date, to_date),
    ).fetchall()
    return [dict(row) for row in rows]


# ============================================================
# 結果データ CRUD
# ============================================================


def save_results(date: str, data: dict) -> None:
    """
    結果データを DB に保存する。
    data = {race_id: {"order": [...], "payouts": {...}}}
    """
    with transaction() as conn:
        for race_id, result in data.items():
            conn.execute(
                """
                INSERT OR REPLACE INTO race_results
                  (date, race_id, order_json, payouts_json)
                VALUES (?,?,?,?)
                """,
                (
                    date,
                    race_id,
                    json.dumps(result.get("order", []), ensure_ascii=False),
                    json.dumps(result.get("payouts", {}), ensure_ascii=False),
                ),
            )


def load_results(date: str) -> Optional[dict]:
    """
    指定日の結果データを DB から読み込み、JSON ファイルと同じ形式で返す。
    Returns: {race_id: {"order": [...], "payouts": {...}}} or None
    """
    conn = get_db()
    rows = conn.execute(
        "SELECT race_id, order_json, payouts_json FROM race_results WHERE date=?",
        (date,),
    ).fetchall()
    if not rows:
        return None

    result = {}
    for row in rows:
        result[row["race_id"]] = {
            "order": json.loads(row["order_json"]),
            "payouts": json.loads(row["payouts_json"]),
        }
    return result


def results_exist(date: str) -> bool:
    """指定日の結果データが存在するか"""
    conn = get_db()
    row = conn.execute(
        "SELECT 1 FROM race_results WHERE date=? LIMIT 1", (date,)
    ).fetchone()
    return row is not None


# ============================================================
# 照合済み集計 CRUD
# ============================================================


def _stats_to_row(date: str, race_id: str, stats: dict) -> tuple:
    """save_match_result(s) 共通: stats dict → INSERT/UPDATE 用タプル"""
    return (
        date,
        race_id,
        stats.get("venue", ""),
        stats.get("race_no", 0),
        stats.get("hit_tickets", 0),
        stats.get("total_tickets", 0),
        stats.get("stake", 0),
        stats.get("ret", 0),
        stats.get("honmei_placed", 0),
        stats.get("honmei_win", 0),
        json.dumps(stats.get("by_mark", {}), ensure_ascii=False),
        json.dumps(stats.get("by_ticket", {}), ensure_ascii=False),
        json.dumps(stats.get("by_ana", {}), ensure_ascii=False),
        json.dumps(stats.get("by_kiken", {}), ensure_ascii=False),
    )


# T-001 (2026-04-25 reviewer HIGH-1): INSERT OR REPLACE は AUTOINCREMENT id と
# created_at を毎回破棄するため、created_at（最初の保存時刻）の監査ログが失われる。
# ON CONFLICT DO UPDATE で UPSERT し、created_at は再照合時も保持する。
_MATCH_RESULTS_UPSERT_SQL = """
INSERT INTO match_results
  (date, race_id, venue, race_no, hit_tickets, total_tickets,
   stake, ret, honmei_placed, honmei_win,
   by_mark_json, by_ticket_json, by_ana_json, by_kiken_json)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
ON CONFLICT(date, race_id) DO UPDATE SET
  venue=excluded.venue, race_no=excluded.race_no,
  hit_tickets=excluded.hit_tickets, total_tickets=excluded.total_tickets,
  stake=excluded.stake, ret=excluded.ret,
  honmei_placed=excluded.honmei_placed, honmei_win=excluded.honmei_win,
  by_mark_json=excluded.by_mark_json, by_ticket_json=excluded.by_ticket_json,
  by_ana_json=excluded.by_ana_json, by_kiken_json=excluded.by_kiken_json
"""


def save_match_result(date: str, race_id: str, stats: dict) -> None:
    """照合済み集計を 1 レース分保存（単発用、created_at 保持）"""
    with transaction() as conn:
        conn.execute(_MATCH_RESULTS_UPSERT_SQL, _stats_to_row(date, race_id, stats))


def save_match_results_bulk(date: str, rows: list) -> int:
    """照合済み集計をバッチで一括保存（reviewer HIGH-2 対応）。
    results_tracker.compare_and_aggregate からの呼び出し用。
    rows: List[Tuple[race_id, stats_dict]]
    Returns: 保存件数
    """
    if not rows:
        return 0
    payload = [_stats_to_row(date, rid, s) for rid, s in rows]
    with transaction() as conn:
        conn.executemany(_MATCH_RESULTS_UPSERT_SQL, payload)
    return len(payload)


def aggregate_results(from_date: str = "2026-01-01", to_date: str = "2099-12-31") -> dict:
    """
    指定期間の集計を SQL 1 クエリで集計して返す。
    Returns: results_tracker.aggregate_all() と同形式の dict
    """
    conn = get_db()

    # 基本集計
    row = conn.execute(
        """
        SELECT
            COUNT(*)       AS total_races,
            SUM(hit_tickets)   AS hit_tickets,
            SUM(total_tickets) AS total_tickets,
            SUM(stake)     AS total_stake,
            SUM(ret)       AS total_return,
            SUM(honmei_placed) AS honmei_placed,
            SUM(honmei_win)    AS honmei_win
        FROM match_results
        WHERE date BETWEEN ? AND ?
        """,
        (from_date, to_date),
    ).fetchone()

    total_races = row["total_races"] or 0
    hit_tickets = row["hit_tickets"] or 0
    total_tickets = row["total_tickets"] or 0
    total_stake = row["total_stake"] or 0
    total_return = row["total_return"] or 0
    honmei_placed = row["honmei_placed"] or 0
    honmei_win = row["honmei_win"] or 0

    # 印別・券種別・穴馬・危険馬集計（Python 側でマージ）
    rows = conn.execute(
        "SELECT by_mark_json, by_ticket_json, by_ana_json, by_kiken_json FROM match_results WHERE date BETWEEN ? AND ?",
        (from_date, to_date),
    ).fetchall()

    by_mark: Dict[str, dict] = {}
    by_ticket: Dict[str, dict] = {}
    by_ana = {"total": 0, "win": 0, "placed": 0}
    by_kiken = {"total": 0, "fell_through": 0}

    def _merge(dest: dict, src: dict):
        for k, v in src.items():
            if k not in dest:
                dest[k] = {kk: 0 for kk in v}
            for kk, vv in v.items():
                dest[k][kk] = dest[k].get(kk, 0) + (vv or 0)

    for r in rows:
        _merge(by_mark, json.loads(r["by_mark_json"] or "{}"))
        _merge(by_ticket, json.loads(r["by_ticket_json"] or "{}"))
        for k in ("total", "win", "placed"):
            val = json.loads(r["by_ana_json"] or "{}")
            by_ana[k] += val.get(k, 0)
        for k in ("total", "fell_through"):
            val = json.loads(r["by_kiken_json"] or "{}")
            by_kiken[k] += val.get(k, 0)

    return {
        "total_races": total_races,
        "hit_tickets": hit_tickets,
        "total_tickets": total_tickets,
        "total_stake": total_stake,
        "total_return": total_return,
        "honmei_placed": honmei_placed,
        "honmei_win": honmei_win,
        "by_mark": by_mark,
        "by_ticket": by_ticket,
        "by_ana": by_ana,
        "by_kiken": by_kiken,
    }


# ============================================================
# 騎手・調教師マスタ CRUD
# ============================================================


def get_personnel_all() -> dict:
    """
    全騎手・全調教師を personnel_db.json と同じ形式で返す。
    Returns: {"jockeys": {id: data}, "trainers": {id: data}}
    idx_personnel_type インデックスで person_type 別に分割取得（高速化）
    """
    conn = get_db()
    result: dict = {"jockeys": {}, "trainers": {}}
    for ptype, key in [("jockey", "jockeys"), ("trainer", "trainers")]:
        rows = conn.execute(
            "SELECT person_id, data_json FROM personnel WHERE person_type=?",
            (ptype,),
        ).fetchall()
        for row in rows:
            result[key][row["person_id"]] = json.loads(row["data_json"])
    return result


def get_personnel(person_id: str, person_type: str) -> Optional[dict]:
    """1人分のデータを返す"""
    conn = get_db()
    row = conn.execute(
        "SELECT data_json FROM personnel WHERE person_id=? AND person_type=?",
        (person_id, person_type),
    ).fetchone()
    return json.loads(row["data_json"]) if row else None


def set_personnel(person_id: str, person_type: str, data: dict) -> None:
    """1人分のデータを保存（INSERT OR REPLACE）"""
    with transaction() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO personnel (person_id, person_type, data_json, updated_at)
            VALUES (?, ?, ?, datetime('now','localtime'))
            """,
            (person_id, person_type, json.dumps(data, ensure_ascii=False)),
        )


def save_personnel_all(data: dict) -> None:
    """
    personnel_db.json 形式の dict を一括保存。
    data = {"jockeys": {id: {...}}, "trainers": {id: {...}}}
    """
    with transaction() as conn:
        for jid, jdata in data.get("jockeys", {}).items():
            conn.execute(
                """
                INSERT OR REPLACE INTO personnel (person_id, person_type, data_json, updated_at)
                VALUES (?, 'jockey', ?, datetime('now','localtime'))
                """,
                (jid, json.dumps(jdata, ensure_ascii=False)),
            )
        for tid, tdata in data.get("trainers", {}).items():
            conn.execute(
                """
                INSERT OR REPLACE INTO personnel (person_id, person_type, data_json, updated_at)
                VALUES (?, 'trainer', ?, datetime('now','localtime'))
                """,
                (tid, json.dumps(tdata, ensure_ascii=False)),
            )


def personnel_count() -> int:
    """登録件数を返す"""
    conn = get_db()
    return conn.execute("SELECT COUNT(*) FROM personnel").fetchone()[0]


# ============================================================
# コース DB CRUD
# ============================================================


def get_course_db(keys: Optional[List[str]] = None) -> dict:
    """
    コース DB エントリを返す。
    keys=None なら全件。keys 指定なら該当のみ。
    Returns: {course_key: list_of_records}
    """
    conn = get_db()
    if keys is None:
        rows = conn.execute("SELECT course_key, data_json FROM course_db").fetchall()
    else:
        if not keys:
            return {}
        placeholders = ",".join("?" * len(keys))
        rows = conn.execute(
            f"SELECT course_key, data_json FROM course_db WHERE course_key IN ({placeholders})",
            keys,
        ).fetchall()
    return {row["course_key"]: json.loads(row["data_json"]) for row in rows}


def set_course_db(entries: dict) -> None:
    """
    コース DB エントリを一括保存。
    entries = {course_key: list_of_records}
    """
    with transaction() as conn:
        for key, value in entries.items():
            conn.execute(
                """
                INSERT OR REPLACE INTO course_db (course_key, data_json, updated_at)
                VALUES (?, ?, datetime('now','localtime'))
                """,
                (key, json.dumps(value, ensure_ascii=False)),
            )


def course_db_count() -> int:
    """コース DB エントリ数を返す"""
    conn = get_db()
    return conn.execute("SELECT COUNT(*) FROM course_db").fetchone()[0]


# ============================================================
# ユーティリティ
# ============================================================


def close_db() -> None:
    """スレッドローカル接続をクローズ"""
    if hasattr(_local, "conn") and _local.conn:
        _local.conn.close()
        _local.conn = None


def get_course_pace_tendency(target_date: str = None) -> dict:
    """
    race_log から venue_code × surface × distance 別のペース傾向を集計。

    Args:
        target_date: ローリングウィンドウ基準日 (YYYY-MM-DD)。
                     指定時は基準日の1年前〜前日のデータのみ使用。

    Returns:
        {(venue_code, surface, distance): {
            "escape_rate": float,   # 逃げ馬比率 (0.0〜1.0)
            "front_rate":  float,   # 逃げ+先行比率
            "slow_rate":   float,   # 差し+追込比率
            "race_cnt":    int,     # 集計レース数
        }}
    """
    conn = get_db()

    date_clause = ""
    params = []
    if target_date:
        from datetime import datetime, timedelta
        cutoff = (datetime.strptime(target_date, "%Y-%m-%d") - timedelta(days=365)).strftime("%Y-%m-%d")
        date_clause = "AND race_date < ? AND race_date >= ?"
        params = [target_date, cutoff]

    rows = conn.execute(
        f"""
        SELECT
            venue_code, surface, distance,
            COUNT(DISTINCT race_id)                                        AS race_cnt,
            SUM(CASE WHEN running_style='逃げ'              THEN 1.0 ELSE 0.0 END)
                / NULLIF(COUNT(*), 0)                                      AS escape_rate,
            SUM(CASE WHEN running_style IN ('逃げ','先行')  THEN 1.0 ELSE 0.0 END)
                / NULLIF(COUNT(*), 0)                                      AS front_rate,
            SUM(CASE WHEN running_style IN ('差し','追込')  THEN 1.0 ELSE 0.0 END)
                / NULLIF(COUNT(*), 0)                                      AS slow_rate
        FROM race_log
        WHERE running_style IS NOT NULL AND running_style != ''
              {date_clause}
        GROUP BY venue_code, surface, distance
        HAVING race_cnt >= 5
        """,
        params,
    ).fetchall()
    result = {}
    for r in rows:
        key = (str(r["venue_code"]), str(r["surface"]), int(r["distance"]))
        result[key] = {
            "escape_rate": float(r["escape_rate"] or 0.0),
            "front_rate":  float(r["front_rate"]  or 0.0),
            "slow_rate":   float(r["slow_rate"]   or 0.0),
            "race_cnt":    int(r["race_cnt"]),
        }
    return result


def get_course_last3f_sigma(target_date: str = None) -> dict:
    """
    race_log から venue_code × surface × distance 別の
    上がり3F タイムの標準偏差（σ）を集計。

    base_score の変換係数 5.0 の動的決定に使用（案F-4）。
    σが大きいコース（=差がつきやすい）→ 係数小さく
    σが小さいコース（=差がつきにくい）→ 係数大きく

    Args:
        target_date: ローリングウィンドウ基準日 (YYYY-MM-DD)。
                     指定時は基準日の1年前〜前日のデータのみ使用。

    Returns:
        {(venue_code, surface, distance): {"sigma": float, "mean": float, "cnt": int}}
    """
    conn = get_db()

    # ローリングウィンドウ日付範囲
    cutoff_date = None
    if target_date:
        from datetime import datetime, timedelta
        cutoff_date = (datetime.strptime(target_date, "%Y-%m-%d") - timedelta(days=365)).strftime("%Y-%m-%d")

    # course_db から last_3f_sec の分布を取得
    result = {}
    try:
        cdb_rows = conn.execute(
            "SELECT course_key, data_json FROM course_db"
        ).fetchall()
        import json as _json
        import statistics as _stat

        # コース別の last_3f リスト構築
        l3f_by_course: dict = {}
        for row in cdb_rows:
            try:
                data = _json.loads(row["data_json"])
                if not isinstance(data, list):
                    continue
                key_parts = row["course_key"].split("_")
                if len(key_parts) < 3:
                    continue
                venue = key_parts[0]
                surface = key_parts[1]
                dist = int(key_parts[2]) if key_parts[2].isdigit() else 0
                if dist == 0:
                    continue

                # ローリングウィンドウ: 日付フィルタ適用
                filtered_data = data
                if target_date and cutoff_date:
                    filtered_data = [
                        r for r in data
                        if isinstance(r, dict) and cutoff_date <= r.get("race_date", "") < target_date
                    ]

                l3f_list = [r.get("last_3f_sec", 0) for r in filtered_data
                            if isinstance(r, dict) and r.get("last_3f_sec", 0) > 30]
                if len(l3f_list) >= 10:
                    key = (venue, surface, dist)
                    if key not in l3f_by_course:
                        l3f_by_course[key] = []
                    l3f_by_course[key].extend(l3f_list)
            except Exception:
                continue

        for key, times in l3f_by_course.items():
            if len(times) >= 20:
                mean = _stat.mean(times)
                sigma = _stat.stdev(times)
                result[key] = {"sigma": round(sigma, 4), "mean": round(mean, 4), "cnt": len(times)}
    except Exception:
        pass

    return result


def get_gate_bias_from_race_log(target_date: str = None) -> dict:
    """
    race_log から venue_code × surface 別（+ 距離別精密版）の枠番（gate_no）
    複勝率を集計し、枠順バイアスDBを自動構築する（案F-5）。

    既存の gate_bias_db を上書きするのではなく、
    race_log ベースのデータとしてエンジンで合成して使用する。

    Args:
        target_date: ローリングウィンドウ基準日 (YYYY-MM-DD)。
                     指定時は基準日の1年前〜前日のデータのみ使用。

    Returns:
        {"venue_surface": {gate_no: bias_score},
         "venue_surface_distance": {gate_no: bias_score}}  # 距離別精密版
        例: {"01_芝": {1: 2.1, ..., 8: -1.8},
             "44_ダート_1600": {1: 1.5, ..., 8: -0.8}}
    """
    conn = get_db()

    # gate_no 列が存在するか確認
    try:
        conn.execute("SELECT gate_no FROM race_log LIMIT 1").fetchone()
    except Exception:
        return {}  # gate_no 列なし

    date_clause = ""
    params = []
    if target_date:
        from datetime import datetime, timedelta
        cutoff = (datetime.strptime(target_date, "%Y-%m-%d") - timedelta(days=365)).strftime("%Y-%m-%d")
        date_clause = "AND race_date < ? AND race_date >= ?"
        params = [target_date, cutoff]

    try:
        # 距離別も含めて一括取得（distance付きGROUP BY）
        rows = conn.execute(
            f"""
            SELECT
                venue_code, surface, distance, gate_no,
                COUNT(*) AS total,
                SUM(CASE WHEN finish_pos <= 3 THEN 1.0 ELSE 0.0 END) AS place3
            FROM race_log
            WHERE gate_no IS NOT NULL AND gate_no BETWEEN 1 AND 8
              AND finish_pos IS NOT NULL AND finish_pos > 0
              AND field_count >= 8
              {date_clause}
            GROUP BY venue_code, surface, distance, gate_no
            HAVING total >= 15
            """,
            params,
        ).fetchall()
    except Exception:
        return {}

    from collections import defaultdict

    # venue_surface ごとに集計（従来互換）
    venue_data: dict = defaultdict(lambda: defaultdict(lambda: {"total": 0, "place3": 0}))
    # venue_surface_distance ごとに集計（距離別精密版）
    dist_data: dict = defaultdict(lambda: defaultdict(lambda: {"total": 0, "place3": 0}))

    for r in rows:
        key = f"{r['venue_code']}_{r['surface']}"
        dist_key = f"{r['venue_code']}_{r['surface']}_{r['distance']}"
        gate = int(r["gate_no"])
        venue_data[key][gate]["total"] += r["total"]
        venue_data[key][gate]["place3"] += r["place3"]
        dist_data[dist_key][gate]["total"] += r["total"]
        dist_data[dist_key][gate]["place3"] += r["place3"]

    def _calc_bias(gates_dict: dict) -> dict:
        """枠別データから偏差値スケールのバイアス値を算出"""
        all_totals = sum(g["total"] for g in gates_dict.values())
        all_place3 = sum(g["place3"] for g in gates_dict.values())
        avg_rate = all_place3 / all_totals if all_totals > 0 else 1 / 3

        gate_bias = {}
        for gate_no, stats in gates_dict.items():
            if stats["total"] < 15:
                continue
            rate = stats["place3"] / stats["total"]
            diff = rate - avg_rate
            bias = max(-5.0, min(5.0, diff * 20.0))
            gate_bias[gate_no] = round(bias, 2)
        return gate_bias

    result = {}

    # 従来の venue_surface キー
    for key, gates in venue_data.items():
        gate_bias = _calc_bias(gates)
        if gate_bias:
            result[key] = gate_bias

    # 距離別精密版（サンプル数30以上の枠が4つ以上ある距離のみ）
    _MIN_DIST_SAMPLES = 30
    for dist_key, gates in dist_data.items():
        qualified_gates = {g: s for g, s in gates.items() if s["total"] >= _MIN_DIST_SAMPLES}
        if len(qualified_gates) >= 4:
            gate_bias = _calc_bias(qualified_gates)
            if gate_bias:
                result[dist_key] = gate_bias

    return result


def get_db_stats() -> dict:
    """各テーブルの件数を返す（ヘルスチェック用）"""
    conn = get_db()
    tables = ["predictions", "race_results", "match_results", "personnel", "course_db"]
    stats = {}
    for t in tables:
        stats[t] = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
    return stats


# ============================================================
# 騎手・調教師 成績集計（predictions × race_results JOIN）
# ============================================================

# JRA開催場名一覧
_JRA_VENUES = frozenset([
    "東京", "中山", "京都", "阪神", "中京", "小倉",
    "新潟", "福島", "札幌", "函館",
])


def _smile_key(dist: int) -> str:
    """距離 → SMILE カテゴリ"""
    if dist <= 1000:
        return "SS"
    elif dist <= 1400:
        return "S"
    elif dist <= 1800:
        return "M"
    elif dist <= 2200:
        return "I"
    elif dist <= 2600:
        return "L"
    else:
        return "E"


def _dist_key(dist_str: str) -> str:
    """距離文字列 → 距離帯キー"""
    try:
        d = int(str(dist_str).replace("m", ""))
    except (ValueError, TypeError):
        return "不明"
    if d <= 1200:
        return "〜1200m"
    elif d <= 1400:
        return "1201〜1400m"
    elif d <= 1600:
        return "1401〜1600m"
    elif d <= 1800:
        return "1601〜1800m"
    elif d <= 2000:
        return "1801〜2000m"
    elif d <= 2200:
        return "2001〜2200m"
    else:
        return "2201m〜"


def _empty_stat() -> dict:
    return {"total": 0, "win": 0, "place2": 0, "place3": 0}


def _add_finish(stat: dict, finish: int) -> None:
    # 取消・除外（着順90以上）は集計対象外
    if finish >= 90:
        return
    stat["total"] += 1
    if finish == 1:
        stat["win"] += 1
    if finish <= 2:
        stat["place2"] += 1
    if finish <= 3:
        stat["place3"] += 1


def _rate(n: int, d: int) -> float:
    return round(n / d * 100, 1) if d else 0.0


def compute_personnel_stats() -> dict:
    """
    predictions × race_results を JOIN して騎手/調教師別の成績を集計する。
    JRA/NAR 分岐・馬場別・SMILE距離区分別の内訳も含む。
    """
    from collections import defaultdict

    conn = get_db()
    rows = conn.execute(
        """
        SELECT p.venue, p.surface, p.distance, p.horses_json, r.order_json
        FROM predictions p
        JOIN race_results r ON p.date = r.date AND p.race_id = r.race_id
        WHERE r.cancelled = 0
        """
    ).fetchall()

    def new_person():
        return {
            "name": "",
            "id": "",   # jockey_id / trainer_id
            # 総合
            "total": 0, "win": 0, "place2": 0, "place3": 0,
            # JRA/NAR 別
            "jra": _empty_stat(),
            "nar": _empty_stat(),
            # 馬場別（総合）
            "by_surface": defaultdict(_empty_stat),
            # JRA 馬場別
            "jra_by_surface": defaultdict(_empty_stat),
            # NAR 馬場別
            "nar_by_surface": defaultdict(_empty_stat),
            # SMILE距離区分別（馬場prefix付: "芝S", "ダートM" 等）総合
            "by_smile": defaultdict(_empty_stat),
            # JRA SMILE別
            "jra_by_smile": defaultdict(_empty_stat),
            # NAR SMILE別
            "nar_by_smile": defaultdict(_empty_stat),
            # 競馬場別
            "by_venue": defaultdict(_empty_stat),
            # 脚質別（逃げ/先行/差し/追込）
            "by_running_style": defaultdict(_empty_stat),
        }

    jockeys  = defaultdict(new_person)
    trainers = defaultdict(new_person)

    for row in rows:
        try:
            horses = json.loads(row["horses_json"])
            orders = {
                o["horse_no"]: o.get("finish", 99)
                for o in json.loads(row["order_json"])
            }
        except Exception:
            continue

        venue   = row["venue"] or "不明"
        surface = row["surface"] or ""
        try:
            dist = int(str(row["distance"]).replace("m", ""))
        except (ValueError, TypeError):
            dist = 0

        is_jra   = venue in _JRA_VENUES
        jra_nar  = "JRA" if is_jra else "NAR"
        smile    = _smile_key(dist) if dist else ""
        surf_key = surface if surface in ("芝", "ダート", "障害") else ""
        smile_key_full = (surf_key + smile) if (surf_key and smile) else ""

        for h in horses:
            finish = orders.get(h.get("horse_no"), 99)
            running_style = h.get("running_style", "")  # 逃げ/先行/差し/追込

            # ---- 騎手 ----
            jid = h.get("jockey_id", "")
            if jid:
                js = jockeys[jid]
                js["name"] = h.get("jockey", jid)
                js["id"] = jid
                _add_finish(js, finish)
                _add_finish(js[jra_nar.lower()], finish)
                if surf_key:
                    _add_finish(js["by_surface"][surf_key], finish)
                    if is_jra:
                        _add_finish(js["jra_by_surface"][surf_key], finish)
                    else:
                        _add_finish(js["nar_by_surface"][surf_key], finish)
                if smile_key_full:
                    _add_finish(js["by_smile"][smile_key_full], finish)
                    if is_jra:
                        _add_finish(js["jra_by_smile"][smile_key_full], finish)
                    else:
                        _add_finish(js["nar_by_smile"][smile_key_full], finish)
                _add_finish(js["by_venue"][venue], finish)
                if running_style in ("逃げ", "先行", "差し", "追込"):
                    _add_finish(js["by_running_style"][running_style], finish)

            # ---- 調教師 ----
            tid = h.get("trainer_id", "") or h.get("trainer", "")
            tname = h.get("trainer", tid)
            if tid:
                ts = trainers[tid]
                ts["name"] = tname
                ts["id"] = tid
                _add_finish(ts, finish)
                _add_finish(ts[jra_nar.lower()], finish)
                if surf_key:
                    _add_finish(ts["by_surface"][surf_key], finish)
                    if is_jra:
                        _add_finish(ts["jra_by_surface"][surf_key], finish)
                    else:
                        _add_finish(ts["nar_by_surface"][surf_key], finish)
                if smile_key_full:
                    _add_finish(ts["by_smile"][smile_key_full], finish)
                    if is_jra:
                        _add_finish(ts["jra_by_smile"][smile_key_full], finish)
                    else:
                        _add_finish(ts["nar_by_smile"][smile_key_full], finish)
                _add_finish(ts["by_venue"][venue], finish)
                if running_style in ("逃げ", "先行", "差し", "追込"):
                    _add_finish(ts["by_running_style"][running_style], finish)

    def _finalize_stat(st: dict) -> dict:
        t = st.get("total", 0)
        return {
            "total": t, "win": st.get("win", 0),
            "place2": st.get("place2", 0), "place3": st.get("place3", 0),
            "win_rate":    _rate(st.get("win", 0), t),
            "place2_rate": _rate(st.get("place2", 0), t),
            "place3_rate": _rate(st.get("place3", 0), t),
        }

    # personnel テーブルから所属情報をロード（ID → location マッピング）
    try:
        _pers_all = get_personnel_all()
        _jockey_location = {
            jid: d.get("location", "")
            for jid, d in _pers_all.get("jockeys", {}).items()
        }
        _trainer_location = {
            tid: d.get("location", "")
            for tid, d in _pers_all.get("trainers", {}).items()
        }
    except Exception:
        _jockey_location = {}
        _trainer_location = {}

    def finalize(persons: dict, location_map: dict) -> dict:
        result = {}
        for pid, st in persons.items():
            t = st["total"]
            result[pid] = {
                "name": st["name"],
                "id": st.get("id", pid),
                "total": t,
                "win": st["win"],
                "place2": st["place2"],
                "place3": st["place3"],
                "win_rate":    _rate(st["win"],    t),
                "place2_rate": _rate(st["place2"], t),
                "place3_rate": _rate(st["place3"], t),
                # JRA/NAR 別
                "jra": _finalize_stat(st["jra"]),
                "nar": _finalize_stat(st["nar"]),
                # 馬場別
                "by_surface":     {k: _finalize_stat(v) for k, v in st["by_surface"].items()},
                "jra_by_surface": {k: _finalize_stat(v) for k, v in st["jra_by_surface"].items()},
                "nar_by_surface": {k: _finalize_stat(v) for k, v in st["nar_by_surface"].items()},
                # SMILE別
                "by_smile":     {k: _finalize_stat(v) for k, v in st["by_smile"].items()},
                "jra_by_smile": {k: _finalize_stat(v) for k, v in st["jra_by_smile"].items()},
                "nar_by_smile": {k: _finalize_stat(v) for k, v in st["nar_by_smile"].items()},
                # 競馬場別
                "by_venue": {k: _finalize_stat(v) for k, v in st["by_venue"].items()},
                # 脚質別
                "by_running_style": {k: _finalize_stat(v) for k, v in st["by_running_style"].items()},
                # 所属（personnel テーブルから）
                "location": location_map.get(pid, ""),
            }
        return result

    return {
        "jockey":  finalize(jockeys, _jockey_location),
        "trainer": finalize(trainers, _trainer_location),
    }


# JRA 開催コード（course_db の venue フィールドはコード）
_JRA_VENUE_CODES = frozenset(["01","02","03","04","05","06","07","08","09","10"])


def compute_personnel_stats_from_course_db() -> dict:
    """
    course_db の全レース記録から騎手/調教師別の成績を集計する。
    predictions に依存せず全期間の成績を返す。
    running_style は position_4c から推定。
    調教師名は personnel テーブルから補完。
    """
    from collections import defaultdict

    conn = get_db()
    rows = conn.execute("SELECT data_json FROM course_db").fetchall()

    def new_person():
        return {
            "name": "", "id": "",
            "total": 0, "win": 0, "place2": 0, "place3": 0,
            "jra": _empty_stat(), "nar": _empty_stat(),
            "by_surface":     defaultdict(_empty_stat),
            "jra_by_surface": defaultdict(_empty_stat),
            "nar_by_surface": defaultdict(_empty_stat),
            "by_smile":       defaultdict(_empty_stat),
            "jra_by_smile":   defaultdict(_empty_stat),
            "nar_by_smile":   defaultdict(_empty_stat),
            "by_venue":       defaultdict(_empty_stat),
            "by_running_style": defaultdict(_empty_stat),
        }

    def _pos_to_style(pos4c, field_count):
        if not pos4c or not field_count or field_count < 4:
            return ""
        r = pos4c / field_count
        if pos4c == 1: return "逃げ"
        if r <= 0.40:  return "先行"
        if r <= 0.65:  return "差し"
        return "追込"

    jockeys  = defaultdict(new_person)
    trainers = defaultdict(new_person)
    all_dates = []

    for row in rows:
        try:
            records = json.loads(row["data_json"])
        except Exception:
            continue

        for r in records:
            finish = r.get("finish_pos")
            if finish is None or finish == 0:
                continue

            venue   = str(r.get("venue", ""))
            surface = r.get("surface", "")
            try:
                dist = int(r.get("distance") or 0)
            except (ValueError, TypeError):
                dist = 0

            is_jra         = venue in _JRA_VENUE_CODES
            jra_nar        = "jra" if is_jra else "nar"
            smile          = _smile_key(dist) if dist else ""
            surf_key       = surface if surface in ("芝", "ダート", "障害") else ""
            smile_key_full = (surf_key + smile) if (surf_key and smile) else ""
            style          = _pos_to_style(r.get("position_4c"), r.get("field_count"))

            d = r.get("race_date")
            if d:
                all_dates.append(d)

            def _agg(person, pid, name):
                person["id"]   = pid
                if name:
                    person["name"] = name
                _add_finish(person, finish)
                _add_finish(person[jra_nar], finish)
                if surf_key:
                    _add_finish(person["by_surface"][surf_key], finish)
                    _add_finish(person["jra_by_surface" if is_jra else "nar_by_surface"][surf_key], finish)
                if smile_key_full:
                    _add_finish(person["by_smile"][smile_key_full], finish)
                    _add_finish(person["jra_by_smile" if is_jra else "nar_by_smile"][smile_key_full], finish)
                if venue:
                    _add_finish(person["by_venue"][venue], finish)
                if style:
                    _add_finish(person["by_running_style"][style], finish)

            jid = r.get("jockey_id", "")
            if jid:
                _agg(jockeys[jid], jid, r.get("jockey", ""))

            tid = r.get("trainer_id", "")
            if tid:
                _agg(trainers[tid], tid, "")

    # personnel DB から調教師名・所属を補完
    try:
        _pers_all = get_personnel_all()
        _jockey_location = {
            jid: d.get("location", "")
            for jid, d in _pers_all.get("jockeys", {}).items()
        }
        _trainer_location = {
            tid: d.get("location", "")
            for tid, d in _pers_all.get("trainers", {}).items()
        }
        _trainer_name_map = {
            tid: (d.get("trainer_name") or d.get("name") or "")
            for tid, d in _pers_all.get("trainers", {}).items()
        }
        for tid, ts in trainers.items():
            if not ts["name"]:
                ts["name"] = _trainer_name_map.get(tid, tid)
    except Exception:
        _jockey_location = {}
        _trainer_location = {}

    def _finalize_stat(st: dict) -> dict:
        t = st.get("total", 0)
        return {
            "total": t, "win": st.get("win", 0),
            "place2": st.get("place2", 0), "place3": st.get("place3", 0),
            "win_rate":    _rate(st.get("win", 0), t),
            "place2_rate": _rate(st.get("place2", 0), t),
            "place3_rate": _rate(st.get("place3", 0), t),
        }

    def finalize(persons: dict, location_map: dict) -> dict:
        result = {}
        for pid, st in persons.items():
            t = st["total"]
            result[pid] = {
                "name": st["name"] or pid,
                "id":   st.get("id", pid),
                "total": t,
                "win": st["win"], "place2": st["place2"], "place3": st["place3"],
                "win_rate":    _rate(st["win"],    t),
                "place2_rate": _rate(st["place2"], t),
                "place3_rate": _rate(st["place3"], t),
                "jra": _finalize_stat(st["jra"]),
                "nar": _finalize_stat(st["nar"]),
                "by_surface":     {k: _finalize_stat(v) for k, v in st["by_surface"].items()},
                "jra_by_surface": {k: _finalize_stat(v) for k, v in st["jra_by_surface"].items()},
                "nar_by_surface": {k: _finalize_stat(v) for k, v in st["nar_by_surface"].items()},
                "by_smile":     {k: _finalize_stat(v) for k, v in st["by_smile"].items()},
                "jra_by_smile": {k: _finalize_stat(v) for k, v in st["jra_by_smile"].items()},
                "nar_by_smile": {k: _finalize_stat(v) for k, v in st["nar_by_smile"].items()},
                "by_venue":     {k: _finalize_stat(v) for k, v in st["by_venue"].items()},
                "by_running_style": {k: _finalize_stat(v) for k, v in st["by_running_style"].items()},
                "location": location_map.get(pid, ""),
            }
        return result

    min_d = min(all_dates) if all_dates else ""
    max_d = max(all_dates) if all_dates else ""
    # レース数（course_db は1馬1レコードなので race_date+venue で重複除去）
    race_count = conn.execute("SELECT COUNT(*) FROM course_db").fetchone()[0]

    return {
        "jockey":  finalize(jockeys, _jockey_location),
        "trainer": finalize(trainers, _trainer_location),
        "_period": {"min": min_d, "max": max_d, "course_keys": race_count},
    }


# ──────────────────────────────────────────────────────────
# race_log 関連ユーティリティ
# ──────────────────────────────────────────────────────────

_JRA_VENUE_CODE_STR = frozenset(["01","02","03","04","05","06","07","08","09","10"])

# NAR 競馬場名プレフィックス（調教師名から除去する）
_NAR_TRAINER_PREFIXES = (
    "ばんえい", "船橋", "大井", "川崎", "浦和", "高知", "佐賀", "笠松",
    "名古屋", "愛知", "園田", "姫路", "金沢", "水沢", "盛岡", "門別", "帯広",
    "荒尾", "福山", "岩見沢", "旭川", "札幌(道", "上山",
)

def _strip_venue_prefix(name: str) -> str:
    """調教師名から競馬場プレフィックスを除去する"""
    if not name:
        return name
    for pf in _NAR_TRAINER_PREFIXES:
        if name.startswith(pf) and len(name) > len(pf):
            return name[len(pf):]
    return name


def _l3f_corners_backfill(conn) -> None:
    """
    race_logの last_3f_sec=0 または positions_corners が空/1要素のレースを
    HTMLキャッシュから自動補完する。
    populate_race_log_from_predictions() の末尾で自動呼出しされる。
    """
    import os as _os
    import re as _re

    # 補完対象のrace_idを取得（last_3f=0 または corners不足）
    need_l3f = conn.execute(
        "SELECT DISTINCT race_id FROM race_log "
        "WHERE (last_3f_sec IS NULL OR last_3f_sec = 0) AND finish_pos < 90"
    ).fetchall()
    need_corners = conn.execute(
        "SELECT DISTINCT race_id FROM race_log "
        "WHERE (positions_corners IS NULL OR positions_corners = '' OR positions_corners = '[]' "
        "  OR (positions_corners NOT LIKE '%,%' AND positions_corners != '')) "
        "AND finish_pos < 90"
    ).fetchall()

    target_ids = set()
    l3f_ids = {r[0] for r in need_l3f}
    corner_ids = {r[0] for r in need_corners}
    target_ids = l3f_ids | corner_ids

    if not target_ids:
        return

    cache_dir = _os.path.join(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))), "data", "cache")
    if not _os.path.isdir(cache_dir):
        return

    try:
        import lz4.frame
    except ImportError:
        return

    try:
        from bs4 import BeautifulSoup
    except ImportError:
        return

    print(f"[race_log投入] HTMLキャッシュ補完: last_3f対象={len(l3f_ids):,}, corners対象={len(corner_ids):,}", flush=True)

    l3f_updated = 0
    corners_updated = 0

    for race_id in target_ids:
        # HTMLキャッシュ読み込み
        html = None
        for prefix in ["nar.netkeiba.com_race_result.html_race_id=",
                        "race.netkeiba.com_race_result.html_race_id="]:
            key = f"{prefix}{race_id}"
            for ext in [".html.lz4", ".html"]:
                path = _os.path.join(cache_dir, f"{key}{ext}")
                if _os.path.exists(path):
                    try:
                        if ext == ".html.lz4":
                            with open(path, "rb") as f:
                                html = lz4.frame.decompress(f.read()).decode("utf-8", errors="replace")
                        else:
                            with open(path, "r", encoding="utf-8", errors="replace") as f:
                                html = f.read()
                        break
                    except Exception:
                        pass
            if html:
                break

        if not html:
            continue

        soup = BeautifulSoup(html, "html.parser")

        # ── last_3f_sec 抽出 ──
        l3f_map = {}
        if race_id in l3f_ids:
            table = soup.select_one("table.race_table_01") or soup.select_one("table.RaceTable01")
            if table:
                for row in table.select("tbody tr"):
                    cells = row.find_all("td")
                    if len(cells) < 5:
                        continue
                    ft = cells[0].get_text(strip=True)
                    if not ft.isdigit():
                        continue
                    hno_t = cells[2].get_text(strip=True)
                    if not hno_t.isdigit():
                        continue
                    hno = int(hno_t)
                    for ci in range(7, min(len(cells), 14)):
                        t = cells[ci].get_text(strip=True)
                        if _re.match(r"^\d{2}\.\d$", t):
                            val = float(t)
                            if 28.0 <= val <= 50.0:
                                l3f_map[hno] = val
                                break

        # ── positions_corners 抽出 ──
        corner_orders = {}
        if race_id in corner_ids:
            ctable = soup.select_one("table.Corner_Num")
            if ctable:
                for tr in ctable.select("tr"):
                    cells_c = tr.find_all(["th", "td"])
                    if len(cells_c) < 2:
                        continue
                    m = _re.search(r"(\d)", cells_c[0].get_text(strip=True))
                    if not m:
                        continue
                    ci = int(m.group(1))
                    raw = cells_c[1].get_text()
                    raw = _re.sub(r'\s*=\s*(\d+)\s*$', '', raw)  # 末尾除外馬のみ除去
                    raw = raw.replace("（", "(").replace("）", ")")
                    raw = raw.replace("=", ",")  # 大差セパレータ→カンマ
                    horse_pos = {}
                    pos = 1
                    i = 0
                    while i < len(raw):
                        ch = raw[i]
                        if ch == '(':
                            end = raw.find(')', i)
                            if end < 0:
                                end = len(raw)
                            group_text = raw[i+1:end]
                            group_nos = [int(x.strip()) for x in _re.split(r'[,\-]', group_text) if x.strip().isdigit()]
                            for hno in group_nos:
                                horse_pos[hno] = pos
                            pos += len(group_nos)
                            i = end + 1
                        elif ch.isdigit():
                            j = i
                            while j < len(raw) and raw[j].isdigit():
                                j += 1
                            hno = int(raw[i:j])
                            horse_pos[hno] = pos
                            pos += 1
                            i = j
                        else:
                            i += 1
                    corner_orders[ci] = horse_pos

        # ── DB更新 ──
        if not l3f_map and not corner_orders:
            continue

        horses = conn.execute(
            "SELECT horse_no, last_3f_sec, positions_corners FROM race_log WHERE race_id = ?",
            (race_id,)
        ).fetchall()

        for h in horses:
            hno = h[0]
            if not hno:
                continue

            # last_3f
            if hno in l3f_map and (not h[1] or h[1] <= 0):
                conn.execute(
                    "UPDATE race_log SET last_3f_sec = ? WHERE race_id = ? AND horse_no = ?",
                    (l3f_map[hno], race_id, hno)
                )
                l3f_updated += 1

            # corners
            if corner_orders:
                positions = []
                for ci_key in sorted(corner_orders.keys()):
                    pm = corner_orders[ci_key]
                    if hno in pm:
                        positions.append(pm[hno])
                if positions and any(p > 0 for p in positions):
                    old_raw = h[2] or ""
                    try:
                        old = json.loads(old_raw) if old_raw.startswith("[") else []
                    except Exception:
                        old = []
                    if len(positions) > len(old) or len(old) <= 1:
                        conn.execute(
                            "UPDATE race_log SET positions_corners = ?, position_4c = ? "
                            "WHERE race_id = ? AND horse_no = ?",
                            (json.dumps(positions), positions[-1], race_id, hno)
                        )
                        corners_updated += 1

    if l3f_updated > 0 or corners_updated > 0:
        conn.commit()
        print(f"[race_log投入] HTMLキャッシュ補完完了: last_3f={l3f_updated:,}件, corners={corners_updated:,}件", flush=True)


def populate_race_log_from_predictions() -> int:
    """
    predictions × race_results を SQL JOIN して race_log テーブルを投入する。
    全馬（着外含む）の着順・騎手・調教師を記録する。
    戻り値: 新規追加行数
    """
    import time as _time
    t0 = _time.time()
    conn = get_db()
    init_schema()  # race_log テーブルが存在しない場合に備えて

    # 既に race_log に存在する race_id を取得
    existing = {r[0] for r in conn.execute("SELECT DISTINCT race_id FROM race_log").fetchall()}

    # JOIN対象のrace_idセットを取得（重いJOINクエリを回避する早期チェック）
    joinable_ids = {r[0] for r in conn.execute(
        """SELECT DISTINCT p.race_id
           FROM predictions p
           INNER JOIN race_results r ON p.race_id = r.race_id
           WHERE r.order_json IS NOT NULL AND r.order_json != '[]' AND r.order_json != 'null'"""
    ).fetchall()}

    # 新規race_idが無ければスキップ（既存race_idとの差分で判定）
    new_ids = joinable_ids - existing
    if not new_ids:
        print(f"[race_log投入] 新規なし (既存={len(existing):,}, JOIN対象={len(joinable_ids):,}) → スキップ", flush=True)
        return 0

    # predictions × race_results を SQL JOIN で一括取得（Python での dict 結合を排除）
    total_pred = conn.execute("SELECT COUNT(*) FROM predictions").fetchone()[0]
    print(f"[race_log投入] 開始... 予測数={total_pred:,}件, 既投入race_id={len(existing):,}件", flush=True)

    rows = conn.execute(
        """
        SELECT p.date, p.race_id, p.surface, p.distance, p.horses_json,
               r.order_json
        FROM predictions p
        INNER JOIN race_results r ON p.race_id = r.race_id
        WHERE r.order_json IS NOT NULL
          AND r.order_json != '[]'
          AND r.order_json != 'null'
        ORDER BY p.date
        """
    ).fetchall()

    print(f"[race_log投入] JOINクエリ完了 ({_time.time()-t0:.1f}秒) 対象={len(rows):,}件", flush=True)

    inserted = 0
    skipped  = 0
    with transaction() as txn:
        for pred in rows:
            race_id = pred["race_id"]
            if race_id in existing:
                skipped += 1
                continue  # 重複スキップ

            race_date = pred["date"]
            surface   = pred["surface"] or ""
            distance  = pred["distance"] or 0

            # race_id からベニューコードを抽出 (YYYYVVKKDDNN 形式)
            venue_code = race_id[4:6] if len(race_id) >= 6 else ""
            is_jra = 1 if venue_code in _JRA_VENUE_CODE_STR else 0

            # horse_no → {jockey_id, jockey_name, trainer_name}
            horses = json.loads(pred["horses_json"])
            field_count = len(horses)
            horse_map = {}
            for h in horses:
                hno = h.get("horse_no")
                if hno is not None:
                    horse_map[int(hno)] = {
                        "jockey_id":    h.get("jockey_id", "") or "",
                        "jockey_name":  h.get("jockey", "") or "",
                        "trainer_id":   h.get("trainer_id", "") or "",
                        "trainer_name": _strip_venue_prefix(h.get("trainer", "") or ""),
                        "sire_name":    h.get("sire", "") or "",
                        "bms_name":     h.get("maternal_grandsire", "") or "",
                        "horse_id":     h.get("horse_id", "") or "",
                        "horse_name":   h.get("horse_name", "") or "",
                    }

            orders = json.loads(pred["order_json"])

            # margin_ahead/behind を finish_time_sec から事前計算
            _time_entries = []
            for _e in orders:
                _hno = _e.get("horse_no")
                _fp = _e.get("finish")
                _ts = 0.0
                try:
                    _ts_raw = _e.get("time_sec")
                    if _ts_raw is not None:
                        _ts = float(_ts_raw)
                except (ValueError, TypeError):
                    pass
                if _hno is not None and _fp is not None and int(_fp) < 90 and _ts > 0:
                    _time_entries.append((int(_hno), int(_fp), _ts))
            _time_entries.sort(key=lambda x: x[1])  # 着順ソート
            _winner_time = _time_entries[0][2] if _time_entries else 0
            _margin_map = {}
            # _winner_time=0 のとき（タイム取得失敗）は全馬 margin を NULL のまま残す
            if _winner_time and _winner_time > 0:
                for _idx, (_hno, _fp, _ft) in enumerate(_time_entries):
                    _ma = round(_ft - _winner_time, 3)
                    _mb = 0.0
                    if _idx + 1 < len(_time_entries):
                        _next_t = _time_entries[_idx + 1][2]
                        if _next_t > _ft:
                            _mb = round(_next_t - _ft, 3)
                    _margin_map[_hno] = (_ma, _mb)

            for entry in orders:
                horse_no  = entry.get("horse_no")
                finish    = entry.get("finish")
                if horse_no is None or finish is None:
                    continue
                horse_no = int(horse_no)
                finish   = int(finish)
                hinfo = horse_map.get(horse_no, {})

                # order_json の odds フィールドから単勝オッズを取得
                win_odds = None
                try:
                    _odds_raw = entry.get("odds")
                    if _odds_raw is not None:
                        win_odds = float(_odds_raw)
                except (ValueError, TypeError):
                    win_odds = None

                # order_json から通過順・タイム・着差を取得
                _corners_json = ""
                _p4c = 0
                _raw_corners = entry.get("corners")
                if _raw_corners and isinstance(_raw_corners, list) and len(_raw_corners) > 0:
                    _cv = _raw_corners[0]
                    if isinstance(_cv, int) and _cv > 0:
                        # netkeibaの通過順数値をパース
                        _s = str(_cv)
                        if all(c in "123456789" for c in _s) and 2 <= len(_s) <= 4:
                            _parsed_c = [int(c) for c in _s]
                            _corners_json = json.dumps(_parsed_c)
                            _p4c = _parsed_c[-1]
                _ft_sec = 0.0
                try:
                    _ts = entry.get("time_sec")
                    if _ts is not None:
                        _ft_sec = float(_ts)
                except (ValueError, TypeError):
                    pass
                _l3f = 0.0
                try:
                    _l3f_raw = entry.get("last_3f")
                    if _l3f_raw is not None:
                        _l3f = float(_l3f_raw)
                except (ValueError, TypeError):
                    pass
                _horse_id = hinfo.get("horse_id", "") or ""
                _margins = _margin_map.get(horse_no, (0.0, 0.0))

                txn.execute(
                    """
                    INSERT OR IGNORE INTO race_log
                      (race_date, race_id, venue_code, surface, distance,
                       horse_no, finish_pos,
                       jockey_id, jockey_name, trainer_id, trainer_name,
                       field_count, is_jra, win_odds,
                       sire_name, bms_name,
                       positions_corners, position_4c,
                       finish_time_sec, last_3f_sec, horse_id,
                       margin_ahead, margin_behind,
                       horse_name)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (race_date, race_id, venue_code, surface, distance,
                     horse_no, finish,
                     hinfo.get("jockey_id", ""),
                     hinfo.get("jockey_name", ""),
                     hinfo.get("trainer_id", ""),
                     hinfo.get("trainer_name", ""),
                     field_count, is_jra, win_odds,
                     hinfo.get("sire_name", ""),
                     hinfo.get("bms_name", ""),
                     _corners_json, _p4c,
                     _ft_sec, _l3f, _horse_id,
                     _margins[0], _margins[1],
                     hinfo.get("horse_name", "")),
                )
                inserted += 1

    # ── NULL win_odds のバックフィル（既存行を race_results.order_json から補完）──
    null_count = conn.execute("SELECT COUNT(*) FROM race_log WHERE win_odds IS NULL").fetchone()[0]
    if null_count > 0:
        print(f"[race_log投入] win_odds NULL行 {null_count:,}件 → バックフィル開始...", flush=True)
        _null_rids = [r[0] for r in conn.execute(
            "SELECT DISTINCT race_id FROM race_log WHERE win_odds IS NULL"
        ).fetchall()]
        _updated = 0
        _batch_size = 200
        for _bi in range(0, len(_null_rids), _batch_size):
            _chunk = _null_rids[_bi:_bi + _batch_size]
            _placeholders = ",".join(["?"] * len(_chunk))
            _rr_rows = conn.execute(
                f"SELECT race_id, order_json FROM race_results WHERE race_id IN ({_placeholders})",
                _chunk,
            ).fetchall()
            for _rr in _rr_rows:
                try:
                    _orders = json.loads(_rr[1])
                    for _oe in _orders:
                        _hno = _oe.get("horse_no")
                        _odds = _oe.get("odds")
                        if _hno is not None and _odds is not None:
                            try:
                                _odds_f = float(_odds)
                            except (ValueError, TypeError):
                                continue
                            r = conn.execute(
                                "UPDATE race_log SET win_odds=? WHERE race_id=? AND horse_no=? AND win_odds IS NULL",
                                (_odds_f, _rr[0], int(_hno)),
                            )
                            _updated += r.rowcount
                except (json.JSONDecodeError, TypeError):
                    continue
            try:
                conn.commit()
            except Exception:
                conn.rollback()
                logger.warning("win_odds バックフィルバッチコミット失敗、rollback実行", exc_info=True)
        print(f"[race_log投入] win_odds バックフィル完了: {_updated:,}件更新", flush=True)

    # ── sire_name / bms_name のバックフィル（既存行を predictions.horses_json から補完）──
    # 前回バックフィルで0件更新だった場合はスキップ（新規挿入があれば再実行）
    _backfill_flag_dir = _os.path.join(_os.path.dirname(DATABASE_PATH), "cache")
    _sire_backfill_flag = _os.path.join(_backfill_flag_dir, "_sire_backfill_done.flag")
    _skip_sire_backfill = inserted == 0 and _os.path.exists(_sire_backfill_flag)
    sire_null = 0 if _skip_sire_backfill else conn.execute("SELECT COUNT(*) FROM race_log WHERE (sire_name IS NULL OR sire_name='')").fetchone()[0]
    if sire_null > 0:
        print(f"[race_log投入] sire/bms 空行 {sire_null:,}件 → バックフィル開始...", flush=True)
        _sire_rids = [r[0] for r in conn.execute(
            "SELECT DISTINCT race_id FROM race_log WHERE (sire_name IS NULL OR sire_name='')"
        ).fetchall()]
        _s_updated = 0
        _batch_size = 200
        for _bi in range(0, len(_sire_rids), _batch_size):
            _chunk = _sire_rids[_bi:_bi + _batch_size]
            _placeholders = ",".join(["?"] * len(_chunk))
            _pred_rows = conn.execute(
                f"SELECT race_id, horses_json FROM predictions WHERE race_id IN ({_placeholders})",
                _chunk,
            ).fetchall()
            for _pr in _pred_rows:
                try:
                    _horses = json.loads(_pr[1])
                    for _h in _horses:
                        _hno = _h.get("horse_no")
                        _sire = _h.get("sire", "") or ""
                        _bms  = _h.get("maternal_grandsire", "") or ""
                        if _hno is not None and (_sire or _bms):
                            r = conn.execute(
                                "UPDATE race_log SET sire_name=?, bms_name=? WHERE race_id=? AND horse_no=? AND (sire_name IS NULL OR sire_name='')",
                                (_sire, _bms, _pr[0], int(_hno)),
                            )
                            _s_updated += r.rowcount
                except (json.JSONDecodeError, TypeError):
                    continue
            try:
                conn.commit()
            except Exception:
                conn.rollback()
                logger.warning("sire/bms バックフィルバッチコミット失敗、rollback実行", exc_info=True)
        print(f"[race_log投入] sire/bms バックフィル完了: {_s_updated:,}件更新", flush=True)
        # 更新0件なら次回スキップ用フラグを作成
        if _s_updated == 0:
            try:
                _os.makedirs(_backfill_flag_dir, exist_ok=True)
                with open(_sire_backfill_flag, "w") as _f:
                    _f.write("done")
            except Exception:
                pass

    # ── condition バックフィル（predictions JSONファイルから馬場状態を補完）──
    _cond_backfill_flag = _os.path.join(_backfill_flag_dir, "_cond_backfill_done.flag")
    _skip_cond_backfill = inserted == 0 and _os.path.exists(_cond_backfill_flag)
    cond_null = 0 if _skip_cond_backfill else conn.execute(
        "SELECT COUNT(*) FROM race_log WHERE condition IS NULL OR condition=''"
    ).fetchone()[0]
    if cond_null > 0:
        import glob as _glob
        _pred_dir = _os.path.join(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))), "data", "predictions")
        pred_jsons = sorted(_glob.glob(_os.path.join(_pred_dir, "*_pred.json")))
        if pred_jsons:
            print(f"[race_log投入] condition 空行 {cond_null:,}件 → JSONバックフィル開始...", flush=True)
            _cond_map: dict = {}  # race_id → condition
            for pf in pred_jsons:
                try:
                    with open(pf, "r", encoding="utf-8") as f:
                        pdata = json.load(f)
                    for rc in pdata.get("races", []):
                        c = rc.get("condition", "")
                        rid = rc.get("race_id", "")
                        if c and rid:
                            _cond_map[rid] = c
                except Exception:
                    continue
            _c_updated = 0
            _batch_size = 500
            _cond_items = list(_cond_map.items())
            for _bi in range(0, len(_cond_items), _batch_size):
                _chunk = _cond_items[_bi:_bi + _batch_size]
                for _rid, _cond in _chunk:
                    r = conn.execute(
                        "UPDATE race_log SET condition=? WHERE race_id=? AND (condition IS NULL OR condition='')",
                        (_cond, _rid),
                    )
                    _c_updated += r.rowcount
                conn.commit()
            print(f"[race_log投入] condition バックフィル完了: {_c_updated:,}件更新", flush=True)
            if _c_updated == 0:
                try:
                    _os.makedirs(_backfill_flag_dir, exist_ok=True)
                    with open(_cond_backfill_flag, "w") as _f:
                        _f.write("done")
                except Exception:
                    pass

    # ── last_3f_sec / positions_corners のHTMLキャッシュ補完 ──
    # race_logに last_3f_sec=0 または positions_corners が空/1要素のレースを
    # HTMLキャッシュから自動補完する（今後データ欠損を防ぐ堅牢な処理）
    _l3f_corners_backfill(conn)

    elapsed = _time.time() - t0
    print(f"[race_log投入] 完了 ({elapsed:.1f}秒) 新規={inserted:,}件挿入, スキップ={skipped:,}件", flush=True)
    return inserted


def _load_ml_name_map(cache_hours: int = 24) -> dict:
    """
    ml/training_ml JSON から trainer/jockey の id→name マッピングを構築してキャッシュする。
    返り値: {"trainer": {id: name}, "jockey": {id: name}}
    """
    import glob
    import os as _os
    import re as _re
    import time as _time
    _PROJECT_ROOT = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
    _CACHE_PATH = _os.path.join(_PROJECT_ROOT, "data", "name_map_cache.json")

    # キャッシュが新鮮なら読み込む
    if _os.path.exists(_CACHE_PATH):
        mtime = _os.path.getmtime(_CACHE_PATH)
        if _time.time() - mtime < cache_hours * 3600:
            try:
                with open(_CACHE_PATH, encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                pass

    # ml JSON を全スキャン
    hw_pat = _re.compile(r'^\d{3,4}\s*[\(（][^)）]*[\)）]$')
    trainer_map: dict = {}
    jockey_map: dict = {}

    ml_dirs = [
        _os.path.join(_PROJECT_ROOT, "data", "ml"),
        _os.path.join(_PROJECT_ROOT, "data", "training_ml"),
    ]
    for ml_dir in ml_dirs:
        for fpath in glob.glob(_os.path.join(ml_dir, "*.json")):
            try:
                with open(fpath, encoding="utf-8") as f:
                    data = json.load(f)
                for race in data.get("races", []):
                    for h in race.get("horses", []):
                        tid = h.get("trainer_id")
                        tname = h.get("trainer")
                        if tid and tname and not hw_pat.match(str(tname)):
                            if tid not in trainer_map:
                                trainer_map[tid] = tname
                        jid = h.get("jockey_id")
                        jname = h.get("jockey")
                        if jid and jname:
                            if jid not in jockey_map:
                                jockey_map[jid] = jname
            except Exception:
                pass

    result = {"trainer": trainer_map, "jockey": jockey_map}
    try:
        _os.makedirs(_os.path.dirname(_CACHE_PATH), exist_ok=True)
        with open(_CACHE_PATH, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False)
    except Exception:
        pass
    return result


_PERSONNEL_CACHE_DIR = _os.path.join(_os.path.dirname(DATABASE_PATH), "cache", "personnel_stats")


def _personnel_cache_path(year_filter: str = None) -> str:
    key = year_filter or "all"
    return _os.path.join(_PERSONNEL_CACHE_DIR, f"personnel_stats_{key}.json")


def _personnel_cache_valid(year_filter: str = None) -> bool:
    """race_log 行数が変わっていなければキャッシュ有効"""
    fpath = _personnel_cache_path(year_filter)
    if not _os.path.exists(fpath):
        return False
    try:
        with open(fpath, "r", encoding="utf-8") as f:
            data = json.load(f)
        cached_count = data.get("_meta", {}).get("race_log_count", -1)
        conn = get_db()
        current_count = conn.execute("SELECT COUNT(*) FROM race_log").fetchone()[0]
        return cached_count == current_count
    except Exception:
        return False


def _load_personnel_cache(year_filter: str = None):
    """ディスクキャッシュからロード。無効なら None を返す"""
    fpath = _personnel_cache_path(year_filter)
    if not _os.path.exists(fpath):
        return None
    try:
        with open(fpath, "r", encoding="utf-8") as f:
            data = json.load(f)
        cached_count = data.get("_meta", {}).get("race_log_count", -1)
        conn = get_db()
        current_count = conn.execute("SELECT COUNT(*) FROM race_log").fetchone()[0]
        if cached_count != current_count:
            return None
        # _meta を除去して返す
        data.pop("_meta", None)
        return data
    except Exception:
        return None


def _save_personnel_cache(result: dict, year_filter: str = None):
    """集計結果をディスクキャッシュに保存"""
    try:
        _os.makedirs(_PERSONNEL_CACHE_DIR, exist_ok=True)
        conn = get_db()
        current_count = conn.execute("SELECT COUNT(*) FROM race_log").fetchone()[0]
        to_save = dict(result)
        to_save["_meta"] = {"race_log_count": current_count}
        fpath = _personnel_cache_path(year_filter)
        with open(fpath, "w", encoding="utf-8") as f:
            json.dump(to_save, f, ensure_ascii=False)
    except Exception:
        pass


def compute_personnel_stats_from_race_log(year_filter: str = None) -> dict:
    """
    race_log テーブル（全馬・着外含む）から騎手/調教師別成績を集計する。
    SQL GROUP BY を使用して高速化（Python 全行ループを廃止）。
    populate_race_log_from_predictions() 後に呼ぶ。
    year_filter: "2024", "2025", "2026" 等で年度絞り込み。None=全期間。
    """
    import re as _re
    import time as _time
    from collections import defaultdict

    t0 = _time.time()

    # ── ディスクキャッシュチェック ─────────────────────────────
    cached = _load_personnel_cache(year_filter)
    if cached is not None:
        year_label = f" ({year_filter}年)" if year_filter else ""
        print(f"[DB集計] ディスクキャッシュヒット{year_label} ({_time.time()-t0:.2f}秒)", flush=True)
        return cached

    conn = get_db()

    # race_log に新規データがあれば差分投入する
    cnt = conn.execute("SELECT COUNT(*) FROM race_log").fetchone()[0]
    new_rows = populate_race_log_from_predictions()
    if new_rows > 0:
        cnt = conn.execute("SELECT COUNT(*) FROM race_log").fetchone()[0]

    year_label = f" ({year_filter}年)" if year_filter else ""
    print(f"[DB集計] 騎手・調教師集計開始{year_label}... race_log={cnt:,}行", flush=True)

    # 年度フィルタ用 WHERE 句
    year_where = f"AND race_date LIKE '{year_filter}%'" if year_filter else ""

    # ── SQL GROUP BY で集計（Python ループを排除）──────────────────────
    # 騎手: (jockey_id, venue_code, surface, distance, is_jra, running_style) 単位に集計
    _DUMMY_IDS_SQL = "('001','002','003')"
    j_rows = conn.execute(
        f"""
        SELECT jockey_id, jockey_name, venue_code, surface, distance,
               is_jra, running_style,
               COUNT(*)                                         AS total,
               SUM(CASE WHEN finish_pos=1 THEN 1 ELSE 0 END)   AS win,
               SUM(CASE WHEN finish_pos<=2 THEN 1 ELSE 0 END)  AS place2,
               SUM(CASE WHEN finish_pos<=3 THEN 1 ELSE 0 END)  AS place3,
               SUM(CASE WHEN finish_pos=1 THEN COALESCE(win_odds,0) ELSE 0 END) AS win_odds_sum
        FROM race_log
        WHERE jockey_id != '' AND jockey_id NOT IN {_DUMMY_IDS_SQL} {year_where}
        GROUP BY jockey_id, venue_code, surface, distance, is_jra, running_style
        """
    ).fetchall()

    # 調教師: (trainer_id, venue_code, surface, distance, is_jra, running_style) 単位に集計
    # trainer_id が空の場合は trainer_name をフォールバックIDとして使用
    t_rows = conn.execute(
        f"""
        SELECT CASE WHEN trainer_id != '' THEN trainer_id ELSE trainer_name END AS trainer_id,
               trainer_name, venue_code, surface, distance,
               is_jra, running_style,
               COUNT(*)                                         AS total,
               SUM(CASE WHEN finish_pos=1 THEN 1 ELSE 0 END)   AS win,
               SUM(CASE WHEN finish_pos<=2 THEN 1 ELSE 0 END)  AS place2,
               SUM(CASE WHEN finish_pos<=3 THEN 1 ELSE 0 END)  AS place3,
               SUM(CASE WHEN finish_pos=1 THEN COALESCE(win_odds,0) ELSE 0 END) AS win_odds_sum
        FROM race_log
        WHERE (trainer_id != '' OR trainer_name != '') {year_where}
        GROUP BY CASE WHEN trainer_id != '' THEN trainer_id ELSE trainer_name END,
                 venue_code, surface, distance, is_jra, running_style
        """
    ).fetchall()

    # 種牡馬: (sire_name, venue_code, surface, distance, is_jra, running_style) 単位に集計
    s_rows = conn.execute(
        f"""
        SELECT sire_name, venue_code, surface, distance,
               is_jra, running_style,
               COUNT(*)                                         AS total,
               SUM(CASE WHEN finish_pos=1 THEN 1 ELSE 0 END)   AS win,
               SUM(CASE WHEN finish_pos<=2 THEN 1 ELSE 0 END)  AS place2,
               SUM(CASE WHEN finish_pos<=3 THEN 1 ELSE 0 END)  AS place3,
               SUM(CASE WHEN finish_pos=1 THEN COALESCE(win_odds,0) ELSE 0 END) AS win_odds_sum
        FROM race_log
        WHERE sire_name IS NOT NULL AND sire_name != '' {year_where}
        GROUP BY sire_name, venue_code, surface, distance, is_jra, running_style
        """
    ).fetchall()

    # 母父馬(BMS): (bms_name, venue_code, surface, distance, is_jra, running_style) 単位に集計
    b_rows = conn.execute(
        f"""
        SELECT bms_name, venue_code, surface, distance,
               is_jra, running_style,
               COUNT(*)                                         AS total,
               SUM(CASE WHEN finish_pos=1 THEN 1 ELSE 0 END)   AS win,
               SUM(CASE WHEN finish_pos<=2 THEN 1 ELSE 0 END)  AS place2,
               SUM(CASE WHEN finish_pos<=3 THEN 1 ELSE 0 END)  AS place3,
               SUM(CASE WHEN finish_pos=1 THEN COALESCE(win_odds,0) ELSE 0 END) AS win_odds_sum
        FROM race_log
        WHERE bms_name IS NOT NULL AND bms_name != '' {year_where}
        GROUP BY bms_name, venue_code, surface, distance, is_jra, running_style
        """
    ).fetchall()

    print(
        f"[DB集計] SQLクエリ完了 ({_time.time()-t0:.1f}秒) "
        f"騎手集計行={len(j_rows):,}, 調教師集計行={len(t_rows):,}, "
        f"種牡馬集計行={len(s_rows):,}, 母父集計行={len(b_rows):,}",
        flush=True,
    )

    # 期間情報
    period_sql = "SELECT MIN(race_date) AS min_d, MAX(race_date) AS max_d, COUNT(DISTINCT race_id) AS rc FROM race_log"
    if year_filter:
        period_sql += f" WHERE race_date LIKE '{year_filter}%'"
    period_row = conn.execute(period_sql).fetchone()
    min_d = period_row["min_d"] or ""
    max_d = period_row["max_d"] or ""
    race_count = period_row["rc"] or 0

    # ── 馬体重パターン（調教師名として混入するケースの除外用）──────────
    _horse_weight_pat = _re.compile(r'^\d{3,4}\s*[\(（][^)）]*[\)）]$')

    # ── 所属推定用定数 ─────────────────────────────────────────────────
    _JRA_EAST = frozenset({"01", "02", "03", "04", "05", "06"})
    _JRA_WEST = frozenset({"07", "08", "09", "10"})
    _NAR_VENUE_MAP = {
        "30": "門別", "35": "盛岡", "36": "水沢", "42": "浦和", "43": "船橋",
        "44": "大井", "45": "川崎", "46": "金沢", "47": "笠松", "48": "名古屋",
        "49": "園田", "50": "園田", "51": "姫路", "52": "帯広", "54": "高知",
        "55": "佐賀", "65": "帯広",
    }

    def _infer_location(by_venue: dict) -> str:
        if not by_venue:
            return ""
        e = sum(v.get("total", 0) for vc, v in by_venue.items() if vc in _JRA_EAST)
        w = sum(v.get("total", 0) for vc, v in by_venue.items() if vc in _JRA_WEST)
        n = sum(v.get("total", 0) for vc, v in by_venue.items() if vc in _NAR_VENUE_MAP)
        if e + w >= n:
            return "美浦" if e >= w else "栗東"
        top_nar = max(
            ((vc, v.get("total", 0)) for vc, v in by_venue.items() if vc in _NAR_VENUE_MAP),
            key=lambda x: x[1],
            default=("", 0),
        )
        return _NAR_VENUE_MAP.get(top_nar[0], "地方")

    def new_person():
        return {
            "name": "", "id": "",
            "name_counts": {},
            "total": 0, "win": 0, "place2": 0, "place3": 0,
            "win_odds_sum": 0.0,
            "jra": _empty_stat(), "nar": _empty_stat(),
            "by_surface":       defaultdict(_empty_stat),
            "jra_by_surface":   defaultdict(_empty_stat),
            "nar_by_surface":   defaultdict(_empty_stat),
            "by_smile":         defaultdict(_empty_stat),
            "jra_by_smile":     defaultdict(_empty_stat),
            "nar_by_smile":     defaultdict(_empty_stat),
            "by_venue":         defaultdict(_empty_stat),
            "by_running_style": defaultdict(_empty_stat),
        }

    def _agg_group(person, pid, name, venue, surface, dist, is_jra_bool,
                   running_style, total, win, place2, place3, win_odds_sum):
        """SQL GROUP BY の1行分を person dict にマージする"""
        person["id"] = pid
        if name:
            person["name_counts"][name] = person["name_counts"].get(name, 0) + total

        jra_nar  = "jra" if is_jra_bool else "nar"
        surf_key = surface if surface in ("芝", "ダート", "障害") else ""
        smile    = _smile_key(dist) if dist else ""
        smile_key_f = (surf_key + smile) if (surf_key and smile) else ""

        def _add(stat):
            stat["total"]  += total
            stat["win"]    += win
            stat["place2"] += place2
            stat["place3"] += place3

        _add(person)
        _add(person[jra_nar])
        if surf_key:
            _add(person["by_surface"][surf_key])
            _add(person["jra_by_surface" if is_jra_bool else "nar_by_surface"][surf_key])
        if smile_key_f:
            _add(person["by_smile"][smile_key_f])
            _add(person["jra_by_smile" if is_jra_bool else "nar_by_smile"][smile_key_f])
        if venue:
            _add(person["by_venue"][venue])
        if running_style:
            _add(person["by_running_style"][running_style])
        person["win_odds_sum"] += win_odds_sum

    jockeys:  dict = defaultdict(new_person)
    trainers: dict = defaultdict(new_person)
    sires:    dict = defaultdict(new_person)
    bmses:    dict = defaultdict(new_person)

    for r in j_rows:
        jid = r["jockey_id"] or ""
        if not jid:
            continue
        _agg_group(
            jockeys[jid], jid, r["jockey_name"] or "",
            str(r["venue_code"] or ""), r["surface"] or "",
            r["distance"] or 0, bool(r["is_jra"]),
            r["running_style"] or "",
            r["total"], r["win"], r["place2"], r["place3"], r["win_odds_sum"] or 0.0,
        )

    for r in t_rows:
        tid = r["trainer_id"] or ""
        if not tid:
            continue
        tname = r["trainer_name"] or ""
        # 馬体重パターン（"480(+24)"等）が trainer_name に混入している場合はスキップ
        if tname and _horse_weight_pat.match(tname.strip()):
            tname = ""
        _agg_group(
            trainers[tid], tid, tname,
            str(r["venue_code"] or ""), r["surface"] or "",
            r["distance"] or 0, bool(r["is_jra"]),
            r["running_style"] or "",
            r["total"], r["win"], r["place2"], r["place3"], r["win_odds_sum"] or 0.0,
        )

    for r in s_rows:
        sname = r["sire_name"] or ""
        if not sname:
            continue
        _agg_group(
            sires[sname], sname, sname,
            str(r["venue_code"] or ""), r["surface"] or "",
            r["distance"] or 0, bool(r["is_jra"]),
            r["running_style"] or "",
            r["total"], r["win"], r["place2"], r["place3"], r["win_odds_sum"] or 0.0,
        )

    for r in b_rows:
        bname = r["bms_name"] or ""
        if not bname:
            continue
        _agg_group(
            bmses[bname], bname, bname,
            str(r["venue_code"] or ""), r["surface"] or "",
            r["distance"] or 0, bool(r["is_jra"]),
            r["running_style"] or "",
            r["total"], r["win"], r["place2"], r["place3"], r["win_odds_sum"] or 0.0,
        )

    print(
        f"[DB集計] dict構築完了 ({_time.time()-t0:.1f}秒) "
        f"騎手={len(jockeys):,}人, 調教師={len(trainers):,}人, "
        f"種牡馬={len(sires):,}頭, 母父={len(bmses):,}頭",
        flush=True,
    )

    # ── 名前正規化用の定数（tryブロック外で定義、マージ関数からも参照）──
    _mark_pat = _re.compile(r'^[☆△▲▼◎○◇★●]+')
    _LOC_PREFIXES = (
        "美浦", "栗東", "北海道",
        "大井", "船橋", "川崎", "浦和",
        "笠松", "名古屋", "愛知",
        "園田", "姫路", "兵庫",
        "佐賀", "高知", "金沢",
        "盛岡", "水沢", "岩手",
        "門別", "帯広",
        "地方", "海外",
    )

    def _clean_name(n: str) -> str:
        s = _mark_pat.sub("", n).strip()
        # 所属サフィックス除去: （兵庫）（大井）等
        s = _re.sub(r'[（(][^)）]*[)）]$', '', s).strip()
        # 所属プレフィックス除去
        for pf in _LOC_PREFIXES:
            if s.startswith(pf) and len(s) > len(pf):
                rest = s[len(pf):]
                if rest.startswith(" "):
                    rest = rest.lstrip()
                s = rest
                break
        return s

    def _best_name(name_counts: dict) -> str:
        """マーカー・所属地名除去後に最多出現のフルネームを選ぶ"""
        cleaned = {}
        for n, c in name_counts.items():
            if not n or _horse_weight_pat.match(n.strip()):
                continue
            cn = _clean_name(n)
            if not cn:
                continue
            cleaned[cn] = cleaned.get(cn, 0) + c
        if not cleaned:
            return ""
        return max(cleaned, key=lambda n: (len(n), cleaned[n]))

    # ── personnel DB から名前・所属を補完 ─────────────────────────────
    try:
        _pers = get_personnel_all()
        _jloc = {jid: d.get("location", "") for jid, d in _pers.get("jockeys", {}).items()}
        _tloc = {tid: d.get("location", "") for tid, d in _pers.get("trainers", {}).items()}
        _tname_map = {
            tid: (d.get("trainer_name") or d.get("name") or "")
            for tid, d in _pers.get("trainers", {}).items()
        }
        _jname_map = {
            jid: (d.get("jockey_name") or d.get("name") or "")
            for jid, d in _pers.get("jockeys", {}).items()
        }
        try:
            _ml_map   = _load_ml_name_map()
            _ml_tname = _ml_map.get("trainer", {})
            _ml_jname = _ml_map.get("jockey", {})
        except Exception:
            _ml_tname = {}
            _ml_jname = {}

        def _resolve_name(pdb_raw, best, ml_name, current, pid):
            """マーカー除去済みの最長名を選択"""
            pdb = _clean_name(pdb_raw) if pdb_raw else ""
            ml  = _clean_name(ml_name) if ml_name else ""
            # 全候補からマーカー除去済みの最長を選ぶ
            candidates = [(best, len(best)) if best else ("", 0),
                          (pdb, len(pdb)) if pdb else ("", 0),
                          (ml, len(ml)) if ml else ("", 0)]
            candidates.sort(key=lambda x: x[1], reverse=True)
            winner = candidates[0][0]
            if winner:
                return winner
            if current and not _horse_weight_pat.match(current.strip()):
                return _clean_name(current)
            return pid

        for tid, ts in trainers.items():
            ts["name"] = _resolve_name(
                _tname_map.get(tid), _best_name(ts.get("name_counts", {})),
                _ml_tname.get(tid), ts["name"], tid)

        for jid, js in jockeys.items():
            js["name"] = _resolve_name(
                _jname_map.get(jid), _best_name(js.get("name_counts", {})),
                _ml_jname.get(jid), js["name"], jid)
    except Exception:
        _jloc = {}
        _tloc = {}

    # ── 同一人物の複数ID統合（土方颯太=05667,a0576,31349 等）──────────
    def _normalize_for_merge(name: str) -> str:
        """マージ用の正規化: マーク・所属・括弧を全除去"""
        s = _re.sub(r'^[☆△▲▼◎○◇★●]+', '', name).strip()
        # 所属サフィックス除去: （兵庫）（大井）等
        s = _re.sub(r'[（(][^)）]*[)）]$', '', s).strip()
        # 所属プレフィックス除去
        for pf in _LOC_PREFIXES:
            if s.startswith(pf) and len(s) > len(pf):
                rest = s[len(pf):].lstrip()
                s = rest
                break
        return s

    def _merge_duplicate_persons(persons: dict) -> dict:
        """同じ正規化名を持つ複数IDの人物を統合する。
        完全一致 + 前方一致（土方颯太↔土方颯, 吉村智洋↔吉村智）の両方で検出。"""
        # 正規化名 → [pid, ...] のマッピング（完全一致）
        name_to_pids = defaultdict(list)
        pid_to_norm = {}
        for pid, pdata in persons.items():
            norm = _normalize_for_merge(pdata.get("name", "") or pid)
            if norm and len(norm) >= 2:
                name_to_pids[norm].append(pid)
                pid_to_norm[pid] = norm

        # 前方一致マージ: 「土方颯」と「土方颯太」を統合
        # 短い名前が長い名前の先頭に含まれている場合
        sorted_names = sorted(name_to_pids.keys(), key=len)
        merged_names = {}  # 短い名前 → 長い名前（正典）へのマッピング
        for i, short in enumerate(sorted_names):
            if short in merged_names:
                continue
            for long in sorted_names[i+1:]:
                if long in merged_names:
                    continue
                # 短い名前が長い名前の先頭に一致（2文字以上共有）
                if long.startswith(short) and len(short) >= 2:
                    # 長い方に統合
                    name_to_pids[long].extend(name_to_pids[short])
                    merged_names[short] = long

        # マージ対象から短い名前を除去
        for short in merged_names:
            if short in name_to_pids:
                del name_to_pids[short]

        merged_count = 0
        for norm_name, pids in name_to_pids.items():
            if len(pids) <= 1:
                continue
            # 最多騎乗のIDを正典IDとする
            pids.sort(key=lambda p: persons[p]["total"], reverse=True)
            canonical_pid = pids[0]
            canonical = persons[canonical_pid]

            for dup_pid in pids[1:]:
                dup = persons[dup_pid]
                # 数値フィールドを合算
                for k in ("total", "win", "place2", "place3", "win_odds_sum"):
                    canonical[k] = canonical.get(k, 0) + dup.get(k, 0)
                # name_counts を合算
                for n, c in dup.get("name_counts", {}).items():
                    canonical.setdefault("name_counts", {})[n] = canonical["name_counts"].get(n, 0) + c
                # jra/nar を合算
                for jn in ("jra", "nar"):
                    for k in ("total", "win", "place2", "place3"):
                        canonical[jn][k] = canonical[jn].get(k, 0) + dup[jn].get(k, 0)
                # by_* を合算
                for by_key in ("by_surface", "jra_by_surface", "nar_by_surface",
                               "by_smile", "jra_by_smile", "nar_by_smile",
                               "by_venue", "by_running_style"):
                    for sk, sv in dup.get(by_key, {}).items():
                        target = canonical.setdefault(by_key, defaultdict(_empty_stat))[sk]
                        for k in ("total", "win", "place2", "place3"):
                            target[k] = target.get(k, 0) + sv.get(k, 0)
                # 重複IDを削除
                del persons[dup_pid]
                merged_count += 1

        if merged_count > 0:
            print(f"[DB集計] 同一人物ID統合: {merged_count}件マージ", flush=True)
        return persons

    jockeys  = _merge_duplicate_persons(jockeys)
    trainers = _merge_duplicate_persons(trainers)

    # 統合後に名前を再解決
    for jid, js in jockeys.items():
        best = _best_name(js.get("name_counts", {})) if "_best_name" in dir() else js.get("name", "")
        if best:
            js["name"] = best
    for tid, ts in trainers.items():
        best = _best_name(ts.get("name_counts", {})) if "_best_name" in dir() else ts.get("name", "")
        if best:
            ts["name"] = best

    def _finalize_stat(st: dict) -> dict:
        t = st.get("total", 0)
        return {
            "total": t, "win": st.get("win", 0),
            "place2": st.get("place2", 0), "place3": st.get("place3", 0),
            "win_rate":    _rate(st.get("win", 0), t),
            "place2_rate": _rate(st.get("place2", 0), t),
            "place3_rate": _rate(st.get("place3", 0), t),
        }

    def finalize(persons: dict, location_map: dict, is_trainer: bool = False, skip_location: bool = False) -> dict:
        result = {}
        for pid, st in persons.items():
            t = st["total"]
            win_odds_s = st.get("win_odds_sum", 0.0)
            roi = round(win_odds_s / t * 100, 1) if t and win_odds_s > 0 else None
            if skip_location:
                loc = ""
            else:
                raw_loc = location_map.get(pid, "")
                if raw_loc in ("JRA", "地方", ""):
                    loc = _infer_location(st.get("by_venue", {})) or raw_loc
                else:
                    loc = raw_loc
            result[pid] = {
                "name": st["name"] or pid,
                "id":   st.get("id", pid),
                "total": t,
                "win": st["win"], "place2": st["place2"], "place3": st["place3"],
                "win_rate":    _rate(st["win"],    t),
                "place2_rate": _rate(st["place2"], t),
                "place3_rate": _rate(st["place3"], t),
                "roi": roi,
                "jra": _finalize_stat(st["jra"]),
                "nar": _finalize_stat(st["nar"]),
                "by_surface":       {k: _finalize_stat(v) for k, v in st["by_surface"].items()},
                "jra_by_surface":   {k: _finalize_stat(v) for k, v in st["jra_by_surface"].items()},
                "nar_by_surface":   {k: _finalize_stat(v) for k, v in st["nar_by_surface"].items()},
                "by_smile":         {k: _finalize_stat(v) for k, v in st["by_smile"].items()},
                "jra_by_smile":     {k: _finalize_stat(v) for k, v in st["jra_by_smile"].items()},
                "nar_by_smile":     {k: _finalize_stat(v) for k, v in st["nar_by_smile"].items()},
                "by_venue":         {k: _finalize_stat(v) for k, v in st["by_venue"].items()},
                "by_running_style": {k: _finalize_stat(v) for k, v in st["by_running_style"].items()},
                "location": loc,
            }

        # 調教師偏差値計算 (J-2B): 勝率分布からZ変換
        if is_trainer:
            all_trainer_win_rates = [
                d.get("win_rate", 0.0) for d in result.values()
                if d.get("total", 0) >= 10
            ]
            if len(all_trainer_win_rates) >= 5:
                mean_wr = sum(all_trainer_win_rates) / len(all_trainer_win_rates)
                std_wr = (
                    sum((x - mean_wr) ** 2 for x in all_trainer_win_rates)
                    / len(all_trainer_win_rates)
                ) ** 0.5
                for d in result.values():
                    if d.get("total", 0) >= 10 and std_wr > 0:
                        raw = 52.0 + 12.5 * (d.get("win_rate", 0.0) - mean_wr) / std_wr
                        d["deviation"] = round(min(90.0, max(40.0, raw)), 1)
                    else:
                        d["deviation"] = 52.0
            else:
                for d in result.values():
                    d["deviation"] = 52.0

        # ── カテゴリ別偏差値計算 ──
        # by_venue, by_running_style, by_smile の各カテゴリで
        # 全人物の複勝率を母集団として偏差値化（10走未満は None）
        _MIN_RUNS_FOR_DEV = 10
        for cat_key in ("by_venue", "by_running_style", "by_smile"):
            # カテゴリ → [複勝率リスト] を収集
            cat_rates: dict = defaultdict(list)
            for d in result.values():
                for ck, cv in d.get(cat_key, {}).items():
                    if cv.get("total", 0) >= _MIN_RUNS_FOR_DEV:
                        cat_rates[ck].append(cv.get("place3_rate", 0.0))
            # カテゴリごとに平均・標準偏差を算出
            cat_stats: dict = {}
            for ck, rates in cat_rates.items():
                if len(rates) >= 3:
                    m = sum(rates) / len(rates)
                    s = (sum((x - m) ** 2 for x in rates) / len(rates)) ** 0.5
                    cat_stats[ck] = (m, s)
            # 各人物のカテゴリに偏差値を付与
            for d in result.values():
                for ck, cv in d.get(cat_key, {}).items():
                    if cv.get("total", 0) >= _MIN_RUNS_FOR_DEV and ck in cat_stats:
                        m, s = cat_stats[ck]
                        if s > 0:
                            # 指数: 40〜90スケール（中央52 = B帯）
                            raw = 52.0 + 12.5 * (cv.get("place3_rate", 0.0) - m) / s
                            cv["dev"] = round(min(90.0, max(40.0, raw)), 1)
                        else:
                            cv["dev"] = 52.0
                    # 10走未満は dev を付与しない（フロントで "—" 表示）

        return result

    # 種牡馬/母父馬は name_counts を使って名前を確定（ID=名前）
    for sname, st in sires.items():
        st["name"] = sname
    for bname, st in bmses.items():
        st["name"] = bname

    result = {
        "jockey":  finalize(jockeys, _jloc, is_trainer=False),
        "trainer": finalize(trainers, _tloc, is_trainer=True),
        "sire":    finalize(sires, {}, is_trainer=False, skip_location=True),
        "bms":     finalize(bmses, {}, is_trainer=False, skip_location=True),
        "_period": {"min": min_d, "max": max_d, "race_count": race_count, "source": "race_log"},
    }

    print(
        f"[DB集計] 完了 ({_time.time()-t0:.1f}秒) "
        f"騎手={len(result['jockey']):,}人, 調教師={len(result['trainer']):,}人, "
        f"種牡馬={len(result['sire']):,}頭, 母父={len(result['bms']):,}頭",
        flush=True,
    )

    # ディスクキャッシュに保存（次回起動時は即座にロード）
    _save_personnel_cache(result, year_filter)

    return result
