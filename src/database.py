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
"""


def init_schema() -> None:
    """テーブルを初期化する（冪等・既存データ保持）"""
    conn = get_db()
    conn.executescript(_SCHEMA_SQL)
    # 既存DBへの列追加・インデックス追加（ALTER TABLE/CREATE INDEX は IF NOT EXISTS 非対応なので try/except）
    for ddl in [
        "ALTER TABLE race_log ADD COLUMN win_odds REAL DEFAULT NULL",
        "ALTER TABLE race_log ADD COLUMN running_style TEXT DEFAULT NULL",
        "ALTER TABLE race_log ADD COLUMN sire_name TEXT DEFAULT ''",
        "ALTER TABLE race_log ADD COLUMN bms_name TEXT DEFAULT ''",
        "ALTER TABLE race_log ADD COLUMN condition TEXT DEFAULT ''",
        "CREATE INDEX IF NOT EXISTS idx_racelog_sire ON race_log(sire_name)",
        "CREATE INDEX IF NOT EXISTS idx_racelog_bms  ON race_log(bms_name)",
        "CREATE INDEX IF NOT EXISTS idx_racelog_venue   ON race_log(venue_code)",
        "CREATE INDEX IF NOT EXISTS idx_racelog_surface ON race_log(surface)",
        "CREATE INDEX IF NOT EXISTS idx_personnel_type  ON personnel(person_type)",
        "CREATE INDEX IF NOT EXISTS idx_pred_race_id    ON predictions(race_id)",
        "CREATE INDEX IF NOT EXISTS idx_result_race_id  ON race_results(race_id)",
    ]:
        try:
            conn.execute(ddl)
        except Exception:
            pass
    conn.commit()


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


def save_match_result(date: str, race_id: str, stats: dict) -> None:
    """照合済み集計を 1 レース分保存"""
    with transaction() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO match_results
              (date, race_id, venue, race_no, hit_tickets, total_tickets,
               stake, ret, honmei_placed, honmei_win,
               by_mark_json, by_ticket_json, by_ana_json, by_kiken_json)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
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
            ),
        )


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
    race_log から venue_code × surface 別の枠番（gate_no）複勝率を集計し、
    枠順バイアスDBを自動構築する（案F-5）。

    既存の gate_bias_db を上書きするのではなく、
    race_log ベースのデータとしてエンジンで合成して使用する。

    Args:
        target_date: ローリングウィンドウ基準日 (YYYY-MM-DD)。
                     指定時は基準日の1年前〜前日のデータのみ使用。

    Returns:
        {"venue_surface": {gate_no: bias_score}}
        例: {"01_芝": {1: 2.1, 2: 1.3, ..., 8: -1.8}}
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
        rows = conn.execute(
            f"""
            SELECT
                venue_code, surface, gate_no,
                COUNT(*) AS total,
                SUM(CASE WHEN finish_pos <= 3 THEN 1.0 ELSE 0.0 END) AS place3
            FROM race_log
            WHERE gate_no IS NOT NULL AND gate_no BETWEEN 1 AND 8
              AND finish_pos IS NOT NULL AND finish_pos > 0
              AND field_count >= 8
              {date_clause}
            GROUP BY venue_code, surface, gate_no
            HAVING total >= 15
            """,
            params,
        ).fetchall()
    except Exception:
        return {}

    # venue_surface ごとに集計
    from collections import defaultdict
    venue_data: dict = defaultdict(lambda: defaultdict(lambda: {"total": 0, "place3": 0}))
    for r in rows:
        key = f"{r['venue_code']}_{r['surface']}"
        gate = int(r["gate_no"])
        venue_data[key][gate]["total"] += r["total"]
        venue_data[key][gate]["place3"] += r["place3"]

    result = {}
    for key, gates in venue_data.items():
        # 全枠の複勝率平均を算出
        all_totals = sum(g["total"] for g in gates.values())
        all_place3 = sum(g["place3"] for g in gates.values())
        avg_rate = all_place3 / all_totals if all_totals > 0 else 1 / 3

        gate_bias = {}
        for gate_no, stats in gates.items():
            if stats["total"] < 15:
                continue
            rate = stats["place3"] / stats["total"]
            # 偏差値スケールへ: 平均からの差を ±5 にスケール
            diff = rate - avg_rate
            bias = max(-5.0, min(5.0, diff * 20.0))
            gate_bias[gate_no] = round(bias, 2)

        if gate_bias:
            result[key] = gate_bias

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

    # JOIN対象のrace_id数をカウント（重いJOINクエリを回避する早期チェック）
    joinable_count = conn.execute(
        """SELECT COUNT(DISTINCT p.race_id)
           FROM predictions p
           INNER JOIN race_results r ON p.race_id = r.race_id
           WHERE r.order_json IS NOT NULL AND r.order_json != '[]' AND r.order_json != 'null'"""
    ).fetchone()[0]

    # 全race_idが既に投入済みならJOINクエリとバックフィルをスキップ
    if joinable_count <= len(existing):
        print(f"[race_log投入] 新規なし (既存={len(existing):,}, 対象={joinable_count:,}) → スキップ", flush=True)
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
                    }

            orders = json.loads(pred["order_json"])
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

                txn.execute(
                    """
                    INSERT OR IGNORE INTO race_log
                      (race_date, race_id, venue_code, surface, distance,
                       horse_no, finish_pos,
                       jockey_id, jockey_name, trainer_id, trainer_name,
                       field_count, is_jra, win_odds,
                       sire_name, bms_name)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (race_date, race_id, venue_code, surface, distance,
                     horse_no, finish,
                     hinfo.get("jockey_id", ""),
                     hinfo.get("jockey_name", ""),
                     hinfo.get("trainer_id", ""),
                     hinfo.get("trainer_name", ""),
                     field_count, is_jra, win_odds,
                     hinfo.get("sire_name", ""),
                     hinfo.get("bms_name", "")),
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
            conn.commit()
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
            conn.commit()
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
        import os as _os
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

    elapsed = _time.time() - t0
    print(f"[race_log投入] 完了 ({elapsed:.1f}秒) 新規={inserted:,}件挿入, スキップ={skipped:,}件", flush=True)
    return inserted


def _load_ml_name_map(cache_hours: int = 24) -> dict:
    """
    ml/training_ml JSON から trainer/jockey の id→name マッピングを構築してキャッシュする。
    返り値: {"trainer": {id: name}, "jockey": {id: name}}
    """
    import os as _os, glob, re as _re, time as _time
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
    from collections import defaultdict
    import re as _re
    import time as _time

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

        _mark_pat = _re.compile(r'^[☆△▲▼◎○◇★●]+')
        # netkeiba の一部データでは trainer_name / jockey_name に所属地名がプレフィクスされている
        _LOC_PREFIXES = (
            "美浦", "栗東", "北海道",          # JRA / 広域
            "大井", "船橋", "川崎", "浦和",     # 南関東
            "笠松", "名古屋", "愛知",           # 東海
            "園田", "姫路",                     # 兵庫
            "佐賀", "高知", "金沢",             # 地方
            "盛岡", "水沢", "岩手",             # 岩手
            "門別", "帯広",                     # 北海道
            "地方", "海外",                     # 汎用プレフィクス (スペース区切りあり)
        )
        def _clean_name(n: str) -> str:
            s = _mark_pat.sub("", n).strip()
            for pf in _LOC_PREFIXES:
                if s.startswith(pf) and len(s) > len(pf):
                    rest = s[len(pf):]
                    # "地方 伊藤強" のようにスペース区切りの場合も除去
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
                # 同じクリーン名は出現数を合算
                cleaned[cn] = cleaned.get(cn, 0) + c
            if not cleaned:
                return ""
            return max(cleaned, key=lambda n: (len(n), cleaned[n]))

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
