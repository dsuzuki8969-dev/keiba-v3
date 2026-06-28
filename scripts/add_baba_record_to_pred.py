"""
add_baba_record_to_pred.py — pred.json に track_condition / baba_record を追加する後処理スクリプト

処理内容:
  1. race["track_condition_turf"] / race["track_condition_dirt"] を追加
     (既存の race["condition"] はそのまま保持・後方互換)
  2. 各馬に baba_record = {bad_p3, bad_n, good_p3, good_n} を追加
     - race_date < 当日(リーク厳禁)の race_log を参照
     - condition は略字/正式名の両方に対応 (良/稍/稍重/重/不/不良)
     - bad_n < 3 の場合は bad_p3 = null (データ不足を明示)
     - good_n < 3 の場合は good_p3 = null (データ不足を明示)

使用方法:
    python scripts/add_baba_record_to_pred.py 20260628
"""

from __future__ import annotations

import contextlib
import json
import os
import re
import shutil
import sqlite3
import sys
import time
from pathlib import Path
from typing import Dict, Optional

# cp932 クラッシュ対策 (Windows scheduler / CLI 経由)
# 競馬記号・絵文字を print するとターミナルが cp932 の場合に UnicodeEncodeError で落ちる
with contextlib.suppress(AttributeError, ValueError):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
with contextlib.suppress(AttributeError, ValueError):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# プロジェクトルートをパスに追加
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

# race_id → venue_code 変換 (JRA 当日馬場詳細の突合用)
from data.masters.venue_master import get_venue_code_from_race_id

# ─── 定数 ───────────────────────────────────────────────────────────────────────

# 道悪馬場の condition 値 (略字 + 正式名 両対応)
_BAD_CONDITIONS = {"稍", "稍重", "重", "不", "不良"}
# 良馬場の condition 値
_GOOD_CONDITIONS = {"良"}
# n がこのしきい値未満の場合 p3 = None (データ不足)
# bad/good 両方に同一基準を適用 (非対称にしない)
_MIN_N = 3

# JRA 当日馬場 (クッション値/含水率) JSON のパス (見える化用・JRA限定)
_TRACK_COND_PATH = _ROOT / "data" / "masters" / "track_condition_daily.json"


def _load_track_condition_day(race_date: str) -> dict:
    """track_condition_daily.json から race_date(YYYY-MM-DD) の {venue_code: {cushion...}} を返す。

    見える化専用 (JRA 当日クッション値/含水率)。ファイル無し/壊れ時は {} で graceful。
    """
    try:
        with open(_TRACK_COND_PATH, encoding="utf-8") as f:
            return json.load(f).get(race_date, {})
    except (OSError, json.JSONDecodeError):
        return {}

# ─── race_log から道悪成績を一括取得 ─────────────────────────────────────────────

def _build_baba_stats(con: sqlite3.Connection, horse_ids: list[str], cutoff_date: str) -> Dict[str, dict]:
    """
    race_date < cutoff_date の race_log から horse_id ごとの道悪/良 複勝率を計算。

    Parameters
    ----------
    con : sqlite3.Connection
        DB 接続 (読み取り専用)
    horse_ids : list[str]
        pred 中に登場する horse_id リスト (重複なし)
    cutoff_date : str
        "YYYY-MM-DD" 形式。この日付未満のみ集計 (リーク防止)

    Returns
    -------
    dict
        { horse_id: {"bad_p3_cnt": int, "bad_n": int, "good_p3_cnt": int, "good_n": int} }

    Notes
    -----
    race_log.race_date には "YYYY-MM-DD" と "YYYYMMDD" が混在する(ALTER TABLE 以前のデータ)。
    SQLite は文字列辞書順比較のため '20260407' < '2026-06-28' が False になり、
    YYYYMMDD 形式の行が cutoff より未来と誤判定される。
    DATE() 変換で両形式を統一してから比較することでリーク防止を確実にする。
    """
    if not horse_ids:
        return {}

    # cutoff を DATE() で評価するための CASE 式
    # YYYYMMDD(8桁) → YYYY-MM-DD に変換して DATE() に渡す
    # YYYY-MM-DD(10桁) → そのまま DATE() に渡す
    _DATE_EXPR = """
        DATE(
            CASE LENGTH(race_date)
                WHEN 8 THEN SUBSTR(race_date,1,4)||'-'||SUBSTR(race_date,5,2)||'-'||SUBSTR(race_date,7,2)
                ELSE race_date
            END
        )
    """

    # SQLite の IN 句用プレースホルダ
    placeholders = ",".join("?" * len(horse_ids))

    sql = f"""
        SELECT
            horse_id,
            condition,
            SUM(CASE WHEN finish_pos <= 3 THEN 1 ELSE 0 END) AS p3_cnt,
            COUNT(*) AS n
        FROM race_log
        WHERE horse_id IN ({placeholders})
          AND {_DATE_EXPR} < DATE(?)
          AND finish_pos > 0
          -- finish_pos = 0 は「未確定」または「競走中止」扱い。
          -- 複勝率の母数に含めると実績を過小評価するため除外する。
        GROUP BY horse_id, condition
    """
    params = [*horse_ids, cutoff_date]
    rows = con.execute(sql, params).fetchall()

    # horse_id → {bad_p3_cnt, bad_n, good_p3_cnt, good_n}
    result: Dict[str, dict] = {}
    for horse_id, condition, p3_cnt, n in rows:
        if not horse_id:
            continue
        if horse_id not in result:
            result[horse_id] = {"bad_p3_cnt": 0, "bad_n": 0, "good_p3_cnt": 0, "good_n": 0}
        stats = result[horse_id]
        if condition in _BAD_CONDITIONS:
            stats["bad_p3_cnt"] += p3_cnt
            stats["bad_n"] += n
        elif condition in _GOOD_CONDITIONS:
            stats["good_p3_cnt"] += p3_cnt
            stats["good_n"] += n
        # condition が空文字や未知の場合は無視

    return result


def _compute_baba_record(stats: Optional[dict]) -> dict:
    """
    集計済み stats から baba_record dict を生成。

    Returns
    -------
    dict
        bad_p3: float(0-100) or None, bad_n: int, good_p3: float(0-100) or None, good_n: int
        n < _MIN_N の場合は p3 = None (データ不足を明示)
    """
    if not stats:
        return {"bad_p3": None, "bad_n": 0, "good_p3": None, "good_n": 0}

    bad_n = stats["bad_n"]
    bad_p3_cnt = stats["bad_p3_cnt"]
    good_n = stats["good_n"]
    good_p3_cnt = stats["good_p3_cnt"]

    # n < _MIN_N の場合はデータ不足で None (bad/good 同一基準)
    bad_p3 = round(bad_p3_cnt * 100 / bad_n, 1) if bad_n >= _MIN_N else None
    good_p3 = round(good_p3_cnt * 100 / good_n, 1) if good_n >= _MIN_N else None

    return {
        "bad_p3": bad_p3,
        "bad_n": bad_n,
        "good_p3": good_p3,
        "good_n": good_n,
    }


# ─── メイン処理 ───────────────────────────────────────────────────────────────────

def add_baba_record(date_key: str, dry_run: bool = False) -> dict:
    """
    pred.json に track_condition / baba_record を追加してファイルを上書き保存。

    Parameters
    ----------
    date_key : str
        YYYYMMDD 形式の日付文字列 (例: "20260628")
    dry_run : bool
        True の場合はファイルを書き換えず、集計結果のみ返す

    Returns
    -------
    dict
        { "races": int, "horses": int, "updated_horses": int, "elapsed_sec": float }
    """
    # ── 入力バリデーション ─────────────────────────────────────────────────────
    if not re.fullmatch(r"\d{8}", date_key):
        raise ValueError(f"date_key は YYYYMMDD 形式で指定してください: {date_key!r}")

    t0 = time.time()
    pred_path = _ROOT / "data" / "predictions" / f"{date_key}_pred.json"
    if not pred_path.exists():
        raise FileNotFoundError(f"pred.json が見つかりません: {pred_path}")

    db_path = _ROOT / "data" / "keiba.db"
    if not db_path.exists():
        raise FileNotFoundError(f"keiba.db が見つかりません: {db_path}")

    # ── pred.json 読み込み ──────────────────────────────────────────────────────
    print(f"[baba_record] pred 読み込み: {pred_path.name}")
    with open(pred_path, encoding="utf-8") as f:
        pred = json.load(f)

    races = pred.get("races", [])
    print(f"[baba_record] レース数: {len(races)}")

    # ── horse_id 一覧を収集 ─────────────────────────────────────────────────────
    horse_ids: list[str] = []
    seen: set[str] = set()
    for race in races:
        for horse in race.get("horses", []):
            hid = horse.get("horse_id", "")
            if hid and hid not in seen:
                horse_ids.append(hid)
                seen.add(hid)

    print(f"[baba_record] ユニーク horse_id 数: {len(horse_ids)}")

    # ── DB クエリ ──────────────────────────────────────────────────────────────
    # date_key (YYYYMMDD) を YYYY-MM-DD に変換
    cutoff_date = f"{date_key[:4]}-{date_key[4:6]}-{date_key[6:8]}"
    print(f"[baba_record] cutoff_date={cutoff_date} (この日付未満のみ集計・リーク防止)")

    # JRA 当日馬場詳細 (クッション値/含水率) を race レベルに付与するため読み込み (見える化用・JRA限定)
    day_baba_detail = _load_track_condition_day(cutoff_date)
    if day_baba_detail:
        print(f"[baba_record] 当日馬場詳細: {len(day_baba_detail)} 会場 (クッション値/含水率)")

    # DB は読み取り専用 URI で接続 (誤書き込み防止)
    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    con.row_factory = None  # tuple で取得(速度優先)
    try:
        baba_stats = _build_baba_stats(con, horse_ids, cutoff_date)
    finally:
        con.close()

    print(f"[baba_record] 集計完了: {len(baba_stats)} 頭分の成績を取得")

    # ── pred.json 更新 ────────────────────────────────────────────────────────
    updated_horses = 0
    total_horses = 0

    for race in races:
        # ─ race レベル: track_condition_turf / track_condition_dirt 追加 ─
        # 既存の condition フィールドはそのまま保持(後方互換)
        surface = race.get("surface", "")
        existing_cond = race.get("condition", "")

        if surface == "芝":
            race["track_condition_turf"] = existing_cond
            race["track_condition_dirt"] = ""
        elif surface == "ダート":
            race["track_condition_turf"] = ""
            race["track_condition_dirt"] = existing_cond
        else:
            # 障害・その他
            race["track_condition_turf"] = ""
            race["track_condition_dirt"] = ""

        # ─ race レベル: JRA 当日馬場詳細 (クッション値/含水率/馬場状態) 付与 (JRA限定・見える化用) ─
        # race_id から venue_code を導出し track_condition_daily.json と突合。
        # NAR・データ無しは None (フロントは None なら非表示)。
        _vc = get_venue_code_from_race_id(race.get("race_id", "") or "")
        _baba_raw = day_baba_detail.get(_vc) if _vc else None
        race["baba_detail"] = _baba_raw

        # ⚠️ 当日ライブ馬場状態(良/重)は baba_detail.condition_turf/dirt に保持済(上記)。
        # race["condition"] / race["track_condition_turf"/"dirt"] へは書かない:
        # これらは engine.py / lgbm_model.py の RaceInfo(ML入力)と同名フィールドで、
        # 予想後取得の当日値を入れると将来 pred 読み戻し時にリーク源となる(keiba-reviewer P1)。
        # 表示・展開ヒントは frontend が baba_detail 経由で参照する(見える化専用)。

        # ─ horse レベル: baba_record 追加 ─
        for horse in race.get("horses", []):
            total_horses += 1
            hid = horse.get("horse_id", "")
            stats = baba_stats.get(hid) if hid else None
            horse["baba_record"] = _compute_baba_record(stats)
            if hid and hid in baba_stats:
                updated_horses += 1

    print(f"[baba_record] 馬更新: {updated_horses}/{total_horses} 頭 (horse_id あり)")

    if dry_run:
        elapsed = time.time() - t0
        print(f"[baba_record] dry_run モード: ファイルは書き換えません ({elapsed:.2f}秒)")
        return {
            "races": len(races),
            "horses": total_horses,
            "updated_horses": updated_horses,
            "elapsed_sec": round(elapsed, 2),
        }

    # ── アトミック書き込み + バックアップ ────────────────────────────────────────
    # 1. tmp ファイルに json.dump 完了まで書き込む
    # 2. 成功後にバックアップをコピー
    # 3. os.replace() でアトミック置換 (json.dump 失敗時は pred.json を破損させない)
    bak_path = pred_path.with_suffix(".json.bak")
    tmp_path = pred_path.with_suffix(".json.tmp")
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(pred, f, ensure_ascii=False, indent=2)
        print(f"[baba_record] バックアップ: {bak_path.name}")
        shutil.copy2(pred_path, bak_path)
        os.replace(tmp_path, pred_path)  # アトミック置換
    except Exception:
        tmp_path.unlink(missing_ok=True)  # tmp ゴミファイル削除
        raise

    elapsed = time.time() - t0
    print(f"[baba_record] 完了: {pred_path.name} 上書き保存 ({elapsed:.2f}秒)")
    return {
        "races": len(races),
        "horses": total_horses,
        "updated_horses": updated_horses,
        "elapsed_sec": round(elapsed, 2),
    }


# ─── CLI エントリーポイント ──────────────────────────────────────────────────────

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("使用方法: python scripts/add_baba_record_to_pred.py YYYYMMDD [--dry-run]")
        sys.exit(1)

    _date_key = sys.argv[1]
    _dry_run = "--dry-run" in sys.argv

    try:
        result = add_baba_record(_date_key, dry_run=_dry_run)
        print("\n[baba_record] 結果サマリ:")
        print(f"  レース数       : {result['races']}")
        print(f"  馬数           : {result['horses']}")
        print(f"  baba_record付き: {result['updated_horses']}")
        print(f"  所要時間       : {result['elapsed_sec']}秒")
    except Exception as e:
        print(f"[baba_record] エラー: {e}", file=sys.stderr)
        raise
