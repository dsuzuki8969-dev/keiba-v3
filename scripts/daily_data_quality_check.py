#!/usr/bin/env python3
"""
毎日のデータ品質チェックスクリプト
====================================
当日 + 前日の pred.json および直近 race_log を読み込み、
各カラムの欠損率を計算して閾値超えなら exit 1 を返す。

使い方:
    python scripts/daily_data_quality_check.py                # 今日
    python scripts/daily_data_quality_check.py --date 2026-04-26
    python scripts/daily_data_quality_check.py --strict       # 閾値を厳格化
    python scripts/daily_data_quality_check.py --days 7       # 直近N日分のrace_logを対象

出力:
    stdout に集計表（rich テーブル風プログレスバー）
    ログ: logs/daily_data_quality_check.log（追記）
    閾値超え: exit 1（タスクスケジューラ失敗扱い）
"""
from __future__ import annotations

import argparse
import io
import json
import logging
import os
import sqlite3
import sys
from datetime import date, datetime, timedelta
from logging.handlers import RotatingFileHandler
from pathlib import Path

# Windows コンソールの文字化け回避（UTF-8 出力を強制）
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

# ─── パス設定 ────────────────────────────────────────────────
_SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = _SCRIPT_DIR.parent

sys.path.insert(0, str(PROJECT_ROOT))

# ─── ログ設定 ────────────────────────────────────────────────
_LOG_DIR = PROJECT_ROOT / "logs"
_LOG_DIR.mkdir(exist_ok=True)
_LOG_FILE = _LOG_DIR / "daily_data_quality_check.log"

_fmt = logging.Formatter("[%(asctime)s] %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger("dq_check")
logger.setLevel(logging.DEBUG)
if not logger.handlers:
    fh = RotatingFileHandler(str(_LOG_FILE), maxBytes=5 * 1024 * 1024, backupCount=5, encoding="utf-8")
    fh.setFormatter(_fmt)
    logger.addHandler(fh)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(_fmt)
    logger.addHandler(sh)

# ─── 帯広 venue_code 定数 ────────────────────────────────────
# ばんえい競馬の venue_code（last_3f 欠損は仕様上正常）
_BANEI_VENUE_CODES = {"55", "65"}

# ─── デフォルト閾値 ─────────────────────────────────────────
_THRESHOLDS = {
    "run_dev_floor20": 0.05,        # speed_dev floor 20.0: 全体 5% 超え → WARN
    "positions_corners_empty": 0.05, # positions_corners 空: 5% 超え → WARN
    "finish_time_zero": 0.03,        # finish_time_sec=0: 3% 超え → WARN
    "jockey_empty": 0.01,            # jockey 空: 1% 超え → WARN
    "last_3f_zero": 0.05,            # last_3f_sec=0（非帯広）: 5% 超え → WARN
    "margin_ahead_zero": 0.05,       # margin_ahead=0（2着以下）: 5% 超え → WARN
}

# strict モード: 全閾値を半分に
_STRICT_FACTOR = 0.5


def _get_db_path() -> str:
    """keiba.db のパスを返す"""
    try:
        from config.settings import DATABASE_PATH
        return DATABASE_PATH
    except Exception:
        return str(PROJECT_ROOT / "data" / "keiba.db")


def _progress_bar(ratio: float, width: int = 20) -> str:
    """簡易テキストプログレスバーを返す（例: [████░░░░░░] 40.0%）"""
    filled = int(ratio * width)
    bar = "█" * filled + "░" * (width - filled)
    return f"[{bar}] {ratio * 100:.1f}%"


def check_race_log(
    since: date,
    thresholds: dict,
    strict: bool,
) -> tuple[list[dict], bool]:
    """
    race_log テーブルを since 以降でスキャンして品質指標を返す。

    Returns:
        (results: list[dict], has_violation: bool)
        results 各要素: {name, count, total, ratio, threshold, violated}
    """
    db_path = _get_db_path()
    if not os.path.exists(db_path):
        logger.error("DB が見つかりません: %s", db_path)
        return [], False

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    since_str = since.isoformat()
    results = []
    has_violation = False

    try:
        # 総行数
        cur = conn.execute(
            "SELECT COUNT(*) FROM race_log WHERE race_date >= ?", (since_str,)
        )
        total = cur.fetchone()[0]

        if total == 0:
            logger.warning("race_log に %s 以降のデータがありません", since_str)
            return [], False

        def _check(name: str, sql: str, thr_key: str, params: tuple = ()) -> None:
            nonlocal has_violation
            cur2 = conn.execute(sql, (since_str,) + params)
            cnt = cur2.fetchone()[0]
            ratio = cnt / total
            thr = thresholds[thr_key] * (_STRICT_FACTOR if strict else 1.0)
            violated = ratio > thr
            if violated:
                has_violation = True
            results.append({
                "name": name,
                "count": cnt,
                "total": total,
                "ratio": ratio,
                "threshold": thr,
                "violated": violated,
            })

        # ① run_dev floor 20.0（NULL も含む）
        _check(
            "run_dev=NULL or 20.0",
            "SELECT COUNT(*) FROM race_log WHERE race_date >= ? AND (run_dev IS NULL OR run_dev = 20.0)",
            "run_dev_floor20",
        )

        # ② positions_corners 空（非帯広）
        # 帯広（ばんえい）は直走路 200m × 6 区間で「通過順」概念なし → 仕様上欠損正常
        # last_3f_sec=0 と同様に帯広を除外
        placeholders_pc = ",".join("?" * len(_BANEI_VENUE_CODES))
        cur_pc = conn.execute(
            f"SELECT COUNT(*) FROM race_log WHERE race_date >= ? AND (positions_corners IS NULL OR positions_corners = '' OR positions_corners = '[]') AND venue_code NOT IN ({placeholders_pc})",
            (since_str,) + tuple(_BANEI_VENUE_CODES),
        )
        cnt_pc = cur_pc.fetchone()[0]
        ratio_pc = cnt_pc / total
        thr_pc = thresholds["positions_corners_empty"] * (_STRICT_FACTOR if strict else 1.0)
        violated_pc = ratio_pc > thr_pc
        if violated_pc:
            has_violation = True
        results.append({
            "name": "positions_corners 空(非帯広)",
            "count": cnt_pc,
            "total": total,
            "ratio": ratio_pc,
            "threshold": thr_pc,
            "violated": violated_pc,
        })

        # ③ finish_time_sec=0
        _check(
            "finish_time_sec=0",
            "SELECT COUNT(*) FROM race_log WHERE race_date >= ? AND finish_time_sec = 0",
            "finish_time_zero",
        )

        # ④ last_3f_sec=0（非帯広）
        # venue_code が帯広以外で last_3f_sec=0
        placeholders = ",".join("?" * len(_BANEI_VENUE_CODES))
        cur3 = conn.execute(
            f"SELECT COUNT(*) FROM race_log WHERE race_date >= ? AND last_3f_sec = 0 AND venue_code NOT IN ({placeholders})",
            (since_str,) + tuple(_BANEI_VENUE_CODES),
        )
        cnt = cur3.fetchone()[0]
        ratio = cnt / total
        thr = thresholds["last_3f_zero"] * (_STRICT_FACTOR if strict else 1.0)
        violated = ratio > thr
        if violated:
            has_violation = True
        results.append({
            "name": "last_3f_sec=0(非帯広)",
            "count": cnt,
            "total": total,
            "ratio": ratio,
            "threshold": thr,
            "violated": violated,
        })

        # ⑤ jockey 空（2文字未満）
        _check(
            "jockey名 空/短い",
            "SELECT COUNT(*) FROM race_log WHERE race_date >= ? AND (jockey_name IS NULL OR length(jockey_name) < 2)",
            "jockey_empty",
        )

        # ⑥ margin_ahead=0（2着以下: 連鎖バグ）
        cur4 = conn.execute(
            "SELECT COUNT(*) FROM race_log WHERE race_date >= ? AND margin_ahead = 0 AND finish_pos > 1",
            (since_str,),
        )
        cnt = cur4.fetchone()[0]
        ratio = cnt / total
        thr = thresholds["margin_ahead_zero"] * (_STRICT_FACTOR if strict else 1.0)
        violated = ratio > thr
        if violated:
            has_violation = True
        results.append({
            "name": "margin_ahead=0(2着以下)",
            "count": cnt,
            "total": total,
            "ratio": ratio,
            "threshold": thr,
            "violated": violated,
        })

    finally:
        conn.close()

    return results, has_violation


def check_pred_json(target: date) -> tuple[list[dict], bool]:
    """
    当日 + 前日の pred.json から past_runs の欠損率を確認する。
    pred.json に past_runs がなければスキップ。

    Returns:
        (results: list[dict], has_violation: bool)
    """
    results = []
    has_violation = False

    for delta in (0, 1):
        check_date = target - timedelta(days=delta)
        date_str = check_date.strftime("%Y%m%d")
        pred_path = PROJECT_ROOT / "data" / "predictions" / f"{date_str}_pred.json"

        if not pred_path.exists():
            logger.debug("pred.json 不在: %s", pred_path)
            continue

        try:
            with open(pred_path, "r", encoding="utf-8") as f:
                pred = json.load(f)
        except Exception as e:
            logger.error("pred.json 読込失敗: %s → %s", pred_path, e)
            continue

        races = pred.get("races", [])
        total_past_runs = 0
        speed_dev_none = 0
        jockey_short = 0

        for race in races:
            for horse in race.get("horses", []):
                for pr in horse.get("past_runs", []):
                    total_past_runs += 1
                    # speed_dev = None or floor 20.0
                    sd = pr.get("speed_dev")
                    if sd is None or sd == 20.0:
                        speed_dev_none += 1
                    # jockey 5文字未満
                    jk = pr.get("jockey") or ""
                    if len(jk) < 2:
                        jockey_short += 1

        if total_past_runs == 0:
            logger.debug("pred.json(%s) に past_runs なし → スキップ", date_str)
            continue

        # speed_dev floor チェック
        ratio_sd = speed_dev_none / total_past_runs
        thr_sd = _THRESHOLDS["run_dev_floor20"]
        violated_sd = ratio_sd > thr_sd
        if violated_sd:
            has_violation = True
        results.append({
            "name": f"pred.json({check_date}): past_run speed_dev=None/20.0",
            "count": speed_dev_none,
            "total": total_past_runs,
            "ratio": ratio_sd,
            "threshold": thr_sd,
            "violated": violated_sd,
        })

        # jockey 短い チェック
        ratio_jk = jockey_short / total_past_runs
        thr_jk = _THRESHOLDS["jockey_empty"]
        violated_jk = ratio_jk > thr_jk
        if violated_jk:
            has_violation = True
        results.append({
            "name": f"pred.json({check_date}): past_run jockey 空/短い",
            "count": jockey_short,
            "total": total_past_runs,
            "ratio": ratio_jk,
            "threshold": thr_jk,
            "violated": violated_jk,
        })

    return results, has_violation


def print_table(race_log_results: list[dict], pred_results: list[dict], since: date, target: date) -> None:
    """集計結果を stdout にテキスト表示する"""
    sep = "=" * 72
    print(sep)
    print(f"  D-AI Keiba データ品質チェック  対象日: {target}  race_log: {since} 以降")
    print(sep)

    all_results = [
        ("race_log", race_log_results),
        ("pred.json", pred_results),
    ]

    any_violation = False
    for section, items in all_results:
        if not items:
            print(f"  [{section}] データなし / スキップ")
            continue
        print(f"\n  [{section}]")
        print(f"  {'指標':<42} {'件数':>6} {'合計':>6} {'比率':>7} {'閾値':>6} {'状態':>6}")
        print("  " + "-" * 70)
        for r in items:
            bar = _progress_bar(min(r["ratio"], 1.0), width=10)
            status = "⚠ WARN" if r["violated"] else "  OK  "
            if r["violated"]:
                any_violation = True
            print(
                f"  {r['name']:<42} {r['count']:>6,} {r['total']:>6,} "
                f"{r['ratio'] * 100:>6.1f}% {r['threshold'] * 100:>5.1f}% {status}  {bar}"
            )

    print()
    if any_violation:
        print("  *** [FAIL] 閾値超え検出 -> exit 1 ***")
    else:
        print("  [OK] 全指標 正常範囲内")
    print(sep)


def save_result_json(race_log_results: list[dict], pred_results: list[dict], target: date, has_violation: bool) -> None:
    """
    最終結果を JSON ファイルに保存する（/api/data_quality が読み込む）。
    パス: logs/data_quality_latest.json
    """
    out = {
        "checked_at": datetime.now().isoformat(),
        "target_date": target.isoformat(),
        "has_violation": has_violation,
        "race_log": race_log_results,
        "pred_json": pred_results,
    }
    out_path = _LOG_DIR / "data_quality_latest.json"
    try:
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False, indent=2)
        logger.info("品質チェック結果保存: %s", out_path)
    except Exception as e:
        logger.error("結果保存失敗: %s", e)


def main() -> int:
    """
    戻り値:
        0: 全指標 正常範囲内
        1: 閾値超えあり（タスクスケジューラ失敗扱い）
        2: 致命的エラー
    """
    parser = argparse.ArgumentParser(description="D-AI Keiba データ品質チェック")
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        metavar="YYYY-MM-DD",
        help="対象日付（省略: 今日）",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        metavar="N",
        help="race_log の参照期間（日数、デフォルト: 7）",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="閾値を厳格化（通常の半分）",
    )
    args = parser.parse_args()

    # 対象日付の解決
    if args.date:
        try:
            target = datetime.strptime(args.date, "%Y-%m-%d").date()
        except ValueError:
            logger.error("--date フォーマット不正: %s（YYYY-MM-DD）", args.date)
            return 2
    else:
        target = date.today()

    since = target - timedelta(days=args.days)
    strict_label = " [STRICT]" if args.strict else ""
    logger.info(
        "品質チェック開始 target=%s since=%s%s",
        target, since, strict_label,
    )

    # race_log チェック
    race_log_results, race_log_violation = check_race_log(since, _THRESHOLDS, args.strict)

    # pred.json チェック
    pred_results, pred_violation = check_pred_json(target)

    has_violation = race_log_violation or pred_violation

    # テーブル表示
    print_table(race_log_results, pred_results, since, target)

    # 結果 JSON 保存（/api/data_quality 用）
    save_result_json(race_log_results, pred_results, target, has_violation)

    if has_violation:
        logger.warning(
            "品質チェック完了 [FAIL] 閾値超え検出 target=%s",
            target,
        )
        return 1

    logger.info("品質チェック完了 [OK] target=%s", target)
    return 0


if __name__ == "__main__":
    sys.exit(main())
