#!/usr/bin/env python
"""
発走 15 分前 オッズ＋馬体重 自動取得スクリプト
=================================================
当日の data/predictions/YYYYMMDD_pred.json を読み込み、
発走 12〜16 分前のレースに対して /api/race_odds を同期 POST する。

使い方:
    python scripts/auto_fetch_odds_15min.py             # 通常実行
    python scripts/auto_fetch_odds_15min.py --dry-run   # API 呼び出しを skip してログのみ
    python scripts/auto_fetch_odds_15min.py --window-min 10 --window-max 17

スケジューラから 5 分間隔で呼び出される想定。
/api/race_odds 側に 5 分クールダウンがあるため二重取得は自然に防止される。
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, date
from logging.handlers import RotatingFileHandler
from pathlib import Path
from zoneinfo import ZoneInfo

# ───────────────────────────────────────────────────────────
# パス設定
# ───────────────────────────────────────────────────────────
# このスクリプトは scripts/ 以下に置かれ、プロジェクトルートを基準に動作する
_SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = _SCRIPT_DIR.parent

# ログファイルは プロジェクトルート/logs/ に保存
_LOG_DIR = PROJECT_ROOT / "logs"
_LOG_DIR.mkdir(exist_ok=True)
_LOG_FILE = _LOG_DIR / "auto_fetch_odds_15min.log"

# タイムゾーン（JST 固定）
_JST = ZoneInfo("Asia/Tokyo")

# ダッシュボード URL
_DASHBOARD_URL = "http://127.0.0.1:5051/api/race_odds"

# requests は外部依存。インポート失敗時はエラーを出して終了
try:
    import requests as _requests
except ImportError:
    print("[ERROR] requests がインストールされていません。pip install requests を実行してください。")
    sys.exit(1)


# ───────────────────────────────────────────────────────────
# ロガー設定（ファイル＋標準出力の両方に出力）
# ───────────────────────────────────────────────────────────
def _setup_logger() -> logging.Logger:
    """ロガーを設定して返す（ファイル追記＋標準出力）"""
    _fmt = logging.Formatter(
        "[%(asctime)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    _logger = logging.getLogger("auto_fetch_odds")
    _logger.setLevel(logging.DEBUG)

    # 重複ハンドラ防止
    if _logger.handlers:
        return _logger

    # ファイルハンドラ（ローテーション: 5MB × 5世代）
    fh = RotatingFileHandler(str(_LOG_FILE), maxBytes=5 * 1024 * 1024, backupCount=5, encoding="utf-8")
    fh.setFormatter(_fmt)
    _logger.addHandler(fh)

    # 標準出力ハンドラ
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(_fmt)
    _logger.addHandler(sh)

    return _logger


logger = _setup_logger()


# ───────────────────────────────────────────────────────────
# pred.json 読み込み
# ───────────────────────────────────────────────────────────
def _load_pred_json(today: date) -> dict | None:
    """当日の pred.json を読み込んで返す。存在しない場合は None"""
    date_str = today.strftime("%Y%m%d")
    pred_path = PROJECT_ROOT / "data" / "predictions" / f"{date_str}_pred.json"
    if not pred_path.exists():
        logger.warning("pred.json が存在しません: %s", pred_path)
        return None
    try:
        with open(pred_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.error("pred.json の読み込み失敗: %s → %s", pred_path, e)
        return None


# ───────────────────────────────────────────────────────────
# スクラッチ・確定済み判定
# ───────────────────────────────────────────────────────────
def _is_scratched(race: dict) -> bool:
    """
    レースがスクラッチ扱いか判定する。
    bet_decision.skip_reasons に 'scratched' が含まれる場合を検出。
    直接の 'scratched' キーがある場合も対応。
    """
    # 直接 scratched フラグ
    if race.get("scratched"):
        return True
    # bet_decision.skip_reasons で判定
    bd = race.get("bet_decision") or {}
    skip_reasons = bd.get("skip_reasons") or []
    return "scratched" in skip_reasons


def _is_finished(race: dict) -> bool:
    """
    レースが確定済みか判定する。
    results / result_order / payouts 等のキーがある場合に確定済みとみなす。
    """
    if race.get("results"):
        return True
    if race.get("result_order"):
        return True
    if race.get("payouts"):
        return True
    # bet_decision.skip_reasons に 'finished' や 'result_confirmed' がある場合
    bd = race.get("bet_decision") or {}
    skip_reasons = bd.get("skip_reasons") or []
    return any(r in skip_reasons for r in ("finished", "result_confirmed", "completed"))


# ───────────────────────────────────────────────────────────
# 発走時刻パース
# ───────────────────────────────────────────────────────────
def _parse_post_time(race: dict, today: date) -> datetime | None:
    """
    race の post_time（HH:MM 形式）を当日の datetime に変換して返す。
    フォーマット不正または未設定の場合は None。
    """
    raw = race.get("post_time") or race.get("start_time") or ""
    if not raw:
        return None
    try:
        t = datetime.strptime(raw.strip(), "%H:%M")
        # JST 固定で datetime を生成（システムロケール依存を排除）
        return datetime(today.year, today.month, today.day, t.hour, t.minute, 0, tzinfo=_JST)
    except ValueError:
        logger.debug("post_time のパース失敗: '%s'", raw)
        return None


# ───────────────────────────────────────────────────────────
# API 呼び出し
# ───────────────────────────────────────────────────────────
def _post_race_odds(race_id: str, date_str: str, dry_run: bool) -> str:
    """
    /api/race_odds に同期 POST する。
    戻り値: "ok" / "skip_dryrun" / "error:<msg>"
    """
    if dry_run:
        return "skip_dryrun"

    payload = {
        "race_id": race_id,
        "date": date_str,   # YYYY-MM-DD 形式 (pred.json 更新に必要)
        "sync": True,        # 同期モード（fire-and-forget ではなく確実に更新）
    }
    try:
        resp = _requests.post(
            _DASHBOARD_URL,
            json=payload,
            timeout=30,
        )
        if resp.status_code == 200:
            body = resp.json()
            if body.get("ok"):
                return "ok"
            # cooldown でスキップされた場合
            if body.get("skipped") == "cooldown":
                remaining = body.get("remaining", 0)
                return f"cooldown(残{remaining}s)"
            return f"api_ng:{body.get('error','?')}"
        return f"http_{resp.status_code}"
    except _requests.Timeout:
        return "error:timeout"
    except _requests.ConnectionError:
        return "error:connection_refused(ダッシュボード未起動?)"
    except json.JSONDecodeError:
        return "error:json_decode"
    except Exception as e:
        return f"error:{e}"


# ───────────────────────────────────────────────────────────
# メイン処理
# ───────────────────────────────────────────────────────────
def main() -> int:
    """
    0: 正常完了（対象レースなし含む）
    1: 致命的エラー（pred.json 読み込み失敗等）
    """
    parser = argparse.ArgumentParser(
        description="発走 15 分前 オッズ＋馬体重 自動取得",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="API 呼び出しを skip してログのみ出力",
    )
    parser.add_argument(
        "--window-min",
        type=int,
        default=12,
        metavar="N",
        help="発走 N 分前からウィンドウ開始（デフォルト: 12）",
    )
    parser.add_argument(
        "--window-max",
        type=int,
        default=16,
        metavar="N",
        help="発走 N 分前までウィンドウ終了（デフォルト: 16）",
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        metavar="YYYYMMDD",
        help="対象日付を指定（省略時は今日）。テスト用",
    )
    args = parser.parse_args()

    # ── ウィンドウバリデーション ──
    if args.window_min >= args.window_max:
        logger.error(
            "--window-min (%d) は --window-max (%d) より小さくなければなりません",
            args.window_min, args.window_max,
        )
        return 1

    # ── 対象日付 ──
    if args.date:
        try:
            target_date = datetime.strptime(args.date, "%Y%m%d").date()
        except ValueError:
            logger.error("--date のフォーマット不正: %s（YYYYMMDD で指定してください）", args.date)
            return 1
    else:
        target_date = date.today()

    date_str = target_date.strftime("%Y-%m-%d")
    # JST 固定（システムロケール依存を排除）
    now = datetime.now(_JST)

    mode_label = "[DRY-RUN]" if args.dry_run else "[LIVE]"
    logger.info(
        "%s 起動 date=%s now=%s window=%d〜%d 分前",
        mode_label, date_str, now.strftime("%H:%M:%S"),
        args.window_min, args.window_max,
    )

    # ── pred.json 読み込み ──
    # ※ pred.json は起動時に 1 回だけ読み込むスナップショット。
    #   ループ中にレース確定状態が変わっても次回起動まで反映されない（5 分間隔で再判定される）。
    pred = _load_pred_json(target_date)
    if pred is None:
        logger.warning("pred.json 不在のためスキップ（正常終了）")
        return 0  # 非レース日は通常起こる。exit 0 で問題なし

    races = pred.get("races", [])
    logger.info("総レース数: %d", len(races))

    # ── レースを走査 ──
    target_count = 0
    sent_count = 0

    for race in races:
        race_id = race.get("race_id", "")
        post_dt = _parse_post_time(race, target_date)

        # post_time 不明はスキップ
        if post_dt is None:
            logger.debug("race_id=%s post_time 不明 → skip", race_id)
            continue

        delta_min = (post_dt - now).total_seconds() / 60

        # スクラッチ済みはスキップ
        if _is_scratched(race):
            logger.debug(
                "race_id=%s start=%s delta=%.1f min → skip(scratched)",
                race_id, race.get("post_time"), delta_min,
            )
            continue

        # 既に確定済みはスキップ
        if _is_finished(race):
            logger.debug(
                "race_id=%s start=%s delta=%.1f min → skip(finished)",
                race_id, race.get("post_time"), delta_min,
            )
            continue

        # ウィンドウ外はスキップ
        if not (args.window_min <= delta_min <= args.window_max):
            logger.debug(
                "race_id=%s start=%s delta=+%.1f min → skip(window外)",
                race_id, race.get("post_time"), delta_min,
            )
            continue

        # ── 取得対象 ──
        target_count += 1
        status = _post_race_odds(race_id, date_str, args.dry_run)

        logger.info(
            "race_id=%s start=%s delta=+%.1f min status=%s",
            race_id,
            race.get("post_time", "?"),
            delta_min,
            status,
        )

        if status in {"ok", "skip_dryrun"}:
            sent_count += 1

    logger.info(
        "%s 完了 対象=%d件 送信=%d件",
        mode_label, target_count, sent_count,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
