"""
スクレイパー HTML 構造変化検知モジュール

netkeiba 等の HTML レイアウト変更を早期検知し、
データ欠損が発生する前にアラートを発行する。

設計方針:
- O(1) チェック（len() 比較のみ）
- 既存スクレイピングロジックは一切破壊しない（追記のみ）
- 1 日の警告件数を集計し、閾値超過で logs/layout_alert.log に記録
"""

import os
import threading
from datetime import datetime
from typing import Any, Dict, List, Optional

from src.log import get_logger

logger = get_logger(__name__)

# ----------------------------------------------------------------
# 設定値
# ----------------------------------------------------------------

# 1 日あたり警告件数の閾値（この件数を超えると layout_alert.log に記録）
ALERT_THRESHOLD = 10

# アラートログ出力先
_LOGS_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "logs"
)
ALERT_LOG_PATH = os.path.join(_LOGS_DIR, "layout_alert.log")

# ----------------------------------------------------------------
# メトリクスストア（インプロセス、スレッドセーフ）
# ----------------------------------------------------------------

_metrics_lock = threading.Lock()

# {parser_name: {"date": "YYYY-MM-DD", "count": int}}
_daily_warnings: Dict[str, Dict[str, Any]] = {}

# 合計警告カウント（/api/health 用）
_total_warnings_today: int = 0
_metrics_date: str = ""


def _get_today_str() -> str:
    """今日の日付文字列 (YYYY-MM-DD) を返す"""
    return datetime.now().strftime("%Y-%m-%d")


def _increment_warning(parser_name: str) -> int:
    """
    指定パーサーの警告カウントをインクリメントし、
    今日の合計警告件数を返す。日付が変わった場合はリセット。
    """
    global _total_warnings_today, _metrics_date
    today = _get_today_str()

    with _metrics_lock:
        # 日付跨ぎリセット
        if _metrics_date != today:
            _daily_warnings.clear()
            _total_warnings_today = 0
            _metrics_date = today

        entry = _daily_warnings.setdefault(parser_name, {"date": today, "count": 0})
        entry["count"] += 1
        _total_warnings_today += 1
        return _total_warnings_today


def get_layout_warning_count() -> int:
    """
    今日の layout_check 警告総件数を返す。
    /api/health の layout_warnings フィールドで使用。
    """
    today = _get_today_str()
    with _metrics_lock:
        if _metrics_date != today:
            return 0
        return _total_warnings_today


def get_layout_warning_details() -> Dict[str, int]:
    """
    パーサー別の今日の警告件数辞書を返す。
    /api/health の詳細フィールドで使用。
    """
    today = _get_today_str()
    with _metrics_lock:
        if _metrics_date != today:
            return {}
        return {name: entry["count"] for name, entry in _daily_warnings.items()}


def _write_alert_log(message: str) -> None:
    """ALERT_LOG_PATH にアラートを追記する（ファイル未作成時は自動生成）"""
    try:
        os.makedirs(_LOGS_DIR, exist_ok=True)
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(ALERT_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(f"[{ts}] {message}\n")
    except Exception:
        logger.debug("[layout-check] アラートログ書き込み失敗", exc_info=True)


# ----------------------------------------------------------------
# メイン公開 API
# ----------------------------------------------------------------


def check_cell_count(
    cells: List[Any],
    expected: int,
    parser_name: str,
    url: str = "?",
) -> None:
    """
    テーブル行のセル数が期待値を下回る場合に警告を発行する。

    Args:
        cells: row.select("td") の結果リスト
        expected: 期待するセル数の最小値
        parser_name: ログ出力用のパーサー識別名（例: "HorseHistoryParser._parse_row"）
        url: デバッグ用 URL（取得元ページ）

    O(1) 処理（len チェックのみ）
    """
    actual = len(cells)
    if actual >= expected:
        return  # 正常

    total = _increment_warning(parser_name)

    logger.warning(
        "[layout-check] HTML 構造変化検知: parser=%s cells=%d (期待 %d) URL=%s",
        parser_name,
        actual,
        expected,
        url,
    )

    # 閾値到達時にアラートログを記録
    if total >= ALERT_THRESHOLD and (total - 1) < ALERT_THRESHOLD:
        # 閾値を初めて超えたときのみ記録（重複書き込み防止）
        msg = (
            f"[ALERT] layout-check 閾値超過: 本日 {total} 件の構造変化警告が発生。"
            f" 直近パーサー={parser_name} cells={actual} (期待 {expected}) URL={url}"
        )
        _write_alert_log(msg)
        logger.error(
            "[layout-check] %s スクレイパーを要確認。詳細: %s",
            msg,
            ALERT_LOG_PATH,
        )
    elif total % 50 == 0:
        # 50 件ごとに追加アラート（長期継続検知）
        msg = (
            f"[ALERT] layout-check 継続警告: 本日 {total} 件到達。"
            f" 直近パーサー={parser_name}"
        )
        _write_alert_log(msg)


def check_required_classes(
    soup_el: Any,
    required_selectors: List[str],
    parser_name: str,
    url: str = "?",
) -> None:
    """
    ページに必須セレクタが存在するか検査する。

    Args:
        soup_el: BeautifulSoup のルート要素
        required_selectors: 期待する CSS セレクタのリスト
        parser_name: ログ出力用の識別名
        url: デバッグ用 URL

    O(len(required_selectors)) — セレクタ数が少なければ O(1) に近い
    """
    missing = [sel for sel in required_selectors if not soup_el.select_one(sel)]
    if not missing:
        return

    total = _increment_warning(parser_name)

    logger.warning(
        "[layout-check] 必須セレクタ欠損: parser=%s missing=%s URL=%s",
        parser_name,
        missing,
        url,
    )

    if total >= ALERT_THRESHOLD and (total - 1) < ALERT_THRESHOLD:
        msg = (
            f"[ALERT] layout-check 閾値超過: 本日 {total} 件。"
            f" パーサー={parser_name} 欠損セレクタ={missing} URL={url}"
        )
        _write_alert_log(msg)
        logger.error("[layout-check] %s", msg)
