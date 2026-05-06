"""
Slack webhook 通知モジュール。

環境変数 SLACK_WEBHOOK_URL が設定されていれば Slack に通知する。
未設定時は warning ログのみ出力して False を返す (起動阻害禁止)。

Spam 防止: 60 秒以内に同一 level+title の通知は skip する。
"""

import os
import threading
import time
from typing import Dict, Optional

import requests

from src.log import get_logger

logger = get_logger(__name__)

# ================================================================
# 定数
# ================================================================

# level → attachment color マッピング
_LEVEL_COLORS: Dict[str, str] = {
    "info":     "#36a64f",  # 緑
    "warning":  "#ffaa00",  # オレンジ
    "error":    "#ff6b6b",  # 赤
    "critical": "#cc0000",  # 濃赤
}

_VALID_LEVELS = frozenset(_LEVEL_COLORS.keys())

# Spam 防止: 60 秒以内の同一 (level, title) は skip
_SPAM_WINDOW_SEC: int = 60

# ================================================================
# 内部状態 (スレッドセーフ)
# ================================================================

_spam_lock = threading.Lock()
# キー: (level, title_50chars) → 最後に送信した epoch float
_last_sent: Dict[tuple, float] = {}


# ================================================================
# 公開 API
# ================================================================


def is_slack_configured() -> bool:
    """Slack webhook URL が環境変数に設定されているか確認する。

    Returns:
        True: SLACK_WEBHOOK_URL が設定済み
        False: 未設定
    """
    url = os.environ.get("SLACK_WEBHOOK_URL", "").strip()
    return bool(url)


def send_slack_dry_run(
    message: str,
    level: str = "info",
    title: Optional[str] = None,
) -> Dict:
    """send_slack の dry-run 版。実際の HTTP 送信は行わず、
    送信予定のペイロード辞書を返す (設定確認用)。

    Returns:
        dict: webhook_url_configured / level / title / message / color / payload
    """
    _level = level if level in _VALID_LEVELS else "info"
    _title = title or message[:50]
    color = _LEVEL_COLORS[_level]

    payload = {
        "attachments": [
            {
                "color": color,
                "title": _title,
                "text": message,
                "ts": int(time.time()),
            }
        ]
    }
    return {
        "webhook_url_configured": is_slack_configured(),
        "level": _level,
        "title": _title,
        "message": message,
        "color": color,
        "payload": payload,
    }


def send_slack(
    message: str,
    level: str = "info",
    title: Optional[str] = None,
) -> bool:
    """Slack webhook に通知を送る。

    Args:
        message: 本文 (markdown 可)
        level:   "info" / "warning" / "error" / "critical" (色分け用)
        title:   タイトル (省略時は message 先頭 50 文字を使用)

    Returns:
        True:  送信成功
        False: webhook 未設定 または 送信失敗 (spam skip 含む)
    """
    # --- 環境変数チェック ---
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL", "").strip()
    if not webhook_url:
        logger.warning("SLACK_WEBHOOK_URL が未設定のため Slack 通知をスキップします")
        return False

    # --- level バリデーション ---
    if level not in _VALID_LEVELS:
        logger.warning("未知の level '%s' → 'info' にフォールバックします", level)
        level = "info"

    # --- タイトル決定 ---
    _title = title or message[:50]

    # --- Spam 防止チェック (60 秒以内の同一 level+title は skip) ---
    # 注意: spam_key は (level, title) のみで message 内容は含めない。
    # level が escalate (例: error → critical) すれば別 key として扱われるので、
    # 1h cooldown 通知 (error) と 24h cooldown 通知 (critical) は別々に発火する (実害なし)。
    # 同一 level + 同一 title だが message 内容 (count 等) が変わるケースは spam 抑制対象。
    spam_key = (level, _title)
    now = time.time()
    with _spam_lock:
        last = _last_sent.get(spam_key, 0.0)
        if now - last < _SPAM_WINDOW_SEC:
            logger.debug(
                "Slack 通知 spam 防止 skip (残 %.0f 秒): [%s] %s",
                _SPAM_WINDOW_SEC - (now - last),
                level,
                _title,
            )
            return False
        # 送信前に記録 (失敗しても同一内容の大量リトライを防ぐ)
        _last_sent[spam_key] = now

    # --- ペイロード組み立て ---
    color = _LEVEL_COLORS[level]
    payload = {
        "attachments": [
            {
                "color": color,
                "title": _title,
                "text": message,
                "ts": int(now),
            }
        ]
    }

    # --- HTTP 送信 ---
    # requests はファイル先頭で import 済み
    try:
        resp = requests.post(webhook_url, json=payload, timeout=5)
        if resp.status_code == 200:
            logger.info(
                "Slack 通知送信成功: [%s] %s", level, _title
            )
            return True
        else:
            logger.warning(
                "Slack 通知送信失敗: HTTP %d / %s", resp.status_code, resp.text[:200]
            )
            return False
    except Exception as exc:
        logger.warning("Slack 通知例外 (無視して続行): %s", exc)
        return False
