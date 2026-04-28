"""
開催カレンダーユーティリティ
src/scraper/kaisai_calendar_util.py

data/masters/kaisai_calendar.json を読み込み、開催日・会場の真値を提供する。
パイプライン側 (run_analysis_date.py 等) からインポートして利用する。

使用例:
    from src.scraper.kaisai_calendar_util import is_open_day, get_open_venues

    # 特定日・会場が開催日かチェック
    if is_open_day("2026-01-04", "中山", "jra"):
        print("中山開催あり")

    # 特定日の開催会場を取得
    venues = get_open_venues("2026-01-04")
    # → {"jra": ["中山", "京都"], "nar": ["佐賀", "名古屋", "川崎", "帯広"]}
"""

import json
import os
import threading
from pathlib import Path
from typing import Dict, List, Literal, Optional

from src.log import get_logger

logger = get_logger(__name__)

# ============================================================
# パス設定
# ============================================================

_MODULE_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT = _MODULE_DIR.parent.parent
_CALENDAR_PATH = _PROJECT_ROOT / "data" / "masters" / "kaisai_calendar.json"

# ============================================================
# キャッシュ（スレッドセーフなシングルトン）
# ============================================================

_calendar_data: Optional[dict] = None
_calendar_lock = threading.Lock()
_calendar_loaded = False


def _load_calendar() -> Optional[dict]:
    """kaisai_calendar.json を読み込む（初回のみ）。"""
    global _calendar_data, _calendar_loaded

    with _calendar_lock:
        if _calendar_loaded:
            return _calendar_data

        if not _CALENDAR_PATH.exists():
            logger.warning(
                "kaisai_calendar.json が見つかりません: %s"
                " (build_kaisai_calendar.py で先にビルドしてください)",
                _CALENDAR_PATH,
            )
            _calendar_loaded = True
            _calendar_data = None
            return None

        try:
            with open(_CALENDAR_PATH, "r", encoding="utf-8") as f:
                _calendar_data = json.load(f)
            logger.info(
                "kaisai_calendar.json ロード完了: %d 日分",
                len(_calendar_data.get("days", {})),
            )
        except Exception as e:
            logger.error("kaisai_calendar.json 読み込み失敗: %s", e)
            _calendar_data = None

        _calendar_loaded = True
        return _calendar_data


def _get_days() -> Dict[str, Dict[str, List[str]]]:
    """days 辞書を返す。ファイル未存在・読み込み失敗時は空 dict。"""
    data = _load_calendar()
    if data is None:
        return {}
    return data.get("days", {})


# ============================================================
# 公開 API
# ============================================================

def is_open_day(date: str, venue: str, kind: Literal["jra", "nar"]) -> bool:
    """
    指定した日付・会場・種別が開催日かどうかを返す。

    Parameters
    ----------
    date : str
        日付文字列 "YYYY-MM-DD"
    venue : str
        会場名 (例: "中山", "川崎")
    kind : "jra" | "nar"
        競馬種別

    Returns
    -------
    bool
        True = 開催あり、False = 開催なし / データ未登録

    Notes
    -----
    kaisai_calendar.json が未存在の場合は常に False を返す。
    フォールバックで推定埋めは行わない (feedback_no_easy_escape 準拠)。
    """
    days = _get_days()
    if not days or date not in days:
        return False
    return venue in days[date].get(kind, [])


def get_open_venues(date: str) -> Dict[str, List[str]]:
    """
    指定した日付の開催会場一覧を返す。

    Parameters
    ----------
    date : str
        日付文字列 "YYYY-MM-DD"

    Returns
    -------
    dict
        {"jra": [...], "nar": [...]}
        データなし / 開催なしの場合は {"jra": [], "nar": []}
    """
    days = _get_days()
    if not days or date not in days:
        return {"jra": [], "nar": []}
    entry = days[date]
    return {
        "jra": list(entry.get("jra", [])),
        "nar": list(entry.get("nar", [])),
    }


def get_open_dates(kind: Literal["jra", "nar", "all"] = "all") -> List[str]:
    """
    カレンダー全期間の開催日一覧を返す。

    Parameters
    ----------
    kind : "jra" | "nar" | "all"
        "all" のとき JRA または NAR のいずれかが開催している日を返す。

    Returns
    -------
    list[str]
        ソート済み日付文字列 ["YYYY-MM-DD", ...]
    """
    days = _get_days()
    if not days:
        return []

    result = []
    for date_str, entry in sorted(days.items()):
        if kind == "jra" and entry.get("jra"):
            result.append(date_str)
        elif kind == "nar" and entry.get("nar"):
            result.append(date_str)
        elif kind == "all" and (entry.get("jra") or entry.get("nar")):
            result.append(date_str)

    return result


def reload_calendar() -> None:
    """
    キャッシュをクリアして kaisai_calendar.json を再ロードする。
    テストや動的更新が必要な場合に使用する。
    """
    global _calendar_data, _calendar_loaded

    with _calendar_lock:
        _calendar_data = None
        _calendar_loaded = False

    _load_calendar()
    logger.info("kaisai_calendar.json を再ロードしました")


def validate_race_against_calendar(
    race_id: str,
    race_date: str,
    venue: str,
    is_jra: bool,
) -> "tuple[bool, str]":
    """race_id ↔ race_date ↔ venue の三者整合をカレンダーで検証する。

    T-033 型バグ (JRA race_id を誤って NAR の元旦に配置) を
    パイプライン実行時に即時検知するための検証関数。

    Parameters
    ----------
    race_id : str
        検証対象の race_id (ログ出力用)
    race_date : str
        日付文字列 "YYYY-MM-DD"
    venue : str
        会場名 (例: "中山", "川崎")
    is_jra : bool
        True = JRA, False = NAR

    Returns
    -------
    tuple[bool, str]
        (ok, reason)
        ok=True なら問題なし。
        ok=False なら reason に不整合の説明文字列。

    Notes
    -----
    - kaisai_calendar.json が未存在の場合は (False, reason) を返す。
    - 推定で日付・venue を補正しない (feedback_no_easy_escape 準拠)。
    - 呼び出し毎の JSON 再読み込みはしない (_load_calendar() でキャッシュ済み)。
    """
    kind: Literal["jra", "nar"] = "jra" if is_jra else "nar"

    # カレンダーデータが存在しない場合はエラーとして扱う
    days = _get_days()
    if not days:
        return (
            False,
            f"kaisai_calendar.json が未ロード: race_id={race_id} の検証不能"
            " (build_kaisai_calendar.py を先に実行してください)",
        )

    if not is_open_day(race_date, venue, kind):
        expected = get_open_venues(race_date).get(kind, [])
        return (
            False,
            f"{race_date} の {kind} 開催 {expected} に {venue} は含まれない"
            f" (race_id={race_id})",
        )

    return (True, "")
