"""T-038 Phase 4: validate_race_against_calendar() 単体テスト

kaisai_calendar_util.validate_race_against_calendar() の動作を検証する。
- 正常系: 開催日・会場が一致するケース → (True, "")
- 異常系: 開催外日付・異種別 venue → (False, reason)
- カレンダー未ロード時: (False, reason)
"""
import os
import sys
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import src.scraper.kaisai_calendar_util as cal_util
from src.scraper.kaisai_calendar_util import validate_race_against_calendar


# ============================================================
# テスト用ダミーカレンダーデータ
# ============================================================
_DUMMY_CALENDAR = {
    "days": {
        "2026-01-04": {
            "jra": ["中山", "京都"],
            "nar": ["佐賀", "名古屋", "川崎"],
        },
        "2026-01-01": {
            "jra": [],
            "nar": ["帯広", "川崎", "高知"],
        },
    }
}


def _patch_calendar(monkeypatch):
    """テスト用にカレンダーキャッシュを差し替えるヘルパー"""
    monkeypatch.setattr(cal_util, "_calendar_data", _DUMMY_CALENDAR)
    monkeypatch.setattr(cal_util, "_calendar_loaded", True)


# ============================================================
# 正常系テスト
# ============================================================

class TestValidateRaceAgainstCalendarOk:
    """カレンダーと整合する場合は (True, "") を返す"""

    def test_jra_nakayama_on_open_day(self, monkeypatch):
        """JRA 中山 2026-01-04 は開催日 → True"""
        _patch_calendar(monkeypatch)
        ok, reason = validate_race_against_calendar(
            "202606010101", "2026-01-04", "中山", True
        )
        assert ok is True
        assert reason == ""

    def test_jra_kyoto_on_open_day(self, monkeypatch):
        """JRA 京都 2026-01-04 は開催日 → True"""
        _patch_calendar(monkeypatch)
        ok, reason = validate_race_against_calendar(
            "202608010101", "2026-01-04", "京都", True
        )
        assert ok is True
        assert reason == ""

    def test_nar_kawasaki_on_newyear(self, monkeypatch):
        """NAR 川崎 2026-01-01 は開催日 → True"""
        _patch_calendar(monkeypatch)
        ok, reason = validate_race_against_calendar(
            "202645010101", "2026-01-01", "川崎", False
        )
        assert ok is True
        assert reason == ""

    def test_nar_obihiro_on_newyear(self, monkeypatch):
        """NAR 帯広 2026-01-01 は開催日 → True"""
        _patch_calendar(monkeypatch)
        ok, reason = validate_race_against_calendar(
            "202665010101", "2026-01-01", "帯広", False
        )
        assert ok is True
        assert reason == ""


# ============================================================
# 異常系テスト (T-033型バグ検知)
# ============================================================

class TestValidateRaceAgainstCalendarNg:
    """カレンダーと不整合な場合は (False, reason) を返す"""

    def test_jra_race_id_on_newyear_jra_closed(self, monkeypatch):
        """JRA race_id を 2026-01-01 (JRA非開催) に配置 → False (T-033型バグ)"""
        _patch_calendar(monkeypatch)
        ok, reason = validate_race_against_calendar(
            "202606010101", "2026-01-01", "中山", True
        )
        assert ok is False
        assert "中山" in reason or "jra" in reason
        assert "race_id=202606010101" in reason

    def test_jra_venue_not_in_open_list(self, monkeypatch):
        """JRA 東京 2026-01-04 は開催なし (中山・京都のみ) → False"""
        _patch_calendar(monkeypatch)
        ok, reason = validate_race_against_calendar(
            "202605010101", "2026-01-04", "東京", True
        )
        assert ok is False
        assert "東京" in reason

    def test_nar_venue_not_in_open_list(self, monkeypatch):
        """NAR 大井 2026-01-04 は開催なし → False"""
        _patch_calendar(monkeypatch)
        ok, reason = validate_race_against_calendar(
            "202644010101", "2026-01-04", "大井", False
        )
        assert ok is False
        assert "大井" in reason

    def test_nar_venue_on_jra_kind(self, monkeypatch):
        """NAR 川崎 を JRA として検証 → False (種別ミスマッチ)"""
        _patch_calendar(monkeypatch)
        # 川崎は 2026-01-04 の JRA リストに存在しない
        ok, reason = validate_race_against_calendar(
            "202645010101", "2026-01-04", "川崎", True  # is_jra=True (誤った種別)
        )
        assert ok is False

    def test_unknown_date(self, monkeypatch):
        """カレンダーに存在しない日付 → False"""
        _patch_calendar(monkeypatch)
        ok, reason = validate_race_against_calendar(
            "202606010101", "2099-12-31", "中山", True
        )
        assert ok is False


# ============================================================
# カレンダー未ロード時
# ============================================================

class TestValidateRaceCalendarMissing:
    """kaisai_calendar.json が未ロード (days={}) の場合は (False, reason) を返す"""

    def test_calendar_not_loaded(self, monkeypatch):
        """カレンダーデータが None (未ロード) → False"""
        monkeypatch.setattr(cal_util, "_calendar_data", None)
        monkeypatch.setattr(cal_util, "_calendar_loaded", True)
        ok, reason = validate_race_against_calendar(
            "202606010101", "2026-01-04", "中山", True
        )
        assert ok is False
        assert "未ロード" in reason or "kaisai_calendar" in reason

    def test_calendar_empty_days(self, monkeypatch):
        """days が空 dict → False"""
        monkeypatch.setattr(cal_util, "_calendar_data", {"days": {}})
        monkeypatch.setattr(cal_util, "_calendar_loaded", True)
        ok, reason = validate_race_against_calendar(
            "202606010101", "2026-01-04", "中山", True
        )
        assert ok is False
