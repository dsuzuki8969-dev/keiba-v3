"""T-033 Phase 1: date_from_race_id() 単体テスト"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.backfill_ml_from_cache import date_from_race_id


class TestDateFromRaceId:
    def test_nar_kawasaki_newyear(self):
        # NAR 川崎 2026-01-01 1R: 202645010101
        assert date_from_race_id("202645010101") == "2026-01-01"

    def test_nar_takachi(self):
        # NAR 高知 2026-01-01 12R: 202654010112
        assert date_from_race_id("202654010112") == "2026-01-01"

    def test_jra_tokyo_returns_empty(self):
        # JRA 第1回東京第1日1R: 202605010101 (実際は 2026-01-03 土) → 日付不可
        assert date_from_race_id("202605010101") == ""

    def test_jra_nakayama_returns_empty(self):
        # JRA 第1回中山第1日1R: 202606010101
        assert date_from_race_id("202606010101") == ""

    def test_jra_all_venues_return_empty(self):
        # JRA venue_code 01-10 すべて "" を返す
        for vc in ["01","02","03","04","05","06","07","08","09","10"]:
            rid = f"2026{vc}010101"
            assert date_from_race_id(rid) == "", f"venue_code={vc} で空文字を返すべき"

    def test_invalid_short_race_id(self):
        assert date_from_race_id("123") == ""

    def test_invalid_non_digit(self):
        assert date_from_race_id("abc645010101") == ""

    def test_nar_obihiro(self):
        # 帯広 venue_code=65 (NAR) 2026-04-27 5R: 202665042705
        assert date_from_race_id("202665042705") == "2026-04-27"

    def test_jra_10digit_returns_empty(self):
        # 10桁の旧形式 JRA race_id → NAR ロジックに流入しないこと
        assert date_from_race_id("2026050101") == ""

    def test_jra_11digit_returns_empty(self):
        # 11桁の旧形式 JRA race_id → NAR ロジックに流入しないこと
        assert date_from_race_id("20260501011") == ""

    def test_nar_invalid_month_returns_empty(self):
        # 月が 30 (不正) → ValueError を捕捉して "" を返す
        assert date_from_race_id("202630009905") == ""

    def test_nar_invalid_day_returns_empty(self):
        # 日が 32 (不正) → ValueError を捕捉して "" を返す
        assert date_from_race_id("202630013299") == ""


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main([__file__, "-v"]))
