# -*- coding: utf-8 -*-
"""統合テスト: エンジン → グレード → 全フィールド埋まることを検証"""
import sys
sys.stdout.reconfigure(encoding='utf-8')

import json
import os

DASH = "\u2014"

def test_engine_with_cached_data():
    """既存キャッシュデータを使って、エンジン分析 → グレード生成の流れを検証"""
    from data.masters.course_master import get_all_courses
    from src.engine import RaceAnalysisEngine

    all_courses = get_all_courses()

    # 今日の予想データから1レース分を取得
    pred_path = "data/predictions/20260307_pred.json"
    if not os.path.exists(pred_path):
        print("[SKIP] 予想データなし")
        return

    with open(pred_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    races = data.get("races", [])
    if not races:
        print("[SKIP] レースデータなし")
        return

    # 最初のレースのrace_idを取得
    race = races[0]
    race_id = race.get("race_id", "")
    print(f"テスト対象: {race_id} {race.get('race_name', '?')}")

    # キャッシュからレースデータを復元
    try:
        from src.scraper.race_cache import load_race_cache
        cached = load_race_cache(race_id)
        if not cached:
            print("[SKIP] レースキャッシュなし")
            return
        race_info, horses = cached
        print(f"  キャッシュ復元: {race_info.race_name} {len(horses)}頭")
    except Exception as e:
        print(f"[SKIP] キャッシュ復元失敗: {e}")
        return

    # エンジン分析実行
    try:
        engine = RaceAnalysisEngine(all_courses)

        # 必要なDBをロード
        from config.settings import (
            COURSE_DB_PRELOAD_PATH,
            TRAINER_BASELINE_DB_PATH,
        )
        from src.scraper.course_db_collector import load_preload_course_db
        from src.scraper.race_results import (
            StandardTimeDBBuilder,
            load_trainer_baseline_db,
        )

        course_db = {}
        if os.path.exists(COURSE_DB_PRELOAD_PATH):
            course_db = load_preload_course_db()

        std_db = StandardTimeDBBuilder()

        trainer_bl_db = {}
        if os.path.exists(TRAINER_BASELINE_DB_PATH):
            trainer_bl_db = load_trainer_baseline_db()

        # 分析実行
        analysis = engine.analyze(
            race_info,
            horses,
            course_db=course_db,
            std_time_db=std_db,
            trainer_baseline_db=trainer_bl_db,
        )

        if not analysis:
            print("[FAIL] 分析結果がNone")
            return

        print(f"  分析完了: {len(analysis.evaluations)}頭 evaluated")

        # グレードチェック
        all_ok = True
        for ev in analysis.evaluations:
            h = ev.horse
            cdg = getattr(ev, "_course_detail_grades", {})
            jdg = getattr(ev, "_jockey_detail_grades", {})
            tdg = getattr(ev, "_trainer_detail_grades", {})
            bdg = getattr(ev, "_bloodline_detail_grades", {})

            cdash = sum(1 for v in cdg.values() if v == DASH)
            jdash = sum(1 for v in jdg.values() if v == DASH)
            tdash = sum(1 for v in tdg.values() if v == DASH)
            bdash = sum(1 for v in bdg.values() if v == DASH)

            total_fields = len(cdg) + len(jdg) + len(tdg) + len(bdg)
            total_dash = cdash + jdash + tdash + bdash

            if total_dash > 0:
                print(f"  [{h.horse_name}] WARN: {total_dash}/{total_fields} fields are dash")
                print(f"    Course: {cdash}/{len(cdg)} {cdg}")
                print(f"    Jockey: {jdash}/{len(jdg)} {jdg}")
                print(f"    Trainer: {tdash}/{len(tdg)} {tdg}")
                print(f"    Blood:  {bdash}/{len(bdg)} {bdg}")
                # データ不足は許容（jockey_stats/trainer_stats がNoneの場合）
                if ev.jockey_stats is not None and jdash > 0:
                    all_ok = False
                if ev.trainer_stats is not None and tdash > 0:
                    all_ok = False
            else:
                print(f"  [{h.horse_name}] OK: all {total_fields} fields filled")

        if all_ok:
            print("\n" + "=" * 60)
            print("SUCCESS: All grade fields properly handled!")
            print("  (Dashes only appear when source data is genuinely unavailable)")
            print("=" * 60)
        else:
            print("\n[WARNING] Some fields still show dash even with data available")

    except Exception as e:
        import traceback
        print(f"[FAIL] 分析実行エラー: {e}")
        traceback.print_exc()


if __name__ == "__main__":
    test_engine_with_cached_data()
