# keiba-v3 comprehensive import and function test
import sys
sys.stdout.reconfigure(encoding="utf-8")
import os, traceback
os.chdir(os.path.dirname(os.path.abspath(__file__)))

passed = 0
failed = 0
errors = []
DASH = "—"

def check(label, condition, detail=""):
    global passed, failed, errors
    if condition:
        print(f"  [OK]   {label}")
        passed += 1
    else:
        msg = f"  [FAIL] {label}"
        if detail:
            msg += f"  -- {detail}"
        print(msg)
        failed += 1
        errors.append(label)
# ============================================================
# 1. Module imports
# ============================================================
print("=" * 60)
print("1. MODULE IMPORTS")
print("=" * 60)

print()
print("-- src.scraper.official_odds --")
try:
    from src.scraper.official_odds import OfficialOddsScraper, _JRA_VENUE_CODES, _JRA_NAME_TO_CODE
    check("import OfficialOddsScraper", True)
    check("import _JRA_VENUE_CODES", isinstance(_JRA_VENUE_CODES, dict) and len(_JRA_VENUE_CODES) > 0,
          f"type={type(_JRA_VENUE_CODES).__name__}, len={len(_JRA_VENUE_CODES)}")
    check("import _JRA_NAME_TO_CODE", isinstance(_JRA_NAME_TO_CODE, dict) and len(_JRA_NAME_TO_CODE) > 0,
          f"type={type(_JRA_NAME_TO_CODE).__name__}, len={len(_JRA_NAME_TO_CODE)}")
except Exception as e:
    check("import src.scraper.official_odds", False, f"{e}")
    traceback.print_exc()
    OfficialOddsScraper = None

print()
print("-- src.scraper.official_nar --")
try:
    from src.scraper.official_nar import OfficialNARScraper, _NETKEIBA_TO_NAR_BABA
    check("import OfficialNARScraper", True)
    check("import _NETKEIBA_TO_NAR_BABA", isinstance(_NETKEIBA_TO_NAR_BABA, dict) and len(_NETKEIBA_TO_NAR_BABA) > 0,
          f"type={type(_NETKEIBA_TO_NAR_BABA).__name__}, len={len(_NETKEIBA_TO_NAR_BABA)}")
except Exception as e:
    check("import src.scraper.official_nar", False, f"{e}")
    traceback.print_exc()
    OfficialNARScraper = None

print()
print("-- src.scraper.auth --")
try:
    from src.scraper.auth import PremiumNetkeibaScraper
    check("import PremiumNetkeibaScraper", True)
except Exception as e:
    check("import src.scraper.auth", False, f"{e}")
    traceback.print_exc()
    PremiumNetkeibaScraper = None

print()
print("-- src.calculator.grades --")
try:
    from src.calculator.grades import (
        compute_jockey_detail_grades,
        compute_trainer_detail_grades,
        compute_bloodline_detail_grades,
        compute_course_detail_grades,
    )
    check("import compute_jockey_detail_grades", True)
    check("import compute_trainer_detail_grades", True)
    check("import compute_bloodline_detail_grades", True)
    check("import compute_course_detail_grades", True)
except Exception as e:
    check("import src.calculator.grades", False, f"{e}")
    traceback.print_exc()
    compute_jockey_detail_grades = None
    compute_trainer_detail_grades = None
    compute_bloodline_detail_grades = None
    compute_course_detail_grades = None

print()
print("-- src.engine --")
try:
    from src.engine import RaceAnalysisEngine
    check("import RaceAnalysisEngine", True)
except Exception as e:
    check("import src.engine", False, f"{e}")
    traceback.print_exc()
    RaceAnalysisEngine = None

print()
print("-- src.models --")
try:
    from src.models import Horse, PastRun, RaceInfo, CourseMaster
    check("import Horse", True)
    check("import PastRun", True)
    check("import RaceInfo", True)
    check("import CourseMaster", True)
except Exception as e:
    check("import src.models", False, f"{e}")
    traceback.print_exc()

try:
    from src.models import (
        JockeyStats, TrainerStats, CourseAptitude,
        PaceDeviation, AbilityDeviation, HorseEvaluation, RaceAnalysis,
    )
    check("import JockeyStats", True)
    check("import TrainerStats", True)
    check("import CourseAptitude", True)
    check("import PaceDeviation", True)
    check("import AbilityDeviation", True)
    check("import HorseEvaluation", True)
    check("import RaceAnalysis (model)", True)
except Exception as e:
    check("import additional models", False, f"{e}")
    traceback.print_exc()

# ============================================================
# 2. Basic function signatures
# ============================================================
print()
print("=" * 60)
print("2. BASIC FUNCTION SIGNATURES")
print("=" * 60)

print()
print("-- OfficialOddsScraper --")
if OfficialOddsScraper is not None:
    try:
        scraper = OfficialOddsScraper()
        check("OfficialOddsScraper() instantiation", scraper is not None)
    except Exception as e:
        check("OfficialOddsScraper() instantiation", False, str(e))
        scraper = None

    if scraper is not None:
        check("fetch_horse_history exists", hasattr(scraper, "fetch_horse_history"))
        check("fetch_horse_history is callable", callable(getattr(scraper, "fetch_horse_history", None)))

        try:
            result = OfficialOddsScraper._parse_time("1:22.6")
            check("_parse_time(1:22.6) == 82.6", abs(result - 82.6) < 0.01,
                  f"got {result}")
        except Exception as e:
            check("_parse_time", False, str(e))

        try:
            result = scraper._margin_to_seconds("クビ")
            check("_margin_to_seconds(kubi) == 0.08", abs(result - 0.08) < 0.001,
                  f"got {result}")
        except Exception as e:
            check("_margin_to_seconds", False, str(e))
else:
    check("OfficialOddsScraper tests (skipped - import failed)", False)

print()
print("-- OfficialNARScraper --")
if OfficialNARScraper is not None:
    try:
        nar_scraper = OfficialNARScraper()
        check("OfficialNARScraper() instantiation", nar_scraper is not None)
    except Exception as e:
        check("OfficialNARScraper() instantiation", False, str(e))
else:
    check("OfficialNARScraper tests (skipped - import failed)", False)

# ============================================================
# 3. Grades functions with mock data
# ============================================================
print()
print("=" * 60)
print("3. GRADES FUNCTIONS (mock data)")
print("=" * 60)

mock_course = CourseMaster(
    venue="東京", venue_code="05", distance=1600, surface="芝",
    direction="左", straight_m=525, corner_count=2,
    corner_type="大回り", first_corner="長い",
    slope_type="急坂",
    inside_outside="なし", is_jra=True,
)

mock_race_info = RaceInfo(
    race_id="202405050811",
    race_date="2024-05-05",
    venue="東京",
    race_no=8,
    race_name="テスト特別",
    grade="3勝",
    condition="芝1600m",
    course=mock_course,
    field_count=16,
)

mock_all_courses = {
    "05_芝_1600": mock_course,
}

from src.models import KaisyuType

mock_jockey_stats = JockeyStats(
    jockey_id="05212",
    jockey_name="C.ルメール",
    upper_long_dev=72.0,
    upper_short_dev=68.0,
    lower_long_dev=65.0,
    lower_short_dev=60.0,
    momentum_upper="好調",
    momentum_lower="",
    course_records={
        "05_芝_1600": {
            "all_dev": 70.0, "upper_dev": 72.0, "lower_dev": 65.0,
            "sample_n": 30, "wins": 10, "runs": 30,
            "place_rate": 0.50,
        },
        "05_芝_2000": {
            "all_dev": 68.0, "upper_dev": 70.0, "lower_dev": 63.0,
            "sample_n": 20, "wins": 6, "runs": 20,
            "place_rate": 0.40,
        },
    },
    condition_records={
        "良": {"wins": 100, "runs": 400},
        "稍重": {"wins": 20, "runs": 80},
    },
)

from src.models import JushaRank

mock_trainer_stats = TrainerStats(
    trainer_id="01234",
    trainer_name="テスト厩舎",
    stable_name="テスト",
    location="美浦",
    deviation=62.0,
    good_venues=["東京", "中山"],
    bad_venues=["小倉"],
    jockey_combo={
        "05212": {"wins": 5, "runs": 10, "recovery": 120.0, "is_main_jockey": True},
    },
)

mock_course_aptitude = CourseAptitude(
    base_score=55.0,
    course_record=3.0,
    venue_aptitude=2.0,
    venue_contrib_level="Trio",
    jockey_course=1.5,
    ai_adjustment=0.0,
)

mock_bloodline_db = {
    "sire": {
        "sire001": {
            "distance": {
                ("sprint", "芝"): {"runs": 50, "place_rate": 0.35},
                ("mile", "芝"): {"runs": 80, "place_rate": 0.40},
                ("middle", "芝"): {"runs": 60, "place_rate": 0.30},
            },
            "course_condition": {
                ("芝", "良"): {"runs": 100, "place_rate": 0.38},
                ("芝", "稍重"): {"runs": 40, "place_rate": 0.28},
            },
        }
    },
    "bms": {
        "mgs001": {
            "distance": {
                ("mile", "芝"): {"runs": 30, "place_rate": 0.33},
            },
            "course_condition": {
                ("芝", "良"): {"runs": 50, "place_rate": 0.32},
            },
        }
    },
}

# ---- 3a. compute_jockey_detail_grades ----
print()
print("-- compute_jockey_detail_grades --")
if compute_jockey_detail_grades is not None:
    try:
        jg = compute_jockey_detail_grades(
            jockey_stats=mock_jockey_stats,
            race_info=mock_race_info,
            all_courses=mock_all_courses,
            horse_popularity=2,
            trainer_stats=mock_trainer_stats,
            running_style="先行",
            gate_no=5,
            field_count=16,
        )
        check("jockey grades returns dict", isinstance(jg, dict))
        all_keys = ["dev", "venue", "similar_venue", "straight", "corner",
                     "surface", "distance", "same_cond", "style", "gate",
                     "stable_synergy"]
        check("jockey grades has all 11 keys",
              all(k in jg for k in all_keys),
              f"keys={list(jg.keys())}")
        filled = sum(1 for v in jg.values() if v != DASH)
        check(f"jockey grades: {filled}/11 fields filled (valid input)",
              filled > 0, f"values={jg}")
    except Exception as e:
        check("jockey grades (valid)", False, str(e))
        traceback.print_exc()

    try:
        jg_none = compute_jockey_detail_grades(
            jockey_stats=None,
            race_info=mock_race_info,
            all_courses=mock_all_courses,
        )
        all_dash_jg = all(v == DASH for v in jg_none.values())
        check("jockey grades: all dashes when jockey_stats=None", all_dash_jg,
              f"values={jg_none}")
    except Exception as e:
        check("jockey grades (None)", False, str(e))
else:
    check("jockey grades (skipped)", False)


# ---- 3b. compute_trainer_detail_grades ----
print()
print("-- compute_trainer_detail_grades --")
if compute_trainer_detail_grades is not None:
    try:
        tg = compute_trainer_detail_grades(
            trainer_stats=mock_trainer_stats,
            race_info=mock_race_info,
            all_courses=mock_all_courses,
            jockey_id="05212",
        )
        check("trainer grades returns dict", isinstance(tg, dict))
        all_keys_t = ["dev", "venue", "similar_venue", "straight", "corner",
                     "surface", "distance", "same_cond", "style", "gate",
                     "jockey_synergy"]
        check("trainer grades has all 11 keys",
              all(k in tg for k in all_keys_t),
              f"keys={list(tg.keys())}")
        filled_t = sum(1 for v in tg.values() if v != DASH)
        check(f"trainer grades: {filled_t}/11 fields filled (valid input)",
              filled_t > 0, f"values={tg}")
    except Exception as e:
        check("trainer grades (valid)", False, str(e))
        traceback.print_exc()

    try:
        tg_none = compute_trainer_detail_grades(
            trainer_stats=None,
            race_info=mock_race_info,
            all_courses=mock_all_courses,
        )
        all_dash_tg = all(v == DASH for v in tg_none.values())
        check("trainer grades: all dashes when trainer_stats=None", all_dash_tg,
              f"values={tg_none}")
    except Exception as e:
        check("trainer grades (None)", False, str(e))
else:
    check("trainer grades (skipped)", False)

# ---- 3c. compute_bloodline_detail_grades ----
print()
print("-- compute_bloodline_detail_grades --")
if compute_bloodline_detail_grades is not None:
    try:
        bg = compute_bloodline_detail_grades(
            bloodline_db=mock_bloodline_db,
            sire_id="sire001",
            mgs_id="mgs001",
            race_info=mock_race_info,
            all_courses=mock_all_courses,
            jockey_dev=70.0,
        )
        check("bloodline grades returns dict", isinstance(bg, dict))
        all_keys_b = ["sire_dev", "mgs_dev", "venue", "similar_venue",
                     "straight", "corner", "surface", "distance",
                     "same_cond", "style", "gate", "jockey_synergy"]
        check("bloodline grades has all 12 keys",
              all(k in bg for k in all_keys_b),
              f"keys={list(bg.keys())}")
        filled_b = sum(1 for v in bg.values() if v != DASH)
        check(f"bloodline grades: {filled_b}/12 fields filled (valid input)",
              filled_b > 0, f"values={bg}")
    except Exception as e:
        check("bloodline grades (valid)", False, str(e))
        traceback.print_exc()

    try:
        bg_none = compute_bloodline_detail_grades(
            bloodline_db=None,
            sire_id=None,
            mgs_id=None,
            race_info=mock_race_info,
        )
        all_dash_bg = all(v == DASH for v in bg_none.values())
        check("bloodline grades: all dashes when bloodline_db=None", all_dash_bg,
              f"values={bg_none}")
    except Exception as e:
        check("bloodline grades (None)", False, str(e))
else:
    check("bloodline grades (skipped)", False)


# ---- 3d. compute_course_detail_grades ----
print()
print("-- compute_course_detail_grades --")
if compute_course_detail_grades is not None:
    try:
        cg = compute_course_detail_grades(
            course_aptitude=mock_course_aptitude,
            race_info=mock_race_info,
            all_courses=mock_all_courses,
            past_runs=[],
        )
        check("course grades returns dict", isinstance(cg, dict))
        all_keys_c = ["venue", "similar_venue", "straight", "corner",
                     "surface", "distance", "same_cond"]
        check("course grades has all 7 keys",
              all(k in cg for k in all_keys_c),
              f"keys={list(cg.keys())}")
        filled_c = sum(1 for v in cg.values() if v != DASH)
        check(f"course grades: {filled_c}/7 fields filled (valid input)",
              filled_c > 0, f"values={cg}")
    except Exception as e:
        check("course grades (valid)", False, str(e))
        traceback.print_exc()

    try:
        cg_none = compute_course_detail_grades(
            course_aptitude=None,
            race_info=mock_race_info,
            all_courses=mock_all_courses,
        )
        all_dash_cg = all(v == DASH for v in cg_none.values())
        check("course grades: all dashes when course_aptitude=None", all_dash_cg,
              f"values={cg_none}")
    except Exception as e:
        check("course grades (None)", False, str(e))
else:
    check("course grades (skipped)", False)

# ============================================================
# 4. main.py --official flag
# ============================================================
print()
print("=" * 60)
print("4. main.py --official FLAG")
print("=" * 60)

try:
    with open("main.py", "r", encoding="utf-8") as f:
        main_source = f.read()
    has_official = "--official" in main_source
    check("main.py contains --official flag", has_official)

    has_add_arg = "add_argument" in main_source and "--official" in main_source
    check("main.py has add_argument for --official", has_add_arg)

    mlines = main_source.split(chr(10))
    official_lines = [l for l in mlines if "--official" in l]
    has_store_true = any("store_true" in l for l in official_lines)
    check("--official uses action=store_true", has_store_true,
          f"lines: {official_lines}")
except Exception as e:
    check("main.py --official flag check", False, str(e))


# ============================================================
# SUMMARY
# ============================================================
print()
print("=" * 60)
total = passed + failed
print(f"RESULTS: {passed}/{total} passed, {failed} failed")
print("=" * 60)

if errors:
    print()
    print("Failed tests:")
    for err in errors:
        print(f"  - {err}")

sys.exit(0 if failed == 0 else 1)