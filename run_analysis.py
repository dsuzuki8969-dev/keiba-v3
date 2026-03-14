"""
汎用予想実行スクリプト。
使い方:
  python run_analysis.py <race_id> [output_path]
  例: python run_analysis.py 202605010811
"""
import sys, io, os, time
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# ─── 引数処理 ────────────────────────────────────────────────────
if len(sys.argv) < 2:
    print("[ERROR] race_id を指定してください")
    print("  例: python run_analysis.py 202605010811")
    sys.exit(1)

RACE_ID = sys.argv[1].strip()

# 出力パス（省略時は output/<race_id>_<venue><R>.html）
OUTPUT = sys.argv[2] if len(sys.argv) >= 3 else None  # 後でレース情報取得後に決定

print("=" * 60)
print(f"  D-AI 競馬予想  race_id: {RACE_ID}")
print("=" * 60)
t0 = time.time()

# ─── 1. 基盤準備 ─────────────────────────────────────────────────
print("\n[1/5] 初期化...")
from data.masters.course_master import get_all_courses
from data.masters.venue_master import VENUE_MAP, JRA_CODES
from src.scraper.auth import PremiumNetkeibaScraper
from src.scraper.race_results import (
    StandardTimeDBBuilder, Last3FDBBuilder,
    build_course_db_from_past_runs,
    build_course_style_stats_db, build_gate_bias_db,
    build_position_sec_per_rank_db,
    build_trainer_baseline_db, load_trainer_baseline_db,
    merge_trainer_baseline,
)
from src.scraper.course_db_collector import load_preload_course_db
from src.scraper.personnel import PersonnelDBManager, enrich_personnel_with_condition_records
from src.engine import RaceAnalysisEngine, enrich_course_aptitude_with_style_bias
from src.output.formatter import HTMLFormatter
from config.settings import COURSE_DB_PRELOAD_PATH, TRAINER_BASELINE_DB_PATH

all_courses = get_all_courses()
scraper = PremiumNetkeibaScraper(all_courses)
scraper.login()
scraper.training.login()
print(f"  経過: {time.time()-t0:.1f}s")

# ─── 2. 基準タイムDB ─────────────────────────────────────────────
print("\n[2/5] 基準タイムDB読み込み...")
std_db = StandardTimeDBBuilder()
course_db = std_db.get_course_db()
preload = load_preload_course_db(COURSE_DB_PRELOAD_PATH)
for cid, runs in preload.items():
    course_db.setdefault(cid, []).extend(runs)
n_pre = sum(len(v) for v in preload.values())
print(f"  事前DB: {len(preload)}コース / {n_pre:,}走")

course_style_db    = build_course_style_stats_db(course_db)
gate_bias_db       = build_gate_bias_db(course_db)
position_sec_db    = build_position_sec_per_rank_db(course_db)
l3f_db             = Last3FDBBuilder().build(course_db)
trainer_baseline_db = load_trainer_baseline_db(TRAINER_BASELINE_DB_PATH)
print(f"  経過: {time.time()-t0:.1f}s")

# ─── 3. レース・馬データ取得 ─────────────────────────────────────
print(f"\n[3/5] データ取得中... ({RACE_ID})")
race_info, horses = scraper.fetch_race(
    RACE_ID,
    fetch_history=True,
    fetch_odds=True,
    fetch_training=True,
)
if not race_info or not horses:
    print("[ERROR] データ取得失敗")
    sys.exit(1)

# 出力パス決定
if not OUTPUT:
    date_s  = race_info.race_date.replace("-", "")
    venue_s = race_info.venue
    race_s  = f"{race_info.race_no}R"
    OUTPUT  = f"output/{date_s}_{venue_s}{race_s}.html"

print(f"\n  ┌─ レース情報 ──────────────────────────────────┐")
print(f"  │ {race_info.race_name}")
print(f"  │ {race_info.venue} {race_info.race_no}R  {race_info.race_date}")
print(f"  │ {race_info.course.surface}{race_info.course.distance}m  {race_info.condition}")
print(f"  │ 芝馬場: {race_info.track_condition_turf or '-'}  ダート: {race_info.track_condition_dirt or '-'}")
print(f"  │ 出走: {len(horses)}頭  発走: {race_info.post_time}")
print(f"  └────────────────────────────────────────────────┘")

for h in sorted(horses, key=lambda x: x.horse_no):
    odds_s = f"{h.odds:.1f}倍({h.popularity}人)" if h.odds else "オッズ未確定"
    wt_s   = f"{h.horse_weight}kg({h.weight_change:+d})" if h.horse_weight else "-"
    print(f"  [{h.horse_no:2d}] {h.horse_name:<14} {h.sex}{h.age} {h.weight_kg:.0f}kg "
          f"騎手:{h.jockey or '未定':<8} 過去走:{len(h.past_runs)}走  {wt_s}  {odds_s}")

print(f"\n  経過: {time.time()-t0:.1f}s")

# ─── 4. 分析実行 ─────────────────────────────────────────────────
print("\n[4/5] D-AI分析実行...")
course_db = build_course_db_from_past_runs(horses, course_db)
l3f_db    = Last3FDBBuilder().build(course_db)

personnel_mgr = PersonnelDBManager()
jockey_db, trainer_db = personnel_mgr.build_from_horses(horses, scraper.client, course_db=course_db)
enrich_personnel_with_condition_records(jockey_db, trainer_db, course_db)

baseline_new = build_trainer_baseline_db(horses)
trainer_baseline_db = merge_trainer_baseline(baseline_new, trainer_baseline_db)

engine = RaceAnalysisEngine(
    course_db=course_db,
    all_courses=all_courses,
    jockey_db=jockey_db,
    trainer_db=trainer_db,
    trainer_baseline_db=trainer_baseline_db,
    pace_last3f_db=l3f_db,
    course_style_stats_db=course_style_db,
    gate_bias_db=gate_bias_db,
    position_sec_per_rank_db=position_sec_db,
    is_jra=race_info.is_jra,
)
from src.scraper.improvement_dbs import build_bloodline_db
from config.settings import BLOODLINE_DB_PATH
bloodline_db = build_bloodline_db(horses, netkeiba_client=scraper.client, cache_path=BLOODLINE_DB_PATH)

analysis = engine.analyze(race_info, horses, custom_stake=None, netkeiba_client=scraper.client)
analysis = enrich_course_aptitude_with_style_bias(engine, analysis)
print(f"  経過: {time.time()-t0:.1f}s")

# ─── 5. 出力 ─────────────────────────────────────────────────────
print("\n[5/5] HTML出力...")

os.makedirs(os.path.dirname(OUTPUT) or "output", exist_ok=True)
formatter = HTMLFormatter()
html = formatter.render(analysis)
with open(OUTPUT, "w", encoding="utf-8") as f:
    f.write(html)

size_kb = os.path.getsize(OUTPUT) // 1024
abs_out = os.path.abspath(OUTPUT)
print(f"\n  完了: {abs_out}  ({size_kb}KB)")
print(f"  総実行時間: {time.time()-t0:.1f}秒")

# ダッシュボード更新
try:
    import scripts.generate_portfolio as _gp
    _gp.main()
except Exception as _e:
    print(f"  ダッシュボード更新スキップ: {_e}")

# 出力パスを最終行に print（app.py が取得するため）
print(f"OUTPUT_FILE:{abs_out}")
