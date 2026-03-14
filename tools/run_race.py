"""
D-AI 競馬解析 汎用ランナー
使い方:
  python run_race.py 202605010811
  python run_race.py           ← インタラクティブ入力
"""
import sys, io, os, time
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# ─── レースID取得 ─────────────────────────────────────────────
if len(sys.argv) >= 2:
    RACE_ID = sys.argv[1].strip()
else:
    print("=" * 60)
    print("  D-AI 競馬解析システム v3")
    print("=" * 60)
    print()
    print("  レースIDを入力してください（例: 202605010811）")
    print("  ※ netkeiba の URL から取得: race.netkeiba.com/race/result.html?race_id=XXXXXXXXXXXX")
    print()
    RACE_ID = input("  RACE_ID > ").strip()
    if not RACE_ID:
        print("[ERROR] レースIDが入力されていません")
        input("\nEnterキーで終了...")
        sys.exit(1)

# 出力先ファイル名を自動生成
year  = RACE_ID[:4]
month = RACE_ID[4:6]
day   = RACE_ID[6:8]
OUTPUT = f"output/{year}{month}{day}_{RACE_ID}.html"

print("=" * 60)
print(f"  D-AI 競馬解析システム v3")
print(f"  race_id: {RACE_ID}")
print("=" * 60)
t0 = time.time()

# ─── 1. 基盤準備 ─────────────────────────────────────────────
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

# ─── 2. 基準タイムDB ─────────────────────────────────────────
print("\n[2/5] 基準タイムDB読み込み...")
std_db   = StandardTimeDBBuilder()
course_db = std_db.get_course_db()
preload  = load_preload_course_db(COURSE_DB_PRELOAD_PATH)
for cid, runs in preload.items():
    course_db.setdefault(cid, []).extend(runs)
n_pre = sum(len(v) for v in preload.values())
print(f"  事前DB: {len(preload)}コース / {n_pre:,}走")

course_style_db     = build_course_style_stats_db(course_db)
gate_bias_db        = build_gate_bias_db(course_db)
position_sec_db     = build_position_sec_per_rank_db(course_db)
l3f_db              = Last3FDBBuilder().build(course_db)
trainer_baseline_db = load_trainer_baseline_db(TRAINER_BASELINE_DB_PATH)
print(f"  経過: {time.time()-t0:.1f}s")

# ─── 3. レース・馬データ取得 ─────────────────────────────────
print(f"\n[3/5] データ取得... (race_id={RACE_ID})")
race_info, horses = scraper.fetch_race(
    RACE_ID,
    fetch_history=True,
    fetch_odds=True,
    fetch_training=True,
)
if not race_info or not horses:
    print("[ERROR] データ取得失敗。race_idを確認してください")
    input("\nEnterキーで終了...")
    sys.exit(1)

print(f"\n  ┌─ レース情報 ────────────────────────────────┐")
print(f"  │ {race_info.race_name}")
print(f"  │ {race_info.venue} {race_info.race_no}R  {race_info.race_date}")
print(f"  │ {race_info.course.surface}{race_info.course.distance}m  {race_info.condition}")
print(f"  │ 出走: {len(horses)}頭  発走: {race_info.post_time}")
print(f"  └──────────────────────────────────────────────┘")

# 出力ファイル名をレース名入りに更新
OUTPUT = f"output/{year}{month}{day}_{race_info.venue}{race_info.race_no}R.html"

for h in sorted(horses, key=lambda x: x.horse_no):
    odds_s = f"{h.odds:.1f}倍({h.popularity}人)" if h.odds else "未確定"
    print(f"  [{h.horse_no:2d}] {h.horse_name:<14} {h.sex}{h.age}  騎手:{h.jockey or '未定':<8}  {odds_s}")

print(f"\n  経過: {time.time()-t0:.1f}s")

# ─── 4. 分析実行 ─────────────────────────────────────────────
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
bloodline_db = build_bloodline_db(horses, netkeiba_client=None, cache_path=BLOODLINE_DB_PATH)

analysis = engine.analyze(race_info, horses, custom_stake=None, netkeiba_client=scraper.client)
analysis = enrich_course_aptitude_with_style_bias(engine, analysis)
print(f"  経過: {time.time()-t0:.1f}s")

# ─── 5. HTML出力 ─────────────────────────────────────────────
print("\n[5/5] HTML出力...")

print(f"\n  ── 予想結果 ──")
print(f"  ペース: {analysis.pace_type_predicted.value if analysis.pace_type_predicted else '不明'}"
      f"  信頼度: {analysis.pace_reliability.value}")
print(f"  {'印':2s} {'馬名':<14} {'総合':>5}  {'勝率':>6} {'連対率':>7} {'複勝率':>7}  オッズ")
print(f"  {'-'*65}")
for ev in sorted(analysis.evaluations, key=lambda e: e.composite, reverse=True):
    h = ev.horse
    odds_s = f"{h.odds:.1f}倍" if h.odds else "未確定"
    print(f"  {ev.mark.value:2s} {h.horse_name:<14} "
          f"{ev.composite:5.1f}  "
          f"{ev.win_prob*100:5.1f}% {ev.place2_prob*100:6.1f}% {ev.place3_prob*100:6.1f}%  {odds_s}")

os.makedirs("output", exist_ok=True)
formatter = HTMLFormatter()
html = formatter.render(analysis)
with open(OUTPUT, "w", encoding="utf-8") as f:
    f.write(html)
size_kb = os.path.getsize(OUTPUT) // 1024
abs_path = os.path.abspath(OUTPUT)
print(f"\n  ✅ HTML出力完了: {abs_path}")
print(f"  ファイルサイズ: {size_kb}KB")
print(f"  総実行時間: {time.time()-t0:.1f}秒")
print("\n  ブラウザで開いています...")

import subprocess
from pathlib import Path
_url = Path(OUTPUT).resolve().as_uri()
for chrome_path in [
    r"C:\Program Files\Google\Chrome\Application\chrome.exe",
    r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
]:
    if os.path.exists(chrome_path):
        subprocess.Popen([chrome_path, _url])
        break
else:
    import webbrowser
    webbrowser.open(_url)

input("\nEnterキーで終了...")
