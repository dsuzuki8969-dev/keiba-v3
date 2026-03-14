"""
2026-02-22 東京11R 予想実行・検証スクリプト
"""
import sys, io, os, time
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)
sys.path.insert(0, '.')

RACE_ID   = "202605010811"
OUTPUT    = "output/20260222_東京11R.html"

print("=" * 60)
print("  2026-02-22 東京11R 予想検証")
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
scraper.login()           # ログイン（失敗でも一般会員として続行）
scraper.training.login()  # 調教データ（プレミアム必要）
print(f"  経過: {time.time()-t0:.1f}s")

# ─── 2. 基準タイムDB ─────────────────────────────────────────
print("\n[2/5] 基準タイムDB読み込み...")
std_db = StandardTimeDBBuilder()
course_db = std_db.get_course_db()
preload = load_preload_course_db(COURSE_DB_PRELOAD_PATH)
for cid, runs in preload.items():
    course_db.setdefault(cid, []).extend(runs)
n_pre = sum(len(v) for v in preload.values())
print(f"  事前DB: {len(preload)}コース / {n_pre:,}走")

# 静的DB（ループ外）
course_style_db   = build_course_style_stats_db(course_db)
gate_bias_db      = build_gate_bias_db(course_db)
position_sec_db   = build_position_sec_per_rank_db(course_db)
l3f_db            = Last3FDBBuilder().build(course_db)
trainer_baseline_db = load_trainer_baseline_db(TRAINER_BASELINE_DB_PATH)
print(f"  経過: {time.time()-t0:.1f}s")

# ─── 3. レース・馬データ取得 ─────────────────────────────────
print("\n[3/5] 東京11Rデータ取得...")
race_info, horses = scraper.fetch_race(
    RACE_ID,
    fetch_history=True,
    fetch_odds=True,
    fetch_training=True,
)
if not race_info or not horses:
    print("[ERROR] データ取得失敗")
    sys.exit(1)

print(f"\n  ┌─ レース情報 ────────────────────────────────┐")
print(f"  │ {race_info.race_name}")
print(f"  │ {race_info.venue} {race_info.race_no}R  {race_info.race_date}")
print(f"  │ {race_info.course.surface}{race_info.course.distance}m  {race_info.condition}")
print(f"  │ 芝馬場: {race_info.track_condition_turf or '-'}  ダート馬場: {race_info.track_condition_dirt or '-'}")
print(f"  │ 出走: {len(horses)}頭  発走: {race_info.post_time}")
print(f"  └──────────────────────────────────────────────┘")

print(f"\n  ── 出走馬一覧 ──")
for h in sorted(horses, key=lambda x: x.horse_no):
    odds_s = f"{h.odds:.1f}倍({h.popularity}人)" if h.odds else "オッズ未確定"
    wt_s   = f"{h.horse_weight}kg({h.weight_change:+d})" if h.horse_weight else "-"
    n_runs = len(h.past_runs)
    jockey = h.jockey or "未定"
    print(f"  [{h.horse_no:2d}] {h.horse_name:<14} {h.sex}{h.age} {h.weight_kg:.0f}kg  "
          f"騎手:{jockey:<8} 過去走:{n_runs}走  馬体重:{wt_s}  {odds_s}")

print(f"\n  経過: {time.time()-t0:.1f}s")

# ─── 4. 分析実行 ─────────────────────────────────────────────
print("\n[4/5] D-AI分析実行...")
course_db = build_course_db_from_past_runs(horses, course_db)
l3f_db    = Last3FDBBuilder().build(course_db)

personnel_mgr = PersonnelDBManager()
client = scraper.client
jockey_db, trainer_db = personnel_mgr.build_from_horses(horses, client, course_db=course_db)
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
from src.scraper.improvement_dbs import build_bloodline_db, build_pace_stats_db
from config.settings import BLOODLINE_DB_PATH
# キャッシュ優先（未キャッシュIDはフォールバック使用でネットアクセスなし）
bloodline_db = build_bloodline_db(horses, netkeiba_client=None, cache_path=BLOODLINE_DB_PATH)

analysis = engine.analyze(race_info, horses, custom_stake=None, netkeiba_client=client)
analysis = enrich_course_aptitude_with_style_bias(engine, analysis)
print(f"  経過: {time.time()-t0:.1f}s")

# ─── 5. 結果検証 ─────────────────────────────────────────────
print("\n[5/5] 結果検証 & HTML出力...")

print(f"\n  ── 予想結果 ──────────────────────────────────")
print(f"  ペース: {analysis.pace_type_predicted.value if analysis.pace_type_predicted else '不明'}"
      f"  信頼度: {analysis.pace_reliability.value}  自信度: {analysis.overall_confidence.value}")
print(f"  逃げ: {analysis.leading_horses}  先行: {analysis.front_horses}"
      f"  中団: {analysis.mid_horses}  後方: {analysis.rear_horses}")
if analysis.estimated_front_3f:
    print(f"  前半3F: {analysis.estimated_front_3f:.1f}秒  後半3F: {analysis.estimated_last_3f:.1f}秒")
print()
print(f"  {'印':2s} {'馬名':<14} {'総合':>5}  {'能力':>5} {'展開':>5} {'コース':>6}  {'勝率':>6} {'連対率':>7} {'複勝率':>7}  オッズ")
print(f"  {'-'*80}")
for ev in sorted(analysis.evaluations, key=lambda e: e.composite, reverse=True):
    h = ev.horse
    odds_s = f"{h.odds:.1f}倍" if h.odds else "未確定"
    print(f"  {ev.mark.value:2s} {h.horse_name:<14} "
          f"{ev.composite:5.1f}  {ev.ability.total:5.1f} {ev.pace.total:5.1f} {ev.course.total:6.1f}  "
          f"{ev.win_prob*100:5.1f}% {ev.place2_prob*100:6.1f}% {ev.place3_prob*100:6.1f}%  {odds_s}")

print()
if analysis.tickets:
    print(f"  ── 買い目 ───────────────────────────────────")
    for t in analysis.tickets:
        stake = t.get("stake", 0)
        signal = "◎買" if stake > 0 else "見送"
        ev_pct = t.get("ev", 0)
        print(f"  {signal}  {t['type']} {t['a']}-{t['b']}  EV{ev_pct:.0f}%  {stake:,}円")
else:
    print("  買い目: 見送り（期待値基準未達）")

# 検証チェック
errors = []
if len(analysis.evaluations) == 0:
    errors.append("評価馬0頭")
if analysis.pace_type_predicted is None:
    errors.append("ペース予測なし")
from src.models import Mark
marks_assigned = [ev.mark for ev in analysis.evaluations if ev.mark != Mark.NONE]
if len(marks_assigned) == 0:
    errors.append("印が1つも付いていない")
has_composite = all(40 <= ev.composite <= 65 for ev in analysis.evaluations)
if not has_composite:
    out_range = [(ev.horse.horse_name, ev.composite) for ev in analysis.evaluations
                 if not (40 <= ev.composite <= 65)]
    errors.append(f"総合偏差値が範囲外: {out_range}")

if errors:
    print(f"\n  [WARN] 検証警告:")
    for e in errors:
        print(f"    - {e}")
else:
    print(f"\n  ✅ 検証OK: {len(analysis.evaluations)}頭全て正常範囲")

# HTML出力
os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)
formatter = HTMLFormatter()
html = formatter.render(analysis)
with open(OUTPUT, "w", encoding="utf-8") as f:
    f.write(html)
size_kb = os.path.getsize(OUTPUT) // 1024
print(f"\n  HTML出力完了: {os.path.abspath(OUTPUT)}")
print(f"  ファイルサイズ: {size_kb}KB")
print(f"  総実行時間: {time.time()-t0:.1f}秒")
print("\n  ブラウザで開いています...")
import subprocess
from pathlib import Path
_url = Path(OUTPUT).resolve().as_uri()

# ダッシュボード（index.html）を自動更新
try:
    import scripts.generate_portfolio as _gp
    _gp.main()
    print("  ダッシュボード更新完了")
except Exception as _e:
    print(f"  ダッシュボード更新スキップ: {_e}")

# Chrome で直接開く（MSN等のデフォルトブラウザを回避）
_chrome_paths = [
    r"C:\Program Files\Google\Chrome\Application\chrome.exe",
    r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
]
_opened = False
for _cp in _chrome_paths:
    try:
        subprocess.Popen([_cp, _url])
        _opened = True
        break
    except FileNotFoundError:
        continue
if not _opened:
    import webbrowser
    webbrowser.open(_url)
