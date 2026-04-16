#!/usr/bin/env python3
"""
高速バッチ予想再生成スクリプト
モデル・DB・キャッシュを1回だけロードし、複数日分の予想を高速再生成する。
run_analysis_date.py を日付ごとにサブプロセス起動する方式と異なり、
初期化コスト（2-3分）を1回に抑える。

使い方:
  python scripts/batch_regenerate_fast.py
  python scripts/batch_regenerate_fast.py --start 20240101 --end 20240412
"""
import sys, io, os, time, gc, json, glob, shutil, argparse, datetime
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True, errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.log import get_logger
logger = get_logger(__name__)

# === 引数パース ===
parser = argparse.ArgumentParser()
parser.add_argument('--start', default='20240101')
parser.add_argument('--end', default='20240412')
parser.add_argument('--workers', type=int, default=3)
args = parser.parse_args()

START_DATE = args.start
END_DATE = args.end
WORKERS = args.workers
PRED_DIR = "data/predictions"
LOG_FILE = "scripts/batch_regen_fast.log"

print(f"{'='*70}")
print(f"  Fast Batch Regenerate  {START_DATE} -> {END_DATE}")
print(f"{'='*70}")

# === 対象日付リスト ===
def get_targets():
    targets = []
    for f in sorted(glob.glob(os.path.join(PRED_DIR, "*_pred.json"))):
        dt = os.path.basename(f)[:8]
        if not (START_DATE <= dt <= END_DATE):
            continue
        # _prev.json から元データ読み取り（既にバックアップ済みの場合）
        prev = os.path.join(PRED_DIR, f"{dt}_pred_prev.json")
        src = prev if os.path.exists(prev) else f
        try:
            d = json.load(open(src, 'r', encoding='utf-8'))
            races = d.get('races', [])
            if len(races) > 0:
                race_ids = [r['race_id'] for r in races if r.get('race_id')]
                venues = list(set(r.get('venue', '') for r in races))
                targets.append((dt, race_ids, venues, len(races)))
        except Exception:
            pass
    return targets

def is_already_done(dt):
    """既に再生成済みか確認"""
    prev = os.path.join(PRED_DIR, f"{dt}_pred_prev.json")
    cur = os.path.join(PRED_DIR, f"{dt}_pred.json")
    if os.path.exists(prev) and os.path.exists(cur):
        if os.path.getmtime(cur) > os.path.getmtime(prev):
            return True
    return False

def backup_pred(dt):
    """バックアップ（既存ならスキップ）"""
    src = os.path.join(PRED_DIR, f"{dt}_pred.json")
    dst = os.path.join(PRED_DIR, f"{dt}_pred_prev.json")
    if not os.path.exists(dst) and os.path.exists(src):
        shutil.copy2(src, dst)

targets = get_targets()
# 既に完了済みをフィルタ
pending = [(dt, rids, vs, nr) for dt, rids, vs, nr in targets if not is_already_done(dt)]
skipped = len(targets) - len(pending)

total_days = len(pending)
total_races = sum(t[3] for t in pending)
print(f"  Total targets: {len(targets)} days")
print(f"  Already done (skip): {skipped} days")
print(f"  Pending: {total_days} days, {total_races} races")

if total_days == 0:
    print("  Nothing to do!")
    sys.exit(0)

# === 1回限りの初期化 ===
print(f"\n[INIT] Loading models and databases (one-time)...")
t0 = time.time()

from data.masters.course_master import get_all_courses
from src.scraper.auth import PremiumNetkeibaScraper
from src.scraper.race_results import (
    StandardTimeDBBuilder, Last3FDBBuilder,
    build_course_db_from_past_runs,
    build_course_style_stats_db, build_gate_bias_db,
    build_position_sec_per_rank_db,
    build_trainer_baseline_db, load_trainer_baseline_db,
    save_trainer_baseline_db, merge_trainer_baseline,
)
from src.scraper.course_db_collector import load_preload_course_db
from src.scraper.personnel import PersonnelDBManager, enrich_personnel_with_condition_records
from src.engine import RaceAnalysisEngine, reset_engine_caches, enrich_course_aptitude_with_style_bias
from config.settings import COURSE_DB_PRELOAD_PATH, TRAINER_BASELINE_DB_PATH, BLOODLINE_DB_PATH
from src.scraper.improvement_dbs import build_bloodline_db
from src.results_tracker import save_prediction
from config.settings import get_composite_weights

all_courses = get_all_courses()

# スクレイパー初期化（キャッシュ読み出し用）
scraper = PremiumNetkeibaScraper(all_courses, ignore_ttl=True)
scraper.login()
kb_ok = scraper.training.login()
if not kb_ok:
    time.sleep(3)
    kb_ok = scraper.training.login()
print(f"  Scraper login: {'OK' if kb_ok else 'WARN (no keiba-book)'}")

# エンジンキャッシュリセット
reset_engine_caches()

# キャリブレーション事前初期化
get_composite_weights()

# MLモデルウォームアップ（初回ロード）
print(f"  ML model warmup...")
_dummy_engine = RaceAnalysisEngine(
    course_db={}, all_courses=all_courses,
    jockey_db={}, trainer_db={},
    trainer_baseline_db={},
    pace_last3f_db={},
    course_style_stats_db={},
    gate_bias_db={},
    position_sec_per_rank_db={},
    is_jra=True, target_date="2024-01-01",
)
del _dummy_engine
gc.collect()

init_time = time.time() - t0
print(f"  Init complete: {init_time:.0f}s")

# === ログファイル ===
log_f = open(LOG_FILE, 'a', encoding='utf-8')
log_f.write(f"\n{'='*60}\n")
log_f.write(f"batch_regen_fast start: {datetime.datetime.now()}\n")
log_f.write(f"range: {START_DATE}-{END_DATE}, pending: {total_days} days\n\n")

# === メインループ ===
completed = 0
failed_count = 0
total_elapsed = 0

for idx, (dt, race_ids, venues, nr) in enumerate(pending):
    # プログレスバー
    pct = idx / total_days * 100
    elapsed_str = f"{total_elapsed/60:.0f}min"
    if completed > 0:
        avg = total_elapsed / completed
        remaining = avg * (total_days - idx)
        eta_str = f"ETA={remaining/60:.0f}min"
    else:
        eta_str = "..."

    bar_len = 30
    filled = int(bar_len * idx / total_days)
    bar = "#" * filled + "." * (bar_len - filled)
    print(f"  [{bar}] {pct:>5.1f}% ({idx}/{total_days}) {dt} {nr}R  {elapsed_str} {eta_str}", flush=True)

    # バックアップ
    backup_pred(dt)

    date_str = f"{dt[:4]}-{dt[4:6]}-{dt[6:8]}"
    day_t0 = time.time()

    try:
        # 日付ごとのDB構築（軽量）
        std_db = StandardTimeDBBuilder()
        course_db_base = std_db.get_course_db()

        # SQLite course_db追加
        from src.database import get_course_db as _get_sqlite_course_db
        from src.scraper.course_db_collector import _dict_to_past_run
        from datetime import datetime as _dt_cls, timedelta as _td
        _window_start = (_dt_cls.strptime(date_str, "%Y-%m-%d") - _td(days=365)).strftime("%Y-%m-%d")
        _window_end = (_dt_cls.strptime(date_str, "%Y-%m-%d") - _td(days=1)).strftime("%Y-%m-%d")
        _sqlite_db = _get_sqlite_course_db()
        for _cid, _recs in _sqlite_db.items():
            for _r in _recs:
                _rd = _r.get("race_date", "")
                if _rd and _window_start <= _rd <= _window_end:
                    course_db_base.setdefault(_cid, []).append(_dict_to_past_run(_r))

        preload = load_preload_course_db(COURSE_DB_PRELOAD_PATH, target_date=date_str)
        for cid, runs in preload.items():
            course_db_base.setdefault(cid, []).extend(runs)

        course_style_db = build_course_style_stats_db(course_db_base, target_date=date_str)
        gate_bias_db = build_gate_bias_db(course_db_base, target_date=date_str)
        position_sec_db = build_position_sec_per_rank_db(course_db_base, target_date=date_str)
        l3f_db_base = Last3FDBBuilder().build(course_db_base)
        trainer_baseline_db = load_trainer_baseline_db(TRAINER_BASELINE_DB_PATH)

        # プリフェッチ（キャッシュ優先）
        prefetched = {}
        worker_pool = [scraper] + [scraper.clone_worker() for _ in range(min(WORKERS, len(race_ids)) - 1)]

        from concurrent.futures import ThreadPoolExecutor, as_completed

        def _prefetch(fetch_args):
            i2, rid = fetch_args
            w = worker_pool[i2 % len(worker_pool)]
            try:
                ri, hs = w.fetch_race(rid, fetch_history=True, fetch_odds=False,
                                       fetch_training=True, target_date=date_str, prefer_cache=True)
                return rid, ri, hs
            except Exception as e:
                logger.debug("prefetch %s: %s", rid, e)
                return rid, None, []

        pf_workers = max(1, min(WORKERS, len(race_ids)))
        with ThreadPoolExecutor(max_workers=pf_workers) as pool:
            futs = {pool.submit(_prefetch, (i2, rid)): rid for i2, rid in enumerate(race_ids)}
            for fut in as_completed(futs):
                rid, ri, hs = fut.result()
                prefetched[rid] = (ri, hs)

        # 事前構築
        all_horses = []
        for _ri, _hs in prefetched.values():
            if _hs:
                all_horses.extend(_hs)

        if all_horses:
            _baseline = build_trainer_baseline_db(all_horses)
            trainer_baseline_db = merge_trainer_baseline(_baseline, trainer_baseline_db)

            shared_course_db = build_course_db_from_past_runs(all_horses, dict(course_db_base), target_date=date_str)
            shared_l3f_db = Last3FDBBuilder().build(shared_course_db)

            pm = PersonnelDBManager()
            pm.purge_mismatched_nar_trainers()
            all_jockey_db, all_trainer_db = pm.build_from_horses(
                all_horses, scraper.client, course_db=shared_course_db, save=False  # save=False: 高速化
            )

            _bloodline_db = build_bloodline_db(all_horses, netkeiba_client=scraper.client, cache_path=BLOODLINE_DB_PATH)
        else:
            shared_course_db = dict(course_db_base)
            shared_l3f_db = l3f_db_base
            all_jockey_db, all_trainer_db = {}, {}

        # 分析
        results = []
        analyses_by_venue = {}

        def _analyze(rid):
            ri, horses = prefetched.get(rid, (None, []))
            if not ri or not horses:
                return rid, False, None
            try:
                _jids = {h.jockey_id for h in horses if h.jockey_id}
                _tids = {h.trainer_id for h in horses if h.trainer_id}
                jdb = {k: v for k, v in all_jockey_db.items() if k in _jids}
                tdb = {k: v for k, v in all_trainer_db.items() if k in _tids}
                enrich_personnel_with_condition_records(jdb, tdb, shared_course_db)

                engine = RaceAnalysisEngine(
                    course_db=shared_course_db, all_courses=all_courses,
                    jockey_db=jdb, trainer_db=tdb,
                    trainer_baseline_db=trainer_baseline_db,
                    pace_last3f_db=shared_l3f_db,
                    course_style_stats_db=course_style_db,
                    gate_bias_db=gate_bias_db,
                    position_sec_per_rank_db=position_sec_db,
                    is_jra=ri.is_jra,
                    target_date=date_str,
                )
                analysis = engine.analyze(ri, horses, custom_stake=None, netkeiba_client=None)
                analysis = enrich_course_aptitude_with_style_bias(engine, analysis)
                del engine
                return rid, True, (ri, analysis)
            except Exception as e:
                logger.warning("analyze %s: %s", rid, e, exc_info=True)
                return rid, False, None

        analysis_workers = max(1, min(WORKERS, len(race_ids)))
        with ThreadPoolExecutor(max_workers=analysis_workers) as pool:
            futs = {pool.submit(_analyze, rid): rid for rid in race_ids}
            for fut in as_completed(futs):
                rid, ok, result = fut.result()
                if ok and result:
                    ri, analysis = result
                    venue = ri.venue
                    rno = ri.race_no
                    analyses_by_venue.setdefault(venue, {})[rno] = analysis
                    results.append((rid, True))
                else:
                    results.append((rid, False))

        # pred.json保存
        ok_count = sum(1 for r in results if r[1])
        if analyses_by_venue:
            save_prediction(date_str, analyses_by_venue)

        elapsed = time.time() - day_t0
        total_elapsed += elapsed
        completed += 1
        log_f.write(f"OK  {dt} {ok_count}/{nr}R {elapsed:.0f}s\n")
        log_f.flush()

        # メモリ解放
        del prefetched, all_horses, shared_course_db, shared_l3f_db
        del all_jockey_db, all_trainer_db, analyses_by_venue, results
        gc.collect()

    except Exception as e:
        elapsed = time.time() - day_t0
        total_elapsed += elapsed
        failed_count += 1
        log_f.write(f"ERR {dt} {elapsed:.0f}s {str(e)[:200]}\n")
        log_f.flush()
        print(f"  !! {dt} ERROR: {str(e)[:100]}")

# === 完了 ===
bar = "#" * bar_len
print(f"  [{bar}] 100.0% ({total_days}/{total_days}) DONE! {total_elapsed/60:.0f}min")
print(f"\n  OK: {completed}  FAIL: {failed_count}  SKIP: {skipped}")
print(f"  Time: {total_elapsed/3600:.1f}h ({total_elapsed/60:.0f}min)")
if completed > 0:
    print(f"  Avg: {total_elapsed/completed:.0f}s/day")

log_f.write(f"\nDone: {completed}, Failed: {failed_count}, Skipped: {skipped}\n")
log_f.write(f"Time: {total_elapsed:.0f}s ({total_elapsed/3600:.1f}h)\n")
log_f.write(f"Finish: {datetime.datetime.now()}\n")
log_f.close()

print(f"\n  Log: {LOG_FILE}")
