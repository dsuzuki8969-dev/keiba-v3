"""新モデルで2026Q1を一括再予測（初期化1回・インプロセス）

run_analysis_date.py のロジックをインプロセスで再利用。
scraper/DB/MLモデルの初期化は1回だけ行い、82日分をループ。
キャッシュ済みデータのみ使用 → netkeibaへの新規アクセスなし。
"""
import sys, io, os, time, gc, json, sqlite3
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.log import get_logger
logger = get_logger(__name__)

try:
    from rich.console import Console
    console = Console()
    P = console.print
except ImportError:
    P = print

DB_PATH = "data/keiba.db"

# ====================================================================
# 0. 再予測対象日を取得
# ====================================================================
_db = sqlite3.connect(DB_PATH)
dates = [r[0] for r in _db.execute('''
    SELECT DISTINCT p.date
    FROM predictions_old p
    WHERE p.date BETWEEN '2026-01-01' AND '2026-03-31'
      AND EXISTS (SELECT 1 FROM race_log rl WHERE rl.race_date = p.date)
    ORDER BY p.date
''').fetchall()]
_db.close()

P(f"\n[bold white on #0d2b5e]  新モデル一括再予測  {len(dates)}日 ({dates[0]}～{dates[-1]})  [/]\n")
t0 = time.time()

# ====================================================================
# 1. 初期化（1回だけ）
# ====================================================================
P("[bold cyan]\\[1/4][/] 初期化（スクレイパー・DB・MLモデル）...")

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

all_courses = get_all_courses()
scraper = PremiumNetkeibaScraper(all_courses, ignore_ttl=True)
scraper.login()
scraper.training.login()
reset_engine_caches()
P(f"  スクレイパー初期化: {time.time()-t0:.1f}秒")

# MLモデルウォームアップ（1回だけ）
P("  MLモデルウォームアップ...")
_warmup = RaceAnalysisEngine(
    course_db={}, all_courses=all_courses,
    jockey_db={}, trainer_db={},
    trainer_baseline_db={},
    pace_last3f_db={},
    course_style_stats_db={},
    gate_bias_db={},
    position_sec_per_rank_db={},
    is_jra=True, target_date=dates[-1],
)
del _warmup

# ランカーモデルのキャッシュを強制リロード（再学習後のモデルを確実に使用）
import src.engine as _eng
_eng._CACHE_RANKER_LOADED = False
_eng._CACHE_LGBM_RANKER = None
from src.ml.lgbm_ranker import LGBMRanker as _LR
_r = _LR()
if _r.load():
    _eng._CACHE_LGBM_RANKER = _r
    _eng._CACHE_RANKER_LOADED = True
    P(f"  ランカーモデル強制リロード完了 (features={_r._model.num_feature()})")
else:
    P("[red]  ランカーモデルロード失敗[/]")

P(f"  初期化完了: {time.time()-t0:.1f}秒")

# ====================================================================
# 2. 日単位ループ
# ====================================================================
P(f"\n[bold cyan]\\[2/4][/] {len(dates)}日分の再予測開始...")
success = 0
failed_dates = []
durations = []

for di, DATE in enumerate(dates):
    day_start = time.time()
    DATE_KEY = DATE.replace("-", "")
    elapsed = time.time() - t0

    if durations:
        avg = sum(durations) / len(durations)
        eta = avg * (len(dates) - di)
        eta_str = f"{eta/60:.0f}分" if eta < 3600 else f"{eta/3600:.1f}時間"
    else:
        eta_str = "計算中"

    pct = di / len(dates) * 100
    bar_len = 30
    filled = int(bar_len * di / len(dates))
    bar = "█" * filled + "░" * (bar_len - filled)

    P(f"\n[bold]{DATE}[/] [{di+1}/{len(dates)}] |{bar}| {pct:.0f}%  経過{elapsed/60:.1f}分  残り{eta_str}")

    try:
        # 2a. DB構築（日付依存部分のみ）
        reset_engine_caches()

        # 基準タイムDB
        std_db = StandardTimeDBBuilder()
        course_db_base = std_db.get_course_db()

        from src.database import get_course_db as _get_sqlite_course_db
        from src.scraper.course_db_collector import _dict_to_past_run
        from datetime import datetime as _dt, timedelta as _td
        _window_start = (_dt.strptime(DATE, "%Y-%m-%d") - _td(days=365)).strftime("%Y-%m-%d")
        _window_end = (_dt.strptime(DATE, "%Y-%m-%d") - _td(days=1)).strftime("%Y-%m-%d")
        _sqlite_db = _get_sqlite_course_db()
        for _cid, _recs in _sqlite_db.items():
            for _r in _recs:
                _rd = _r.get("race_date", "")
                if _rd and _window_start <= _rd <= _window_end:
                    course_db_base.setdefault(_cid, []).append(_dict_to_past_run(_r))

        preload = load_preload_course_db(COURSE_DB_PRELOAD_PATH, target_date=DATE)
        for cid, runs in preload.items():
            course_db_base.setdefault(cid, []).extend(runs)

        course_style_db = build_course_style_stats_db(course_db_base, target_date=DATE)
        gate_bias_db = build_gate_bias_db(course_db_base, target_date=DATE)
        position_sec_db = build_position_sec_per_rank_db(course_db_base, target_date=DATE)
        l3f_db_base = Last3FDBBuilder().build(course_db_base)
        trainer_baseline_db = load_trainer_baseline_db(TRAINER_BASELINE_DB_PATH)

        # 2b. レースID取得（race_logから・ネットアクセスなし）
        _conn = sqlite3.connect(DB_PATH)
        race_ids = [r[0] for r in _conn.execute(
            "SELECT DISTINCT race_id FROM race_log WHERE race_date = ? ORDER BY race_id",
            (DATE,)).fetchall()]
        _conn.close()

        if not race_ids:
            P(f"  [yellow]レースなし → スキップ[/]")
            continue

        # 2c. プリフェッチ（キャッシュ済みデータのみ）
        prefetched = {}
        for race_id in race_ids:
            try:
                ri, hs = scraper.fetch_race(
                    race_id, fetch_history=True, fetch_odds=True,
                    fetch_training=True, target_date=DATE, prefer_cache=True
                )
                prefetched[race_id] = (ri, hs)
            except Exception as e:
                logger.debug("fetch %s: %s", race_id, e)
                prefetched[race_id] = (None, [])

        # 2d. 事前構築
        all_horses = []
        for _ri, _hs in prefetched.values():
            if _hs:
                all_horses.extend(_hs)

        if all_horses:
            _bl = build_trainer_baseline_db(all_horses)
            trainer_baseline_db = merge_trainer_baseline(_bl, trainer_baseline_db)

            shared_course_db = build_course_db_from_past_runs(
                all_horses, dict(course_db_base), target_date=DATE)
            shared_l3f_db = Last3FDBBuilder().build(shared_course_db)

            pmgr = PersonnelDBManager()
            all_jockey_db, all_trainer_db = pmgr.build_from_horses(
                all_horses, scraper.client, course_db=shared_course_db, save=False)

            _bloodline_db = build_bloodline_db(
                all_horses, netkeiba_client=scraper.client, cache_path=BLOODLINE_DB_PATH)
        else:
            shared_course_db = dict(course_db_base)
            shared_l3f_db = l3f_db_base
            all_jockey_db, all_trainer_db = {}, {}

        # 2e. 各レース分析
        results = []
        for race_id in race_ids:
            try:
                race_info, horses = prefetched.get(race_id, (None, []))
                if not race_info or not horses:
                    continue

                _race_jids = {h.jockey_id for h in horses if h.jockey_id}
                _race_tids = {h.trainer_id for h in horses if h.trainer_id}
                jockey_db = {k: v for k, v in all_jockey_db.items() if k in _race_jids}
                trainer_db = {k: v for k, v in all_trainer_db.items() if k in _race_tids}
                enrich_personnel_with_condition_records(jockey_db, trainer_db, shared_course_db)

                engine = RaceAnalysisEngine(
                    course_db=shared_course_db, all_courses=all_courses,
                    jockey_db=jockey_db, trainer_db=trainer_db,
                    trainer_baseline_db=trainer_baseline_db,
                    pace_last3f_db=shared_l3f_db,
                    course_style_stats_db=course_style_db,
                    gate_bias_db=gate_bias_db,
                    position_sec_per_rank_db=position_sec_db,
                    is_jra=race_info.is_jra,
                    target_date=DATE,
                )
                analysis = engine.analyze(race_info, horses, custom_stake=None, netkeiba_client=None)
                analysis = enrich_course_aptitude_with_style_bias(engine, analysis)

                venue = race_info.venue
                race_no = race_info.race_no
                results.append((venue, race_no, analysis))
                del engine
            except Exception as e:
                logger.debug("analyze %s: %s", race_id, e)

        # 2f. 予想JSON保存
        if results:
            analyses_by_venue = {}
            for venue, race_no, analysis in results:
                if venue not in analyses_by_venue:
                    analyses_by_venue[venue] = {}
                analyses_by_venue[venue][race_no] = analysis
            save_prediction(DATE, analyses_by_venue)

        day_dur = time.time() - day_start
        durations.append(day_dur)
        success += 1
        P(f"  ✓ {len(results)}レース完了 ({day_dur:.0f}秒)")

        # メモリ解放
        del prefetched, all_horses, results
        gc.collect()

    except Exception as e:
        failed_dates.append(DATE)
        logger.warning("日付 %s 失敗: %s", DATE, e, exc_info=True)
        P(f"  [red]✗ エラー: {e}[/]")

total_time = time.time() - t0
avg_dur = sum(durations) / len(durations) if durations else 0
P(f"\n[bold green]再予測完了: {success}/{len(dates)}日  平均{avg_dur:.0f}秒/日  総時間{total_time/60:.1f}分[/]")
if failed_dates:
    P(f"[yellow]失敗: {failed_dates}[/]")


# ====================================================================
# 3. 新旧比較
# ====================================================================
P(f"\n\n{'#'*80}")
P(f"# 新旧モデル成績比較（engine.pyフルパイプライン）")
P(f"# 旧 = predictions_old / 新 = predictions (再予測後)")
P(f"{'#'*80}")

db = sqlite3.connect(DB_PATH)
db.row_factory = sqlite3.Row

def collect_stats(table_name, d_from, d_to):
    rows = db.execute(f'''
        SELECT p.date, p.race_id, p.confidence, p.horses_json
        FROM {table_name} p WHERE p.date BETWEEN ? AND ? ORDER BY p.date
    ''', (d_from, d_to)).fetchall()
    ms = {m: {'n':0,'w':0,'p2':0,'p3':0,'tp':0} for m in ['◉','◎','○','▲','△','★','☆','×']}
    cm = {c: {m: {'n':0,'w':0,'p2':0,'p3':0,'tp':0} for m in ['◉','◎','○','▲','△','★','☆']} for c in ['SS','S','A','B','C','D','E']}
    ss = {s: {m: {'n':0,'w':0,'p2':0,'p3':0,'tp':0} for m in ['◉','◎','○','▲','△','★','☆','×']} for s in ['JRA','NAR']}
    mo = defaultdict(lambda: {'n':0,'w':0,'p3':0,'tp':0})
    mh = defaultdict(lambda: {'n':0,'w':0,'p3':0,'tp':0})
    nr = 0
    for row in rows:
        rid, conf, month = row['race_id'], row['confidence'] or 'C', row['date'][:7]
        try: horses = json.loads(row['horses_json']) if row['horses_json'] else []
        except: continue
        vc = rid[4:6] if len(rid) >= 6 else ''
        scope = 'JRA' if vc in ['01','02','03','04','05','06','07','08','09','10'] else 'NAR'
        has = False
        for h in horses:
            mark, hno = h.get('mark',''), h.get('horse_no')
            if not mark or hno is None: continue
            rl = db.execute('SELECT finish_pos, tansho_odds FROM race_log WHERE race_id=? AND horse_no=?', (rid, hno)).fetchone()
            if not rl or not rl['finish_pos'] or rl['finish_pos'] <= 0: continue
            has = True; fp = rl['finish_pos']; to = rl['tansho_odds'] or 0
            def u(s):
                s['n'] += 1
                if fp == 1: s['w'] += 1; s['tp'] += int(to * 100)
                if fp <= 2: s['p2'] += 1
                if fp <= 3: s['p3'] += 1
            if mark in ms: u(ms[mark])
            if conf in cm and mark in cm.get(conf,{}): u(cm[conf][mark])
            if mark in ss.get(scope,{}): u(ss[scope][mark])
            if mark == '◎': u(mo[month])
            elif mark == '◉': u(mh[month])
        if has: nr += 1
    return {"mark": ms, "conf": cm, "scope": ss, "mo": mo, "mh": mh, "nr": nr}

d_from, d_to = dates[0], dates[-1]
P(f"\n集計中...")
old_s = collect_stats("predictions_old", d_from, d_to)
new_s = collect_stats("predictions", d_from, d_to)
P(f"  旧: {old_s['nr']:,}R / 新: {new_s['nr']:,}R")

def da(ov, nv, th=0.5):
    d = nv - ov; a = "↑" if d > th else ("↓" if d < -th else "→"); return f"{d:>+6.1f}%{a}"

# A. 印別
P(f"\n{'='*95}")
P(f"  A. 印別成績 新旧比較 ({d_from}～{d_to})")
P(f"{'='*95}")
P(f"  {'印':>3} {'モデル':>8} {'件数':>7} {'勝率':>7} {'連対率':>7} {'複勝率':>7} {'単回収':>7}")
P("  " + "-" * 75)
for mark in ['◉','◎','○','▲','△','★','☆','×']:
    o, n = old_s['mark'][mark], new_s['mark'][mark]
    for lb, s in [("旧",o),("新",n)]:
        c = s['n']
        if c == 0: continue
        P(f"  {mark}  {lb}モデル {c:>6,}件 {s['w']/c*100:>6.1f}% {s['p2']/c*100:>6.1f}% {s['p3']/c*100:>6.1f}% {s['tp']/(c*100)*100:>6.1f}%")
    if o['n'] > 0 and n['n'] > 0:
        P(f"       差分                {da(o['w']/o['n']*100, n['w']/n['n']*100)}         {da(o['p3']/o['n']*100, n['p3']/n['n']*100)} {da(o['tp']/(o['n']*100)*100, n['tp']/(n['n']*100)*100, 1.0)}")
    P()

# B. 自信度×◎◉
P(f"\n{'='*95}")
P(f"  B. 自信度×印 新旧比較")
P(f"{'='*95}")
for mark in ['◉','◎']:
    P(f"\n  [{mark}]")
    P(f"  {'自信度':>5} {'モデル':>8} {'件数':>6} {'勝率':>7} {'複勝率':>7} {'単回収':>7}")
    P("  " + "-" * 55)
    for conf in ['SS','S','A','B','C']:
        o = old_s['conf'].get(conf,{}).get(mark,{'n':0}); n = new_s['conf'].get(conf,{}).get(mark,{'n':0})
        for lb, s in [("旧",o),("新",n)]:
            c = s['n']
            if c < 10: continue
            P(f"  {conf:>5}  {lb}モデル {c:>5,}件 {s['w']/c*100:>6.1f}% {s['p3']/c*100:>6.1f}% {s['tp']/(c*100)*100:>6.1f}%")
        if o['n'] >= 10 and n['n'] >= 10:
            P(f"         差分          {da(o['w']/o['n']*100, n['w']/n['n']*100)} {da(o['p3']/o['n']*100, n['p3']/n['n']*100)} {da(o['tp']/(o['n']*100)*100, n['tp']/(n['n']*100)*100, 1.0)}")
        P()

# C. JRA/NAR別
P(f"\n{'='*95}")
P(f"  C. JRA / NAR 別")
P(f"{'='*95}")
for scope in ['JRA','NAR']:
    P(f"\n  [{scope}]")
    for mark in ['◉','◎','○','▲']:
        o = old_s['scope'].get(scope,{}).get(mark,{'n':0}); n = new_s['scope'].get(scope,{}).get(mark,{'n':0})
        for lb, s in [("旧",o),("新",n)]:
            c = s['n']
            if c < 10: continue
            P(f"    {mark} {lb}: {c:>5,}件 勝{s['w']/c*100:>5.1f}% 複{s['p3']/c*100:>5.1f}% 回{s['tp']/(c*100)*100:>5.1f}%")
        if o['n'] >= 10 and n['n'] >= 10:
            P(f"       差分: 勝{da(o['w']/o['n']*100, n['w']/n['n']*100)} 複{da(o['p3']/o['n']*100, n['p3']/n['n']*100)} 回{da(o['tp']/(o['n']*100)*100, n['tp']/(n['n']*100)*100, 1.0)}")
        P()

# D. 月別推移
P(f"\n{'='*95}")
P(f"  D. 月別 ◎/◉ 成績推移")
P(f"{'='*95}")
for mark_label, key in [("◎","mo"),("◉","mh")]:
    P(f"\n  [{mark_label}]")
    P(f"  {'月':>8}  {'旧件数':>5} {'旧勝率':>6} {'旧複勝':>6} {'旧単回':>6}  |  {'新件数':>5} {'新勝率':>6} {'新複勝':>6} {'新単回':>6}")
    P("  " + "-" * 80)
    months = sorted(set(list(old_s[key].keys()) + list(new_s[key].keys())))
    for month in months:
        om, nm = old_s[key].get(month,{'n':0}), new_s[key].get(month,{'n':0})
        def f(m):
            if m['n'] == 0: return "    -      -      -      -"
            return f"{m['n']:>5} {m['w']/m['n']*100:>5.1f}% {m['p3']/m['n']*100:>5.1f}% {m['tp']/(m['n']*100)*100:>5.1f}%"
        P(f"  {month}  {f(om)}  |  {f(nm)}")

db.close()
total_all = time.time() - t0
P(f"\n\n[bold]総実行時間: {total_all/60:.1f}分 ({total_all/3600:.1f}時間)[/]")
