"""
日付指定・全レース予想スクリプト。
使い方:
  python run_analysis_date.py YYYY-MM-DD
  python run_analysis_date.py YYYY-MM-DD --no-html        # HTML不要・JSON保存のみ
  python run_analysis_date.py YYYY-MM-DD --venues 園田     # 園田のみ分析
  python run_analysis_date.py YYYY-MM-DD --venues 園田,船橋 # 複数場指定（カンマ区切り）
  python run_analysis_date.py YYYY-MM-DD --force           # 既存予測を上書き再生成
  python run_analysis_date.py YYYY-MM-DD --workers 2       # 並列ワーカー数
中央（JRA）・地方（NAR）の当日全レースを順次分析し、
個別HTMLと YYYYMMDD_全レース.html を生成する。
"""
import gc
import io
import os
import sys
import time

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import re

from src.log import get_logger

logger = get_logger(__name__)

try:
    from rich.console import Console
    from rich.progress import (
        BarColumn,
        Progress,
        SpinnerColumn,
        TaskProgressColumn,
        TextColumn,
        TimeElapsedColumn,
        TimeRemainingColumn,
    )
    console = Console(force_terminal=True, file=sys.stdout)
    P = console.print
    HAS_RICH = True
except ImportError:
    P = print
    HAS_RICH = False

if len(sys.argv) < 2:
    P("[bold red]日付を指定してください（例: 2026-02-22）[/]")
    sys.exit(1)

DATE = sys.argv[1].strip()
if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", DATE):
    P(f"[bold red]日付フォーマット不正: {DATE}  → YYYY-MM-DD で指定[/]")
    sys.exit(1)

NO_HTML = "--html" not in sys.argv  # デフォルトHTML不要、--html指定時のみ生成
NO_PURGE = "--no-purge" in sys.argv
IGNORE_TTL = "--ignore-ttl" in sys.argv
RACE_IDS_FROM_DB = "--race-ids-from-db" in sys.argv
RACE_IDS_FROM_PRED = "--race-ids-from-pred" in sys.argv
FORCE_RERUN = "--force" in sys.argv
# --workers N: 並列プリフェッチのワーカー数（デフォルト3）
_workers_idx = next((i for i, a in enumerate(sys.argv) if a == "--workers"), -1)
WORKERS = int(sys.argv[_workers_idx + 1]) if _workers_idx >= 0 and _workers_idx + 1 < len(sys.argv) else 5
# --venues 園田,船橋: 指定した競馬場のみ分析（カンマ区切り）
_venues_idx = next((i for i, a in enumerate(sys.argv) if a == "--venues"), -1)
VENUE_FILTER = sys.argv[_venues_idx + 1].split(",") if _venues_idx >= 0 and _venues_idx + 1 < len(sys.argv) else []

DATE_KEY = DATE.replace("-", "")
P(f"\n[bold white on #0d2b5e]  D-AI 競馬予想  日付: {DATE}（全レース）  [/]\n")
t0 = time.time()

# ─── 1. 基盤準備 ─────────────────────────────────────────────────
P("[bold cyan]\\[1/N][/] 初期化...")
from data.masters.course_master import get_all_courses
from src.engine import RaceAnalysisEngine, enrich_course_aptitude_with_style_bias
from src.scraper.auth import PremiumNetkeibaScraper
from src.scraper.course_db_collector import load_preload_course_db
from src.scraper.personnel import PersonnelDBManager, enrich_personnel_with_condition_records
from src.scraper.race_results import (
    Last3FDBBuilder,
    StandardTimeDBBuilder,
    build_course_db_from_past_runs,
    build_course_style_stats_db,
    build_gate_bias_db,
    build_position_sec_per_rank_db,
    build_trainer_baseline_db,
    load_trainer_baseline_db,
    merge_trainer_baseline,
    save_trainer_baseline_db,
)

if not NO_HTML:
    from src.output.formatter import HTMLFormatter, minify_html
from config.settings import BLOODLINE_DB_PATH, COURSE_DB_PRELOAD_PATH, TRAINER_BASELINE_DB_PATH
from src.scraper.improvement_dbs import build_bloodline_db

try:
    from tqdm import tqdm
except ImportError:
    def tqdm(iterable, **_kw):
        return iterable

all_courses = get_all_courses()
scraper = PremiumNetkeibaScraper(all_courses, ignore_ttl=IGNORE_TTL)
scraper.login()
kb_ok = scraper.training.login()
if not kb_ok:
    logger.warning("競馬ブックログイン失敗。3秒後にリトライ...")
    time.sleep(3)
    kb_ok = scraper.training.login()
if not kb_ok:
    P("[bold yellow]⚠ 競馬ブック未認証: 調教・厩舎コメントはnetkeiba経由のみ[/]")
else:
    grade = "プレミアム" if scraper.training.is_premium else "一般"
    P(f"  競馬ブック: ログイン成功 ({grade})")

# 起動時に古いキャッシュを自動パージ（30日超）
if not NO_PURGE:
    from src.scraper.netkeiba import purge_old_cache
    _purge = purge_old_cache(max_age_days=30)
    if _purge["removed"]:
        P(f"  キャッシュパージ: {_purge['removed']}件削除 ({_purge['freed_mb']}MB解放)")
    # race_cache（JSON形式）の期限切れもパージ
    try:
        from src.scraper.race_cache import purge_expired_cache
        _rc_purge = purge_expired_cache()
        if _rc_purge:
            P(f"  レースキャッシュパージ: {_rc_purge}件削除")
    except Exception:
        pass
else:
    P("  [dim]キャッシュパージ: スキップ (--no-purge)[/dim]")

# --force 時: 対象日付のKB（競馬ブック）HTTPキャッシュも削除
# 早朝の空レスポンスが24h TTLで残り、調教データ0件になる問題の恒久対策
if FORCE_RERUN:
    try:
        from config.settings import KEIBABOOK_CACHE_DIR
        _mmdd = DATE_KEY[4:]  # "0415"
        # まず対象ファイルを収集してからまとめて削除（Windows scandir + 削除の同時実行回避）
        _to_delete = []
        if os.path.isdir(KEIBABOOK_CACHE_DIR):
            for entry in os.scandir(KEIBABOOK_CACHE_DIR):
                if not entry.is_file():
                    continue
                fn = entry.name
                # パターン1: nittei等 → ファイル名に YYYYMMDD を含む
                # パターン2: cyokyo/danwa → ファイル名末尾が MMDD.html
                if DATE_KEY in fn or fn.endswith(f"{_mmdd}.html"):
                    _to_delete.append(entry.path)
        _kb_removed = 0
        for _f in _to_delete:
            try:
                os.remove(_f)
                _kb_removed += 1
            except OSError:
                pass
        if _kb_removed:
            P(f"  KBキャッシュパージ: {_kb_removed}件削除 (日付={DATE_KEY})")
    except Exception as e:
        logger.warning(f"KBキャッシュパージ失敗: {e}")

# エンジンのグローバルキャッシュをリセット（前回実行の残骸を排除）
from src.engine import reset_engine_caches

reset_engine_caches()
P(f"  経過: {time.time()-t0:.1f}s")

# ─── 2. 基準タイムDB ─────────────────────────────────────────────
P("[bold cyan]\\[2/N][/] 基準タイムDB読み込み...")
P(f"  ローリングウィンドウ基準日: {DATE}")
std_db = StandardTimeDBBuilder()
course_db_base = std_db.get_course_db()

# SQLite course_dbテーブルからも読み込み（ローリングウィンドウ適用）
from datetime import datetime as _dt
from datetime import timedelta as _td

from src.database import get_course_db as _get_sqlite_course_db
from src.scraper.course_db_collector import _dict_to_past_run

_window_start = (_dt.strptime(DATE, "%Y-%m-%d") - _td(days=365)).strftime("%Y-%m-%d")
_window_end = (_dt.strptime(DATE, "%Y-%m-%d") - _td(days=1)).strftime("%Y-%m-%d")
_sqlite_db = _get_sqlite_course_db()
_sqlite_count = 0
for _cid, _recs in _sqlite_db.items():
    for _r in _recs:
        _rd = _r.get("race_date", "")
        if _rd and _window_start <= _rd <= _window_end:
            course_db_base.setdefault(_cid, []).append(_dict_to_past_run(_r))
            _sqlite_count += 1
P(f"  SQLite course_db: {_sqlite_count:,}走追加")

preload = load_preload_course_db(COURSE_DB_PRELOAD_PATH, target_date=DATE)
for cid, runs in preload.items():
    course_db_base.setdefault(cid, []).extend(runs)
P(f"  {len(course_db_base)}コース / {sum(len(v) for v in course_db_base.values()):,}走")

course_style_db     = build_course_style_stats_db(course_db_base, target_date=DATE)
gate_bias_db        = build_gate_bias_db(course_db_base, target_date=DATE)
position_sec_db     = build_position_sec_per_rank_db(course_db_base, target_date=DATE)
l3f_db_base         = Last3FDBBuilder().build(course_db_base)
trainer_baseline_db = load_trainer_baseline_db(TRAINER_BASELINE_DB_PATH)
P(f"  経過: {time.time()-t0:.1f}s")

# ─── 3. 当日レースID取得 ──────────────────────────────────────────
P(f"[bold cyan]\\[3/N][/] {DATE} のレースID取得...")
if RACE_IDS_FROM_PRED:
    # 既存の予想JSONからrace_idを抽出（バッチ再分析用）
    import json as _json

    from config.settings import PREDICTIONS_DIR
    _pred_path = os.path.join(PREDICTIONS_DIR, f"{DATE_KEY}_pred.json")
    try:
        with open(_pred_path, "r", encoding="utf-8") as _f:
            _pred = _json.load(_f)
        race_ids = [r["race_id"] for r in _pred.get("races", [])]
        P(f"  [yellow]pred.jsonから取得: {len(race_ids)}レース（JRA/NAR公式フェッチをスキップ）[/]")
    except Exception as _e:
        P(f"  [red]pred.json読込失敗: {_e} → 通常フェッチにフォールバック[/]")
        race_ids = scraper.fetch_date(DATE)
elif RACE_IDS_FROM_DB:
    import sqlite3 as _sql3
    _conn = _sql3.connect("data/keiba.db")
    race_ids = [r[0] for r in _conn.execute(
        "SELECT DISTINCT race_id FROM race_log WHERE race_date = ? ORDER BY race_id",
        (DATE,),
    ).fetchall()]
    _conn.close()
    P(f"  [yellow]race_logから取得: {len(race_ids)}レース[/]")
else:
    race_ids = scraper.fetch_date(DATE)
# ばんえい（帯広 vc=65）: venue_65 LightGBMモデル構築済み（Phase 3）

# --venues フィルタ: 指定した競馬場のレースのみ残す
if VENUE_FILTER:
    from data.masters.venue_master import VENUE_NAME_TO_CODE
    _vc_set = set()
    for _vn in VENUE_FILTER:
        _vn = _vn.strip()
        _vc = VENUE_NAME_TO_CODE.get(_vn, "")
        if _vc:
            _vc_set.add(_vc)
            # 園田(49)/姫路(50)相互補完
            if _vc == "49": _vc_set.add("50")
            elif _vc == "50": _vc_set.add("49")
        else:
            P(f"  [yellow]警告: 不明な競馬場名 '{_vn}'[/]")
    if _vc_set:
        _before = len(race_ids)
        race_ids = [rid for rid in race_ids if rid[4:6] in _vc_set]
        P(f"  [yellow]--venues {','.join(VENUE_FILTER)}: {_before}R → {len(race_ids)}R にフィルタ[/]")

if not race_ids:
    P(f"[bold red]{DATE} のレースが見つかりません[/]")
    sys.exit(1)
P(f"  {len(race_ids)}レース: {race_ids}")

# ─── [3b/N] カレンダー突合検証 (T-038) ──────────────────────────────
# kaisai_calendar.json とのカレンダー整合チェック。
# JRA race_id が NAR の元旦に配置される等の汚染 race_id を即時排除する。
from src.scraper.kaisai_calendar_util import validate_race_against_calendar
from data.masters.venue_master import VENUE_CODE_TO_NAME, JRA_VENUE_CODES as _JRA_VC

_calendar_skipped: list = []
_calendar_valid: list = []
for _rid in race_ids:
    _vc = _rid[4:6] if len(_rid) >= 6 else ""
    _vname = VENUE_CODE_TO_NAME.get(_vc, "")
    _is_jra = _vc in _JRA_VC
    if not _vname:
        # 場コード不明は検証できないためスルー（ログのみ）
        logger.warning("[T-038] race_id=%s: 場コード=%s 不明 → カレンダー検証スキップ", _rid, _vc)
        _calendar_valid.append(_rid)
        continue
    _ok, _reason = validate_race_against_calendar(_rid, DATE, _vname, _is_jra)
    if not _ok:
        logger.warning("[T-038] カレンダー不整合 → skip: %s", _reason)
        _calendar_skipped.append(_rid)
    else:
        _calendar_valid.append(_rid)

if _calendar_skipped:
    P(f"  [bold yellow][T-038] カレンダー突合 skip: {len(_calendar_skipped)}件"
      f" → {_calendar_skipped}[/]")
    race_ids = _calendar_valid
    P(f"  有効 race_ids: {len(race_ids)}件")
else:
    P(f"  [T-038] カレンダー突合: 全 {len(race_ids)}件 整合 (skip=0)")

if not race_ids:
    P(f"[bold red]{DATE} のレースが全件カレンダー不整合でスキップされました[/]")
    sys.exit(1)

# ─── 4. 各レースを分析 ────────────────────────────────────────────
if not NO_HTML:
    os.makedirs("output", exist_ok=True)
    formatter = HTMLFormatter()
else:
    formatter = None
results = []   # (race_id, html_path, race_name, ok, race_meta_dict)
failed  = []

# 中断再開: 完了済みレースをスキップ（--force 時は無視）
_done_marker = f"output/.done_{DATE_KEY}.txt"
_done_ids: set = set()
if FORCE_RERUN and os.path.exists(_done_marker):
    os.remove(_done_marker)
    P("  [yellow]--force: 中断マーカー削除[/]")
elif os.path.exists(_done_marker):
    with open(_done_marker, "r") as _f:
        _done_ids = {line.strip() for line in _f if line.strip()}
    if _done_ids:
        P(f"  [yellow]中断再開: {len(_done_ids)}レース完了済み → スキップ[/]")

# 未処理レースを特定（中断再開対応）
_ids_to_fetch = [rid for rid in race_ids if rid not in _done_ids]
for rid in _done_ids:
    if rid in race_ids:
        results.append((rid, "", rid, True, {}))

# ─── 4a. 並列プリフェッチ ─────────────────────────────────────────
prefetched: dict = {}  # race_id → (race_info, horses)
effective_workers = max(1, min(WORKERS, len(_ids_to_fetch)))

if _ids_to_fetch and effective_workers > 1:
    from concurrent.futures import ThreadPoolExecutor, as_completed

    P(f"[bold cyan]\\[4a/N][/] 並列プリフェッチ中... (ワーカー数: {effective_workers}, {len(_ids_to_fetch)}R)")
    worker_pool = [scraper] + [scraper.clone_worker() for _ in range(effective_workers - 1)]

    def _prefetch(args):
        idx, race_id = args
        w = worker_pool[idx % len(worker_pool)]
        for attempt in range(2):
            try:
                ri, hs = w.fetch_race(race_id, fetch_history=True, fetch_odds=True, fetch_training=True, target_date=DATE, prefer_cache=(RACE_IDS_FROM_DB or RACE_IDS_FROM_PRED))
                return race_id, ri, hs
            except Exception as e:
                if attempt == 0:
                    logger.debug("prefetch retry %s: %s", race_id, e)
                else:
                    logger.warning("prefetch failed %s: %s", race_id, e)
        return race_id, None, []

    done_count = 0
    _pf_t0 = time.time()
    if HAS_RICH:
        _pf_progress = Progress(
            SpinnerColumn(),
            TextColumn("[bold cyan]4a プリフェッチ[/]"),
            BarColumn(bar_width=40),
            TaskProgressColumn(),
            TextColumn("{task.fields[status]}"),
            TimeElapsedColumn(),
            TextColumn("残り"),
            TimeRemainingColumn(),
            console=console,
        )
        _pf_task = _pf_progress.add_task("prefetch", total=len(_ids_to_fetch), status="")
        _pf_progress.start()
    with ThreadPoolExecutor(max_workers=effective_workers) as pool:
        futs = {pool.submit(_prefetch, (i, rid)): rid for i, rid in enumerate(_ids_to_fetch)}
        for fut in as_completed(futs):
            race_id, ri, hs = fut.result()
            prefetched[race_id] = (ri, hs)
            done_count += 1
            if HAS_RICH:
                _desc = f"{ri.venue}{ri.race_no}R ({len(hs)}頭)" if ri else f"{race_id} [スキップ]"
                _pf_progress.update(_pf_task, advance=1, status=_desc)
            else:
                _pf_elapsed = time.time() - _pf_t0
                _pf_pct = done_count / len(_ids_to_fetch) * 100
                _pf_remain = _pf_elapsed / done_count * (len(_ids_to_fetch) - done_count) if done_count > 0 else 0
                if ri:
                    P(f"  ({done_count}/{len(_ids_to_fetch)}) {_pf_pct:.1f}% {ri.venue}{ri.race_no}R ({len(hs)}頭)  経過{_pf_elapsed:.0f}秒 残り{_pf_remain:.0f}秒")
                else:
                    P(f"  ({done_count}/{len(_ids_to_fetch)}) {_pf_pct:.1f}% {race_id} [スキップ]  経過{_pf_elapsed:.0f}秒 残り{_pf_remain:.0f}秒")
    if HAS_RICH:
        _pf_progress.stop()
    P(f"  プリフェッチ完了: {done_count}R  経過: {time.time()-t0:.1f}s")
elif _ids_to_fetch:
    P(f"[bold cyan]\\[4a/N][/] 逐次取得中... ({len(_ids_to_fetch)}R)")
    for race_id in _ids_to_fetch:
        try:
            ri, hs = scraper.fetch_race(race_id, fetch_history=True, fetch_odds=True, fetch_training=True, target_date=DATE, prefer_cache=(RACE_IDS_FROM_DB or RACE_IDS_FROM_PRED))
            prefetched[race_id] = (ri, hs)
        except Exception as e:
            logger.warning("fetch failed %s: %s", race_id, e)
            prefetched[race_id] = (None, [])

# ─── 4a+. プリフェッチ後の一括事前構築（並列化安全のため4bから移動）────
_all_prefetched_horses = []
for _ri, _hs in prefetched.values():
    if _hs:
        _all_prefetched_horses.extend(_hs)

_all_jockey_db: dict = {}   # 必ずここで初期化（4a+例外時のNameError防止）
_all_trainer_db: dict = {}

if _all_prefetched_horses:
    P(f"[bold cyan]\\[4a+/N][/] 事前構築中... ({len(_all_prefetched_horses)}頭)")

    # trainer_baseline: 全馬分を一括マージ・保存（4bループ内でのマージを不要にする）
    _t_sub = time.time()
    _baseline_all = build_trainer_baseline_db(_all_prefetched_horses)
    trainer_baseline_db = merge_trainer_baseline(_baseline_all, trainer_baseline_db)
    save_trainer_baseline_db(TRAINER_BASELINE_DB_PATH, trainer_baseline_db)
    P(f"  [dim]  trainer_baseline: {time.time()-_t_sub:.1f}s[/dim]")

    # personnel: 全馬分の騎手・厩舎DBを一括構築・保存
    # ★ NAR騎手/調教師のcourse_dbフォールバックを有効にするため、
    #    preload(JRAのみ) + 全馬過去走から構築したcourse_dbを渡す
    _t_sub = time.time()
    _shared_course_db = build_course_db_from_past_runs(
        _all_prefetched_horses, dict(course_db_base), target_date=DATE
    )
    P(f"  [dim]  shared_course_db: {time.time()-_t_sub:.1f}s  ({len(_shared_course_db)}コース/{sum(len(v) for v in _shared_course_db.values()):,}走)[/dim]")
    # l3f_dbも全馬分を一括構築（各レースでの再構築を省略）
    _shared_l3f_db = Last3FDBBuilder().build(_shared_course_db)

    _t_sub = time.time()
    _all_personnel_mgr = PersonnelDBManager()
    # NAR調教師IDの汚染データ（netkeiba fetchで別人データが保存されたもの）を削除
    _purged = _all_personnel_mgr.purge_mismatched_nar_trainers()
    if _purged:
        P(f"  [dim]  NAR調教師汚染データ {_purged}件を削除[/dim]")
    _all_jockey_db, _all_trainer_db = _all_personnel_mgr.build_from_horses(
        _all_prefetched_horses, scraper.client, course_db=_shared_course_db, save=True
    )
    P(f"  [dim]  personnel_build: {time.time()-_t_sub:.1f}s  (騎手{len(_all_jockey_db)} / 厩舎{len(_all_trainer_db)})[/dim]")

    # bloodline: 未キャッシュ血統IDを事前取得・キャッシュ保存
    _t_sub = time.time()
    _bloodline_db = build_bloodline_db(_all_prefetched_horses, netkeiba_client=scraper.client, cache_path=BLOODLINE_DB_PATH)
    P(f"  [dim]  bloodline_db: {time.time()-_t_sub:.1f}s[/dim]")

    # キャリブレーションキャッシュを事前初期化（並列時のTOCTOU競合防止）
    _t_sub = time.time()
    from config.settings import get_composite_weights
    get_composite_weights()
    P(f"  [dim]  calibration_cache: {time.time()-_t_sub:.1f}s[/dim]")

    # 事前構築の一時オブジェクトを解放
    del _all_prefetched_horses
    gc.collect()
    P(f"  事前構築完了  経過: {time.time()-t0:.1f}s")
else:
    _all_jockey_db, _all_trainer_db = {}, {}
    _shared_course_db = dict(course_db_base)
    _shared_l3f_db = l3f_db_base

# ─── 4b. 並列分析 ─────────────────────────────────────────────────

# MLモデルキャッシュをウォームアップ（並列時のTOCTOU防止）
if _ids_to_fetch:
    P("[dim]  MLモデルキャッシュ ウォームアップ...[/dim]")
    _warmup = RaceAnalysisEngine(
        course_db=dict(course_db_base), all_courses=all_courses,
        jockey_db={}, trainer_db={},
        trainer_baseline_db=trainer_baseline_db,
        pace_last3f_db=l3f_db_base,
        course_style_stats_db=course_style_db,
        gate_bias_db=gate_bias_db,
        position_sec_per_rank_db=position_sec_db,
        is_jra=True, target_date=DATE,
    )
    del _warmup


def _analyze_one_race(race_id):
    """1レースを分析して結果タプルを返す（スレッドセーフ）"""
    try:
        race_info, horses = prefetched.get(race_id, (None, []))
        if not race_info or not horses:
            logger.warning("データ取得失敗: %s", race_id)
            return (race_id, "", race_id, False, {})

        race_name = race_info.race_name or f"{race_info.venue}{race_info.race_no}R"

        # 事前構築済みの共有course_db/l3f_dbを使用（読み取り専用）
        # _shared_course_db は4a+で全馬分を一括構築済み
        course_db = _shared_course_db
        l3f_db    = _shared_l3f_db
        # personnel: 事前構築済みDBからレース出走馬分をフィルタ
        _race_jids = {h.jockey_id for h in horses if h.jockey_id}
        _race_tids = {h.trainer_id for h in horses if h.trainer_id}
        jockey_db = {k: v for k, v in _all_jockey_db.items() if k in _race_jids}
        trainer_db = {k: v for k, v in _all_trainer_db.items() if k in _race_tids}
        enrich_personnel_with_condition_records(jockey_db, trainer_db, course_db)

        engine = RaceAnalysisEngine(
            course_db=course_db, all_courses=all_courses,
            jockey_db=jockey_db, trainer_db=trainer_db,
            trainer_baseline_db=trainer_baseline_db,
            pace_last3f_db=l3f_db,
            course_style_stats_db=course_style_db,
            gate_bias_db=gate_bias_db,
            position_sec_per_rank_db=position_sec_db,
            is_jra=race_info.is_jra,
            target_date=DATE,
        )
        analysis = engine.analyze(race_info, horses, custom_stake=None, netkeiba_client=None)
        analysis = enrich_course_aptitude_with_style_bias(engine, analysis)

        if not NO_HTML:
            out_file = f"output/{DATE_KEY}_{race_info.venue}{race_info.race_no}R.html"
            html = minify_html(formatter.render(analysis))
            with open(out_file, "w", encoding="utf-8") as f:
                f.write(html)
        else:
            out_file = ""
        meta = {
            "venue":        race_info.venue,
            "race_no":      race_info.race_no,
            "name":         race_name,
            "surface":      getattr(race_info.course, "surface", ""),
            "distance":     getattr(race_info.course, "distance", 0),
            "grade":        race_info.grade or "",
            "post_time":    race_info.post_time or "",
            "head_count":   len(horses),
            "analysis_obj": analysis,
        }
        # 一時オブジェクトを解放（メモリ圧力軽減）
        del engine
        return (race_id, out_file, race_name, True, meta)

    except Exception as e:
        logger.warning("race analysis failed %s: %s", race_id, e, exc_info=True)
        return (race_id, "", race_id, False, {})


if _ids_to_fetch:
    from concurrent.futures import ThreadPoolExecutor, as_completed
    _4b_workers = max(1, min(WORKERS, len(_ids_to_fetch)))
    P(f"[bold cyan]\\[4b/N][/] 各レース並列分析中... ({len(_ids_to_fetch)}R, {_4b_workers}ワーカー)")

    # 並列分析前にGCを実行してメモリを確保
    gc.collect()
    # GCしきい値を緩めて並列中のGC頻度を下げる（GIL競合軽減）
    _gc_thresh_orig = gc.get_threshold()
    gc.set_threshold(50000, 50, 50)

    done_4b = 0
    _4b_t0 = time.time()
    if HAS_RICH:
        _4b_progress = Progress(
            SpinnerColumn(),
            TextColumn("[bold green]4b 分析[/]"),
            BarColumn(bar_width=40),
            TaskProgressColumn(),
            TextColumn("{task.fields[status]}"),
            TimeElapsedColumn(),
            TextColumn("残り"),
            TimeRemainingColumn(),
            console=console,
        )
        _4b_task = _4b_progress.add_task("analysis", total=len(_ids_to_fetch), status="")
        _4b_progress.start()
    with ThreadPoolExecutor(max_workers=_4b_workers) as pool:
        futs = {pool.submit(_analyze_one_race, rid): rid for rid in _ids_to_fetch}
        for fut in as_completed(futs):
            result_tuple = fut.result()
            rid_done = result_tuple[0]
            ok = result_tuple[3]
            meta = result_tuple[4] if len(result_tuple) > 4 else {}

            # メインスレッドで結果収集（スレッドセーフ）
            results.append(result_tuple)
            if not ok:
                failed.append(rid_done)
            done_4b += 1

            # プログレスバー更新
            _v = meta.get("venue", "")
            _rn = meta.get("race_no", "")
            if HAS_RICH:
                _4b_progress.update(_4b_task, advance=1,
                    status=f"{_v}{_rn}R {'✓' if ok else '✗'}")
            else:
                _elapsed = time.time() - _4b_t0
                _pct = done_4b / len(_ids_to_fetch) * 100
                _remaining = _elapsed / done_4b * (len(_ids_to_fetch) - done_4b) if done_4b > 0 else 0
                P(f"  ({done_4b}/{len(_ids_to_fetch)}) {_pct:.1f}% {_v}{_rn}R {'完了' if ok else '失敗'}  経過{_elapsed:.0f}秒 残り{_remaining:.0f}秒")

            # 中断再開用マーカー記録
            with open(_done_marker, "a") as _df:
                _df.write(rid_done + "\n")

            # pred.json を段階的に保存（5レースごと、lightweight=コメント/LLMスキップ）
            _ok_in_run = sum(1 for r in results if r[3] and r[4].get("analysis_obj"))
            if _ok_in_run % 5 == 0 and _ok_in_run > 0:
                try:
                    from src.results_tracker import save_prediction as _sp_inc
                    _abv_inc: dict = {}
                    for _r in results:
                        if not _r[3]:
                            continue
                        _m = _r[4] if len(_r) > 4 else {}
                        _ao = _m.get("analysis_obj")
                        if _ao:
                            _vv = _m.get("venue", "不明")
                            _rrn = _m.get("race_no", 0)
                            _abv_inc.setdefault(_vv, {})[_rrn] = _ao
                    if _abv_inc:
                        _sp_inc(DATE, _abv_inc, lightweight=True)
                except Exception as _se:
                    logger.warning("pred.json incremental save failed: %s", _se, exc_info=True)
                # 5レースごとにメインスレッドでGC実行（メモリ蓄積防止）
                gc.collect()

    if HAS_RICH:
        _4b_progress.stop()
    # GCしきい値を復元してメモリ回収
    gc.set_threshold(*_gc_thresh_orig)
    gc.collect()
    P(f"  並列分析完了: {done_4b}R  経過: {time.time()-t0:.1f}s")

# 全レース完了 → 中断再開マーカー削除
if os.path.exists(_done_marker) and not failed:
    os.remove(_done_marker)

# ─── 5. 予想JSON保存（結果照合用） ──────────────────────────────────
P("[bold cyan]\\[N/N-1][/] 予想JSON保存...")
try:
    from src.results_tracker import save_prediction
    analyses_by_venue: dict = {}
    for row in results:
        if not row[3]:
            continue
        meta = row[4] if len(row) > 4 else {}
        venue = meta.get("venue", "不明")
        race_no = meta.get("race_no", 0)
        analysis_obj = meta.get("analysis_obj")
        if analysis_obj:
            if venue not in analyses_by_venue:
                analyses_by_venue[venue] = {}
            analyses_by_venue[venue][race_no] = analysis_obj
    if analyses_by_venue:
        pred_path = save_prediction(DATE, analyses_by_venue)
        P(f"  予想JSON保存: {pred_path}")
        # JRA出馬表CNAME注入（フロントエンドで公式リンク表示用）
        try:
            if scraper._official_odds and scraper._official_odds._jra_shutuba_cname_cache:
                import json as _json_cname
                with open(pred_path, "r", encoding="utf-8") as _f:
                    _pred_data = _json_cname.load(_f)
                _cache = scraper._official_odds._jra_shutuba_cname_cache
                _injected = 0
                for _race in _pred_data.get("races", []):
                    if not _race.get("is_jra"):
                        continue
                    _rid = _race.get("race_id", "")
                    if len(_rid) < 12:
                        continue
                    _vc = _rid[4:6]
                    _kk_nn = _rid[6:10]
                    _rno = int(_rid[10:12])
                    _ck = f"{_vc}_{_kk_nn}_{_rno:02d}"
                    _cn = _cache.get(_ck, "")
                    if _cn:
                        _race["shutuba_cname"] = _cn
                        _injected += 1
                if _injected > 0:
                    with open(pred_path, "w", encoding="utf-8") as _f:
                        _json_cname.dump(_pred_data, _f, ensure_ascii=False, indent=2)
                    P(f"  JRA出馬表CNAME注入: {_injected}レース")
        except Exception as _ce:
            logger.warning("shutuba_cname injection failed: %s", _ce)
    else:
        P("  予想JSONなし（analysisオブジェクト未格納）")
except Exception as e:
    logger.warning("prediction JSON save failed: %s", e, exc_info=True)

# ─── 6. 全レースまとめHTML生成（ネット競馬風UI） ─────────────────
if NO_HTML:
    ok_count = sum(1 for r in results if r[3])
    total_t  = time.time() - t0
    P(f"\n[bold green]完了: {ok_count}/{len(results)}レース  総実行時間: {total_t:.0f}秒[/]")
    sys.exit(0)

P("[bold cyan]\\[N/N][/] 全レースまとめHTML生成...")
ok_count = sum(1 for r in results if r[3])
total_t  = time.time() - t0

# 競馬場ごとにグループ化（出馬表順を維持）
venue_order = []
venue_races = {}
for row in results:
    race_id, path, name, ok = row[0], row[1], row[2], row[3]
    meta = row[4] if len(row) > 4 else {}
    venue = meta.get("venue", "") or name[:3]
    if venue not in venue_races:
        venue_order.append(venue)
        venue_races[venue] = []
    venue_races[venue].append({"race_id": race_id, "path": path, "name": name, "ok": ok, **meta})

# タブHTML生成
def _surf_badge(surface):
    if surface in ("芝", "障"):
        return f'<span style="color:#1a7a3a;font-weight:700;font-size:11px">{surface}</span>'
    if surface == "ダート":
        return '<span style="color:#8b5e2a;font-weight:700;font-size:11px">ダ</span>'
    return f'<span style="font-size:11px">{surface or "?"}</span>'

def _grade_badge(grade):
    if grade in ("G1",):
        return '<span style="background:#c0392b;color:#fff;font-size:10px;font-weight:700;padding:1px 5px;border-radius:3px">G1</span>'
    if grade in ("G2",):
        return '<span style="background:#2c6dbf;color:#fff;font-size:10px;font-weight:700;padding:1px 5px;border-radius:3px">G2</span>'
    if grade in ("G3",):
        return '<span style="background:#27ae60;color:#fff;font-size:10px;font-weight:700;padding:1px 5px;border-radius:3px">G3</span>'
    if grade in ("L", "OP"):
        return f'<span style="background:#e67e22;color:#fff;font-size:10px;font-weight:700;padding:1px 5px;border-radius:3px">{grade}</span>'
    if "重賞" in grade:
        return '<span style="background:#c0392b;color:#fff;font-size:10px;font-weight:700;padding:1px 5px;border-radius:3px">重賞</span>'
    return ""

tab_buttons = ""
tab_panels  = ""
for vi, venue in enumerate(venue_order):
    races = venue_races[venue]
    active = "active" if vi == 0 else ""
    tab_buttons += f'<button class="vtab {active}" onclick="showVenue({vi})" id="vtab-{vi}">{venue}</button>\n'
    cards = ""
    for r in sorted(races, key=lambda x: x.get("race_no", 0)):
        fname   = os.path.basename(r["path"]) if r["ok"] else ""
        race_no = r.get("race_no", "?")
        rname   = r.get("name", f"{race_no}R")
        surface = r.get("surface", "")
        dist    = r.get("distance", 0)
        grade   = r.get("grade", "")
        ptime   = r.get("post_time", "")
        heads   = r.get("head_count", 0)
        sbadge  = _surf_badge(surface)
        gbadge  = _grade_badge(grade)
        dist_s  = f"{dist}m" if dist else ""
        head_s  = f"{heads}頭" if heads else ""
        time_s  = ptime or ""
        if r["ok"] and fname:
            card_inner = f'<a href="{fname}" class="race-card-link">'
        else:
            card_inner = '<div class="race-card-ng">'
        card_close = "</a>" if (r["ok"] and fname) else "</div>"
        status_icon = "" if r["ok"] else '<span style="color:#c0392b;font-size:10px">❌</span>'
        cards += f"""
<div class="race-card{"" if r["ok"] else " race-card--ng"}">
  {card_inner}
    <div class="rc-header">
      <span class="rc-no">{race_no}R</span>
      {gbadge}
      {status_icon}
    </div>
    <div class="rc-name">{rname}</div>
    <div class="rc-meta">
      <span style="font-size:11px;color:#6b7280">{time_s}</span>
      {sbadge}{dist_s} {head_s}
    </div>
    <div class="rc-ipat">予想を見る</div>
  {card_close}
</div>"""
    tab_panels += f'<div class="vpanel {active}" id="vpanel-{vi}">\n<div class="race-grid">{cards}</div>\n</div>\n'

combined_html = f"""<!DOCTYPE html>
<html lang="ja"><head>
<meta charset="UTF-8">
<title>{DATE} 全レース予想</title>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:"Hiragino Sans","Yu Gothic UI",sans-serif;background:#f2f4f8;color:#1a1a2e;min-height:100vh}}
.header{{background:#0d2b5e;color:#fff;padding:10px 20px;display:flex;align-items:center;gap:12px}}
.header h1{{font-size:16px;font-weight:700}}
.header .date-badge{{background:#c9952a;color:#fff;font-size:12px;font-weight:700;
  padding:3px 10px;border-radius:4px}}
.meta-bar{{background:#fff;border-bottom:1px solid #dde3ee;padding:6px 20px;
  font-size:12px;color:#6b7280;display:flex;gap:16px}}
.container{{max-width:960px;margin:0 auto;padding:16px 12px}}
/* 場タブ */
.venue-tabs{{display:flex;gap:0;background:#fff;border-radius:8px 8px 0 0;
  box-shadow:0 1px 3px rgba(0,0,0,.1);overflow:hidden;flex-wrap:wrap}}
.vtab{{flex:1;min-width:80px;padding:10px 8px;font-size:13px;font-weight:700;
  border:none;background:#f8f9fb;color:#555;cursor:pointer;
  border-bottom:3px solid transparent;transition:.15s}}
.vtab:hover{{background:#eef1f9;color:#0d2b5e}}
.vtab.active{{background:#fff;color:#0d2b5e;border-bottom:3px solid #c9952a}}
.vpanel{{display:none;background:#fff;border-radius:0 0 8px 8px;
  box-shadow:0 2px 8px rgba(0,0,0,.08);padding:14px}}
.vpanel.active{{display:block}}
/* レースカードグリッド */
.race-grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(160px,1fr));gap:10px}}
.race-card{{border:1px solid #dde3ee;border-radius:8px;overflow:hidden;
  background:#fafbfc;transition:.15s}}
.race-card:hover{{border-color:#9ab3cc;box-shadow:0 2px 8px rgba(0,0,0,.12);transform:translateY(-1px)}}
.race-card--ng{{opacity:.5}}
.race-card-link{{display:block;text-decoration:none;color:inherit;padding:10px 12px}}
.race-card-ng{{display:block;padding:10px 12px}}
.rc-header{{display:flex;align-items:center;gap:5px;margin-bottom:5px}}
.rc-no{{font-weight:700;font-size:14px;color:#0d2b5e;min-width:28px}}
.rc-name{{font-size:12px;font-weight:700;color:#1a1a2e;margin-bottom:4px;
  white-space:nowrap;overflow:hidden;text-overflow:ellipsis}}
.rc-meta{{font-size:11px;color:#6b7280;margin-bottom:6px;display:flex;align-items:center;gap:4px}}
.rc-ipat{{font-size:10px;color:#2563eb;font-weight:700;text-align:right}}
.footer{{text-align:center;font-size:11px;color:#9ca3af;margin:20px 0 8px}}
</style>
</head><body>
<div class="header">
  <h1>🏇 D-AI 競馬予想</h1>
  <span class="date-badge">{DATE}</span>
</div>
<div class="meta-bar">
  <span>分析: {ok_count}/{len(results)}レース</span>
  <span>失敗: {len(failed)}レース</span>
  <span>所要: {total_t:.0f}秒</span>
</div>
<div class="container">
  <div class="venue-tabs">
{tab_buttons}  </div>
{tab_panels}
</div>
<div class="footer">D-AI 競馬予想システム</div>
<script>
function showVenue(idx){{
  document.querySelectorAll('.vtab').forEach((t,i)=>t.classList.toggle('active',i===idx));
  document.querySelectorAll('.vpanel').forEach((p,i)=>p.classList.toggle('active',i===idx));
}}
</script>
</body></html>"""

combined_path = f"output/{DATE_KEY}_全レース.html"
# 既存の統合HTMLがあればバックアップ
if os.path.isfile(combined_path):
    import shutil
    try:
        shutil.copy2(combined_path, combined_path.replace(".html", "_prev.html"))
    except Exception:
        pass
with open(combined_path, "w", encoding="utf-8") as f:
    f.write(combined_html)
P(f"  → {combined_path}")

# ダッシュボード更新
try:
    import scripts.generate_portfolio as _gp
    _gp.main()
except Exception as _e:
    logger.warning("dashboard update skipped: %s", _e, exc_info=True)

# 配布用1ファイルHTML生成
P("[bold cyan]\\[N/N+1][/] 配布用HTML生成...")
try:
    import subprocess as _sp
    _r = _sp.run(
        [sys.executable, "run_export_daily.py", DATE],
        cwd=os.path.dirname(os.path.abspath(__file__)),
        capture_output=True, text=True, encoding="utf-8"
    )
    if _r.returncode == 0:
        for _line in _r.stdout.strip().splitlines():
            P(f"  {_line}")
    else:
        logger.warning("配布用HTML生成失敗: %s", _r.stderr[:200])
except Exception as _e:
    logger.warning("export HTML generation skipped: %s", _e, exc_info=True)

# ────────────────────────────────────────────────────────────
# 厩舎コメント + 調教 Bullets 自動生成（v6.0.1 マスター指示 2026-04-23）
#   claude -p subprocess で stable_comment を箇条書き化。
#   失敗しても規則ベース bulletize にフォールバック。
#
# v6.1.22 根本修正（2026-04-25）:
#   paraphrase の timeout を 1800s → 10800s (3 時間) に延長。
#   LLM 呼び出しは 1 件 約 15-20 秒 × 500+ 件 = 約 2 時間 必要。
#   旧 30 分 timeout では途中 stop → bulletize フォールバックも走らず、
#   調教 bullets が 0 件のまま公開される問題が発生していた。
#
#   また bulletize を **paraphrase の結果に関係なく** 独立して必ず実行する
#   よう try/finally 構造に変更。timeout や例外で paraphrase が失敗しても
#   調教コメント bullets は規則ベースで確実に注入される。
# ────────────────────────────────────────────────────────────
P("[bold cyan]\\[N/N+2][/] 厩舎コメント bullets 自動生成...")
import subprocess as _sp
try:
    # 1) Claude CLI paraphrase を試行（成功すれば人間レベル）
    _llm = _sp.run(
        [sys.executable, "scripts/paraphrase_stable_comments.py", DATE],
        cwd=os.path.dirname(os.path.abspath(__file__)),
        capture_output=True, text=True, encoding="utf-8",
        timeout=10800,  # 3 時間: 500 件 LLM 呼び出しを完走可能
    )
    _ok = 0
    for _line in (_llm.stdout or "").strip().splitlines()[-10:]:
        if _line.strip():
            P(f"  {_line}")
        if "注入:" in _line or "キャッシュ命中:" in _line:
            _ok += 1
except _sp.TimeoutExpired:
    logger.warning("paraphrase timeout: 3時間超過しました。bulletize は続行します")
    P("  [WARN] paraphrase timeout → bulletize フォールバックで調教 bullets を注入します")
except Exception as _e:
    logger.warning("paraphrase 実行失敗: %s", _e, exc_info=True)
    P(f"  [WARN] paraphrase 実行失敗: {_e} → bulletize で調教分のみ注入")

# 2) 規則ベース bulletize — paraphrase の成否に関係なく **必ず** 実行
#    特に調教 comment は LLM ではなく規則ベースで bulletize する設計なので、
#    paraphrase timeout で skip されると調教 bullets が 0 件のままになる。
try:
    _rule = _sp.run(
        [sys.executable, "scripts/bulletize_stable_comments.py", DATE],
        cwd=os.path.dirname(os.path.abspath(__file__)),
        capture_output=True, text=True, encoding="utf-8", timeout=120,
    )
    for _line in (_rule.stdout or "").strip().splitlines()[-5:]:
        if _line.strip():
            P(f"  [規則]{_line}")
except Exception as _e:
    logger.warning("bulletize 実行失敗: %s", _e, exc_info=True)

nc = scraper.client
kc = scraper.training.client if hasattr(scraper, "training") and hasattr(scraper.training, "client") else None
ne_c, ne_f, ne_s = getattr(nc, "_stats_cache", 0), getattr(nc, "_stats_fetch", 0), getattr(nc, "_stats_skip", 0)
kb_c, kb_f = (getattr(kc, "_stats_cache", 0), getattr(kc, "_stats_fetch", 0)) if kc else (0, 0)
total_req = ne_c + ne_f + kb_c + kb_f
cache_pct = (ne_c + kb_c) / max(total_req, 1) * 100
P(f"\n[bold green]完了: {ok_count}/{len(results)}レース  総実行時間: {total_t:.0f}秒[/]")
P(f"  リクエスト: netkeiba={ne_c}cache+{ne_f}fetch+{ne_s}skip  keibabook={kb_c}cache+{kb_f}fetch  キャッシュ率={cache_pct:.0f}%")
abs_out = os.path.abspath(combined_path)

# ────────────────────────────────────────────────────────────
# refresh_pred_speed_dev: race_log の speed_dev を pred.json に注入
# (2026-04-28 T-033 再発防止: 毎回 run_analysis_date.py 末尾で自動実行)
# 失敗してもバッチ全体は止めない (warning のみで継続)
# ────────────────────────────────────────────────────────────
try:
    import subprocess as _sp_spd
    _refresh_spd = os.path.join(os.path.dirname(os.path.abspath(__file__)), "scripts", "refresh_pred_speed_dev.py")
    if os.path.exists(_refresh_spd):
        P(f"[bold cyan][N/N+3][/] speed_dev 再注入 (refresh_pred_speed_dev)...")
        _r_spd = _sp_spd.run(
            [sys.executable, _refresh_spd, DATE_KEY],
            cwd=os.path.dirname(os.path.abspath(__file__)),
            capture_output=True, text=True, encoding="utf-8",
            timeout=180,  # 180 秒: 大量レース対応
        )
        if _r_spd.returncode == 0:
            for _line in (_r_spd.stdout or "").strip().splitlines()[-5:]:
                if _line.strip():
                    P(f"  {_line}")
            P(f"[green]  [refresh_pred_speed_dev] {DATE_KEY} 反映完了[/]")
        else:
            logger.warning("refresh_pred_speed_dev 失敗 (rc=%d): %s", _r_spd.returncode, _r_spd.stderr[:300])
            P(f"[yellow]  [WARN] refresh_pred_speed_dev rc={_r_spd.returncode} → pred.json 未更新[/]")
    else:
        logger.warning("refresh_pred_speed_dev.py が見つかりません: %s", _refresh_spd)
except _sp_spd.TimeoutExpired:
    logger.warning("refresh_pred_speed_dev タイムアウト (180秒超過): pred.json speed_dev 未更新")
    P("[yellow]  [WARN] refresh_pred_speed_dev タイムアウト → pred.json 未更新[/]")
except Exception as _e_spd:
    logger.warning("refresh_pred_speed_dev 統合エラー: %s", _e_spd, exc_info=True)
    P(f"[yellow]  [WARN] refresh_pred_speed_dev 例外: {_e_spd}[/]")
P(f"OUTPUT_FILE:{abs_out}")
