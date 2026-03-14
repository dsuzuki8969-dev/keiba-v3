"""
予想データ一括再生成スクリプト（フルパイプライン）

使い方:
  python run_batch_regenerate.py                    # 全日程（Phase1〜4すべて）
  python run_batch_regenerate.py --year 2025        # 2025年のみ
  python run_batch_regenerate.py --start 2025-06-01 --end 2025-12-31  # 範囲指定
  python run_batch_regenerate.py --match-only       # Phase 2〜4のみ（再生成スキップ）
  python run_batch_regenerate.py --resume           # 中断再開モード
  python run_batch_regenerate.py --dry-run          # ドライラン
  python run_batch_regenerate.py --skip-results     # 結果取得スキップ
  python run_batch_regenerate.py --skip-db          # DB更新スキップ

動作（4 Phase）:
  Phase 1: 予想再生成（--ignore-ttl でキャッシュ有効期限を無視）
  Phase 2: 結果取得（レース結果をnetkeiba から取得）
  Phase 3: 結果照合（予想と結果を突き合わせ → 成績タブに反映）
  Phase 4: DB更新（コースDB・騎手/調教師キャッシュを再構築）

ログ: output/batch_regen.log に書き出し
"""

import argparse
import atexit
import io
import json
import logging
import os
import subprocess
import sys
import time
from datetime import datetime

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# ── PIDロック（多重起動防止）──
_PID_LOCK = os.path.join("output", ".batch_regen.pid")

def _acquire_lock():
    """PIDロックを取得。既に別プロセスが動いていれば即終了。"""
    os.makedirs("output", exist_ok=True)
    if os.path.exists(_PID_LOCK):
        try:
            with open(_PID_LOCK, "r") as f:
                old_pid = int(f.read().strip())
            # プロセスが生きているか確認
            import ctypes
            kernel32 = ctypes.windll.kernel32
            handle = kernel32.OpenProcess(0x100000, False, old_pid)  # SYNCHRONIZE
            if handle:
                kernel32.CloseHandle(handle)
                print(f"[FATAL] 別のバッチプロセスが実行中 (PID={old_pid})。終了します。")
                print(f"  強制解除: del output\\.batch_regen.pid")
                sys.exit(1)
            # プロセスが死んでいる → stale lock
            print(f"[WARN] stale lock 検出 (PID={old_pid})。上書きします。")
        except (ValueError, OSError):
            pass
    with open(_PID_LOCK, "w") as f:
        f.write(str(os.getpid()))
    atexit.register(_release_lock)

def _release_lock():
    """終了時にロックファイルを削除"""
    try:
        if os.path.exists(_PID_LOCK):
            with open(_PID_LOCK, "r") as f:
                if f.read().strip() == str(os.getpid()):
                    os.remove(_PID_LOCK)
    except Exception:
        pass

# ── nohup検出: TTYがなければRich無効 ──
_IS_BACKGROUND = not sys.stdout.isatty()

if not _IS_BACKGROUND:
    try:
        from rich.console import Console
        console = Console()
        P = console.print
    except ImportError:
        P = print
else:
    P = print

# ── コンソール出力もログファイルに複製 ──
_stdout_log = os.path.join("output", "batch_regen_progress.log")
def _P_and_log(msg, **kwargs):
    """コンソール＋進捗ログファイルの両方に出力"""
    try:
        P(msg, **kwargs)
    except Exception:
        pass
    try:
        import re
        clean = re.sub(r'\[/?[a-z# ]+\]', '', str(msg))  # Rich tag除去
        with open(_stdout_log, "a", encoding="utf-8") as f:
            f.write(clean + "\n")
    except Exception:
        pass

# ── ログ設定 ──
os.makedirs("output", exist_ok=True)
_log_path = os.path.join("output", "batch_regen.log")
_file_handler = logging.FileHandler(_log_path, encoding="utf-8", mode="a")
_file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
_logger = logging.getLogger("batch_regen")
_logger.setLevel(logging.INFO)
_logger.addHandler(_file_handler)


def log(msg: str, level: str = "info"):
    """コンソールとファイルの両方にログを出す"""
    getattr(_logger, level, _logger.info)(msg)


# ============================================================
# ユーティリティ
# ============================================================


def get_prediction_dates(year=None, start=None, end=None):
    """DB から予想日付一覧を取得"""
    from src.database import get_db, init_schema

    init_schema()
    conn = get_db()
    rows = conn.execute(
        "SELECT DISTINCT date FROM predictions ORDER BY date"
    ).fetchall()
    dates = [r["date"] for r in rows]

    if year:
        dates = [d for d in dates if d.startswith(str(year))]
    if start:
        dates = [d for d in dates if d >= start]
    if end:
        dates = [d for d in dates if d <= end]

    return dates


def load_done_marker(path):
    """完了マーカーファイルを読み込み"""
    if not os.path.exists(path):
        return set()
    with open(path, "r") as f:
        return {line.strip() for line in f if line.strip()}


def save_done_marker(path, date):
    """完了マーカーに日付を追記"""
    with open(path, "a") as f:
        f.write(date + "\n")


def fmt_elapsed(seconds: float) -> str:
    """秒を '3h 12m' 形式にフォーマット"""
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    if h > 0:
        return f"{h}h {m:02d}m"
    return f"{m}m {int(seconds % 60):02d}s"


def fmt_eta(avg_sec: float, remaining: int) -> str:
    """ETA を計算して表示"""
    eta_sec = avg_sec * remaining
    return fmt_elapsed(eta_sec)


# ============================================================
# Phase 1: 予想再生成（インプロセス — 初期化1回で全日付を高速処理）
# ============================================================


_PREFETCH_WORKERS = 5     # 並列プリフェッチワーカー数
_DATE_TIMEOUT = 10800     # 1日あたりの最大処理時間（秒）= 3時間


def _init_engine_context():
    """エンジン実行に必要な基盤オブジェクトを1回だけ初期化して返す。"""
    from data.masters.course_master import get_all_courses
    from src.scraper.auth import PremiumNetkeibaScraper
    from src.scraper.race_results import (
        StandardTimeDBBuilder, Last3FDBBuilder,
        load_trainer_baseline_db,
    )
    from src.scraper.course_db_collector import load_preload_course_db
    from config.settings import COURSE_DB_PRELOAD_PATH, TRAINER_BASELINE_DB_PATH

    P("  [1/3] scraper 初期化・ログイン...")
    all_courses = get_all_courses()
    scraper = PremiumNetkeibaScraper(all_courses, ignore_ttl=True)
    scraper.login()
    scraper.training.login()

    P("  [2/3] 基準タイムDB読み込み...")
    std_db = StandardTimeDBBuilder()
    course_db_base = std_db.get_course_db()
    preload = load_preload_course_db(COURSE_DB_PRELOAD_PATH)
    for cid, runs in preload.items():
        course_db_base.setdefault(cid, []).extend(runs)
    P(f"       {len(course_db_base)}コース / {sum(len(v) for v in course_db_base.values()):,}走")

    P("  [3/3] 補助DB + ワーカープール構築...")
    trainer_baseline_db = load_trainer_baseline_db(TRAINER_BASELINE_DB_PATH)

    # scraper ワーカープールを事前に作成
    workers = [scraper] + [scraper.clone_worker() for _ in range(_PREFETCH_WORKERS - 1)]

    ctx = {
        "scraper": scraper,
        "workers": workers,
        "all_courses": all_courses,
        "course_db_base": course_db_base,
        "trainer_baseline_db": trainer_baseline_db,
    }
    return ctx


def _run_date_inprocess(date, ctx):
    """1日分の予想をインプロセスで実行。成功時は (True, レース数) を返す。"""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from src.scraper.race_results import (
        Last3FDBBuilder,
        build_course_db_from_past_runs,
        build_course_style_stats_db, build_gate_bias_db,
        build_position_sec_per_rank_db,
        build_trainer_baseline_db as build_tb, merge_trainer_baseline,
    )
    from src.scraper.personnel import PersonnelDBManager, enrich_personnel_with_condition_records
    from src.engine import RaceAnalysisEngine, enrich_course_aptitude_with_style_bias
    from src.scraper.improvement_dbs import build_bloodline_db
    from src.results_tracker import save_prediction
    from config.settings import BLOODLINE_DB_PATH

    scraper = ctx["scraper"]
    workers = ctx["workers"]
    all_courses = ctx["all_courses"]
    course_db_base = ctx["course_db_base"]
    trainer_baseline_db = ctx["trainer_baseline_db"]

    # 日付ごとにスタイルDB等を構築
    course_style_db = build_course_style_stats_db(course_db_base, target_date=date)
    gate_bias_db    = build_gate_bias_db(course_db_base, target_date=date)
    position_sec_db = build_position_sec_per_rank_db(course_db_base, target_date=date)

    # レースID取得（帯広/ばんえいを除外）
    from data.masters.venue_master import is_banei, get_venue_code_from_race_id
    race_ids = scraper.fetch_date(date)
    race_ids = [r for r in race_ids if not is_banei(get_venue_code_from_race_id(r) or "")]
    if not race_ids:
        return False, 0

    # ★ 並列プリフェッチ（5ワーカー）
    prefetched = {}

    def _fetch_one(args):
        idx, rid = args
        w = workers[idx % len(workers)]
        for attempt in range(2):
            try:
                ri, hs = w.fetch_race(rid, fetch_history=True, fetch_odds=True, fetch_training=True, target_date=date)
                return rid, ri, hs
            except Exception as e:
                if attempt == 0:
                    _logger.debug("prefetch retry %s: %s", rid, e)
        return rid, None, []

    try:
        with ThreadPoolExecutor(max_workers=len(workers)) as pool:
            futs = {pool.submit(_fetch_one, (i, rid)): rid for i, rid in enumerate(race_ids)}
            for fut in as_completed(futs):
                try:
                    rid, ri, hs = fut.result(timeout=600)
                    prefetched[rid] = (ri, hs)
                except Exception as e:
                    _logger.warning("prefetch failed: %s", e)
    except Exception as e:
        _logger.error("prefetch pool error for %s: %s", date, e)

    # 各レース分析（直列 + PersonnelDBManager日単位再利用 + DBキャッシュ）
    analyses_by_venue = {}
    ok_count = 0
    personnel_mgr = PersonnelDBManager()
    n_total = len(race_ids)
    for ri, race_id in enumerate(race_ids):
        try:
            race_info, horses = prefetched.get(race_id, (None, []))
            if not race_info or not horses:
                continue

            race_t0 = time.time()
            course_db = build_course_db_from_past_runs(horses, dict(course_db_base), target_date=date)
            l3f_db    = Last3FDBBuilder().build(course_db)
            jockey_db, trainer_db = personnel_mgr.build_from_horses(horses, scraper.client, course_db=course_db)
            enrich_personnel_with_condition_records(jockey_db, trainer_db, course_db)
            baseline_new = build_tb(horses)
            trainer_baseline_db = merge_trainer_baseline(baseline_new, trainer_baseline_db)
            build_bloodline_db(horses, netkeiba_client=scraper.client, cache_path=BLOODLINE_DB_PATH)

            engine = RaceAnalysisEngine(
                course_db=course_db, all_courses=all_courses,
                jockey_db=jockey_db, trainer_db=trainer_db,
                trainer_baseline_db=trainer_baseline_db,
                pace_last3f_db=l3f_db,
                course_style_stats_db=course_style_db,
                gate_bias_db=gate_bias_db,
                position_sec_per_rank_db=position_sec_db,
                is_jra=race_info.is_jra,
                target_date=date,
            )
            analysis = engine.analyze(race_info, horses, custom_stake=None, netkeiba_client=scraper.client)
            analysis = enrich_course_aptitude_with_style_bias(engine, analysis)

            venue = race_info.venue
            race_no = race_info.race_no
            analyses_by_venue.setdefault(venue, {})[race_no] = analysis
            ok_count += 1
            race_elapsed = time.time() - race_t0
            if (ri + 1) % 10 == 0 or ri == n_total - 1:
                _logger.info("  %s: %d/%d races done (%.1fs/race)", date, ri+1, n_total, race_elapsed)
        except Exception as e:
            _logger.warning("race analysis failed %s: %s", race_id, e, exc_info=True)

    # 予想JSON保存
    if analyses_by_venue:
        try:
            save_prediction(date, analyses_by_venue)
        except Exception as e:
            _logger.warning("save_prediction failed %s: %s", date, e, exc_info=True)

    ctx["trainer_baseline_db"] = trainer_baseline_db
    return True, ok_count


def phase1_predict(dates, done_dates, script_dir, t0):
    """Phase 1: 予想再生成（順次日付 × 並列プリフェッチ）"""
    remaining = [d for d in dates if d not in done_dates]
    if not remaining:
        P("  [green]全日付完了済み[/]")
        return 0, 0

    _P_and_log(f"Phase 1: 予想再生成（{len(remaining)}日・{_PREFETCH_WORKERS}並列fetch）")
    log(f"Phase 1 開始: {len(remaining)}日 (prefetch×{_PREFETCH_WORKERS})")

    # 基盤を1回だけ初期化
    _P_and_log("  エンジン基盤 初期化中...")
    init_t0 = time.time()
    ctx = _init_engine_context()
    _P_and_log(f"  初期化完了: {time.time() - init_t0:.1f}s")

    marker_path = os.path.join(script_dir, "output", ".batch_regen_done.txt")
    success = 0
    fail = 0
    phase_t0 = time.time()

    for i, date in enumerate(remaining):
        date_t0 = time.time()
        try:
            # タイムアウト付き実行（ThreadPoolで包む）
            from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutTimeout
            with ThreadPoolExecutor(max_workers=1) as ex:
                fut = ex.submit(_run_date_inprocess, date, ctx)
                ok, n_races = fut.result(timeout=_DATE_TIMEOUT)
            elapsed = time.time() - phase_t0
            avg = elapsed / (i + 1)
            eta = fmt_eta(avg, len(remaining) - (i + 1))
            date_elapsed = time.time() - date_t0
            if ok and n_races > 0:
                success += 1
                msg = f"  [{i+1}/{len(remaining)}] {date} ✓ {n_races}R ({fmt_elapsed(date_elapsed)})  ETA: {eta}"
                _P_and_log(msg)
                save_done_marker(marker_path, date)
                log(f"  ✓ {date} {n_races}R ({fmt_elapsed(date_elapsed)})  ETA: {eta}")
            else:
                fail += 1
                msg = f"  [{i+1}/{len(remaining)}] {date} — スキップ ({fmt_elapsed(date_elapsed)})"
                _P_and_log(msg)
                save_done_marker(marker_path, date)
                log(f"  — {date}: スキップ")
        except FutTimeout:
            fail += 1
            msg = f"  [{i+1}/{len(remaining)}] {date} ✗ タイムアウト({_DATE_TIMEOUT}s)"
            _P_and_log(msg)
            log(f"  ✗ {date}: タイムアウト({_DATE_TIMEOUT}s)", "error")
            save_done_marker(marker_path, date)  # スキップして次へ
        except Exception as e:
            fail += 1
            msg = f"  [{i+1}/{len(remaining)}] {date} ✗ {e}"
            _P_and_log(msg)
            log(f"  ✗ {date}: {e}", "error")
            import traceback
            _logger.error(traceback.format_exc())

    phase1_elapsed = time.time() - phase_t0
    _P_and_log(f"  Phase 1 完了: {success}成功 / {fail}失敗  ({fmt_elapsed(phase1_elapsed)})")
    log(f"Phase 1 完了: {success}成功 / {fail}失敗  ({fmt_elapsed(phase1_elapsed)})")
    return success, fail


# ============================================================
# Phase 2: 結果取得
# ============================================================


def phase2_fetch_results(dates):
    """Phase 2: 全日付の結果を取得"""
    from config.settings import RESULTS_DIR

    # 結果JSON未取得の日付を特定
    missing = []
    for d in dates:
        fpath = os.path.join(RESULTS_DIR, f"{d.replace('-', '')}_results.json")
        if not os.path.exists(fpath):
            missing.append(d)

    P(f"\n[bold cyan]Phase 2: 結果取得（{len(missing)}/{len(dates)}日未取得）[/]")
    log(f"Phase 2 開始: {len(missing)}日未取得")

    if not missing:
        P("  [green]全日付取得済み[/]")
        return 0, 0

    from src.scraper.netkeiba import NetkeibaClient
    from src.results_tracker import fetch_actual_results

    client = NetkeibaClient(ignore_ttl=True)
    success = 0
    fail = 0
    phase_t0 = time.time()

    for i, date in enumerate(missing):
        elapsed = time.time() - phase_t0
        if i > 0:
            avg_time = elapsed / i
            eta_str = f"  ETA: {fmt_eta(avg_time, len(missing) - i)}"
        else:
            eta_str = ""

        P(f"  [{i + 1}/{len(missing)}] {date} ...{eta_str}", end="")
        try:
            results = fetch_actual_results(date, client)
            if results:
                n_races = len(results)
                success += 1
                P(f"  [green]✓[/] {n_races}R")
                log(f"  ✓ {date}: {n_races}R")
            else:
                fail += 1
                P(f"  [yellow]— 結果なし[/]")
                log(f"  — {date}: 結果なし")
        except Exception as e:
            fail += 1
            P(f"  [red]✗ {e}[/]")
            log(f"  ✗ {date}: {e}", "error")

    phase2_elapsed = time.time() - phase_t0
    P(f"\n  Phase 2 完了: {success}成功 / {fail}失敗  ({fmt_elapsed(phase2_elapsed)})")
    log(f"Phase 2 完了: {success}成功 / {fail}失敗  ({fmt_elapsed(phase2_elapsed)})")
    return success, fail


# ============================================================
# Phase 3: 結果照合
# ============================================================


def phase3_match(dates):
    """Phase 3: 全日付の結果照合"""
    from src.results_tracker import compare_and_aggregate

    P(f"\n[bold cyan]Phase 3: 結果照合（{len(dates)}日）[/]")
    log(f"Phase 3 開始: {len(dates)}日")

    matched = 0
    failed = 0
    total_races = 0
    total_stake = 0
    total_return = 0
    phase_t0 = time.time()

    for i, date in enumerate(dates):
        try:
            r = compare_and_aggregate(date)
            if r:
                matched += 1
                total_races += r.get("total_races", 0)
                total_stake += r.get("total_stake", 0)
                total_return += r.get("total_return", 0)
            else:
                failed += 1
        except Exception as e:
            P(f"  [red]照合エラー {date}: {e}[/]")
            log(f"  ✗ 照合 {date}: {e}", "error")
            failed += 1

        # 100日ごとに進捗表示
        if (i + 1) % 100 == 0:
            P(f"  ... {i + 1}/{len(dates)} 照合済み")

    phase3_elapsed = time.time() - phase_t0
    P(f"\n  Phase 3 完了: {matched}成功 / {failed}失敗  ({fmt_elapsed(phase3_elapsed)})")
    log(f"Phase 3 完了: {matched}成功 / {failed}失敗  ({fmt_elapsed(phase3_elapsed)})")

    return {
        "matched": matched,
        "failed": failed,
        "total_races": total_races,
        "total_stake": total_stake,
        "total_return": total_return,
    }


# ============================================================
# Phase 4: DB更新
# ============================================================


def phase4_db_update(dates):
    """Phase 4: コースDB・人事DBを再構築"""
    P(f"\n[bold cyan]Phase 4: DB更新[/]")
    log("Phase 4 開始")
    phase_t0 = time.time()

    try:
        P("  [1/2] コースDB更新中...")
        from config.settings import COURSE_DB_PRELOAD_PATH
        from src.scraper.course_db_collector import collect_course_db_from_results
        from src.scraper.netkeiba import NetkeibaClient, RaceListScraper

        client = NetkeibaClient(ignore_ttl=True)
        rls = RaceListScraper(client)
        sd = dates[0] if dates else "2024-01-01"
        ed = dates[-1] if dates else "2026-03-05"

        state_path = os.path.join("data", "course_db_collector_state.json")
        collect_course_db_from_results(
            client,
            rls,
            sd,
            ed,
            COURSE_DB_PRELOAD_PATH,
            state_path=state_path,
        )
        P("  [green]✓ コースDB更新完了[/]")
        log("  ✓ コースDB更新完了")

        P("  [2/2] 騎手・調教師キャッシュリセット...")
        # インメモリキャッシュはプロセスごとなので、ここではスキップ
        P("  [green]✓ 完了（次回ダッシュボード起動時に再構築）[/]")
        log("  ✓ 騎手・調教師キャッシュリセット完了")
    except Exception as e:
        P(f"  [red]✗ DB更新エラー: {e}[/]")
        log(f"  ✗ DB更新エラー: {e}", "error")

    phase4_elapsed = time.time() - phase_t0
    P(f"\n  Phase 4 完了: {fmt_elapsed(phase4_elapsed)}")
    log(f"Phase 4 完了: {fmt_elapsed(phase4_elapsed)}")


# ============================================================
# サマリー
# ============================================================


def print_summary(match_result, total_elapsed, phase1_stats=None):
    """最終サマリー出力"""
    P("\n" + "=" * 60)
    P("[bold green]◆ 一括再生成パイプライン 完了[/]")

    if phase1_stats:
        s, f = phase1_stats
        P(f"  Phase 1 (予想再生成): {s}成功 / {f}失敗")

    P(f"  Phase 3 (照合): {match_result['matched']}成功 / {match_result['failed']}失敗")
    P(f"  総レース数: {match_result['total_races']:,}")
    P(f"  総投資額: ¥{match_result['total_stake']:,}")
    P(f"  総回収額: ¥{match_result['total_return']:,}")
    if match_result["total_stake"] > 0:
        roi = match_result["total_return"] / match_result["total_stake"] * 100
        P(f"  回収率: {roi:.1f}%")
    P(f"  総所要時間: {fmt_elapsed(total_elapsed)}")
    P("=" * 60)

    log(
        f"完了: 照合 {match_result['matched']}日 / "
        f"レース {match_result['total_races']:,} / "
        f"投資 ¥{match_result['total_stake']:,} / "
        f"回収 ¥{match_result['total_return']:,} / "
        f"時間 {fmt_elapsed(total_elapsed)}"
    )


# ============================================================
# メイン
# ============================================================


def main():
    _acquire_lock()

    parser = argparse.ArgumentParser(
        description="予想データ一括再生成（フルパイプライン）"
    )
    parser.add_argument("--year", type=int, help="対象年（例: 2025）")
    parser.add_argument("--start", type=str, help="開始日（例: 2025-01-01）")
    parser.add_argument("--end", type=str, help="終了日（例: 2025-12-31）")
    parser.add_argument(
        "--match-only", action="store_true", help="Phase 2〜4のみ（予想再生成スキップ）"
    )
    parser.add_argument("--resume", action="store_true", help="中断再開モード")
    parser.add_argument(
        "--dry-run", action="store_true", help="ドライラン（対象日付一覧のみ表示）"
    )
    parser.add_argument(
        "--skip-results", action="store_true", help="結果取得スキップ（Phase 2）"
    )
    parser.add_argument(
        "--skip-db", action="store_true", help="DB更新スキップ（Phase 4）"
    )
    args = parser.parse_args()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    marker_path = os.path.join(script_dir, "output", ".batch_regen_done.txt")
    os.makedirs(os.path.join(script_dir, "output"), exist_ok=True)

    P("\n[bold white on #0d2b5e]  D-AI 競馬予想  一括再生成パイプライン  [/]\n")
    log("=" * 50)
    log("一括再生成パイプライン 開始")

    # 対象日付取得
    dates = get_prediction_dates(year=args.year, start=args.start, end=args.end)
    if not dates:
        P("[red]対象日付がありません[/]")
        return

    P(f"  対象: {len(dates)}日（{dates[0]} 〜 {dates[-1]}）")
    log(f"対象: {len(dates)}日 ({dates[0]} 〜 {dates[-1]})")

    # フェーズ表示
    phases = []
    if not args.match_only:
        phases.append("Phase 1: 予想再生成")
    if not args.skip_results:
        phases.append("Phase 2: 結果取得")
    phases.append("Phase 3: 結果照合")
    if not args.skip_db:
        phases.append("Phase 4: DB更新")
    P(f"  実行フェーズ: {' → '.join(phases)}")

    # 中断再開: 完了済みをスキップ
    done_dates = set()
    if args.resume:
        done_dates = load_done_marker(marker_path)
        if done_dates:
            P(f"  [yellow]中断再開: {len(done_dates)}日完了済み → スキップ[/]")
            log(f"中断再開: {len(done_dates)}日完了済み")
    elif not args.match_only:
        # 新規開始時はマーカーリセット
        if os.path.exists(marker_path):
            os.remove(marker_path)

    remaining = [d for d in dates if d not in done_dates]
    P(f"  処理対象: {len(remaining)}日")

    if args.dry_run:
        P("\n[cyan]--- ドライラン: 対象日付一覧 ---[/]")
        for d in remaining:
            P(f"  {d}")
        P(f"\n  合計: {len(remaining)}日")
        return

    t0 = time.time()

    # ── Phase 1: 予想再生成 ──
    phase1_stats = None
    if not args.match_only:
        s, f = phase1_predict(remaining, set(), script_dir, t0)
        phase1_stats = (s, f)
        # 完了マーカークリーンアップ
        if f == 0 and os.path.exists(marker_path):
            os.remove(marker_path)
    else:
        P("\n[yellow]Phase 1 スキップ（--match-only）[/]")
        log("Phase 1 スキップ")

    # ── Phase 2: 結果取得 ──
    if not args.skip_results:
        phase2_fetch_results(dates)
    else:
        P("\n[yellow]Phase 2 スキップ（--skip-results）[/]")
        log("Phase 2 スキップ")

    # ── Phase 3: 結果照合 ──
    match_result = phase3_match(dates)

    # ── Phase 4: DB更新 ──
    if not args.skip_db:
        phase4_db_update(dates)
    else:
        P("\n[yellow]Phase 4 スキップ（--skip-db）[/]")
        log("Phase 4 スキップ")

    total_elapsed = time.time() - t0
    print_summary(match_result, total_elapsed, phase1_stats)


if __name__ == "__main__":
    main()
