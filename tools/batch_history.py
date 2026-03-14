# -*- coding: utf-8 -*-
"""
過去予想バッチ生成スクリプト

指定期間の JRA+NAR 全レースを遡及予想し、keiba.db に反映する。
HTML は生成せず、軽量テキストサマリーのみ保存。

使い方:
  python tools/batch_history.py
  python tools/batch_history.py --from 2025-01-01 --to 2026-03-01
  python tools/batch_history.py --from 2025-06-01         # 途中から再開
  python tools/batch_history.py --results-only            # 予想済みの結果照合のみ
  python tools/batch_history.py --skip-results            # 結果照合をスキップ

進捗ログ: data/batch_history.log
テキスト出力: data/predictions/YYYYMMDD_summary.txt
"""

import argparse
import copy
import json
import os
import sys
import time
import traceback
from datetime import date, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
os.chdir(PROJECT_ROOT)

import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True)
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", line_buffering=True)

from src.log import get_logger
logger = get_logger(__name__)

# ── tqdm オプション ─────────────────────────────────────────────────────
try:
    from tqdm import tqdm
    _HAS_TQDM = True
except ImportError:
    def tqdm(it, **kw):
        return it
    _HAS_TQDM = False

# ── カラーログ ─────────────────────────────────────────────────────────
try:
    from rich.console import Console
    _console = Console()
    def _p(msg): _console.print(msg)
except ImportError:
    def _p(msg): print(msg)

# ─────────────────────────────────────────────────────────────────────────────
# 日付ユーティリティ
# ─────────────────────────────────────────────────────────────────────────────

def daterange(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def _fmt_elapsed(sec: float) -> str:
    h = int(sec // 3600)
    m = int((sec % 3600) // 60)
    s = int(sec % 60)
    if h:
        return f"{h}h{m:02d}m{s:02d}s"
    return f"{m}m{s:02d}s"


def _eta(elapsed: float, done: int, total: int) -> str:
    if done == 0:
        return "?"
    per = elapsed / done
    remain = per * (total - done)
    return _fmt_elapsed(remain)


# ─────────────────────────────────────────────────────────────────────────────
# テキストサマリー生成
# ─────────────────────────────────────────────────────────────────────────────

_MARK_ORDER = {"◉": 0, "◎": 1, "○": 2, "▲": 3, "△": 4, "★": 5, "☆": 6, "×": 7}

def _build_text_summary(date_str: str, analyses_by_venue: dict) -> str:
    """分析結果から軽量テキストサマリーを生成"""
    lines = []
    lines.append(f"=== {date_str} ===")

    for venue, race_map in sorted(analyses_by_venue.items()):
        lines.append(f"\n[{venue}]")
        for race_no, analysis in sorted(race_map.items()):
            race_info = analysis.race
            race_name = race_info.race_name or f"{venue}{race_no}R"
            course = race_info.course
            surf = getattr(course, "surface", "") if course else ""
            dist = getattr(course, "distance", 0) if course else 0
            heads = len(analysis.evaluations)

            lines.append(
                f"  {race_no:2d}R [{race_name}] "
                f"{surf}{dist}m {heads}頭 "
                f"自信:{analysis.overall_confidence.value if analysis.overall_confidence else '?'}"
            )

            # 印順で馬を出力
            evs = sorted(
                analysis.evaluations,
                key=lambda e: (_MARK_ORDER.get(e.mark.value, 9), e.horse.horse_no)
                if e.mark else (9, e.horse.horse_no)
            )
            for ev in evs:
                if not ev.mark or ev.mark.value in ("-", ""):
                    continue
                mk = ev.mark.value
                no = ev.horse.horse_no
                nm = ev.horse.horse_name
                # 穴/危険 判定
                ana_t = getattr(ev, "ana_type", None)
                kiken_t = getattr(ev, "kiken_type", None)
                tag = ""
                if ana_t and ana_t.value not in ("none", "なし", "該当なし", "-", ""):
                    tag = "【穴】"
                elif kiken_t and kiken_t.value not in ("none", "なし", "該当なし", "-", ""):
                    tag = "【危】"
                lines.append(f"    {mk:2s}  {no:2d} {nm}{tag}")

            # 買い目（stake > 0 のみ）
            fm = analysis.formation or {}
            ticket_parts = []
            for t in fm.get("umaren", []):
                if t.get("stake", 0) > 0:
                    ticket_parts.append(
                        f"馬連 {t['a']}-{t['b']} EV{t.get('ev',0):.0f}%"
                    )
            for t in fm.get("sanrenpuku", []):
                if t.get("stake", 0) > 0:
                    ticket_parts.append(
                        f"三連複 {t['a']}-{t['b']}-{t['c']} EV{t.get('ev',0):.0f}%"
                    )
            if ticket_parts:
                lines.append(f"    買: " + " | ".join(ticket_parts))
            else:
                lines.append("    買: 見送り")

    lines.append("")
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# 進捗ログ
# ─────────────────────────────────────────────────────────────────────────────

LOG_PATH = PROJECT_ROOT / "data" / "batch_history.log"

def _log(msg: str):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(line + "\n")


# ─────────────────────────────────────────────────────────────────────────────
# 予測済みチェック
# ─────────────────────────────────────────────────────────────────────────────

def _pred_exists(date_str: str) -> bool:
    """予想JSONが既に存在するか確認"""
    from config.settings import PREDICTIONS_DIR
    fpath = os.path.join(PREDICTIONS_DIR, f"{date_str.replace('-','')}_pred.json")
    return os.path.exists(fpath)


def _results_exist(date_str: str) -> bool:
    """結果JSONが既に存在するか確認"""
    from config.settings import RESULTS_DIR
    fpath = os.path.join(RESULTS_DIR, f"{date_str.replace('-','')}_results.json")
    return os.path.exists(fpath)


def _summary_exists(date_str: str) -> bool:
    """テキストサマリーが既に存在するか確認"""
    from config.settings import PREDICTIONS_DIR
    fpath = os.path.join(PREDICTIONS_DIR, f"{date_str.replace('-','')}_summary.txt")
    return os.path.exists(fpath)


# ─────────────────────────────────────────────────────────────────────────────
# メイン処理: 1日分の予想
# ─────────────────────────────────────────────────────────────────────────────

def analyze_one_date(
    date_str: str,
    scraper,
    all_courses,
    course_db_base: dict,
    course_style_db: dict,
    gate_bias_db: dict,
    position_sec_db: dict,
    l3f_db_base,
    trainer_baseline_db: dict,
) -> dict:
    """
    1日分のレースを分析して analyses_by_venue を返す。
    失敗した race_id は failed リストに追加。
    Returns: {"analyses": {venue: {race_no: analysis}}, "failed": [...], "ok": int, "total": int}
    """
    from src.scraper.race_results import (
        Last3FDBBuilder, build_course_db_from_past_runs,
        build_trainer_baseline_db as build_tb_db,
        merge_trainer_baseline,
    )
    from src.scraper.personnel import PersonnelDBManager, enrich_personnel_with_condition_records
    from src.engine import RaceAnalysisEngine, enrich_course_aptitude_with_style_bias
    from src.scraper.improvement_dbs import build_bloodline_db
    from config.settings import BLOODLINE_DB_PATH

    race_ids = scraper.fetch_date(date_str)
    if not race_ids:
        return {"analyses": {}, "failed": [], "ok": 0, "total": 0}

    analyses_by_venue: dict = {}
    failed = []
    ok_count = 0

    for race_id in race_ids:
        try:
            race_info, horses = scraper.fetch_race(
                race_id, fetch_history=True, fetch_odds=True, fetch_training=True
            )
            if not race_info or not horses:
                failed.append(race_id)
                continue

            course_db = build_course_db_from_past_runs(horses, dict(course_db_base))
            l3f_db = Last3FDBBuilder().build(course_db)
            personnel_mgr = PersonnelDBManager()
            jockey_db, trainer_db = personnel_mgr.build_from_horses(
                horses, scraper.client, course_db=course_db
            )
            enrich_personnel_with_condition_records(jockey_db, trainer_db, course_db)
            baseline_new = build_tb_db(horses)
            tb_db = merge_trainer_baseline(baseline_new, trainer_baseline_db)
            build_bloodline_db(horses, netkeiba_client=scraper.client, cache_path=BLOODLINE_DB_PATH)

            engine = RaceAnalysisEngine(
                course_db=course_db,
                all_courses=all_courses,
                jockey_db=jockey_db,
                trainer_db=trainer_db,
                trainer_baseline_db=tb_db,
                pace_last3f_db=l3f_db,
                course_style_stats_db=course_style_db,
                gate_bias_db=gate_bias_db,
                position_sec_per_rank_db=position_sec_db,
                is_jra=race_info.is_jra,
            )
            analysis = engine.analyze(race_info, horses, custom_stake=None, netkeiba_client=scraper.client)
            analysis = enrich_course_aptitude_with_style_bias(engine, analysis)

            venue = race_info.venue
            race_no = race_info.race_no
            if venue not in analyses_by_venue:
                analyses_by_venue[venue] = {}
            analyses_by_venue[venue][race_no] = analysis
            ok_count += 1

        except Exception as e:
            logger.warning("race %s failed: %s", race_id, e, exc_info=True)
            failed.append(race_id)

    return {
        "analyses": analyses_by_venue,
        "failed": failed,
        "ok": ok_count,
        "total": len(race_ids),
    }


# ─────────────────────────────────────────────────────────────────────────────
# メインループ
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="過去予想バッチ生成")
    parser.add_argument("--from", dest="date_from", default="2025-01-01",
                        help="開始日 YYYY-MM-DD (default: 2025-01-01)")
    parser.add_argument("--to", dest="date_to", default="2026-03-01",
                        help="終了日 YYYY-MM-DD (default: 2026-03-01)")
    parser.add_argument("--results-only", action="store_true",
                        help="予想済みの結果照合のみ実行（予想は生成しない）")
    parser.add_argument("--skip-results", action="store_true",
                        help="実際の着順取得・照合をスキップ")
    parser.add_argument("--force", action="store_true",
                        help="既存の予想JSONを上書きして再実行")
    args = parser.parse_args()

    date_from = date.fromisoformat(args.date_from)
    date_to   = date.fromisoformat(args.date_to)
    today     = date.today()

    all_dates = list(daterange(date_from, min(date_to, today)))
    total_dates = len(all_dates)

    _log(f"バッチ開始: {args.date_from} → {args.date_to.split('T')[0] if 'T' in args.date_to else args.date_to}"
         f"  ({total_dates}日間)")
    _log(f"results-only={args.results_only}  skip-results={args.skip_results}  force={args.force}")

    batch_t0 = time.time()

    # ── 共通DBを一度だけロード ──────────────────────────────────────────
    if not args.results_only:
        _log("[1/3] 共通DB読み込み中...")
        t_init = time.time()

        from data.masters.course_master import get_all_courses
        from src.scraper.auth import PremiumNetkeibaScraper
        from src.scraper.race_results import (
            StandardTimeDBBuilder, Last3FDBBuilder,
            build_course_style_stats_db, build_gate_bias_db,
            build_position_sec_per_rank_db,
            load_trainer_baseline_db, merge_trainer_baseline,
        )
        from src.scraper.course_db_collector import load_preload_course_db
        from config.settings import COURSE_DB_PRELOAD_PATH, TRAINER_BASELINE_DB_PATH
        from src.scraper.netkeiba import purge_old_cache

        all_courses = get_all_courses()
        scraper = PremiumNetkeibaScraper(all_courses)
        scraper.login()
        scraper.training.login()

        purge_old_cache(max_age_days=30)

        std_db = StandardTimeDBBuilder()
        course_db_base = std_db.get_course_db()
        preload = load_preload_course_db(COURSE_DB_PRELOAD_PATH)
        for cid, runs in preload.items():
            course_db_base.setdefault(cid, []).extend(runs)

        course_style_db = build_course_style_stats_db(course_db_base)
        gate_bias_db    = build_gate_bias_db(course_db_base)
        position_sec_db = build_position_sec_per_rank_db(course_db_base)
        l3f_db_base     = Last3FDBBuilder().build(course_db_base)
        trainer_baseline_db = load_trainer_baseline_db(TRAINER_BASELINE_DB_PATH)

        _log(f"  共通DB: {len(course_db_base)}コース  ({_fmt_elapsed(time.time()-t_init)})")
    else:
        _log("[1/3] results-only モード: 共通DB読み込みスキップ")
        scraper = all_courses = course_db_base = None
        course_style_db = gate_bias_db = position_sec_db = l3f_db_base = None
        trainer_baseline_db = None

    # ── 予想済み日付をカウント ──────────────────────────────────────────
    already_done = [d for d in all_dates if _pred_exists(d.isoformat())]
    _log(f"[2/3] 対象: {total_dates}日  既存: {len(already_done)}日  未処理: {total_dates - len(already_done)}日")

    # ── ループ ─────────────────────────────────────────────────────────
    _log("[3/3] 予想・照合ループ開始...")

    stats = {
        "pred_ok": 0, "pred_skip": 0, "pred_fail": 0,
        "results_ok": 0, "results_skip": 0, "results_fail": 0,
        "compare_ok": 0,
    }
    date_elapsed = []

    from src.results_tracker import (
        save_prediction, fetch_actual_results, compare_and_aggregate
    )
    from config.settings import PREDICTIONS_DIR

    for idx, d in enumerate(all_dates):
        date_str = d.isoformat()
        date_key = d.strftime("%Y%m%d")
        d_t0 = time.time()

        _log(f"\n--- {date_str} ({idx+1}/{total_dates}) ---")

        # ── 予想生成 ──────────────────────────────────────────────────
        if not args.results_only:
            if _pred_exists(date_str) and not args.force:
                _log(f"  予想: スキップ（既存）")
                stats["pred_skip"] += 1
            else:
                try:
                    result = analyze_one_date(
                        date_str, scraper, all_courses,
                        course_db_base, course_style_db, gate_bias_db,
                        position_sec_db, l3f_db_base, trainer_baseline_db,
                    )
                    analyses = result["analyses"]
                    if analyses:
                        # JSON + DB 保存
                        pred_path = save_prediction(date_str, analyses)
                        stats["pred_ok"] += 1

                        # テキストサマリー保存
                        txt = _build_text_summary(date_str, analyses)
                        summary_path = os.path.join(
                            PREDICTIONS_DIR, f"{date_key}_summary.txt"
                        )
                        with open(summary_path, "w", encoding="utf-8") as f:
                            f.write(txt)

                        ok = result["ok"]
                        total = result["total"]
                        fail = len(result["failed"])
                        _log(
                            f"  予想: OK {ok}/{total}レース"
                            + (f"  失敗:{fail}" if fail else "")
                        )
                    else:
                        _log(f"  予想: 対象レースなし")
                        stats["pred_fail"] += 1

                except Exception as e:
                    _log(f"  予想: ERROR - {e}")
                    logger.warning("date %s analysis failed: %s", date_str, e, exc_info=True)
                    stats["pred_fail"] += 1

        # ── 実際の着順取得（過去日のみ）────────────────────────────────
        if not args.skip_results and d < today:
            if _results_exist(date_str):
                _log(f"  結果: スキップ（既存）")
                stats["results_skip"] += 1
            elif not _pred_exists(date_str):
                _log(f"  結果: スキップ（予想なし）")
            else:
                try:
                    # 結果取得には scraper.client が必要
                    if scraper is not None:
                        client = scraper.client
                    else:
                        # results-only でも client が必要なため scraper を起動
                        from data.masters.course_master import get_all_courses as _gac
                        from src.scraper.auth import PremiumNetkeibaScraper as _PNS
                        _ac = _gac()
                        _sc = _PNS(_ac)
                        _sc.login()
                        client = _sc.client
                        # 次のループでも使えるよう保持
                        scraper = _sc
                        all_courses = _ac

                    results = fetch_actual_results(date_str, client)
                    if results:
                        stats["results_ok"] += 1
                        _log(f"  結果: {len(results)}レース取得")
                    else:
                        stats["results_fail"] += 1
                        _log(f"  結果: 取得なし")

                except Exception as e:
                    _log(f"  結果: ERROR - {e}")
                    stats["results_fail"] += 1

        # ── 予想 vs 実際 照合 ──────────────────────────────────────────
        if not args.skip_results and d < today:
            try:
                agg = compare_and_aggregate(date_str)
                if agg:
                    stats["compare_ok"] += 1
                    roi = agg.get("roi", 0)
                    ht = agg.get("hit_rate", 0)
                    races = agg.get("total_races", 0)
                    _log(f"  照合: {races}レース  的中率{ht:.1f}%  ROI {roi:.1f}%")
            except Exception as e:
                _log(f"  照合: ERROR - {e}")

        # ── 経過時間 ──────────────────────────────────────────────────
        d_elapsed = time.time() - d_t0
        date_elapsed.append(d_elapsed)
        total_elapsed = time.time() - batch_t0
        eta_str = _eta(total_elapsed, idx + 1, total_dates)
        _log(
            f"  [{date_str}] 完了 {_fmt_elapsed(d_elapsed)}  "
            f"通算: {_fmt_elapsed(total_elapsed)}  ETA: {eta_str}"
        )

    # ── 最終サマリー ──────────────────────────────────────────────────────
    total_elapsed = time.time() - batch_t0
    _log("\n" + "=" * 60)
    _log("バッチ完了サマリー")
    _log("=" * 60)
    _log(f"  対象期間  : {args.date_from} → {args.date_to}")
    _log(f"  総日数    : {total_dates}日")
    _log(f"  予想生成  : {stats['pred_ok']}日成功  {stats['pred_skip']}日スキップ  {stats['pred_fail']}日失敗")
    _log(f"  結果取得  : {stats['results_ok']}日成功  {stats['results_skip']}日スキップ  {stats['results_fail']}日失敗")
    _log(f"  照合完了  : {stats['compare_ok']}日")
    _log(f"  総作業時間: {_fmt_elapsed(total_elapsed)}")
    if date_elapsed:
        avg = sum(date_elapsed) / len(date_elapsed)
        _log(f"  1日平均   : {_fmt_elapsed(avg)}")
    _log("=" * 60)
    _log(f"ログ: {LOG_PATH}")


if __name__ == "__main__":
    main()
