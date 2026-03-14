"""
競馬解析マスターシステム v3.0 - メインCLI (完全版)

使い方:
  python main.py --analyze_date 2025-03-15          # 日付→全レース分析→1HTML
  python main.py --analyze_date 2025-12-28 --jra_only   # 中央競馬のみ
  python main.py --analyze_date 2025-12-28 --limit 5   # 先頭5レースのみ
  python main.py --analyze_date 2025-12-28 --no_cache  # キャッシュ無効
  python main.py --race_id 202501050511                # 1レース分析
  python main.py --date 2025-01-05                     # レース一覧
  python main.py --date 2025-01-05 --venue 東京        # 東京のみ
  python main.py --collect --start 2025-01-01 --end 2025-03-31  # DB構築
  python main.py --collect_course_db --start 2024-01-01 --end 2025-12-31  # 全件収集
  python main.py --collect_course_db --resume   # 途中再開
  python main.py --collect_course_db --append  # 新規分のみ追加
  python main.py --serve  # Web管理画面（ブラウザで日付・ボタン操作）
"""

import argparse
import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.log import get_logger

logger = get_logger(__name__)

from config.settings import (
    COURSE_DB_COLLECTOR_STATE_PATH,
    COURSE_DB_PRELOAD_PATH,
    TRAINER_BASELINE_DB_PATH,
)
from data.masters.course_master import get_all_courses
from data.masters.venue_master import JRA_CODES, VENUE_MAP, get_venue_name
from src.engine import RaceAnalysisEngine, enrich_course_aptitude_with_style_bias
from src.output.formatter import HTMLFormatter, minify_html, render_date_analysis_html
from src.scraper.auth import PremiumNetkeibaScraper
from src.scraper.course_db_collector import (
    collect_course_db_from_results,
    load_preload_course_db,
)
from src.scraper.netkeiba import NetkeibaClient, NetkeibaScraper
from src.scraper.personnel import PersonnelDBManager, enrich_personnel_with_condition_records
from src.scraper.race_results import (
    Last3FDBBuilder,
    RaceHistoryCollector,
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

# ============================================================
# 1レース分析
# ============================================================


def run_analysis(
    race_id: str,
    output_dir: str = "output",
    open_browser: bool = False,
    custom_stake: int = None,
):
    all_courses = get_all_courses()
    print("\n[競馬解析マスターシステム v3.0]")
    print(f"   race_id: {race_id}")
    print(f"   コースマスタ: {len(all_courses)}コース\n")

    scraper = PremiumNetkeibaScraper(all_courses)
    scraper.login()  # netkeiba ログイン（失敗しても一般会員として続行）
    scraper.training.login()  # 競馬ブックスマートプレミアム ログイン（調教データ用）
    client = scraper.client  # 認証済みクライアントを使用

    # --- データ取得 ---
    print("[ 1/5 ] データ取得中...")
    race_info, horses = scraper.fetch_race(
        race_id,
        fetch_history=True,
        fetch_odds=True,
        fetch_training=True,  # プレミアム会員なら調教ラップ取得
    )
    if not race_info or not horses:
        print("[エラー] データ取得失敗")
        sys.exit(1)
    print(
        f"       {race_info.race_name}  {race_info.venue} {race_info.race_no}R  {len(horses)}頭\n"
    )

    # --- 基準タイムDB ---
    print("[ 2/5 ] 基準タイムDB読み込み中...")
    std_db_builder = StandardTimeDBBuilder()
    course_db = std_db_builder.get_course_db()
    # 事前収集DB（約77,000走）があればマージ
    preload = load_preload_course_db(COURSE_DB_PRELOAD_PATH)
    for cid, runs in preload.items():
        course_db.setdefault(cid, []).extend(runs)
    # 今回の出走馬の過去走もDBに追加（オンザフライ補充）
    course_db = build_course_db_from_past_runs(horses, course_db)
    l3f_db = Last3FDBBuilder().build(course_db)
    n_runs = sum(len(v) for v in course_db.values())
    print(
        f"       {len(course_db)}コース / {n_runs}走 利用可能"
        + (f" (事前DB: {sum(len(v) for v in preload.values())}走)" if preload else "")
    )

    # --- 騎手・厩舎DB ---
    print("[ 3/5 ] 騎手・厩舎成績取得中...")
    personnel_mgr = PersonnelDBManager()
    jockey_db, trainer_db = personnel_mgr.build_from_horses(horses, client, course_db=course_db)
    # course_db から馬場状態別集計をマージ
    enrich_personnel_with_condition_records(jockey_db, trainer_db, course_db)

    # --- エンジン起動 ---
    print("[ 4/5 ] 分析エンジン起動中...")
    course_style_db = build_course_style_stats_db(course_db)
    gate_bias_db = build_gate_bias_db(course_db)
    position_sec_db = build_position_sec_per_rank_db(course_db)
    baseline_new = build_trainer_baseline_db(horses)
    trainer_baseline_db = merge_trainer_baseline(
        baseline_new, load_trainer_baseline_db(TRAINER_BASELINE_DB_PATH)
    )
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

    # --- 分析実行 ---
    print("[ 5/5 ] 分析・HTML出力中...")
    analysis = engine.analyze(race_info, horses, custom_stake, netkeiba_client=client)
    analysis = enrich_course_aptitude_with_style_bias(engine, analysis)
    save_trainer_baseline_db(TRAINER_BASELINE_DB_PATH, trainer_baseline_db)

    os.makedirs(output_dir, exist_ok=True)
    fname = f"{race_id}_{race_info.venue}{race_info.race_no}R.html"
    fpath = os.path.join(output_dir, fname)
    engine.render_html(analysis, fpath)

    # --- サマリー ---
    print(f"\n{'=' * 52}")
    print(f"  {race_info.race_name}  {race_info.venue} {race_info.race_no}R")
    print(f"  {race_info.race_date}　{race_info.course.surface}{race_info.course.distance}m")
    print(f"{'=' * 52}")
    print(
        f"  ペース: {analysis.pace_type_predicted.value if analysis.pace_type_predicted else '不明'}"
        f"  自信度: {analysis.overall_confidence.value}"
    )
    print()
    for ev in sorted(analysis.evaluations, key=lambda e: e.composite, reverse=True):
        if ev.mark.value == "－":
            continue
        od = f"{ev.horse.odds:.1f}倍" if ev.horse.odds else "  —"
        print(f"  {ev.mark.value} {ev.horse.horse_name:<14} 総合{ev.composite:.1f}  {od}")

    if analysis.tickets:
        print()
        for t in analysis.tickets:
            print(
                f"  {t['type']} {t['a']}-{t['b']}  EV{t['ev']:.0f}%  {t['signal']}  {t.get('stake', 0):,}円"
            )
    else:
        print("\n  見送り（期待値基準未達）")

    print(f"\n出力: {os.path.abspath(fpath)}\n")

    if open_browser:
        try:
            import webbrowser
            from pathlib import Path

            webbrowser.open(Path(fpath).resolve().as_uri())
        except Exception:
            logger.debug("browser open failed", exc_info=True)

    return analysis


# ============================================================
# 日付指定でその日の全レースを分析 → 1つのHTML（競馬場タブ+1R～12Rタブ）
# ============================================================


def run_date_analysis(
    date: str,
    output_dir: str = "output",
    open_browser: bool = True,
    jra_only: bool = False,
    nar_only: bool = False,
    no_cache: bool = False,
    quiet: bool = False,
    limit: int = 0,
    venues: list | None = None,
    workers: int = 3,
    official_only: bool = False,
):
    """
    ①日付を入力 → ②その日の中央競馬・地方競馬の全レースを分析
    → ③1つのHTMLで競馬場タブ・1R～12Rタブで全て見られる
    """
    all_courses = get_all_courses()
    print("\n[競馬解析マスターシステム v3.0] 日付別全レース分析")
    print(f"   日付: {date}")
    if jra_only:
        print("   対象: 中央競馬のみ")
    elif nar_only:
        print("   対象: 地方競馬のみ")
    if no_cache:
        print("   キャッシュ: 無効（常に最新取得）")
    if official_only:
        print("   モード: JRA/NAR公式のみ（netkeiba不使用）")
    if quiet:
        print("   表示: 簡潔モード")
    if limit:
        print(f"   件数制限: 先頭{limit}レースのみ")

    scraper = PremiumNetkeibaScraper(all_courses, no_cache=no_cache, quiet=quiet)
    if official_only:
        scraper._official_only = True
        logger.info("--official モード: ネット競馬ログインをスキップ")
    else:
        scraper.login()
        scraper.training.login()

    # その日の全レースID取得
    print("\n[1/3] レース一覧取得中...")
    race_ids = scraper.fetch_date(date)
    if not race_ids:
        print("[エラー] 該当日のレースが見つかりません")
        sys.exit(1)
    if jra_only and nar_only:
        print("[エラー] --jra_only と --nar_only は同時指定できません")
        sys.exit(1)
    if venues:
        # 会場名が渡された場合はコードに変換
        venues = [VENUE_MAP.get(v, v) for v in venues]
        race_ids = [r for r in race_ids if r[4:6] in venues]
        venue_names = [get_venue_name(v) or v for v in venues]
        print(f"   競馬場フィルタ: {', '.join(venue_names)}")
    elif jra_only:
        race_ids = [r for r in race_ids if r[4:6] in JRA_CODES]
    elif nar_only:
        race_ids = [r for r in race_ids if r[4:6] not in JRA_CODES]
    if not race_ids:
        print("[エラー] フィルタ後にレースが0件です")
        sys.exit(1)
    if limit and limit < len(race_ids):
        race_ids = race_ids[:limit]
        print(f"       {len(race_ids)}レース (--limit={limit})")
    else:
        print(f"       {len(race_ids)}レース")

    # ── 基準タイムDB（ループ前に1回だけ構築） ───────────────────────────
    std_db_builder = StandardTimeDBBuilder()
    course_db = std_db_builder.get_course_db()
    preload = load_preload_course_db(COURSE_DB_PRELOAD_PATH)
    for cid, runs in preload.items():
        course_db.setdefault(cid, []).extend(runs)
    if preload and not quiet:
        n_pre = sum(len(v) for v in preload.values())
        print(f"      事前DB: {len(preload)}コース / {n_pre}走 マージ済")

    # ── 静的DB（事前DB全体から1回だけ計算） ─────────────────────────────
    # ループ内で毎回再構築していたが、10万走×30レース=非常に遅かったため修正
    course_style_db = build_course_style_stats_db(course_db)
    gate_bias_db_base = build_gate_bias_db(course_db)
    position_sec_db = build_position_sec_per_rank_db(course_db)
    l3f_db = Last3FDBBuilder().build(course_db)
    trainer_baseline_db = load_trainer_baseline_db(TRAINER_BASELINE_DB_PATH)

    personnel_mgr = PersonnelDBManager()
    client = scraper.client  # 認証済みクライアントを使用

    analyses_by_venue = {}  # {"東京": {1: analysis, 2: ...}, ...}
    formatter = HTMLFormatter()

    # ── フェーズ1: 全レースデータを並列プリフェッチ ─────────────────────────
    # ネットワークI/Oが主ボトルネック。複数ワーカーが独立したレートリミットで
    # 同時リクエストし、スループットを workers 倍にする。
    # ワーカーはメインセッションのCookieを引き継ぎ再ログイン不要。
    effective_workers = max(1, min(workers, len(race_ids)))
    if effective_workers > 1:
        print(f"\n[2/3] 全レースデータ並列取得中... (ワーカー数: {effective_workers})")
    else:
        print("\n[2/3] 各レースデータ取得・分析中...")

    prefetched: dict = {}  # race_id → (race_info, horses)
    if effective_workers > 1:
        from concurrent.futures import ThreadPoolExecutor, as_completed

        # メインスクレイパー + (N-1) 個のワーカークローン
        worker_pool = [scraper] + [scraper.clone_worker() for _ in range(effective_workers - 1)]

        def _prefetch(args):
            idx, race_id = args
            w = worker_pool[idx % len(worker_pool)]
            for attempt in range(2):
                ri, hs = w.fetch_race(
                    race_id, fetch_history=True, fetch_odds=True, fetch_training=True,
                    target_date=date,
                )
                if ri and hs:
                    return race_id, ri, hs
                if attempt == 0 and not quiet:
                    logger.debug("リトライ: %s", race_id)
            return race_id, None, []

        total = len(race_ids)
        done = 0
        with ThreadPoolExecutor(max_workers=effective_workers) as pool:
            futs = {pool.submit(_prefetch, (i, rid)): rid for i, rid in enumerate(race_ids)}
            for fut in as_completed(futs):
                race_id, ri, hs = fut.result()
                prefetched[race_id] = (ri, hs)
                done += 1
                if not quiet:
                    status = f"{ri.venue}{ri.race_no}R ({len(hs)}頭)" if ri else "スキップ"
                    print(f"   [{done}/{total}] {race_id} → {status}")
    else:
        # ワーカー数=1: 従来通り1件ずつ取得
        total = len(race_ids)
        for i, race_id in enumerate(race_ids):
            pct = 100 * (i + 1) // total if total else 0
            print(f"   ({i + 1}/{total} {pct}%) {race_id} ...", end="", flush=True)
            for attempt in range(2):
                ri, hs = scraper.fetch_race(
                    race_id, fetch_history=True, fetch_odds=True, fetch_training=True,
                    target_date=date,
                )
                if ri and hs:
                    break
                if attempt == 0:
                    print(" リトライ...", end="", flush=True)
            prefetched[race_id] = (ri, hs)
            status = f"{ri.venue}{ri.race_no}R OK" if ri else "スキップ"
            print(f" {status}")

    # ── フェーズ2: 順次分析（course_db を累積しながら） ──────────────────────
    from src.models import PaceType as _PaceType
    if effective_workers > 1:
        print("\n[3/3] 各レース分析・HTML生成中...")
    ok_count, skip_count = 0, 0
    total = len(race_ids)
    for i, race_id in enumerate(race_ids):
        race_info, horses = prefetched.get(race_id, (None, []))
        if not race_info or not horses:
            reason = "データなし" if not race_info else "馬0頭"
            pct = 100 * (i + 1) // total if total else 0
            print(f"   ({i + 1}/{total} {pct}%) {race_id} [スキップ:{reason}]")
            skip_count += 1
            continue

        venue = race_info.venue
        if venue is None:
            # 未登録の競馬場コード → race_id から推定してスキップ
            venue_code = race_id[4:6] if len(race_id) >= 6 else "XX"
            print(f"   ({i + 1}/{total}) {race_id} [スキップ: 競馬場コード{venue_code}が未登録]")
            skip_count += 1
            continue
        race_no = race_info.race_no

        # 過去走をDBに追加（オンザフライ補充）
        old_course_ids = set(course_db.keys())
        course_db = build_course_db_from_past_runs(horses, course_db)
        new_course_ids = set(course_db.keys()) - old_course_ids
        # 変更されたコースのみ l3f_db を更新（全体再構築を回避）
        for cid in (new_course_ids or set(course_db.keys())):
            runs = course_db[cid]
            is_dirt = "ダート" in cid
            lo, hi = (34.0, 42.0) if is_dirt else (32.0, 40.0)
            by_pace: dict = {}
            for r in runs:
                t = r.last_3f_sec
                if not (lo <= t <= hi):
                    continue
                pk = r.pace.value if r.pace else _PaceType.MM.value
                if pk not in by_pace:
                    by_pace[pk] = []
                by_pace[pk].append(t)
            l3f_db[cid] = by_pace

        # 騎手・厩舎（馬ごとに変わるため毎レース）
        jockey_db, trainer_db = personnel_mgr.build_from_horses(horses, client, course_db=course_db)
        enrich_personnel_with_condition_records(jockey_db, trainer_db, course_db)

        # trainer_baseline は馬ごとの調教ベースライン（軽量・毎回マージのみ）
        baseline_new = build_trainer_baseline_db(horses)
        trainer_baseline_db = merge_trainer_baseline(baseline_new, trainer_baseline_db)

        engine = RaceAnalysisEngine(
            course_db=course_db,
            all_courses=all_courses,
            jockey_db=jockey_db,
            trainer_db=trainer_db,
            trainer_baseline_db=trainer_baseline_db,
            pace_last3f_db=l3f_db,
            course_style_stats_db=course_style_db,  # ループ前に構築した静的DB
            gate_bias_db=gate_bias_db_base,  # 同上
            position_sec_per_rank_db=position_sec_db,  # 同上
            is_jra=race_info.is_jra,
        )
        analysis = engine.analyze(race_info, horses, custom_stake=None)
        analysis = enrich_course_aptitude_with_style_bias(engine, analysis)

        if venue not in analyses_by_venue:
            analyses_by_venue[venue] = {}
        analyses_by_venue[venue][race_no] = analysis

        # 個別レースHTML生成（ダッシュボードHOMEタブが {date}_{venue}XR.html を参照するため）
        date_key = date.replace("-", "")
        ind_fname = f"{date_key}_{venue}{race_no}R.html"
        ind_fpath = os.path.join(output_dir, ind_fname)
        os.makedirs(output_dir, exist_ok=True)
        with open(ind_fpath, "w", encoding="utf-8") as _f:
            _f.write(minify_html(formatter.render(analysis)))

        ok_count += 1
        # 進捗行は quiet でも常に出力（ダッシュボードの進捗パース用）
        pct = 100 * (i + 1) // total if total else 0
        print(f"   ({i + 1}/{total} {pct}%) {venue}{race_no}R OK")

        # pred.json を段階的に保存（5レースごと）— 途中中断しても保持される
        if ok_count % 5 == 0 and analyses_by_venue:
            try:
                from src.results_tracker import save_prediction as _sp
                _sp(date, analyses_by_venue)
            except Exception as _save_err:
                logger.warning("pred.json incremental save failed: %s", _save_err, exc_info=True)
                print(f"       [警告] pred.json段階保存失敗: {_save_err}")

    # trainer_baseline をループ後に1回だけ保存（毎レース書き込みを廃止）
    save_trainer_baseline_db(TRAINER_BASELINE_DB_PATH, trainer_baseline_db)

    if not analyses_by_venue:
        print("\n[エラー] 分析できたレースが1件もありません")
        sys.exit(1)

    # 統合HTML生成
    print("\n[3/3] 統合HTML出力中...")
    html = render_date_analysis_html(analyses_by_venue, date, formatter)
    os.makedirs(output_dir, exist_ok=True)
    fname = f"{date.replace('-', '')}_全レース.html"
    fpath = os.path.join(output_dir, fname)
    with open(fpath, "w", encoding="utf-8") as f:
        f.write(html)

    # 予想JSONを保存（結果照合・成績集計に使用）
    try:
        from src.results_tracker import save_prediction, generate_simple_html

        pred_path = save_prediction(date, analyses_by_venue)
        print(f"       予想データ保存: {os.path.basename(pred_path)}")

        # 配布用HTML（印・買い目のみ）を自動生成
        try:
            simple_path = generate_simple_html(date, output_dir)
            if simple_path:
                print(f"       配布用HTML生成: {os.path.basename(simple_path)}")
        except Exception as _e:
            logger.warning("simple html generation failed: %s", _e, exc_info=True)
            print(f"       [警告] 配布用HTML生成失敗: {_e}")
    except Exception as e:
        logger.warning("prediction save failed: %s", e, exc_info=True)
        print(f"       [警告] 予想データ保存失敗: {e}")

    n_races = sum(len(r) for r in analyses_by_venue.values())
    print(f"\n[完了] {os.path.abspath(fpath)}")
    print(f"       競馬場: {list(analyses_by_venue.keys())}")
    print(f"       分析成功: {n_races}レース  (スキップ: {skip_count})")
    if skip_count > 0 and total > 0:
        pct_skip = 100 * skip_count / total
        if pct_skip >= 30:
            print(f"\n[!] 警告: スキップ率 {pct_skip:.0f}% と高く、出力が不十分です。")
            print("    キャッシュ削除(--no_cache)またはネット接続を確認してください。")
    if open_browser:
        try:
            import webbrowser
            from pathlib import Path

            webbrowser.open(Path(fpath).resolve().as_uri())
        except Exception:
            logger.debug("browser open failed", exc_info=True)


# ============================================================
# レース一覧
# ============================================================


def list_races(date: str, venue_filter: str = ""):
    all_courses = get_all_courses()
    scraper = NetkeibaScraper(all_courses)
    ids = scraper.fetch_date(date)
    if venue_filter:
        vc = next((k for k, v in VENUE_MAP.items() if venue_filter in v), "")
        if vc:
            ids = [r for r in ids if r[4:6] == vc]

    print(f"\n{date} レース一覧 ({len(ids)}件)")
    for rid in ids:
        vn = get_venue_name(rid[4:6])
        print(f"  {rid}  {vn} {int(rid[10:12])}R")


# ============================================================
# DB構築 (バッチ)
# ============================================================


def collect_db(start: str, end: str):
    client = NetkeibaClient()
    db = StandardTimeDBBuilder()
    collector = RaceHistoryCollector(client, db)
    print(f"[collect] {start} 〜 {end} の基準タイムDB構築開始")
    collector.collect_date_range(start, end)
    print("\n--- DB統計 ---")
    print(db.stats())


# ============================================================
# CLI
# ============================================================

if __name__ == "__main__":
    p = argparse.ArgumentParser(description="競馬解析マスターシステム v3.0")
    p.add_argument("--race_id", type=str)
    p.add_argument("--date", type=str)
    p.add_argument("--venue", type=str, default="")
    p.add_argument("--output", type=str, default="output")
    p.add_argument("--open", action="store_true")
    p.add_argument("--stake", type=int, default=None)
    p.add_argument("--collect", action="store_true", help="DB構築モード")
    p.add_argument(
        "--collect_course_db",
        action="store_true",
        help="基準タイムDBを事前収集（結果ページから1〜3着を取得）",
    )
    p.add_argument("--resume", action="store_true", help="途中再開（前回の続きから）")
    p.add_argument("--append", action="store_true", help="新規分のみ追加（最終収集日〜今日）")
    p.add_argument(
        "--merge", action="store_true", help="指定期間を既存DBにマージ追加（state非依存）"
    )
    p.add_argument("--start", type=str, help="収集開始日 YYYY-MM-DD")
    p.add_argument("--end", type=str, help="収集終了日 YYYY-MM-DD")
    p.add_argument("--db_stats", action="store_true", help="DB統計表示")
    p.add_argument("--analyze_date", type=str, help="日付指定で全レース分析 (YYYY-MM-DD)")
    p.add_argument("--no_open", action="store_true", help="完了後にブラウザを開かない")
    p.add_argument("--jra_only", action="store_true", help="中央競馬のみ分析")
    p.add_argument("--nar_only", action="store_true", help="地方競馬のみ分析")
    p.add_argument(
        "--venues", type=str, default="", help="競馬場コードをカンマ区切りで指定 (例: 05,09,10)"
    )
    p.add_argument("--no_cache", action="store_true", help="キャッシュを使わず常に最新取得")
    p.add_argument("--official", action="store_true",
                    help="JRA/NAR公式のみで予想生成（netkeiba不使用）")
    p.add_argument("--quiet", "-q", action="store_true", help="冗長ログを抑制")
    p.add_argument("--limit", type=int, default=0, help="先頭Nレースのみ処理（テスト用）")
    p.add_argument("--workers", type=int, default=3, help="並列フェッチのワーカー数 (デフォルト:3)")
    p.add_argument("--serve", action="store_true", help="基準タイム収集のWeb管理画面を起動")
    p.add_argument(
        "--collect_ml_data",
        action="store_true",
        help="ML学習用データ収集（結果ページから全馬の着順・タイム・オッズを取得）",
    )
    p.add_argument("--ml_stats", action="store_true", help="ML収集データの統計表示")
    p.add_argument(
        "--collect_training",
        action="store_true",
        help="調教データ一括収集（競馬ブックスマートプレミアム）",
    )
    p.add_argument("--training_stats", action="store_true", help="調教収集データの統計表示")
    p.add_argument(
        "--ml_train",
        action="store_true",
        help="LightGBM 学習・評価・特徴量重要度分析を実行",
    )
    p.add_argument(
        "--ml_venue",
        action="store_true",
        help="競馬場別にLightGBMを学習し、特徴量重要度を比較",
    )
    p.add_argument(
        "--ml_prob",
        action="store_true",
        help="三連率予測モデル (win/top2/top3) を学習",
    )
    p.add_argument(
        "--ml_all",
        action="store_true",
        help="全モデルを一括学習（複勝+三連率）",
    )
    p.add_argument(
        "--ml_backtest",
        action="store_true",
        help="予想オッズ精度検証・期待値ベースバックテスト",
    )
    args = p.parse_args()

    if args.analyze_date:
        venues_list = (
            [v.strip() for v in args.venues.split(",") if v.strip()] if args.venues else None
        )
        run_date_analysis(
            args.analyze_date,
            args.output,
            open_browser=not args.no_open,
            jra_only=args.jra_only,
            nar_only=args.nar_only,
            no_cache=args.no_cache,
            quiet=args.quiet,
            limit=args.limit,
            venues=venues_list,
            workers=args.workers,
            official_only=getattr(args, "official", False),
        )
    elif args.db_stats:
        print(StandardTimeDBBuilder().stats())
    elif args.collect_course_db:
        from src.scraper.netkeiba import RaceListScraper

        client = NetkeibaClient(no_cache=True)
        race_list = RaceListScraper(client)
        today = datetime.now().strftime("%Y-%m-%d")
        if args.append:
            start, end = "", today
            print(f"[新規分のみ追加] 最終収集日〜{end}")
        else:
            end = args.end or today
            start = args.start or (
                datetime.strptime(end, "%Y-%m-%d") - timedelta(days=90)
            ).strftime("%Y-%m-%d")
            if args.resume:
                print(f"[途中再開] {start} 〜 {end}")
            elif args.merge:
                print(f"[マージ収集] {start} 〜 {end}  ※既存DBに追加")
            else:
                print(f"[全件収集] {start} 〜 {end}")
        print(f"  保存先: {COURSE_DB_PRELOAD_PATH}")
        mode = (
            "append"
            if args.append
            else ("resume" if args.resume else ("merge" if args.merge else "full"))
        )
        n = collect_course_db_from_results(
            client,
            race_list,
            start,
            end,
            COURSE_DB_PRELOAD_PATH,
            state_path=COURSE_DB_COLLECTOR_STATE_PATH,
            mode=mode,
        )
        print(f"[完了] {n}走 を追加しました")
    elif args.collect_ml_data:
        from src.scraper.ml_data_collector import collect_ml_data
        from src.scraper.netkeiba import RaceListScraper

        client = NetkeibaClient(request_interval=1.0)
        race_list = RaceListScraper(client)
        today = datetime.now().strftime("%Y-%m-%d")
        start = args.start or "2024-01-01"
        end = args.end or today
        collect_ml_data(
            client,
            race_list,
            start,
            end,
            jra_only=args.jra_only,
            nar_only=args.nar_only,
            resume=args.resume,
        )
    elif args.ml_stats:
        from src.scraper.ml_data_collector import ml_data_stats

        ml_data_stats()
    elif args.collect_training:
        from src.scraper.keibabook_training import KeibabookClient
        from src.scraper.netkeiba import RaceListScraper
        from src.scraper.training_collector import collect_training_data

        ne_client = NetkeibaClient(request_interval=1.0)
        race_list = RaceListScraper(ne_client)
        kb_client = KeibabookClient()
        today = datetime.now().strftime("%Y-%m-%d")
        start = args.start or "2024-01-01"
        end = args.end or today
        collect_training_data(
            ne_client,
            race_list,
            kb_client,
            start,
            end,
            jra_only=args.jra_only,
            nar_only=args.nar_only,
            resume=args.resume,
        )
    elif args.training_stats:
        from src.scraper.training_collector import training_data_stats

        training_data_stats()
    elif args.ml_train:
        from src.ml.trainer import train_and_evaluate

        start = args.start or "2024-01-01"
        end = args.end or datetime.now().strftime("%Y-%m-%d")
        train_and_evaluate(start_date=start, end_date=end)
    elif args.ml_venue:
        from src.ml.trainer import train_by_venue

        start = args.start or "2024-01-01"
        end = args.end or datetime.now().strftime("%Y-%m-%d")
        train_by_venue(start_date=start, end_date=end)
    elif args.ml_prob:
        from src.ml.trainer import train_probability_models

        train_probability_models()
    elif args.ml_all:
        from src.ml.trainer import train_all_models

        start = args.start or "2024-01-01"
        end = args.end or datetime.now().strftime("%Y-%m-%d")
        train_all_models(start_date=start, end_date=end)
    elif args.ml_backtest:
        from src.ml.backtest import run_backtest

        start = args.start or "2024-06-01"
        end = args.end or datetime.now().strftime("%Y-%m-%d")
        run_backtest(start_date=start, end_date=end)
    elif args.collect and args.start and args.end:
        collect_db(args.start, args.end)
    elif args.race_id:
        run_analysis(args.race_id, args.output, args.open, args.stake)
    elif args.date:
        list_races(args.date, args.venue)
    elif args.serve:
        from src.dashboard import run_server

        run_server(5051)
    else:
        p.print_help()
