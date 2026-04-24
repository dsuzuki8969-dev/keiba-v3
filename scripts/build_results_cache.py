"""成績・実績ページ用サマリ JSON キャッシュを事前生成するスクリプト

マスター指示 2026-04-24 (方針3):
  `/api/results/summary` などで 234 秒かかっていた集計を、夜間メンテで
  year=all/2024/2025/2026 × 4種のAPI分を事前生成しておくことで、
  API 応答を <100ms 化する。

生成ファイル (data/cache/results/):
  summary_{year}.json            … aggregate_all(by_date 除去済) のレスポンス
  sanrentan_summary_{year}.json  … get_sanrentan_summary のレスポンス
  detailed_{year}.json           … aggregate_detailed(by_venue の肥大項目除去済) のレスポンス
  trend_{year}.json              … cum ROI / 月別収支を事前整形（Chart.js用）
  manifest.json                  … 生成時刻・対象日付数などのメタ情報

使い方:
  # 全 year を並列生成（既定 4 ワーカー）
  python scripts/build_results_cache.py

  # 特定 year だけ
  python scripts/build_results_cache.py --years all 2026

  # 並列数を指定
  python scripts/build_results_cache.py --workers 2

  # 既存キャッシュがあっても強制再生成
  python scripts/build_results_cache.py --force

日次メンテ (scripts/daily_maintenance.bat) の末尾で呼ばれる。
"""
from __future__ import annotations
import argparse
import os
import re
import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import List

# プロジェクトルートを sys.path に追加（単体実行用）
_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from src.log import get_logger
from src.utils.atomic_json import atomic_write_json

logger = get_logger(__name__)

# ============================================================
# キャッシュ格納ディレクトリ
# ============================================================
RESULTS_CACHE_DIR = os.path.join(_PROJECT_ROOT, "data", "cache", "results")
os.makedirs(RESULTS_CACHE_DIR, exist_ok=True)

# 既定の対象 year（2024/2025/2026 + all）
DEFAULT_YEARS = ["all", "2026", "2025", "2024"]


# ============================================================
# ファイル命名規則
# ============================================================

# reviewer HIGH #2 対応: year パラメータは許可リストで検証
# （Windows バックスラッシュ経由の path traversal 対策）
_VALID_YEAR_RE = re.compile(r"^(all|\d{4})$")


def cache_path(kind: str, year: str) -> str:
    """キャッシュ JSON のパスを返す。

    kind: "summary" / "sanrentan_summary" / "detailed" / "trend"
    year: "all" / "2026" / ...
    """
    # 許可リスト検証。path traversal を根本的に防ぐ
    if not _VALID_YEAR_RE.fullmatch(year):
        raise ValueError(f"不正な year: {year!r} (許可: all/YYYY)")
    if kind not in ("summary", "sanrentan_summary", "detailed", "trend"):
        raise ValueError(f"不正な kind: {kind!r}")
    return os.path.join(RESULTS_CACHE_DIR, f"{kind}_{year}.json")


def manifest_path() -> str:
    return os.path.join(RESULTS_CACHE_DIR, "manifest.json")


# ============================================================
# API レスポンス整形ヘルパー（dashboard.py と同等処理）
# ============================================================

def _shape_summary(result: dict) -> dict:
    """aggregate_all 結果から by_date を除去して返す（/api/results/summary 相当）"""
    return {k: v for k, v in result.items() if k != "by_date"}


def _shape_detailed(result: dict) -> dict:
    """aggregate_detailed 結果から by_venue 内の by_surface/by_dist_zone を除去

    dashboard.py の同等ロジックをそのまま移植（フロント非改修のため完全互換）。
    """
    _trim_keys = {"by_surface", "by_dist_zone"}
    for cat_key in ("all", "jra", "nar"):
        cat_data = result.get(cat_key)
        if not isinstance(cat_data, dict):
            continue
        by_venue = cat_data.get("by_venue")
        if isinstance(by_venue, dict):
            for venue_data in by_venue.values():
                if isinstance(venue_data, dict):
                    for tk in _trim_keys:
                        venue_data.pop(tk, None)
    return result


def _shape_trend(agg: dict) -> dict:
    """aggregate_all 結果（by_date 付き）から trend レスポンスを構築する

    dashboard.py /api/results/trend と同一のロジック。
    """
    by_date = agg.get("by_date", [])
    if not by_date:
        return {
            "labels": [],
            "ticket_roi_cum": [],
            "honmei_tansho_roi_cum": [],
            "monthly_labels": [],
            "monthly_profit": [],
        }

    by_date_sorted = sorted(by_date, key=lambda r: r.get("date", ""))

    labels: List[str] = []
    ticket_roi_cum: List[float] = []
    honmei_roi_cum: List[float] = []
    cum_stake = 0
    cum_ret = 0
    cum_h_stake = 0
    cum_h_ret = 0
    monthly_profit_map: dict = {}

    for r in by_date_sorted:
        d = r.get("date", "")
        if not d:
            continue
        labels.append(d)
        cum_stake += r.get("total_stake", 0)
        cum_ret += r.get("total_return", 0)
        cum_h_stake += r.get("honmei_tansho_stake", r.get("honmei_total", 0) * 100)
        cum_h_ret += r.get("honmei_tansho_ret", 0)
        roi_c = round(cum_ret / cum_stake * 100, 1) if cum_stake > 0 else 0.0
        h_roi_c = round(cum_h_ret / cum_h_stake * 100, 1) if cum_h_stake > 0 else 0.0
        ticket_roi_cum.append(roi_c)
        honmei_roi_cum.append(h_roi_c)

        month_key = d[:7]
        profit_day = r.get("honmei_tansho_ret", 0) - r.get("honmei_tansho_stake", r.get("honmei_total", 0) * 100)
        monthly_profit_map[month_key] = monthly_profit_map.get(month_key, 0) + profit_day

    monthly_sorted = sorted(monthly_profit_map.items())
    monthly_labels = [m for m, _ in monthly_sorted]
    monthly_profit = [v for _, v in monthly_sorted]

    return {
        "labels": labels,
        "ticket_roi_cum": ticket_roi_cum,
        "honmei_tansho_roi_cum": honmei_roi_cum,
        "monthly_labels": monthly_labels,
        "monthly_profit": monthly_profit,
    }


# ============================================================
# year 単位のキャッシュ生成
# ============================================================

def build_year_cache(year: str, force: bool = False) -> dict:
    """指定 year の 4 種 JSON を生成する。

    戻り値: {"year": year, "ok": bool, "elapsed": {...}, "error": str | None, "skipped": bool}

    v6.1.18 reviewer HIGH #1 対応:
      force=False (既定) のとき、既存キャッシュが 1 時間以内なら再生成スキップ
      force=True なら常に再生成
    """
    # 遅延 import（並列 worker で 1 回ずつ）
    from src.results_tracker import (
        aggregate_all,
        aggregate_detailed,
    )
    from src.analytics.sanrentan_summary import get_sanrentan_summary

    stats = {"year": year, "ok": False, "elapsed": {}, "error": None, "skipped": False}

    # force=False の場合は新鮮なキャッシュをスキップ
    if not force:
        kinds = ("summary", "sanrentan_summary", "detailed", "trend")
        try:
            all_exist = all(os.path.exists(cache_path(k, year)) for k in kinds)
            if all_exist:
                newest = max(os.path.getmtime(cache_path(k, year)) for k in kinds)
                age = time.time() - newest
                if age < 3600:  # 1時間以内なら十分新鮮
                    stats["ok"] = True
                    stats["skipped"] = True
                    stats["elapsed"]["skipped_age_sec"] = round(age, 1)
                    return stats
        except Exception:
            # 検査中エラーでも続行して再生成（保険）
            pass

    try:
        # 1) aggregate_all（trend 用に by_date 付きでまず取得し、summary は除去版を書く）
        t0 = time.time()
        agg_all = aggregate_all(year_filter=year)
        stats["elapsed"]["aggregate_all"] = round(time.time() - t0, 2)

        summary = _shape_summary(agg_all)
        atomic_write_json(cache_path("summary", year), summary, separators=(",", ":"))

        trend = _shape_trend(agg_all)
        atomic_write_json(cache_path("trend", year), trend, separators=(",", ":"))

        # 2) aggregate_detailed
        t1 = time.time()
        detailed_raw = aggregate_detailed(year_filter=year)
        stats["elapsed"]["aggregate_detailed"] = round(time.time() - t1, 2)

        detailed = _shape_detailed(detailed_raw)
        atomic_write_json(cache_path("detailed", year), detailed, separators=(",", ":"))

        # 3) sanrentan_summary（force=True で内部キャッシュ無視して再計算）
        t2 = time.time()
        sanrentan = get_sanrentan_summary(year_filter=year, force=True)
        stats["elapsed"]["sanrentan_summary"] = round(time.time() - t2, 2)
        atomic_write_json(cache_path("sanrentan_summary", year), sanrentan, separators=(",", ":"))

        stats["ok"] = True
    except Exception as e:
        stats["error"] = f"{type(e).__name__}: {e}"
        stats["traceback"] = traceback.format_exc()
        logger.warning("year=%s キャッシュ生成失敗: %s", year, e, exc_info=True)
    return stats


# ============================================================
# マニフェスト
# ============================================================

def write_manifest(results: List[dict]) -> None:
    """生成結果のマニフェストを書く（/api/health 露出用）"""
    payload = {
        "generated_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "years": [r["year"] for r in results],
        "results": results,
    }
    try:
        atomic_write_json(manifest_path(), payload, indent=2)
    except Exception as e:
        logger.warning("manifest 書き込み失敗: %s", e)


# ============================================================
# CLI
# ============================================================

def main() -> int:
    parser = argparse.ArgumentParser(description="成績ページ用サマリ JSON キャッシュ事前生成")
    parser.add_argument("--years", nargs="+", default=DEFAULT_YEARS,
                        help=f"対象年（既定: {' '.join(DEFAULT_YEARS)}）")
    parser.add_argument("--workers", type=int, default=4,
                        help="並列 worker 数（既定 4）")
    parser.add_argument("--force", action="store_true",
                        help="既存キャッシュを無視して再生成")
    args = parser.parse_args()

    years = args.years
    workers = max(1, min(args.workers, len(years)))
    total_t0 = time.time()

    print(f"[build_results_cache] 開始: years={years} workers={workers}")
    results: List[dict] = []

    # 並列生成（aggregate_all/detailed は DB/JSON の読み込み中心で GIL 解放あり）
    #
    # v6.1.22 reviewer MEDIUM 対応: SQLite 接続のスレッド安全性について
    # ------------------------------------------------------------------
    # `src/database.py` 側の `HorseDB` は `threading.local()` で接続を保持し、
    # 各スレッド別にコネクションが作成される。従って ThreadPoolExecutor 配下の
    # 各 worker が `aggregate_all(year_filter=y)` を呼ぶと、内部で取得される
    # DB 接続はスレッド毎に独立し、SQLite の WAL モード下で並列読み取りが
    # 安全に成立する。書き込みは `atomic_write_json` が FileLock で
    # シリアライズするため JSON 破損も起きない。
    # ProcessPoolExecutor ではなく ThreadPoolExecutor を採用したのは、
    # SQLite の接続プールがスレッド分離済みなのと、プロセス起動コストを
    # 避けるため（4 worker で 5-7 分の集計を 80-90 秒に短縮できる）。
    if workers <= 1:
        for y in years:
            print(f"  [year={y}] 生成中...")
            r = build_year_cache(y, force=args.force)
            results.append(r)
            _print_year_result(r)
    else:
        with ThreadPoolExecutor(max_workers=workers) as ex:
            fut_map = {ex.submit(build_year_cache, y, args.force): y for y in years}
            for fut in as_completed(fut_map):
                r = fut.result()
                results.append(r)
                _print_year_result(r)

    # マニフェスト書き出し
    write_manifest(results)

    total_elapsed = round(time.time() - total_t0, 2)
    ok_count = sum(1 for r in results if r["ok"])
    print(f"[build_results_cache] 完了: {ok_count}/{len(results)} 成功 "
          f"(total={total_elapsed}s) -> {RESULTS_CACHE_DIR}")
    # 失敗が 1 つでもあれば exit code 1
    return 0 if ok_count == len(results) else 1


def _print_year_result(r: dict) -> None:
    status = "OK" if r["ok"] else "FAIL"
    elapsed = r.get("elapsed", {})
    detail = " / ".join(f"{k}={v}s" for k, v in elapsed.items())
    err = f" err={r['error']}" if r.get("error") else ""
    print(f"  [year={r['year']}] {status} {detail}{err}")


if __name__ == "__main__":
    sys.exit(main())
