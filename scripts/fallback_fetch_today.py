"""
本日結果未取得 race を MultiSourceEnricher 経由で fallback 取得。

- 1st try: NAR 公式 (keiba.go.jp)
- 2nd try: 競馬ブック (keibabook)
- 両方失敗 → skip + warn (フォールバック禁止 — 推定で埋めない)

netkeiba は一切使わない（本日 403 制限中のため絶対禁止）。

使用法:
    python scripts/fallback_fetch_today.py --date 2026-04-28
    python scripts/fallback_fetch_today.py --date 2026-04-28 --dry-run
    python scripts/fallback_fetch_today.py --date 2026-04-28 --refresh
      --refresh: 取得済み keibabook エントリを削除して再取得（フィールド補完用）
"""

import argparse
import hashlib
import json
import os
import sys
import time
import shutil
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# UTF-8 出力強制（Windows 環境対応）
if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
if sys.stderr and hasattr(sys.stderr, "reconfigure"):
    try:
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

# プロジェクトルートを sys.path に追加
_PROJ_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJ_ROOT))

from src.log import get_logger

logger = get_logger("fallback_fetch_today")


# ================================================================
# パス定数
# ================================================================

PREDICTIONS_DIR = _PROJ_ROOT / "data" / "predictions"
RESULTS_DIR     = _PROJ_ROOT / "data" / "results"

# レート制限 (秒): NAR 公式 / 競馬ブック それぞれ 2.0s 以上
_NAR_SLEEP   = 2.0
_KB_SLEEP    = 2.5


# ================================================================
# 補助: SHA256 ダイジェスト
# ================================================================

def _sha256_file(fpath: Path) -> str:
    """ファイルの SHA256 を返す。ファイルが存在しない場合は空文字列。"""
    if not fpath.exists():
        return ""
    h = hashlib.sha256()
    with open(fpath, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


# ================================================================
# 補助: atomic JSON 書き込み
# ================================================================

def _atomic_write_json(fpath: Path, data: dict) -> None:
    """JSON をアトミックに書き込む (.tmp → rename)"""
    tmp_path = fpath.with_suffix(".tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    tmp_path.replace(fpath)


# ================================================================
# 補助: 未取得 race_id の特定
# ================================================================

def _clear_keibabook_result_cache(date: str) -> int:
    """
    --refresh 時に keibabook 結果ページのキャッシュを削除する。
    新規フェッチを強制して拡張フィールドを取得するために使用。

    Returns: 削除したキャッシュファイル数
    """
    from pathlib import Path
    import re

    date_key = date.replace("-", "")  # 例: "20260428"
    year = date_key[:4]               # 例: "2026"

    kb_cache_dir = _PROJ_ROOT / "data" / "cache" / "keibabook"
    if not kb_cache_dir.exists():
        return 0

    deleted = 0
    # chihou_seiseki_{date_key}*.html にマッチするキャッシュを削除
    pattern = re.compile(rf".*chihou_seiseki_{date_key}.*\.html$", re.IGNORECASE)
    for f in kb_cache_dir.iterdir():
        if f.is_file() and pattern.match(f.name):
            try:
                f.unlink()
                logger.info("keibabook キャッシュ削除: %s", f.name)
                deleted += 1
            except Exception as e:
                logger.warning("キャッシュ削除失敗 %s: %s", f.name, e)

    print(f"[--refresh] keibabook キャッシュ {deleted} 件削除 (date={date_key})")
    return deleted


def _find_missing_race_ids(date: str, refresh: bool = False) -> List[dict]:
    """
    pred.json と results.json を比較して、
    着順データ (order) が未取得の NAR レースを返す。

    Args:
        refresh: True のとき、source="keibabook" の取得済みエントリも再取得対象に含める
                 （フィールド補完目的）

    Returns:
        [{"race_id": str, "venue": str, "race_no": int, "post_time": str}, ...]
    """
    date_key = date.replace("-", "")
    pred_path    = PREDICTIONS_DIR / f"{date_key}_pred.json"
    results_path = RESULTS_DIR    / f"{date_key}_results.json"

    if not pred_path.exists():
        logger.warning("pred.json が見つかりません: %s", pred_path)
        return []

    with open(pred_path, "r", encoding="utf-8") as f:
        pred_data = json.load(f)

    # 取得済み race_id（order が空でないもの）
    existing_ids: set = set()
    if results_path.exists():
        try:
            with open(results_path, "r", encoding="utf-8") as f:
                results_data = json.load(f)
            for rid, entry in results_data.items():
                if not isinstance(entry, dict):
                    continue
                if not entry.get("order"):
                    continue
                # --refresh 時は keibabook 取得済みも再取得対象にする（keibabook キャッシュを削除して再フェッチ）
                if refresh and entry.get("source") == "keibabook":
                    logger.info("--refresh: keibabook 取得済みを再取得対象に追加: %s", rid)
                    continue
                existing_ids.add(rid)
        except Exception as e:
            logger.warning("results.json 読み込み失敗: %s", e)
            existing_ids = set()

    # JRA venue コード
    try:
        from data.masters.venue_master import JRA_CODES
    except Exception:
        JRA_CODES = {"01", "02", "03", "04", "05", "06", "07", "08", "09", "10"}

    missing: List[dict] = []
    for race in pred_data.get("races", []):
        rid = str(race.get("race_id", ""))
        if not rid:
            continue
        # JRA レースはスキップ（NAR 専用スクリプト）
        venue_code = rid[4:6]
        if venue_code in JRA_CODES:
            continue
        # 取得済みはスキップ
        if rid in existing_ids:
            continue
        missing.append({
            "race_id":   rid,
            "venue":     race.get("venue", "?"),
            "race_no":   race.get("race_no", 0),
            "post_time": race.get("post_time", ""),
        })

    return missing


# ================================================================
# 取得コア: NAR公式 → 競馬ブック fallback
# ================================================================

def _fetch_one_race(
    race_id: str,
    date: str,
    nar_scraper,
    kb_result_scraper,
    dry_run: bool = False,
) -> Tuple[Optional[dict], str]:
    """
    1レース分の結果を NAR公式 → 競馬ブック の順に試みる。

    Returns:
        (result_dict, source_str) または (None, "failed")
    """
    # ── 1st: NAR 公式 ──
    result = None
    source = ""
    if nar_scraper is not None:
        try:
            if not dry_run:
                result = nar_scraper.get_result(race_id, date)
                time.sleep(_NAR_SLEEP)
            if result and result.get("order"):
                source = "nar_official"
                logger.info("[NAR公式] 取得成功: %s (%d頭)", race_id, len(result["order"]))
                return result, source
            else:
                logger.debug("[NAR公式] 取得失敗または着順なし: %s", race_id)
        except Exception as e:
            logger.debug("[NAR公式] 例外 race_id=%s: %s", race_id, e)

    # ── 2nd: 競馬ブック fallback ──
    if kb_result_scraper is not None:
        try:
            if not dry_run:
                result = kb_result_scraper.fetch_result(race_id, race_date=date)
                time.sleep(_KB_SLEEP)
            if result and result.get("order"):
                source = "keibabook"
                logger.info("[競馬ブック] fallback 成功: %s (%d頭)", race_id, len(result["order"]))
                return result, source
            else:
                logger.debug("[競馬ブック] 取得失敗または着順なし: %s", race_id)
        except Exception as e:
            logger.debug("[競馬ブック] 例外 race_id=%s: %s", race_id, e)

    # ── 両方失敗 ──
    logger.warning("[fallback失敗] NAR公式+競馬ブック両方失敗: %s", race_id)
    return None, "failed"


# ================================================================
# race_log INSERT
# ================================================================

def _insert_race_log_if_available(
    date: str,
    race_id: str,
    result_entry: dict,
) -> bool:
    """
    取得した結果を race_log に INSERT する（重複は OR IGNORE）。
    DB が利用不可の場合は False を返す。
    """
    try:
        from src import database as _db
        _db.save_results(date, {race_id: result_entry})
        return True
    except Exception as e:
        logger.debug("race_log INSERT 失敗 %s: %s", race_id, e)
        return False


# ================================================================
# メイン処理
# ================================================================

def main(date: str, dry_run: bool = False, refresh: bool = False) -> int:
    """
    メイン処理。
    Args:
        refresh: True のとき keibabook 取得済みエントリを削除して再取得
    Returns: 取得成功件数
    """
    date_key = date.replace("-", "")
    results_path = RESULTS_DIR / f"{date_key}_results.json"

    print(f"[fallback_fetch_today] 対象日: {date}")
    print(f"[fallback_fetch_today] dry_run: {dry_run}")
    print(f"[fallback_fetch_today] refresh: {refresh}")

    # ── --refresh 時: keibabook キャッシュを削除して新規フェッチを強制 ──
    if refresh:
        _clear_keibabook_result_cache(date)

    # ── 未取得レース特定 ──
    missing = _find_missing_race_ids(date, refresh=refresh)
    if not missing:
        print("[fallback_fetch_today] 未取得レースなし。終了。")
        return 0

    print(f"[fallback_fetch_today] 未取得レース: {len(missing)} 件")
    for m in missing:
        print(f"  {m['race_id']} ({m['venue']} {m['race_no']}R {m['post_time']})")

    if dry_run:
        print("[DRY-RUN] 実際の取得は行いません。")
        return 0

    # ── results.json バックアップ ──
    sha_before = _sha256_file(results_path)
    if results_path.exists():
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        bak_path = results_path.with_name(f"{date_key}_results.json.bak_{ts}")
        shutil.copy2(results_path, bak_path)
        print(f"[backup] {bak_path}")

    # ── 既存 results.json 読み込み ──
    existing: Dict[str, dict] = {}
    if results_path.exists():
        try:
            with open(results_path, "r", encoding="utf-8") as f:
                existing = json.load(f)
        except Exception as e:
            logger.warning("results.json 読み込み失敗: %s", e)
            existing = {}

    # ── スクレイパー初期化 ──
    print("[init] NAR公式スクレイパー初期化...")
    try:
        from src.scraper.official_nar import OfficialNARScraper
        nar_scraper = OfficialNARScraper()
    except Exception as e:
        logger.warning("OfficialNARScraper 初期化失敗: %s", e)
        nar_scraper = None

    print("[init] 競馬ブック ResultScraper 初期化...")
    try:
        from src.scraper.keibabook_training import KeibabookClient
        from src.scraper.keibabook_result import KeibabookResultScraper
        kb_client = KeibabookClient()
        kb_logged_in = kb_client.login()
        if kb_logged_in:
            print("[init] 競馬ブック ログイン成功")
        else:
            print("[init] 競馬ブック ログイン失敗（KB fallback 無効）")
        kb_result_scraper = KeibabookResultScraper(kb_client) if kb_logged_in else None
    except Exception as e:
        logger.warning("KeibabookClient 初期化失敗: %s", e)
        kb_result_scraper = None

    if nar_scraper is None and kb_result_scraper is None:
        print("[ERROR] スクレイパーが両方とも使用不可。終了。")
        return 0

    # ── 各レース取得 ──
    stats = {"success": 0, "failed": 0, "nar_official": 0, "keibabook": 0}
    total = len(missing)

    for i, m in enumerate(missing, 1):
        race_id = m["race_id"]
        venue   = m["venue"]
        race_no = m["race_no"]
        print(f"\n[{i:2d}/{total}] {race_id} ({venue} {race_no}R) 取得中...")

        result, source = _fetch_one_race(
            race_id, date, nar_scraper, kb_result_scraper
        )

        if result and result.get("order"):
            result_entry = {
                "order":   result["order"],
                "payouts": result.get("payouts", {}),
                "source":  source,
            }
            # lap_times があれば保存
            if result.get("lap_times"):
                result_entry["lap_times"] = result["lap_times"]

            # results.json に追記
            existing[race_id] = result_entry
            _atomic_write_json(results_path, existing)

            # race_log INSERT
            _insert_race_log_if_available(date, race_id, result_entry)

            stats["success"] += 1
            stats[source] = stats.get(source, 0) + 1
            print(f"  -> 成功 (source={source}, {len(result['order'])}頭)")
        else:
            stats["failed"] += 1
            print(f"  -> 失敗 (skip)")

    # ── キャッシュ無効化 (T-039 バッジ即反映) ──
    try:
        from src.results_tracker import invalidate_aggregate_cache
        invalidate_aggregate_cache()
        print("\n[cache] aggregate_cache を invalidate しました。")
    except Exception as e:
        logger.debug("invalidate_aggregate_cache 失敗: %s", e)

    # ── サマリ表示 ──
    sha_after = _sha256_file(results_path)
    print("\n" + "=" * 60)
    print(f"[サマリ] 対象日: {date}")
    print(f"  未取得レース数   : {total}")
    print(f"  取得成功         : {stats['success']}")
    print(f"    - NAR公式      : {stats.get('nar_official', 0)}")
    print(f"    - 競馬ブック   : {stats.get('keibabook', 0)}")
    print(f"  取得失敗(skip)   : {stats['failed']}")
    print(f"  results.json SHA256 変化: {sha_before[:8]}... → {sha_after[:8]}...")
    print("=" * 60)

    return stats["success"]


# ================================================================
# エントリポイント
# ================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="NAR公式+競馬ブック fallback で本日の未取得レース結果を補完する"
    )
    parser.add_argument(
        "--date",
        default=datetime.now().strftime("%Y-%m-%d"),
        help="対象日 YYYY-MM-DD (デフォルト: 本日)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="実際の HTTP リクエストを送らず、対象レースだけ表示する",
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="keibabook 取得済みエントリのキャッシュを削除して再取得する（フィールド補完用）",
    )
    args = parser.parse_args()

    success_count = main(args.date, dry_run=args.dry_run, refresh=args.refresh)
    print(f"\n[完了] 取得成功: {success_count} レース")
