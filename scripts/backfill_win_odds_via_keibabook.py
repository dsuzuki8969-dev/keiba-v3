"""
race_log の win_odds / tansho_odds NULL バックフィル (競馬ブック経由)

netkeiba には一切アクセスしない。
取得経路: 競馬ブック (keibabook) → 楽天競馬 (rakuten) fallback

使い方:
    python scripts/backfill_win_odds_via_keibabook.py            # dry-run (変更なし)
    python scripts/backfill_win_odds_via_keibabook.py --execute  # 実際に更新
    python scripts/backfill_win_odds_via_keibabook.py --execute --max-fetch 5  # サンプル5件のみ
    python scripts/backfill_win_odds_via_keibabook.py --execute --venue 44     # 特定venue_codeのみ
    python scripts/backfill_win_odds_via_keibabook.py --resume   # 中断再開（done.txtを引き継ぐ）
    python scripts/backfill_win_odds_via_keibabook.py --execute --min-null-horses 3  # NULL3頭以上のレースのみ

注意: NULL馬1-2頭は取消馬・除外馬 (keibabook でも取得不可) のため、
      デフォルトで --min-null-horses 3 以上のレースのみ対象にする。
      全件処理したい場合は --min-null-horses 1 を指定する。

安全装置:
    - 危険時間帯 (06:00-06:30 / 22:00-23:30) 自動 abort
    - 競合プロセス検出 (auto_fetch / Predict_Tomorrow 等) 自動 abort
    - keibabook login 失敗時は即 abort
    - リトライ最大 3 回 (各 5 秒間隔)
    - レート: 2.5 秒/件 (2.0 秒 + マージン 0.5 秒)
"""

import argparse
import datetime
import os
import re
import sqlite3
import sys
import time
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

logger = get_logger("backfill_win_odds")

# ================================================================
# パス定数
# ================================================================

_DB_PATH      = str(_PROJ_ROOT / "data" / "keiba.db")
_DONE_FILE    = _PROJ_ROOT / "tmp" / "backfill_win_odds_done.txt"
_RATE_SLEEP   = 2.5   # 秒/件 (2.0秒以上厳守)
_RETRY_MAX    = 3
_RETRY_SLEEP  = 5.0

# 危険時間帯: (開始h, 開始m, 終了h, 終了m) のリスト
_DANGER_TIMES = [
    (6,  0,  6, 30),   # 06:00-06:30 (Predict スケジューラ)
    (22, 0, 23, 30),   # 22:00-23:30 (Results + Maintenance スケジューラ)
]

# 競合プロセス検出キーワード
_CONFLICT_KEYWORDS = [
    "auto_fetch",
    "Predict_Tomorrow",
    "Predict",
    "results_tracker",
    "run_analysis_date",
    "fallback_fetch_today",
]


# ================================================================
# 安全装置
# ================================================================

def _check_danger_time() -> bool:
    """現在が危険時間帯なら True を返す"""
    now = datetime.datetime.now()
    h, m = now.hour, now.minute
    current_min = h * 60 + m
    for (sh, sm, eh, em) in _DANGER_TIMES:
        start = sh * 60 + sm
        end   = eh * 60 + em
        if start <= current_min <= end:
            return True
    return False


def _check_conflict_processes() -> List[str]:
    """競合プロセスが実行中なら識別子リストを返す"""
    try:
        import subprocess
        # Windows では tasklist / Unix では ps
        if sys.platform == "win32":
            result = subprocess.run(
                ["tasklist", "/FO", "CSV", "/NH"],
                capture_output=True, text=True, timeout=10
            )
            lines = result.stdout
        else:
            result = subprocess.run(
                ["ps", "-ef"],
                capture_output=True, text=True, timeout=10
            )
            lines = result.stdout

        found = []
        for kw in _CONFLICT_KEYWORDS:
            if kw in lines:
                found.append(kw)
        return found
    except Exception as e:
        logger.debug(f"プロセスチェック失敗: {e}")
        return []


# ================================================================
# 中断再開管理
# ================================================================

def _load_done_set() -> set:
    """処理済み race_id セットを読み込む"""
    done = set()
    if _DONE_FILE.exists():
        try:
            with open(_DONE_FILE, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        done.add(line)
        except Exception as e:
            logger.warning(f"done.txt 読込失敗: {e}")
    return done


def _mark_done(race_id: str) -> None:
    """race_id を処理済みとしてマーク"""
    _DONE_FILE.parent.mkdir(parents=True, exist_ok=True)
    try:
        with open(_DONE_FILE, "a", encoding="utf-8") as f:
            f.write(race_id + "\n")
    except Exception as e:
        logger.warning(f"done.txt 書込失敗: {e}")


# ================================================================
# DB操作
# ================================================================

def _get_null_race_ids(
    db_path: str,
    venue_filter: Optional[str] = None,
    min_null_horses: int = 3,
) -> List[Tuple[str, str]]:
    """
    win_odds IS NULL AND tansho_odds IS NULL の
    (race_id, race_date) リストを返す (重複除外・日付昇順)

    Args:
        min_null_horses: 1レース内の NULL 馬数がこの値以上のレースのみ対象にする。
                         デフォルト 3: NULL 1-2 頭 = 取消馬・除外馬 (取得不可) をスキップ。
                         全件処理するには 1 を指定する。
    """
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    try:
        base_sql = """
            SELECT race_id, MIN(race_date) as race_date
            FROM race_log
            WHERE win_odds IS NULL AND tansho_odds IS NULL
            {venue_clause}
            GROUP BY race_id
            HAVING COUNT(*) >= {min_null}
            ORDER BY race_date, race_id
        """
        venue_clause = "AND venue_code = ?" if venue_filter else ""
        sql = base_sql.format(
            venue_clause=venue_clause,
            min_null=int(min_null_horses),
        )
        params = (venue_filter,) if venue_filter else ()
        cur.execute(sql, params)
        return cur.fetchall()
    finally:
        conn.close()


def _get_null_horse_count(db_path: str) -> int:
    """win_odds NULL の馬数を返す（検証用）"""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT COUNT(*) FROM race_log WHERE win_odds IS NULL AND tansho_odds IS NULL"
        )
        return cur.fetchone()[0]
    finally:
        conn.close()


def _update_race_log(
    db_path: str,
    race_id: str,
    horse_no: int,
    win_odds: Optional[float],
    popularity: Optional[int],
) -> bool:
    """
    race_log の win_odds + tansho_odds + popularity を UPDATE する。
    win_odds がNoneの場合は更新しない。
    """
    if win_odds is None:
        return False

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    try:
        # win_odds と tansho_odds は同一カラム（別名）として両方更新
        cur.execute(
            """
            UPDATE race_log
            SET win_odds    = ?,
                tansho_odds = ?,
                popularity  = CASE WHEN popularity IS NULL THEN ? ELSE popularity END
            WHERE race_id = ? AND horse_no = ?
              AND win_odds IS NULL AND tansho_odds IS NULL
            """,
            (win_odds, win_odds, popularity, race_id, horse_no)
        )
        conn.commit()
        return cur.rowcount > 0
    except Exception as e:
        logger.warning(f"UPDATE失敗 race_id={race_id} horse_no={horse_no}: {e}")
        return False
    finally:
        conn.close()


# ================================================================
# 楽天競馬フォールバック: win_odds 取得
# ================================================================

def _fetch_via_rakuten(
    race_id: str,
    race_date: str,
) -> Optional[Dict[int, Tuple[float, int]]]:
    """
    楽天競馬から win_odds + popularity を取得する。
    Returns: {horse_no: (win_odds, popularity)} or None

    注意: 楽天競馬は NAR 専用。JRA の race_id は None を返す。
    楽天競馬の result URL は /race_performance/list/RACEID/{18桁} だが、
    netkeiba race_id は 12桁。find_race_id() で変換が必要。
    """
    # JRA はスキップ
    venue_code = race_id[4:6] if len(race_id) >= 6 else "00"
    jra_codes = {"01","02","03","04","05","06","07","08","09","10"}
    if venue_code in jra_codes:
        return None

    try:
        from src.scraper.rakuten_keiba import RakutenKeibaScraper

        # 場名マッピング（venue_code → 楽天場名）
        NAR_VENUE_NAMES = {
            "30": "門別", "35": "盛岡", "36": "水沢",
            "42": "浦和", "43": "船橋", "44": "大井",
            "45": "川崎", "46": "金沢", "47": "笠松",
            "48": "名古屋", "49": "園田", "50": "園田",
            "51": "姫路", "54": "高知", "55": "佐賀",
            "65": "帯広",
        }
        venue_name = NAR_VENUE_NAMES.get(venue_code)
        if not venue_name:
            return None

        race_no = int(race_id[10:12]) if len(race_id) >= 12 else 0
        if race_no == 0:
            return None

        scraper = RakutenKeibaScraper()
        rakuten_race_id = scraper.find_race_id(race_date, venue_name, race_no)
        if not rakuten_race_id:
            logger.debug(f"楽天競馬: race_id解決失敗 {race_id}")
            return None

        result = scraper.get_result(rakuten_race_id, race_date)
        if not result:
            return None

        # 払戻から単勝オッズを取得
        payouts = result.get("payouts") or {}
        tansho = payouts.get("tansho", [])
        odds_map: Dict[int, Tuple[float, int]] = {}
        for i, entry in enumerate(tansho):
            combo_str = entry.get("combination") or entry.get("combo", "")
            payout_val = entry.get("payout", 0)
            if not combo_str:
                continue
            try:
                horse_no = int(re.sub(r"[^\d]", "", combo_str))
            except ValueError:
                continue
            # 楽天競馬は払戻金(100円払い戻し基準)→オッズ変換
            # 例: payout=3600 → odds=36.0
            win_odds = round(payout_val / 100.0, 1) if payout_val else None
            pop = entry.get("popularity", i + 1)
            if win_odds is not None and win_odds > 0:
                odds_map[horse_no] = (win_odds, pop)

        return odds_map if odds_map else None

    except Exception as e:
        logger.warning(f"楽天競馬フォールバック失敗 {race_id}: {e}")
        return None


# ================================================================
# メイン取得ロジック
# ================================================================

def _fetch_odds_from_keibabook(
    race_id: str,
    race_date: str,
    kb_result_scraper,
) -> Optional[Dict[int, Tuple[float, int]]]:
    """
    競馬ブックから (horse_no → (win_odds, popularity)) を取得する。
    Returns: {horse_no: (win_odds, popularity)} or None
    """
    result = kb_result_scraper.fetch_result(race_id, race_date)
    if not result:
        return None

    order = result.get("order", [])
    if not order:
        return None

    odds_map: Dict[int, Tuple[float, int]] = {}
    for entry in order:
        horse_no   = entry.get("horse_no")
        win_odds   = entry.get("win_odds")
        popularity = entry.get("popularity")
        if horse_no and win_odds is not None:
            odds_map[horse_no] = (win_odds, popularity)

    return odds_map if odds_map else None


def _fetch_with_retry(
    race_id: str,
    race_date: str,
    kb_result_scraper,
    source_label: str = "keibabook",
) -> Tuple[Optional[Dict[int, Tuple[float, int]]], str]:
    """
    リトライ付き取得。
    Returns: (odds_map or None, source_name)
    """
    for attempt in range(1, _RETRY_MAX + 1):
        try:
            if source_label == "keibabook":
                result = _fetch_odds_from_keibabook(race_id, race_date, kb_result_scraper)
            else:
                result = _fetch_via_rakuten(race_id, race_date)

            if result is not None:
                return result, source_label
        except Exception as e:
            logger.warning(f"[{source_label}] 試行 {attempt}/{_RETRY_MAX} 失敗 {race_id}: {e}")

        if attempt < _RETRY_MAX:
            time.sleep(_RETRY_SLEEP)

    return None, source_label


def _fetch_race_odds(
    race_id: str,
    race_date: str,
    kb_result_scraper,
) -> Tuple[Optional[Dict[int, Tuple[float, int]]], str]:
    """
    競馬ブック → 楽天競馬 の順に取得。
    Returns: (odds_map or None, source_name)
    """
    # 1st: 競馬ブック
    odds_map, _ = _fetch_with_retry(race_id, race_date, kb_result_scraper, "keibabook")
    if odds_map is not None:
        return odds_map, "keibabook"

    # 2nd: 楽天競馬 (NAR のみ)
    odds_map, _ = _fetch_with_retry(race_id, race_date, kb_result_scraper, "rakuten")
    if odds_map is not None:
        return odds_map, "rakuten"

    return None, "failed"


# ================================================================
# メイン処理
# ================================================================

def main():
    parser = argparse.ArgumentParser(
        description="race_log win_odds / tansho_odds NULL バックフィル (競馬ブック経由)"
    )
    parser.add_argument(
        "--execute", action="store_true",
        help="実際にDBを更新する (省略時は dry-run)"
    )
    parser.add_argument(
        "--max-fetch", type=int, default=0,
        help="取得上限レース数 (0=無制限)"
    )
    parser.add_argument(
        "--venue", type=str, default="",
        help="特定 venue_code のみ対象 (例: 44=大井)"
    )
    parser.add_argument(
        "--resume", action="store_true",
        help="中断再開 (done.txt を引き継ぐ)"
    )
    parser.add_argument(
        "--reset-done", action="store_true",
        help="done.txt をリセットして全件再処理"
    )
    parser.add_argument(
        "--min-null-horses", type=int, default=3,
        help="1レース内の NULL 馬数がこの値以上のレースのみ対象 (デフォルト=3。取消馬スキップ用)"
    )
    args = parser.parse_args()

    dry_run          = not args.execute
    venue_filt       = args.venue.strip() or None
    max_fetch        = args.max_fetch
    min_null_horses  = args.min_null_horses

    # done.txt リセット
    if args.reset_done:
        if _DONE_FILE.exists():
            _DONE_FILE.unlink()
            print("[INFO] done.txt をリセットしました")

    mode_str = "[DRY-RUN]" if dry_run else "[EXECUTE]"
    print(f"[INFO] {mode_str} backfill_win_odds_via_keibabook 開始", flush=True)
    print(f"[INFO] DB: {_DB_PATH}", flush=True)

    # ── 安全装置チェック ──────────────────────────────────
    if _check_danger_time():
        now = datetime.datetime.now().strftime("%H:%M")
        print(f"[ABORT] 危険時間帯 ({now}) のため中断します。スケジューラと競合するリスクがあります。")
        sys.exit(1)

    conflicts = _check_conflict_processes()
    if conflicts:
        print(f"[ABORT] 競合プロセス検出: {conflicts}")
        print("       競合プロセスが終了してから再実行してください。")
        sys.exit(1)

    # ── 対象レース取得 ──────────────────────────────────
    print("[INFO] 対象レースをDBから抽出中...", flush=True)
    race_list = _get_null_race_ids(_DB_PATH, venue_filt, min_null_horses)
    total_races = len(race_list)
    total_horses_before = _get_null_horse_count(_DB_PATH)

    print(f"[INFO] 対象レース数: {total_races} レース / {total_horses_before} 馬")
    if venue_filt:
        print(f"[INFO] venue_code フィルタ: {venue_filt}")
    print(f"[INFO] --min-null-horses={min_null_horses}: NULL{min_null_horses}頭以上のレースのみ対象")

    if total_races == 0:
        print("[INFO] 対象レースなし。終了します。")
        sys.exit(0)

    # 推定所要時間
    est_sec = total_races * _RATE_SLEEP
    est_min = est_sec / 60
    print(f"[INFO] 推定所要時間: {est_min:.1f} 分 ({total_races} 件 × {_RATE_SLEEP}秒/件)")
    if max_fetch > 0:
        print(f"[INFO] --max-fetch={max_fetch}: {max_fetch} 件で停止")

    if dry_run:
        print("[DRY-RUN] 更新はしません。--execute を付けて実行すると DB を更新します。")
        print(f"[DRY-RUN] 対象: {total_races} レース / {total_horses_before} 馬")
        sys.exit(0)

    # ── 中断再開対応 ──────────────────────────────────
    done_set = _load_done_set() if args.resume else set()
    if done_set:
        race_list = [(rid, rd) for rid, rd in race_list if rid not in done_set]
        print(f"[INFO] --resume: {len(done_set)} 件スキップ → 残り {len(race_list)} レース")

    # max_fetch 制限
    if max_fetch > 0 and len(race_list) > max_fetch:
        race_list = race_list[:max_fetch]
        print(f"[INFO] --max-fetch: {max_fetch} 件に制限")

    # ── 競馬ブック ログイン ──────────────────────────────
    print("[INFO] 競馬ブック ログイン中...", flush=True)
    try:
        from src.scraper.keibabook_training import KeibabookClient
        from src.scraper.keibabook_result import KeibabookResultScraper

        client = KeibabookClient()
        if not client.login():
            print("[ABORT] 競馬ブック ログイン失敗。認証情報を確認してください。")
            print("        設定: python -m src.scraper.keibabook_training --setup")
            sys.exit(1)

        kb_scraper = KeibabookResultScraper(client)
        print("[INFO] 競馬ブック ログイン成功", flush=True)
    except Exception as e:
        print(f"[ABORT] 競馬ブック 初期化失敗: {e}")
        sys.exit(1)

    # ── バックフィル本体 ──────────────────────────────────
    stats = {
        "total":          len(race_list),
        "kb_success":     0,
        "rakuten_success": 0,
        "failed":         0,
        "updated_horses": 0,
        "skip_no_odds":   0,
    }
    fail_log: List[Tuple[str, str]] = []   # (race_id, reason)

    print(f"\n[START] {len(race_list)} レースのバックフィル開始", flush=True)
    print("=" * 60, flush=True)

    for idx, (race_id, race_date) in enumerate(race_list, 1):
        # 危険時間帯チェック (各レース処理前に確認)
        if _check_danger_time():
            now_str = datetime.datetime.now().strftime("%H:%M")
            print(f"\n[ABORT] 危険時間帯 ({now_str}) に突入。処理を中断します。")
            print(f"  処理済み: {idx-1}/{len(race_list)} レース")
            print(f"  再開: python scripts/backfill_win_odds_via_keibabook.py --execute --resume")
            break

        pct = idx / len(race_list) * 100
        elapsed_min = (idx - 1) * _RATE_SLEEP / 60
        remaining_min = (len(race_list) - idx + 1) * _RATE_SLEEP / 60
        print(
            f"[{idx:4d}/{len(race_list)}] ({pct:5.1f}%) "
            f"race_id={race_id} date={race_date} "
            f"| 経過{elapsed_min:.1f}m 残{remaining_min:.1f}m",
            flush=True,
        )

        # 取得
        odds_map, source = _fetch_race_odds(race_id, race_date, kb_scraper)

        if odds_map is None:
            stats["failed"] += 1
            fail_log.append((race_id, "取得失敗(keibabook+rakuten両方失敗)"))
            print(f"  [FAIL] {race_id}: 取得失敗", flush=True)
            _mark_done(race_id)
            time.sleep(_RATE_SLEEP)
            continue

        # DBへの反映
        updated = 0
        for horse_no, (win_odds, popularity) in odds_map.items():
            if win_odds is not None and win_odds > 0:
                if _update_race_log(_DB_PATH, race_id, horse_no, win_odds, popularity):
                    updated += 1

        if updated > 0:
            if source == "keibabook":
                stats["kb_success"] += 1
            else:
                stats["rakuten_success"] += 1
            stats["updated_horses"] += updated
            print(f"  [OK:{source}] {updated}頭更新", flush=True)
        else:
            # オッズ取得成功だがDB更新0件:
            # → keibabook返却馬番と race_log の NULL 馬番が一致しないケース
            # (除外・取消・競走中止馬はオッズデータがないため NULL のままが正常)
            stats["skip_no_odds"] += 1
            print(
                f"  [SKIP] {race_id}: source={source} 取得成功だがDB更新対象なし"
                f" (取消・除外馬の NULL が残る可能性あり・正常)",
                flush=True,
            )

        _mark_done(race_id)
        time.sleep(_RATE_SLEEP)

    # ── 完了報告 ──────────────────────────────────────────
    print("\n" + "=" * 60, flush=True)
    print("[DONE] バックフィル完了", flush=True)
    print(f"  対象レース    : {stats['total']}", flush=True)
    print(f"  KB 成功       : {stats['kb_success']} レース", flush=True)
    print(f"  楽天 fallback : {stats['rakuten_success']} レース", flush=True)
    print(f"  失敗          : {stats['failed']} レース", flush=True)
    print(f"  DB更新なし    : {stats['skip_no_odds']} レース", flush=True)
    print(f"  更新馬数      : {stats['updated_horses']} 馬", flush=True)

    # 完了後の NULL 件数を再集計
    null_after = _get_null_horse_count(_DB_PATH)
    null_fixed = total_horses_before - null_after
    success_rate = null_fixed / total_horses_before * 100 if total_horses_before > 0 else 0.0
    print(f"\n  [CHECK] win_odds NULL 馬数:", flush=True)
    print(f"    変更前: {total_horses_before} 馬", flush=True)
    print(f"    変更後: {null_after} 馬", flush=True)
    print(f"    補完数: {null_fixed} 馬 (取得成功率 {success_rate:.1f}%)", flush=True)

    # 失敗理由一覧
    if fail_log:
        print(f"\n  [FAIL LOG] 失敗 {len(fail_log)} 件:", flush=True)
        for fid, reason in fail_log[:20]:
            print(f"    {fid}: {reason}", flush=True)
        if len(fail_log) > 20:
            print(f"    ... 他 {len(fail_log) - 20} 件 (ログ省略)", flush=True)

    print("\n[完了] スクリプト終了", flush=True)
    return 0 if stats["failed"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
