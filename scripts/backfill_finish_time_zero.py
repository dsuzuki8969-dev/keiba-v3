"""
finish_time_sec = 0 の race_log 行を段階バックフィルするスクリプト。

処理手順:
  1. race_log から finish_time_sec=0 or NULL の対象レコードを抽出
  2. DB バックアップ取得 (失敗時は本実行禁止)
  3. HTML キャッシュがある race_id は BeautifulSoup でパース → finish_time_sec を抽出
  4. キャッシュなし race は --max-fetch 上限まで netkeiba から取得
  5. UPDATE をトランザクション単位で実行
  6. 修正後の残件数を再カウントして検証

実行例:
  # dry-run (本実行禁止・件数確認のみ)
  PYTHONIOENCODING=utf-8 python scripts/backfill_finish_time_zero.py --dry-run

  # キャッシュのみ修正 (取得 0 件・安全策)
  PYTHONIOENCODING=utf-8 python scripts/backfill_finish_time_zero.py --apply --max-fetch 0

  # キャッシュ + 最大 1000 件ネット取得
  PYTHONIOENCODING=utf-8 python scripts/backfill_finish_time_zero.py --apply --max-fetch 1000
"""

import argparse
import os
import re
import shutil
import sys
import time
from typing import Optional

# プロジェクトルートを sys.path に追加
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import lz4.frame
    HAS_LZ4 = True
except ImportError:
    HAS_LZ4 = False

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False

from bs4 import BeautifulSoup

from src.log import get_logger
from src.database import get_db

logger = get_logger(__name__)

# ── パス定数 ──────────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CACHE_DIR = os.path.join(BASE_DIR, "data", "cache")
DB_PATH   = os.path.join(BASE_DIR, "data", "keiba.db")
BACKUP_PATH = os.path.join(BASE_DIR, "data", "keiba.db.bak_pre_finishtime_20260428")

# netkeiba NAR / JRA 結果ページ URL テンプレート
URL_TEMPLATE_NAR  = "https://nar.netkeiba.com/race/result.html?race_id={race_id}"
URL_TEMPLATE_RACE = "https://race.netkeiba.com/race/result.html?race_id={race_id}"

# JRA 会場コード (2桁)
JRA_VENUE_CODES = frozenset(
    ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10"]
)

# レート制限 (秒) — netkeiba 規約準拠
FETCH_SLEEP = 1.0

# バッチサイズ (DB コミット単位)
BATCH_SIZE = 100


# ── ユーティリティ ────────────────────────────────────────────────────────────

def _is_jra(race_id: str) -> bool:
    """race_id[4:6] が JRA 会場コードか判定する。"""
    return race_id[4:6] in JRA_VENUE_CODES if len(race_id) >= 6 else False


def _cache_filename(race_id: str) -> Optional[str]:
    """
    race_id に対応するキャッシュファイルパスを返す。
    NAR / JRA 両方のパターンを試し、存在するファイルを返す。
    見つからなければ None。
    """
    # NAR パターン
    candidates = [
        f"nar.netkeiba.com_race_result.html_race_id={race_id}.html.lz4",
        f"nar.netkeiba.com_race_result.html_race_id={race_id}.html",
        f"race.netkeiba.com_race_result.html_race_id={race_id}.html.lz4",
        f"race.netkeiba.com_race_result.html_race_id={race_id}.html",
    ]
    for fname in candidates:
        fpath = os.path.join(CACHE_DIR, fname)
        if os.path.exists(fpath):
            return fpath
    return None


def _read_html(path: str) -> str:
    """lz4 圧縮 / 非圧縮 HTML を読み込んで文字列を返す。"""
    if path.endswith(".lz4"):
        if not HAS_LZ4:
            raise ImportError("lz4 がインストールされていません: pip install lz4")
        with open(path, "rb") as f:
            return lz4.frame.decompress(f.read()).decode("utf-8", errors="replace")
    with open(path, encoding="utf-8", errors="replace") as f:
        return f.read()


def _parse_time(time_str: str) -> float:
    """'1:34.5' -> 94.5 / '112.3' -> 112.3 / 失敗 -> 0.0"""
    s = time_str.strip()
    try:
        if ":" in s:
            parts = s.split(":")
            return int(parts[0]) * 60 + float(parts[1])
        v = float(s)
        if v > 0:
            return v
    except (ValueError, IndexError):
        pass
    return 0.0


def _extract_finish_time_from_html(html: str, race_id: str, horse_no: int) -> float:
    """
    HTML から指定の horse_no 行の finish_time_sec を抽出する。

    BeautifulSoup を使い ml_data_collector.py と同等のロジックを適用。
    タイム列を動的に特定し、フォールバックとして全セル走査も行う。

    Returns:
        0.0 より大きい秒数 or 0.0 (取得失敗・非完走)
    """
    try:
        soup = BeautifulSoup(html, "html.parser")
    except Exception as e:
        logger.warning("HTML パース失敗 race_id=%s: %s", race_id, e)
        return 0.0

    table = soup.select_one(".ResultTableWrap table")
    if not table:
        return 0.0

    # ヘッダーから列位置を動的に特定 (ml_data_collector.py と同一ロジック)
    col_map: dict = {}
    header_row = table.select_one("thead tr")
    if header_row:
        for i, th in enumerate(header_row.select("th")):
            t = th.get_text(strip=True)
            if t == "着順":
                col_map["finish"] = i
            elif t == "馬番":
                col_map["horse_no"] = i
            elif t == "タイム":
                col_map["time"] = i

    col_map.setdefault("finish",   0)
    col_map.setdefault("horse_no", 2)
    col_map.setdefault("time",     7)

    for row in table.select("tbody tr"):
        cells = row.select("td")
        if len(cells) < 4:
            continue
        # 馬番を確認
        hno_text = cells[col_map["horse_no"]].get_text(strip=True) if col_map["horse_no"] < len(cells) else ""
        try:
            hno = int(hno_text)
        except ValueError:
            continue
        if hno != horse_no:
            continue

        # タイム列から抽出
        finish_time_sec = 0.0
        if col_map["time"] < len(cells):
            finish_time_sec = _parse_time(cells[col_map["time"]].get_text(strip=True))

        # フォールバック: 全セル走査 ("1:xx.x" パターン)
        if finish_time_sec <= 0:
            for c in cells:
                t = c.get_text(strip=True)
                if re.match(r"\d+:\d{2}\.\d", t):
                    finish_time_sec = _parse_time(t)
                    if finish_time_sec > 0:
                        break

        return finish_time_sec

    # 当該 horse_no の行が見つからなかった
    return 0.0


def _fetch_html_from_web(race_id: str) -> Optional[str]:
    """
    netkeiba からレース結果 HTML を取得して返す。
    レート制限として FETCH_SLEEP 秒スリープ。
    取得失敗時は None を返す (推定値での補完禁止)。
    """
    import urllib.request
    import urllib.error

    if _is_jra(race_id):
        url = URL_TEMPLATE_RACE.format(race_id=race_id)
    else:
        url = URL_TEMPLATE_NAR.format(race_id=race_id)

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "ja,en-US;q=0.9",
    }
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            html = resp.read().decode("utf-8", errors="replace")
        logger.debug("取得成功: %s", url)
        return html
    except urllib.error.HTTPError as e:
        logger.warning("HTTP エラー %s: %s", e.code, url)
    except urllib.error.URLError as e:
        logger.warning("URL エラー %s: %s", e.reason, url)
    except Exception as e:
        logger.warning("取得失敗 %s: %s", url, e)
    return None


# ── メイン処理 ────────────────────────────────────────────────────────────────

def _query_targets(conn) -> list[tuple[str, str, int, str]]:
    """
    race_log から finish_time_sec=0 or NULL の行を抽出する。
    Returns: [(race_id, race_date, horse_no, venue_code), ...]
    """
    rows = conn.execute(
        """
        SELECT race_id, race_date, horse_no, venue_code
        FROM race_log
        WHERE finish_time_sec = 0 OR finish_time_sec IS NULL
        ORDER BY race_date, race_id, horse_no
        """
    ).fetchall()
    return rows


def run(dry_run: bool, max_fetch: int) -> None:
    """
    メイン処理。
    dry_run=True の場合は DB を書き換えない。
    max_fetch: 0 = キャッシュのみ、N = 最大 N 件まで netkeiba 取得。
    """
    print("=" * 60)
    print(f"[backfill_finish_time_zero] {'DRY-RUN' if dry_run else 'APPLY'} モード")
    print(f"  max_fetch={max_fetch}, HAS_LZ4={HAS_LZ4}, HAS_TQDM={HAS_TQDM}")
    print("=" * 60)

    # ── ステップ 1: 対象抽出 ─────────────────────────────────────────────────
    conn = get_db()
    targets = _query_targets(conn)
    total_rows = len(targets)
    print(f"\n[1/5] 対象抽出: {total_rows:,} 行")

    # race_id 別にグルーピング
    from collections import defaultdict
    race_to_rows: dict = defaultdict(list)
    for race_id, race_date, horse_no, venue_code in targets:
        race_to_rows[race_id].append((race_date, horse_no, venue_code))
    race_ids = list(race_to_rows.keys())
    total_races = len(race_ids)
    print(f"    ユニーク race_id: {total_races:,} レース")

    # ── ステップ 2: キャッシュ照合 ───────────────────────────────────────────
    cache_hit:   list[str] = []
    cache_miss:  list[str] = []
    for rid in race_ids:
        if _cache_filename(rid) is not None:
            cache_hit.append(rid)
        else:
            cache_miss.append(rid)

    print(f"\n[2/5] キャッシュ照合:")
    print(f"    キャッシュあり: {len(cache_hit):,} レース")
    print(f"    キャッシュなし: {len(cache_miss):,} レース")
    print(f"    netkeiba 取得予定: {min(len(cache_miss), max_fetch):,} レース")

    if dry_run:
        # dry-run では件数を表示して終了
        print("\n[DRY-RUN] 本実行はスキップします。--apply で実行してください。")
        print(f"\n  修正可能 (キャッシュあり) 行数の概算:")
        cacheable_rows = sum(len(race_to_rows[rid]) for rid in cache_hit)
        print(f"    キャッシュあり行: ~{cacheable_rows:,} 行")
        fetchable_rows = sum(len(race_to_rows[rid]) for rid in cache_miss[:max_fetch])
        print(f"    取得可能行:      ~{fetchable_rows:,} 行 (max_fetch={max_fetch})")
        total_fixable = cacheable_rows + fetchable_rows
        print(f"    合計修正可能:    ~{total_fixable:,} 行 / {total_rows:,} 行")
        return

    # ── ステップ 3: バックアップ ─────────────────────────────────────────────
    print(f"\n[3/5] バックアップ: {DB_PATH} -> {BACKUP_PATH}")
    conn.close()  # コピー前に接続を閉じる
    # get_db() はスレッドローカルにキャッシュするため、close 後にリセットが必要
    from src import database as _db_mod
    _db_mod._local.conn = None
    try:
        shutil.copy2(DB_PATH, BACKUP_PATH)
        print(f"    バックアップ完了: {os.path.getsize(BACKUP_PATH):,} bytes")
    except Exception as e:
        # バックアップ失敗時は本実行禁止
        print(f"    [ERROR] バックアップ失敗: {e}")
        assert False, f"バックアップ取得失敗のため本実行を中断します: {e}"

    # ── ステップ 4: キャッシュからの更新 ────────────────────────────────────
    conn = get_db()  # 再接続 (_local.conn リセット済みなので新規接続が得られる)
    t0 = time.time()

    updated_from_cache = 0
    skipped_cache      = 0  # タイム 0 のまま (非完走など)

    print(f"\n[4/5] キャッシュ {len(cache_hit):,} レースを処理...")
    iter_cache = tqdm(cache_hit, desc="キャッシュ解析", unit="race") if HAS_TQDM else cache_hit

    update_batch: list[tuple] = []

    def flush_updates(batch: list, conn) -> int:
        """バッチ UPDATE をトランザクションで実行。"""
        if not batch:
            return 0
        conn.executemany(
            """
            UPDATE race_log
            SET finish_time_sec = ?
            WHERE race_id = ? AND horse_no = ? AND (finish_time_sec = 0 OR finish_time_sec IS NULL)
            """,
            batch,
        )
        conn.commit()
        n = len(batch)
        batch.clear()
        return n

    for rid in iter_cache:
        fpath = _cache_filename(rid)
        if fpath is None:
            continue
        try:
            html = _read_html(fpath)
        except Exception as e:
            logger.warning("HTML 読み込み失敗 race_id=%s path=%s: %s", rid, fpath, e)
            skipped_cache += 1
            continue

        for race_date, horse_no, venue_code in race_to_rows[rid]:
            ft = _extract_finish_time_from_html(html, rid, horse_no)
            if ft > 0:
                update_batch.append((ft, rid, horse_no))
            else:
                # タイム取得失敗 (非完走・除外等) は skip + 警告ログ
                logger.warning(
                    "タイム抽出失敗 (skip) race_id=%s horse_no=%s",
                    rid, horse_no,
                )
                skipped_cache += 1

        # バッチコミット
        if len(update_batch) >= BATCH_SIZE:
            updated_from_cache += flush_updates(update_batch, conn)

    # 残余フラッシュ
    updated_from_cache += flush_updates(update_batch, conn)
    print(f"    キャッシュ由来 UPDATE: {updated_from_cache:,} 行 / スキップ: {skipped_cache:,} 行")

    # ── ステップ 4b: netkeiba 取得 ───────────────────────────────────────────
    updated_from_fetch = 0
    skipped_fetch      = 0
    fetch_targets = cache_miss[:max_fetch] if max_fetch > 0 else []

    if fetch_targets:
        print(f"\n    netkeiba 取得: {len(fetch_targets):,} レース (sleep={FETCH_SLEEP}s/req)...")
        iter_fetch = tqdm(fetch_targets, desc="netkeiba取得", unit="race") if HAS_TQDM else fetch_targets

        for rid in iter_fetch:
            time.sleep(FETCH_SLEEP)  # レート制限遵守
            html = _fetch_html_from_web(rid)
            if html is None:
                # 取得失敗: skip + 警告ログ (推定値禁止)
                logger.warning("netkeiba 取得失敗 (skip) race_id=%s", rid)
                skipped_fetch += len(race_to_rows[rid])
                continue

            for race_date, horse_no, venue_code in race_to_rows[rid]:
                ft = _extract_finish_time_from_html(html, rid, horse_no)
                if ft > 0:
                    update_batch.append((ft, rid, horse_no))
                else:
                    logger.warning(
                        "タイム抽出失敗 (skip) race_id=%s horse_no=%s",
                        rid, horse_no,
                    )
                    skipped_fetch += 1

            if len(update_batch) >= BATCH_SIZE:
                updated_from_fetch += flush_updates(update_batch, conn)

        updated_from_fetch += flush_updates(update_batch, conn)
        print(f"    netkeiba 由来 UPDATE: {updated_from_fetch:,} 行 / スキップ: {skipped_fetch:,} 行")

    # ── ステップ 5: 検証 ─────────────────────────────────────────────────────
    remaining = conn.execute(
        "SELECT COUNT(*) FROM race_log WHERE finish_time_sec=0 OR finish_time_sec IS NULL"
    ).fetchone()[0]
    elapsed = time.time() - t0

    total_updated = updated_from_cache + updated_from_fetch
    print(f"\n[5/5] 検証:")
    print(f"    修正前: {total_rows:,} 行 (行単位)")
    print(f"    UPDATE 件数: {total_updated:,} 行")
    print(f"    修正後残件: {remaining:,} 行")
    print(f"    経過時間: {elapsed:.1f} 秒")
    print()
    print("=" * 60)
    print("[完了] バックアップ:", BACKUP_PATH)
    print(f"  キャッシュあり: {len(cache_hit):,} レース / キャッシュなし: {len(cache_miss):,} レース")
    print(f"  今回修正: {total_updated:,} 行 / 残: {remaining:,} 行")
    if len(cache_miss) > max_fetch:
        unfetched = len(cache_miss) - max_fetch
        print(f"  未取得 {unfetched:,} レースは --max-fetch を増やして再実行してください。")
    print("=" * 60)
    conn.close()


# ── CLI エントリーポイント ─────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="race_log の finish_time_sec=0 を段階バックフィルする"
    )
    mode_grp = parser.add_mutually_exclusive_group(required=True)
    mode_grp.add_argument(
        "--dry-run",
        action="store_true",
        help="対象件数・解決可能数を表示するだけ。DB は変更しない",
    )
    mode_grp.add_argument(
        "--apply",
        action="store_true",
        help="バックアップ取得後に UPDATE を実行する",
    )
    parser.add_argument(
        "--max-fetch",
        type=int,
        default=0,
        metavar="N",
        help="キャッシュなしの race を netkeiba から取得する最大件数 (0=取得しない, 既定=0)",
    )
    args = parser.parse_args()

    if args.dry_run:
        # dry-run では max_fetch はあくまで件数表示に使う (取得しない)
        run(dry_run=True, max_fetch=args.max_fetch)
    else:
        run(dry_run=False, max_fetch=args.max_fetch)


if __name__ == "__main__":
    main()
